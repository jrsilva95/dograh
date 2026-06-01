"""Singleton registry of pyVoIP ``VoIPPhone`` instances keyed by credentials.

Each credential tuple (``username@server:port``) gets at most one VoIPPhone
instance — pyVoIP's SIP registration is per-process, so we share instances
across calls to avoid duplicate REGISTER traffic.

The phone is created with a ``callCallback`` that routes inbound INVITEs to
the matching workflow. Without this callback pyVoIP responds 486 Busy Here
automatically and the call is never answered.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Optional

from pyVoIP import RTP
from pyVoIP.VoIP.VoIP import CallState, VoIPCall, VoIPPhone

logger = logging.getLogger(__name__)


@dataclass
class SIPPhoneEntry:
    """Tracks a VoIPPhone alongside the tenant context needed to route
    inbound calls landing on it back to the right workflow."""

    phone: VoIPPhone
    organization_id: int
    telephony_configuration_id: Optional[int]
    loop: asyncio.AbstractEventLoop
    country: Optional[str] = None


def place_call(phone: VoIPPhone, number: str) -> VoIPCall:
    """Originate an outbound call prioritizing PCMU (G.711 μ-law).

    pyVoIP's default ``phone.call`` advertises ``{8: PCMA, 0: PCMU, 101: EVENT}``
    with PCMA prioritized. We reverse the order so PCMU comes first while
    keeping PCMA as a fallback (otherwise PCMA-only SIP gateways respond
    488 Not Acceptable Here and the INVITE never completes).
    """
    port = phone.request_port()
    medias: Dict[int, Dict[int, RTP.PayloadType]] = {
        port: {
            0: RTP.PayloadType.PCMU,
            8: RTP.PayloadType.PCMA,
            101: RTP.PayloadType.EVENT,
        }
    }
    request, call_id, sess_id = phone.sip.invite(
        number, medias, RTP.TransmitType.SENDRECV
    )
    call = VoIPCall(
        phone,
        CallState.DIALING,
        request,
        sess_id,
        phone.myIP,
        ms=medias,
        sendmode=phone.sendmode,
    )
    phone.calls[call_id] = call
    logger.info(
        f"[SIP] Outbound INVITE to {number} | call_id={call_id} | "
        f"local_port={port} | codec_offer=[PCMU(0), PCMA(8)]"
    )
    return call


def _safe_reject_call(call: VoIPCall) -> None:
    """Reject/end a call regardless of its current state.

    ``call.hangup()`` only works on ANSWERED calls and ``call.deny()`` only
    works on RINGING calls — both raise InvalidStateError otherwise. This
    helper picks the right one or silently no-ops if the call is already
    ended.
    """
    try:
        if call.state == CallState.RINGING:
            call.deny()  # 486 Busy Here
        elif call.state == CallState.ANSWERED:
            call.hangup()  # BYE
        # DIALING/ENDED: nothing to do
    except Exception as e:
        logger.debug(f"[SIP] _safe_reject_call ({call.state}) failed: {e}")


async def _handle_inbound_call(
    call: VoIPCall,
    organization_id: int,
    telephony_configuration_id: Optional[int],
    country: Optional[str] = None,
) -> None:
    """Resolve the inbound workflow, create a run, answer, and drive the pipeline.

    Mirrors ``ari_manager._handle_inbound_stasis_start``: lookup phone
    number → workflow → user → quota → create run → answer → run pipeline.
    """
    # Imports inside the function avoid pulling DB/quota deps at module
    # import time (which runs before the FastAPI lifespan is ready).
    from api.db import db_client
    from api.enums import CallType, WorkflowRunMode

    from .pipeline_runner import run_pipeline_sip

    call_id = call.call_id
    caller_number = "unknown"
    called_number = "unknown"
    try:
        try:
            from_header = call.request.headers.get("From", {})
            to_header = call.request.headers.get("To", {})
            caller_number = from_header.get("number", "unknown")
            called_number = to_header.get("number", "unknown")
        except Exception:
            logger.warning("[SIP inbound] failed to parse From/To headers")

        logger.info(
            f"[SIP inbound] new call_id={call_id} from={caller_number} "
            f"to={called_number} org={organization_id} "
            f"config={telephony_configuration_id}"
        )

        # 1. Resolve the inbound workflow from the called number. Pass the
        # SIP config's country as hint so bare local digits from the SIP
        # operator (e.g. "8431911601") normalize to the same E.164 the UI
        # stored (e.g. "+558431911601").
        phone_row = await db_client.find_active_phone_number_for_inbound(
            organization_id, called_number, "sip", country_hint=country
        )
        if not phone_row:
            # Diagnostic: show what we tried to match against, plus the
            # stored numbers for this config (if we have a config id), so the
            # operator can see the exact mismatch.
            try:
                from api.utils.telephony_address import normalize_telephony_address

                attempted = normalize_telephony_address(
                    called_number, country_hint=country
                ).canonical
            except Exception:
                attempted = "?"
            candidates: list = []
            if telephony_configuration_id is not None:
                try:
                    rows = await db_client.list_phone_numbers_for_config(
                        telephony_configuration_id
                    )
                    candidates = [
                        f"{r.address_normalized} "
                        f"(active={r.is_active}, inbound_wf={r.inbound_workflow_id})"
                        for r in rows
                    ]
                except Exception:
                    pass
            logger.warning(
                f"[SIP inbound] no active phone_number for "
                f"called='{called_number}' (normalized='{attempted}', "
                f"country_hint={country}) in org {organization_id} "
                f"config={telephony_configuration_id}. "
                f"Numbers on this config: {candidates or 'none'}. "
                f"Hanging up."
            )
            _safe_reject_call(call)
            return
        if (
            telephony_configuration_id is not None
            and phone_row.telephony_configuration_id != telephony_configuration_id
        ):
            logger.warning(
                f"[SIP inbound] phone {called_number} is on config "
                f"{phone_row.telephony_configuration_id}, not {telephony_configuration_id} "
                f"— hanging up"
            )
            _safe_reject_call(call)
            return
        if not phone_row.inbound_workflow_id:
            logger.warning(
                f"[SIP inbound] phone {called_number} has no "
                f"inbound_workflow_id assigned, hanging up"
            )
            _safe_reject_call(call)
            return

        # 2. Load the workflow (scoped to org for tenant isolation)
        workflow = await db_client.get_workflow(
            phone_row.inbound_workflow_id, organization_id=organization_id
        )
        if not workflow:
            logger.warning(
                f"[SIP inbound] workflow {phone_row.inbound_workflow_id} "
                f"not found or wrong org, hanging up"
            )
            _safe_reject_call(call)
            return

        # 3. Create the workflow run
        workflow_run = await db_client.create_workflow_run(
            name=f"SIP Inbound {caller_number}",
            workflow_id=phone_row.inbound_workflow_id,
            mode=WorkflowRunMode.SIP.value,
            user_id=workflow.user_id,
            call_type=CallType.INBOUND,
            initial_context={
                "caller_number": caller_number,
                "called_number": called_number,
                "direction": "inbound",
                "provider": "sip",
                "telephony_configuration_id": phone_row.telephony_configuration_id,
            },
            gathered_context={"call_id": call_id},
            organization_id=organization_id,
        )
        logger.info(
            f"[SIP inbound] workflow_run {workflow_run.id} created for {call_id}"
        )

        # 4. Answer the SIP INVITE (200 OK + SDP) — this starts pyVoIP's
        # RTP transmitter/receiver threads.
        await asyncio.to_thread(call.answer)
        logger.info(f"[SIP inbound] answered call_id={call_id}")

        # 5. Drive the pipeline (same helper as outbound)
        await run_pipeline_sip(
            call=call,
            workflow_run_id=workflow_run.id,
            workflow_id=phone_row.inbound_workflow_id,
            user_id=workflow.user_id,
        )
    except Exception:
        logger.exception(f"[SIP inbound] failure handling call_id={call_id}")
        _safe_reject_call(call)


class SIPManager:
    _instances: Dict[str, SIPPhoneEntry] = {}
    _lock = asyncio.Lock()

    @classmethod
    async def get_phone(
        cls,
        sip_server: str,
        sip_port: int,
        username: str,
        password: str,
        my_ip: str,
        my_sip_port: int,
        organization_id: int,
        telephony_configuration_id: Optional[int] = None,
        country: Optional[str] = None,
    ) -> VoIPPhone:
        key = f"{username}@{sip_server}:{sip_port}"
        async with cls._lock:
            if key not in cls._instances:
                logger.info(
                    f"Creating new VoIPPhone instance for {key} "
                    f"(org={organization_id}, config={telephony_configuration_id}, "
                    f"country={country})"
                )
                loop = asyncio.get_running_loop()

                # pyVoIP invokes this callback from its SIP receive thread.
                # We bridge to asyncio so the inbound handler can hit the DB
                # and run the pipeline. Capture org/config/country via closure.
                _org_id = organization_id
                _cfg_id = telephony_configuration_id
                _country = country

                def _inbound_callback(call: VoIPCall) -> None:
                    try:
                        asyncio.run_coroutine_threadsafe(
                            _handle_inbound_call(
                                call, _org_id, _cfg_id, _country
                            ),
                            loop,
                        )
                    except Exception:
                        logger.exception(
                            "[SIP inbound] failed to dispatch callback to asyncio"
                        )

                phone = VoIPPhone(
                    sip_server,
                    sip_port,
                    username,
                    password,
                    myIP=my_ip,
                    sipPort=my_sip_port,
                    callCallback=_inbound_callback,
                )
                phone.start()
                cls._instances[key] = SIPPhoneEntry(
                    phone=phone,
                    organization_id=organization_id,
                    telephony_configuration_id=telephony_configuration_id,
                    loop=loop,
                    country=country,
                )
            return cls._instances[key].phone

    @classmethod
    async def stop_all(cls):
        async with cls._lock:
            for key, entry in cls._instances.items():
                logger.info(f"Stopping VoIPPhone instance for {key}")
                entry.phone.stop()
            cls._instances.clear()


sip_manager = SIPManager()


async def startup_phones() -> None:
    """Pre-create VoIPPhone instances for every SIP telephony config in the DB.

    pyVoIP must be REGISTERed with the SIP server before any inbound INVITE
    can reach us. With lazy-on-outbound creation, inbound only works after
    the user happens to place an outbound first. This eager startup is what
    makes inbound work straight out of a backend restart.

    Mirrors the spirit of ``ari_manager`` (which maintains long-lived
    connections per config) but in-process because pyVoIP is lightweight.

    Called from ``api/app.py::lifespan`` once per FastAPI worker boot.
    Failures of individual configs are logged but don't block startup —
    one broken SIP config shouldn't take down the whole API.
    """
    from api.db import db_client

    try:
        configs = await db_client.list_all_telephony_configurations_by_provider("sip")
    except Exception:
        logger.exception("[SIP startup] failed to list SIP telephony configs")
        return

    if not configs:
        logger.info("[SIP startup] no SIP telephony configs in DB; skipping")
        return

    logger.info(f"[SIP startup] pre-registering {len(configs)} SIP phone(s)")
    for cfg in configs:
        try:
            creds = cfg.credentials or {}
            sip_server = creds.get("sip_server")
            username = creds.get("username")
            password = creds.get("password")
            if not (sip_server and username and password):
                logger.warning(
                    f"[SIP startup] config id={cfg.id} org={cfg.organization_id} "
                    f"is missing sip_server/username/password — skipping"
                )
                continue
            await sip_manager.get_phone(
                sip_server=sip_server,
                sip_port=int(creds.get("sip_port", 5060)),
                username=username,
                password=password,
                my_ip=creds.get("my_ip", "192.168.3.34"),
                my_sip_port=int(creds.get("my_sip_port", 5060)),
                organization_id=cfg.organization_id,
                telephony_configuration_id=cfg.id,
                country=creds.get("country"),
            )
            logger.info(
                f"[SIP startup] phone ready for config id={cfg.id} "
                f"org={cfg.organization_id} ({username}@{sip_server})"
            )
        except Exception:
            logger.exception(
                f"[SIP startup] failed to start phone for config id={cfg.id} "
                f"org={cfg.organization_id}"
            )
