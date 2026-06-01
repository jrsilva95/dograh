"""SIP (pyVoIP) implementation of the TelephonyProvider interface."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from pyVoIP.VoIP.VoIP import CallState

from api.enums import WorkflowRunMode
from api.services.telephony.base import (
    CallInitiationResult,
    NormalizedInboundData,
    TelephonyProvider,
)

from .pipeline_runner import run_pipeline_sip
from .sip_manager import place_call, sip_manager

if TYPE_CHECKING:
    from fastapi import WebSocket

logger = logging.getLogger(__name__)


ANSWER_TIMEOUT_SECS = 60.0
ANSWER_POLL_INTERVAL_SECS = 0.1


class SIPProvider(TelephonyProvider):
    """pyVoIP-backed SIP provider.

    Unlike carrier providers (Twilio, Plivo, etc.), pyVoIP handles RTP
    locally in-process and does not call back into our webhook URL. The
    pipeline is started by a background task spawned from ``initiate_call``
    that waits for the call to be answered, then drives the SIP transport.
    """

    PROVIDER_NAME = WorkflowRunMode.SIP.value
    WEBHOOK_ENDPOINT = "sip"

    def __init__(self, config: Dict[str, Any]):
        self.sip_server = config.get("sip_server")
        self.sip_port = int(config.get("sip_port", 5060))
        self.username = config.get("username")
        self.password = config.get("password")
        self.my_ip = config.get("my_ip", "192.168.3.34")
        self.my_sip_port = int(config.get("my_sip_port", 5060))
        self.from_numbers = config.get("from_numbers", [])
        self.country = config.get("country")

    async def _get_phone(
        self,
        organization_id: int,
        telephony_configuration_id: Optional[int] = None,
    ):
        return await sip_manager.get_phone(
            self.sip_server,
            self.sip_port,
            self.username,
            self.password,
            self.my_ip,
            self.my_sip_port,
            organization_id=organization_id,
            telephony_configuration_id=telephony_configuration_id,
            country=self.country,
        )

    async def initiate_call(
        self,
        to_number: str,
        webhook_url: str,
        workflow_run_id: Optional[int] = None,
        from_number: Optional[str] = None,
        **kwargs: Any,
    ) -> CallInitiationResult:
        """Place an outbound SIP call and spawn the pipeline runner task."""
        if not self.validate_config():
            raise ValueError("SIP provider not properly configured")

        # Resolve tenant context so the VoIPPhone, once created, can route
        # inbound INVITEs back to the right org/workflow. We fetch the
        # workflow (without tenant scoping — we only have workflow_id here)
        # and the workflow_run (for telephony_configuration_id).
        from api.db import db_client

        organization_id: Optional[int] = None
        telephony_configuration_id: Optional[int] = None
        workflow_id = kwargs.get("workflow_id")
        if workflow_id:
            workflow = await db_client.get_workflow_by_id(workflow_id)
            if workflow:
                organization_id = workflow.organization_id
        if workflow_run_id:
            wf_run = await db_client.get_workflow_run(workflow_run_id)
            if wf_run and wf_run.initial_context:
                telephony_configuration_id = wf_run.initial_context.get(
                    "telephony_configuration_id"
                )
        if organization_id is None:
            raise ValueError(
                "[SIP] could not resolve organization_id from workflow_id "
                f"{workflow_id}; inbound routing would not work"
            )

        phone = await self._get_phone(
            organization_id=organization_id,
            telephony_configuration_id=telephony_configuration_id,
        )

        logger.info(f"[SIP] Initiating call to {to_number}")
        call = await asyncio.to_thread(place_call, phone, to_number)

        workflow_id = kwargs.get("workflow_id")
        user_id = kwargs.get("user_id")
        if workflow_run_id and workflow_id and user_id:
            asyncio.create_task(
                self._drive_call(
                    call=call,
                    workflow_run_id=workflow_run_id,
                    workflow_id=workflow_id,
                    user_id=user_id,
                ),
                name=f"sip-call-{call.call_id}",
            )
        else:
            logger.warning(
                "[SIP] Skipping pipeline runner — missing workflow_run_id / "
                "workflow_id / user_id; call will be silent."
            )

        return CallInitiationResult(
            call_id=call.call_id,
            status=call.state.value,
            caller_number=self.username,
            provider_metadata={"call_id": call.call_id},
            raw_response={"call_id": call.call_id, "state": call.state.value},
        )

    async def _drive_call(
        self,
        *,
        call: Any,
        workflow_run_id: int,
        workflow_id: int,
        user_id: int,
    ) -> None:
        """Wait for the call to be answered, then run the pipeline on it."""
        deadline = asyncio.get_event_loop().time() + ANSWER_TIMEOUT_SECS
        try:
            while call.state not in (CallState.ANSWERED, CallState.ENDED):
                if asyncio.get_event_loop().time() > deadline:
                    logger.warning(
                        f"[SIP] Answer timeout for call {call.call_id}; hanging up"
                    )
                    try:
                        call.hangup()
                    except Exception:
                        pass
                    return
                await asyncio.sleep(ANSWER_POLL_INTERVAL_SECS)

            if call.state == CallState.ENDED:
                logger.info(f"[SIP] Call {call.call_id} ended before answer")
                return

            logger.info(
                f"[SIP] Call {call.call_id} answered, starting pipeline "
                f"(workflow_run {workflow_run_id})"
            )
            await run_pipeline_sip(
                call=call,
                workflow_run_id=workflow_run_id,
                workflow_id=workflow_id,
                user_id=user_id,
            )
        except Exception:
            logger.exception(f"[SIP] _drive_call failed for {call.call_id}")
            try:
                if call.state == CallState.ANSWERED:
                    call.hangup()
            except Exception:
                pass

    async def get_call_status(self, call_id: str) -> Dict[str, Any]:
        # Look up the cached VoIPPhone instance without creating one — by
        # the time anyone asks for call status, the phone must already exist
        # (either because we initiated the call, or because inbound came in).
        key = f"{self.username}@{self.sip_server}:{self.sip_port}"
        entry = sip_manager._instances.get(key)
        if entry is None:
            return {"status": "ended"}
        call = entry.phone.calls.get(call_id)
        if not call:
            return {"status": "ended"}
        return {"call_id": call_id, "status": call.state.value}

    async def get_available_phone_numbers(self) -> List[str]:
        return self.from_numbers or [self.username]

    def validate_config(self) -> bool:
        return bool(self.sip_server and self.username and self.password)

    async def verify_webhook_signature(
        self, url: str, params: Dict[str, Any], signature: str
    ) -> bool:
        return True

    async def get_webhook_response(
        self, workflow_id: int, user_id: int, workflow_run_id: int
    ) -> str:
        return ""

    async def get_call_cost(self, call_id: str) -> Dict[str, Any]:
        return {
            "cost_usd": 0.0,
            "duration": 0,
            "status": "unknown",
            "error": "SIP does not support cost retrieval",
        }

    def parse_status_callback(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "call_id": data.get("call_id", ""),
            "status": data.get("status", ""),
            "from_number": data.get("from"),
            "to_number": data.get("to"),
            "duration": data.get("duration"),
            "extra": data,
        }

    async def handle_websocket(
        self,
        websocket: "WebSocket",
        workflow_id: int,
        user_id: int,
        workflow_run_id: int,
    ) -> None:
        # pyVoIP does not produce inbound carrier WebSockets — calls are
        # driven directly by the pipeline runner task spawned in
        # ``initiate_call``.
        raise NotImplementedError("SIP provider does not use WebSocket transport")

    @classmethod
    def can_handle_webhook(
        cls, webhook_data: Dict[str, Any], headers: Dict[str, str]
    ) -> bool:
        return False

    @staticmethod
    def parse_inbound_webhook(webhook_data: Dict[str, Any]) -> NormalizedInboundData:
        return NormalizedInboundData(
            provider="sip",
            call_id=webhook_data.get("call_id", ""),
            from_number=webhook_data.get("from", ""),
            to_number=webhook_data.get("to", ""),
            direction="inbound",
            call_status=webhook_data.get("status", ""),
            raw_data=webhook_data,
        )

    @staticmethod
    def validate_account_id(config_data: dict, webhook_account_id: str) -> bool:
        return True

    async def verify_inbound_signature(
        self,
        url: str,
        webhook_data: Dict[str, Any],
        headers: Dict[str, str],
        body: str = "",
    ) -> bool:
        return True

    async def start_inbound_stream(
        self,
        *,
        websocket_url: str,
        workflow_run_id: int,
        normalized_data: NormalizedInboundData,
        backend_endpoint: str,
    ) -> Any:
        from fastapi import Response

        return Response(content="", status_code=204)

    async def transfer_call(
        self,
        destination: str,
        transfer_id: str,
        conference_name: str,
        timeout: int = 30,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        raise NotImplementedError("SIP call transfer not yet implemented")

    def supports_transfers(self) -> bool:
        return False

    @staticmethod
    def generate_error_response(error_type: str, message: str) -> tuple:
        from fastapi import Response

        return Response(content=message, status_code=500), "text/plain"
