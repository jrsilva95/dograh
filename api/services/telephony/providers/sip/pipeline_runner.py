"""Pipeline runner for SIP/pyVoIP calls.

pyVoIP terminates RTP locally and has no carrier WebSocket, so the normal
``run_pipeline_telephony`` entry point (invoked from a WebSocket handler) is
not reachable for SIP. This helper drives the same pipeline setup, passing
the pyVoIP call object into the transport factory via ``transport_kwargs``
and a ``None`` websocket (which the SIP transport factory ignores).

``run_pipeline_telephony`` is imported lazily inside the function — at module
load time it would close an import cycle (sip → run_pipeline → event_handlers
→ tasks.arq → s3_upload → workflow_run_cost → telephony.factory → sip).
The cycle silently breaks the arq worker, which means post-call uploads
(recording_url / transcript_url) never run for any provider.
"""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger


async def run_pipeline_sip(
    *,
    call: Any,
    workflow_run_id: int,
    workflow_id: int,
    user_id: int,
) -> None:
    """Run the Pipecat pipeline against a live pyVoIP call.

    Blocks until the call ends (the pipeline terminates when the SIP input
    transport's read loop is cancelled or when an ``EndFrame`` flows
    through).
    """
    # Lazy import: see module docstring — top-level import closes a cycle
    # through tasks.arq that silently kills the arq worker.
    from api.services.pipecat.run_pipeline import run_pipeline_telephony

    try:
        await run_pipeline_telephony(
            websocket=None,
            provider_name="sip",
            workflow_id=workflow_id,
            workflow_run_id=workflow_run_id,
            user_id=user_id,
            call_id=call.call_id,
            transport_kwargs={"call": call},
        )
    except asyncio.CancelledError:
        logger.info(f"[SIP] pipeline cancelled for workflow_run {workflow_run_id}")
        raise
    except Exception:
        logger.exception(
            f"[SIP] pipeline failure for workflow_run {workflow_run_id}"
        )
        raise
    finally:
        try:
            from pyVoIP.VoIP.VoIP import CallState

            if call.state == CallState.ANSWERED:
                call.hangup()
        except Exception as e:
            logger.debug(f"[SIP] hangup on pipeline exit failed: {e}")
