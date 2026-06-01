"""SIP transport for pyVoIP-managed calls.

Unlike other telephony providers, pyVoIP terminates RTP locally in-process
instead of relaying audio over a WebSocket from a carrier. This transport
adapts pyVoIP's blocking ``call.read_audio`` / ``call.write_audio`` API to
Pipecat's async input/output transports.

Audio wire format from pyVoIP: 16-bit signed linear PCM @ 8000 Hz mono
(pyVoIP decodes PCMU/PCMA to linear via ``audioop.ulaw2lin`` / ``alaw2lin``).
"""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from fastapi import WebSocket
from loguru import logger

from api.services.pipecat.audio_config import AudioConfig
from api.services.pipecat.audio_mixer import build_audio_out_mixer
from api.services.pipecat.transport_params import realtime_param_overrides
from pipecat.frames.frames import (
    InputAudioRawFrame,
    OutputAudioRawFrame,
    StartFrame,
)
from pipecat.processors.frame_processor import FrameProcessor
from pipecat.frames.frames import EndFrame
from pipecat.processors.frame_processor import FrameDirection
from pipecat.transports.base_input import BaseInputTransport
from pipecat.transports.base_output import BaseOutputTransport
from pipecat.transports.base_transport import BaseTransport, TransportParams
from pyVoIP.VoIP.VoIP import CallState

# 20ms of 16-bit mono @ 8000 Hz = 320 bytes. Matches the chunk size pyVoIP
# uses internally in its RTP transmit loop.
SIP_CHUNK_BYTES = 320


class SIPTransportParams(TransportParams):
    """Transport params for SIP/pyVoIP. No extra knobs beyond the base set."""

    pass


class SIPInputTransport(BaseInputTransport):
    """Reads decoded PCM from pyVoIP's RTP receive buffer and pushes it
    downstream as ``InputAudioRawFrame``."""

    _params: SIPTransportParams

    def __init__(self, transport: "SIPTransport", call: Any, params: SIPTransportParams):
        super().__init__(params)
        self._transport = transport
        self._call = call
        self._read_task: asyncio.Task | None = None
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._sample_rate = 0
        self._client_connected_fired = False

    async def start(self, frame: StartFrame):
        await super().start(frame)
        self._sample_rate = self._params.audio_in_sample_rate or frame.audio_in_sample_rate
        await self.set_transport_ready(frame)

        codecs = [
            f"{c.preference.name}@{c.preference.rate}Hz "
            f"(in:{c.inIP}:{c.inPort} out:{c.outIP}:{c.outPort})"
            for c in (self._call.RTPClients or [])
        ]
        logger.info(
            f"[SIP] input start | call={self._call.call_id} | "
            f"sample_rate={self._sample_rate} | RTPClients={len(self._call.RTPClients or [])} | "
            f"codecs={codecs}"
        )

        if not self._read_task:
            self._read_task = self.create_task(self._read_audio_loop(), name="sip-rtp-in")
        if not self._client_connected_fired:
            self._client_connected_fired = True
            logger.debug(f"[SIP] firing on_client_connected for call {self._call.call_id}")
            await self._transport._call_event_handler("on_client_connected", self._call)

    async def cleanup(self):
        await super().cleanup()
        if self._read_task:
            await self.cancel_task(self._read_task)
            self._read_task = None
        self._executor.shutdown(wait=False)
        if self._client_connected_fired:
            self._client_connected_fired = False
            try:
                await self._transport._call_event_handler(
                    "on_client_disconnected", self._call
                )
            except Exception as e:
                logger.debug(f"[SIP] on_client_disconnected handler error: {e}")

    async def _read_audio_loop(self):
        """Drain pyVoIP's RTP receive buffer at the rate RTP arrives.

        Uses ``blocking=True`` so pyVoIP returns as soon as a chunk lands
        (sleeps internally 2ms when the buffer is empty). With a fixed
        ``asyncio.sleep(20ms)`` plus executor overhead the loop ran at
        ~31 iter/s instead of the required 50 iter/s, accumulating ~75ms
        of buffer lag per second of call — by the time the user spoke the
        STT was several seconds behind real time.

        When the call ends, pyVoIP's ``read_audio`` returns ``b""``
        immediately (NSD=False), so we'd spin-loop forever — detect ENDED
        and push an EndFrame downstream so the pipeline shuts down cleanly.
        """
        loop = asyncio.get_running_loop()
        bytes_read = 0
        iterations = 0
        while True:
            try:
                if self._call.state == CallState.ENDED:
                    logger.info(
                        f"[SIP] call {self._call.call_id} ended — "
                        f"signalling pipeline end (iter={iterations}, "
                        f"bytes={bytes_read})"
                    )
                    await self.push_frame(EndFrame(), FrameDirection.DOWNSTREAM)
                    return

                data = await loop.run_in_executor(
                    self._executor,
                    lambda: self._call.read_audio(SIP_CHUNK_BYTES, True),
                )
                if data:
                    bytes_read += len(data)
                    await self.push_audio_frame(
                        InputAudioRawFrame(
                            audio=data,
                            sample_rate=self._sample_rate,
                            num_channels=self._params.audio_in_channels,
                        )
                    )
                iterations += 1
                if iterations % 250 == 0:  # 250 chunks × 20ms = 5s of real audio
                    logger.debug(
                        f"[SIP] RTP-in stats call={self._call.call_id} "
                        f"iter={iterations} total_bytes={bytes_read}"
                    )
                # Yield to event loop without adding wall-clock delay; pacing
                # comes from pyVoIP's blocking read which only returns when
                # the next RTP chunk has actually arrived.
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"[SIP] read_audio loop error: {e}")
                await asyncio.sleep(0.02)


class SIPOutputTransport(BaseOutputTransport):
    """Writes ``OutputAudioRawFrame`` audio into pyVoIP's RTP transmit queue."""

    _params: SIPTransportParams

    def __init__(self, call: Any, params: SIPTransportParams):
        super().__init__(params)
        self._call = call
        self._bytes_written = 0
        self._writes = 0
        self._first_write_logged = False
        self._send_interval = 0.0
        self._next_send_time = 0.0

    async def start(self, frame: StartFrame):
        await super().start(frame)
        await self.set_transport_ready(frame)
        # Real-time pacing: pretend we're sending audio at wire speed so the
        # pipeline producer can't outrun pyVoIP's RTP transmitter. Without
        # this, write_audio() returns instantly (it just enqueues into
        # pmout), and with audio_out_auto_silence the mixer floods pmout at
        # CPU speed — call audio ends up minutes behind real time.
        # Formula mirrors FastAPIWebsocketOutputTransport: chunk duration
        # in seconds = audio_chunk_size / sample_rate / 2 (the /2 accounts
        # for 16-bit width, i.e. 2 bytes per sample).
        if self._sample_rate > 0:
            self._send_interval = (self._audio_chunk_size / self._sample_rate) / 2
        logger.info(
            f"[SIP] output start | call={self._call.call_id} | "
            f"sample_rate={self._sample_rate} | chunk={self._audio_chunk_size}B | "
            f"send_interval={self._send_interval * 1000:.1f}ms"
        )

    async def write_audio_frame(self, frame: OutputAudioRawFrame) -> bool:
        if not self._call.RTPClients:
            logger.warning(
                f"[SIP] write_audio_frame skipped — no RTPClients on call "
                f"{self._call.call_id}"
            )
            return False
        if not self._first_write_logged:
            self._first_write_logged = True
            logger.info(
                f"[SIP] first audio out | call={self._call.call_id} | "
                f"frame_sr={frame.sample_rate} | bytes={len(frame.audio)} | "
                f"clients={len(self._call.RTPClients)}"
            )
        self._call.write_audio(frame.audio)
        self._bytes_written += len(frame.audio)
        self._writes += 1
        if self._writes % 100 == 0:
            logger.debug(
                f"[SIP] RTP-out stats call={self._call.call_id} "
                f"writes={self._writes} total_bytes={self._bytes_written}"
            )
        await self._write_audio_sleep()
        return True

    async def _write_audio_sleep(self):
        """Pace writes to real time so we don't flood pyVoIP's pmout buffer."""
        if self._send_interval <= 0:
            return
        current_time = time.monotonic()
        sleep_duration = max(0.0, self._next_send_time - current_time)
        if sleep_duration > 0:
            await asyncio.sleep(sleep_duration)
            self._next_send_time += self._send_interval
        else:
            # We're behind schedule (or this is the first write): reset the
            # clock so we don't accumulate slip.
            self._next_send_time = time.monotonic() + self._send_interval


class SIPTransport(BaseTransport):
    """Composite transport bridging a pyVoIP VoIPCall to a Pipecat pipeline."""

    def __init__(self, call: Any, params: SIPTransportParams):
        super().__init__()
        self._call = call
        self._params = params
        self._input: SIPInputTransport | None = None
        self._output: SIPOutputTransport | None = None

        self._register_event_handler("on_client_connected")
        self._register_event_handler("on_client_disconnected")

    def input(self) -> FrameProcessor:
        if not self._input:
            self._input = SIPInputTransport(self, self._call, self._params)
        return self._input

    def output(self) -> FrameProcessor:
        if not self._output:
            self._output = SIPOutputTransport(self._call, self._params)
        return self._output


async def create_transport(
    websocket: WebSocket | None,
    workflow_run_id: int,
    audio_config: AudioConfig,
    organization_id: int,
    *,
    ambient_noise_config: dict | None = None,
    telephony_configuration_id: int | None = None,
    is_realtime: bool = False,
    call: Any,
    **_: Any,
) -> SIPTransport:
    """Create a transport for a pyVoIP-managed SIP call.

    ``websocket`` is ignored (pyVoIP terminates RTP locally; there is no
    carrier WebSocket). ``call`` is the ``VoIPCall`` returned by
    ``phone.call(...)`` and must be supplied via ``transport_kwargs``.
    """
    if call is None:
        raise ValueError(
            "SIP transport requires a pyVoIP call object in transport_kwargs['call']"
        )

    mixer = await build_audio_out_mixer(
        audio_config.transport_out_sample_rate, ambient_noise_config
    )

    params = SIPTransportParams(
        audio_in_enabled=True,
        audio_out_enabled=True,
        audio_in_sample_rate=audio_config.transport_in_sample_rate,
        audio_out_sample_rate=audio_config.transport_out_sample_rate,
        audio_out_mixer=mixer,
        **realtime_param_overrides(is_realtime),
    )

    return SIPTransport(call, params)
