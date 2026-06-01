"""Workaround subclass for the ElevenLabs ``multi-stream-input`` WebSocket.

ElevenLabs' multi-stream WS protocol enforces:

    "voice_settings field must be provided in the first message and then
    either be not provided or not change."

The upstream ``ElevenLabsTTSService`` sends ``voice_settings`` on the first
message of *every* audio context it creates. That works fine when the user
context is the first thing sent over the WS, but the service ALSO starts a
keepalive task on ``_connect()`` that sleeps 10s and then sends an empty
``{"text": ""}`` message. If the first ``run_tts`` doesn't happen within 10s
(e.g. SIP call setup + pre-call fetch + first LLM TTFB easily exceeds that),
the keepalive becomes the literal first WS message — without
``voice_settings``. Then ElevenLabs sees ``voice_settings`` in a later
message and closes the connection with 1008 policy violation.

Rather than racing the keepalive, we just don't send ``voice_settings`` at
all. ElevenLabs falls back to the voice's saved character settings
(stability / similarity_boost / style configured in the Voice Library) and
the speed setting is rarely changed from 1.0 in practice. This matches what
``pipecat.services.dograh.tts`` (the MPS-proxied variant) effectively does.

If a future caller needs custom voice settings, the correct path is to
configure them on the ElevenLabs Voice itself, not per-call.
"""

from pipecat.services.elevenlabs.tts import ElevenLabsTTSService


class ElevenLabsTTSServiceNoVoiceSettings(ElevenLabsTTSService):
    """``ElevenLabsTTSService`` that never includes ``voice_settings`` in messages.

    Sidesteps the multi-stream-input WS policy violation 1008 that fires
    when the keepalive task posts an empty message before the first audio
    context, making ``voice_settings`` arrive on a non-first WS message.
    """

    def _set_voice_settings(self):
        # Upstream returns either a dict or None. Forcing None means the
        # context init message in ``run_tts`` never includes
        # ``voice_settings`` (see ``if self._voice_settings:`` guard there).
        return None
