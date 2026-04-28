from __future__ import annotations

import time

import httpx

from .config import Settings
from .metrics import tts_latency


class ElevenLabsClient:
    """ElevenLabs HTTP TTS client producing mulaw 8kHz audio for Twilio."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._base_url = "https://api.elevenlabs.io/v1/text-to-speech"

    async def synthesize_mulaw_8khz(self, text: str) -> bytes:
        """Synthesize text to mulaw 8kHz audio bytes."""
        url = f"{self._base_url}/{self._settings.elevenlabs_voice_id}/stream?output_format=ulaw_8000"
        payload = {
            "text": text,
            "model_id": "eleven_turbo_v2_5",
            "voice_settings": {"stability": 0.6, "similarity_boost": 0.75},
        }
        headers = {
            "xi-api-key": self._settings.elevenlabs_api_key,
            "content-type": "application/json",
        }
        start = time.monotonic()
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            audio = response.content
        tts_latency.observe(time.monotonic() - start)
        return audio
