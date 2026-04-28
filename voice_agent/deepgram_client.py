from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

from websockets.legacy.client import WebSocketClientProtocol, connect

from .config import Settings


class DeepgramClient:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._ws: WebSocketClientProtocol | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        url = (
            "wss://api.deepgram.com/v1/listen"
            "?encoding=mulaw&sample_rate=8000&channels=1"
            "&model=nova-2-phonecall&endpointing=500"
            "&interim_results=true&utterance_end_ms=1000"
        )
        self._ws = await connect(
            url,
            extra_headers={"Authorization": f"Token {self._settings.deepgram_api_key}"},
        )

    async def send_audio(self, audio_chunk: bytes) -> None:
        async with self._lock:
            if self._ws is None:
                raise RuntimeError("Deepgram websocket is not connected")
            await self._ws.send(audio_chunk)

    async def transcripts(self) -> AsyncIterator[dict]:
        if self._ws is None:
            raise RuntimeError("Deepgram websocket is not connected")
        async for message in self._ws:
            payload = json.loads(message)
            if payload.get("type") == "Results":
                utterance = (
                    payload.get("channel", {}).get("alternatives", [{}])[0].get("transcript", "").strip()
                )
                if payload.get("speech_final") and utterance:
                    yield {"type": "transcript", "text": utterance}
            elif payload.get("type") == "UtteranceEnd":
                yield {"type": "utterance_end"}

    async def close(self) -> None:
        async with self._lock:
            if self._ws is not None:
                await self._ws.close()
                self._ws = None
