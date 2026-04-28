from __future__ import annotations

import asyncio

from twilio.rest import Client

from .config import Settings


class TwilioClient:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._client = Client(settings.twilio_account_sid, settings.twilio_auth_token)

    async def create_outbound_call(self, to_number: str) -> str:
        stream_url = self._settings.base_url.replace("https://", "wss://").replace("http://", "ws://")
        twiml = (
            f"<Response><Connect><Stream url=\"{stream_url}/media-stream\"/>"
            "</Connect></Response>"
        )
        call = await asyncio.to_thread(
            self._client.calls.create,
            to=self._to_e164(to_number),
            from_=self._settings.twilio_from_number,
            twiml=twiml,
        )
        return call.sid

    async def send_dtmf(self, call_sid: str, digits: str) -> None:
        twiml = f"<Response><Play digits=\"{digits}\"/></Response>"
        await asyncio.to_thread(self._client.calls(call_sid).update, twiml=twiml)

    async def end_call(self, call_sid: str) -> None:
        await asyncio.to_thread(self._client.calls(call_sid).update, status="completed")

    @staticmethod
    def _to_e164(number: str) -> str:
        return number if number.startswith("+") else f"+1{number}"
