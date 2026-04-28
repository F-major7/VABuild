from __future__ import annotations

import json
import os
import re
import traceback
import asyncio
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket
from pydantic import BaseModel, Field
from twilio.rest import Client

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import EndFrame
from pipecat.frames.frames import Frame, InterimTranscriptionFrame, StartFrame, TTSSpeakFrame, TranscriptionFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.serializers.twilio import TwilioFrameSerializer
from pipecat.services.deepgram.stt import DeepgramSTTService, LiveOptions
from pipecat.services.elevenlabs.tts import ElevenLabsTTSService
from pipecat.services.openai.llm import OpenAILLMService

try:
    from pipecat.transports.network.fastapi_websocket import (
        FastAPIWebsocketParams,
        FastAPIWebsocketTransport,
    )
except ModuleNotFoundError:
    from pipecat.transports.websocket.fastapi import FastAPIWebsocketParams, FastAPIWebsocketTransport


def load_environment() -> None:
    root_env = Path(__file__).resolve().parent / ".env"
    voice_agent_env = Path(__file__).resolve().parent / "voice_agent" / ".env"
    if root_env.exists():
        load_dotenv(root_env)
    if voice_agent_env.exists():
        load_dotenv(voice_agent_env)


load_environment()


class PizzaModel(BaseModel):
    size: str
    crust: str
    toppings: list[str]
    acceptable_topping_subs: list[str]
    no_go_toppings: list[str]


class SideModel(BaseModel):
    first_choice: str
    backup_options: list[str]
    if_all_unavailable: str


class DrinkModel(BaseModel):
    first_choice: str
    alternatives: list[str]
    skip_if_over_budget: bool


class OrderModel(BaseModel):
    customer_name: str
    phone_number: str = Field(pattern=r"^\d{10}$")
    delivery_address: str
    pizza: PizzaModel
    side: SideModel
    drink: DrinkModel
    budget_max: float
    special_instructions: str


app = FastAPI(title="Pizza Outbound Agent (Pipecat)")

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM_NUMBER = os.getenv("TWILIO_FROM_NUMBER", "")
BASE_URL = os.getenv("BASE_URL", "")
DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY", "")
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY", "")
ELEVENLABS_VOICE_ID = os.getenv("ELEVENLABS_VOICE_ID", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
CALL_STORE: dict[str, dict[str, Any]] = {}


def pretty_log(event: str, **fields: Any) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    details = " | ".join(f"{k}={v}" for k, v in fields.items())
    if details:
        print(f"[{ts}] {event} | {details}")
    else:
        print(f"[{ts}] {event}")


def to_e164(number: str) -> str:
    return number if number.startswith("+") else f"+1{number}"


def extract_zip_from_address(address: str) -> str:
    match = re.search(r"\b(\d{5})\b", address)
    return match.group(1) if match else ""


class IvrOrchestrator(FrameProcessor):
    def __init__(self, call_state: dict[str, Any], twilio: Client):
        super().__init__()
        self.call_state = call_state
        self.twilio = twilio
        self.order = call_state["order"]
        self.call_sid = call_state["call_sid"]
        self._last_prompt_key: str | None = None
        self._last_transcript_seen: str = ""
        self._kickoff_sent = False
        self._script_task: asyncio.Task | None = None

    async def start(self, frame: StartFrame):
        await super().start(frame)
        # Deterministic fallback script for this fixed IVR flow.
        if self.call_state.get("phase") == "ivr_or_hold" and self._script_task is None:
            self._script_task = asyncio.create_task(self._run_fixed_ivr_script())

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if isinstance(frame, (TranscriptionFrame, InterimTranscriptionFrame)):
            transcript = frame.text.lower().strip()
            if not transcript:
                await self.push_frame(frame, direction)
                return
            if transcript == self._last_transcript_seen:
                await self.push_frame(frame, direction)
                return
            self._last_transcript_seen = transcript
            phase = self.call_state.get("phase", "ivr_or_hold")

            if phase in {"ivr_or_hold", "hold"}:
                handled = await self._handle_ivr_or_hold(transcript)
                if handled:
                    return

        await self.push_frame(frame, direction)

    async def _run_fixed_ivr_script(self) -> None:
        try:
            pretty_log("IVR_SCRIPT_START", call_sid=self.call_sid)
            # Prompt 1: delivery -> press 1
            await asyncio.sleep(1.0)
            await self._send_dtmf("1", prompt_key="welcome")
            # Prompt 2: name
            await asyncio.sleep(2.0)
            await self._speak(self.order["customer_name"], prompt_key="name")
            # Prompt 3: callback phone (DTMF digits)
            await asyncio.sleep(2.0)
            await self._send_dtmf(self.order["phone_number"], prompt_key="phone")
            # Prompt 4: zip
            await asyncio.sleep(2.0)
            zip_code = extract_zip_from_address(self.order["delivery_address"])
            if zip_code:
                await self._speak(zip_code, prompt_key="zip")
            # Prompt 5: confirm yes
            await asyncio.sleep(2.0)
            await self._speak("yes", prompt_key="confirm")
            # Prompt 6: hold transfer
            self.call_state["phase"] = "hold"
            pretty_log("PHASE_CHANGE", call_sid=self.call_sid, phase="hold")
            pretty_log("IVR_SCRIPT_END", call_sid=self.call_sid)
        except Exception as exc:
            pretty_log("IVR_SCRIPT_ERROR", call_sid=self.call_sid, error=exc)

    async def _handle_ivr_or_hold(self, transcript: str) -> bool:
        if not transcript:
            return True

        pretty_log("TRANSCRIPT_FINAL", call_sid=self.call_sid, phase=self.call_state.get("phase"), text=transcript)

        if self.call_state.get("phase") == "hold":
            # Keep silent on hold; once a likely human greeting appears, hand off to LLM.
            if any(
                marker in transcript
                for marker in ("what can i get", "can i help", "hello", "thanks for calling", "hi")
            ):
                self.call_state["phase"] = "human"
                pretty_log("PHASE_CHANGE", call_sid=self.call_sid, phase="human")
                return False
            return True

        if any(
            k in transcript
            for k in (
                "press 1 for delivery",
                "press one for delivery",
                "for delivery press 1",
                "for delivery press one",
            )
        ):
            pretty_log("IVR_MATCH", call_sid=self.call_sid, prompt="WELCOME")
            await self._send_dtmf("1", prompt_key="welcome")
            return True

        if any(k in transcript for k in ("say the name", "name for the order", "please say the name")):
            pretty_log("IVR_MATCH", call_sid=self.call_sid, prompt="NAME")
            await self._speak(self.order["customer_name"], prompt_key="name")
            return True

        if any(
            k in transcript
            for k in ("10-digit callback", "ten-digit callback", "callback number", "enter your callback")
        ):
            pretty_log("IVR_MATCH", call_sid=self.call_sid, prompt="PHONE")
            await self._send_dtmf(self.order["phone_number"], prompt_key="phone")
            return True

        if any(k in transcript for k in ("delivery zip", "zip code", "say your zip")):
            pretty_log("IVR_MATCH", call_sid=self.call_sid, prompt="ZIP")
            zip_code = extract_zip_from_address(self.order["delivery_address"])
            if zip_code:
                await self._speak(zip_code, prompt_key="zip")
            return True

        if any(k in transcript for k in ("is that correct", "say yes to confirm", "yes to confirm")):
            pretty_log("IVR_MATCH", call_sid=self.call_sid, prompt="CONFIRM")
            await self._speak("yes", prompt_key="confirm")
            return True

        if any(k in transcript for k in ("please hold", "connect you to a team member", "got it please hold")):
            self.call_state["phase"] = "hold"
            pretty_log("PHASE_CHANGE", call_sid=self.call_sid, phase="hold")
            return True

        # Unknown prompt while still in IVR: suppress to avoid free-form chatter.
        pretty_log("IVR_UNMATCHED", call_sid=self.call_sid, transcript=transcript)
        return True

    async def _send_dtmf(self, digits: str, prompt_key: str) -> None:
        # Avoid double-send on repeated finalized duplicate transcription chunks.
        if self._last_prompt_key == prompt_key:
            pretty_log("DTMF_SKIP_DUPLICATE", call_sid=self.call_sid, prompt_key=prompt_key)
            return
        self._last_prompt_key = prompt_key
        twiml = f"<Response><Play digits=\"{digits}\"/></Response>"
        self.twilio.calls(self.call_sid).update(twiml=twiml)
        pretty_log("DTMF_SENT", call_sid=self.call_sid, digits=digits)

    async def _speak(self, text: str, prompt_key: str) -> None:
        if self._last_prompt_key == prompt_key:
            pretty_log("TTS_SKIP_DUPLICATE", call_sid=self.call_sid, prompt_key=prompt_key)
            return
        self._last_prompt_key = prompt_key
        pretty_log("TTS_QUEUED", call_sid=self.call_sid, text=text)
        await self.push_frame(TTSSpeakFrame(text), FrameDirection.DOWNSTREAM)


def build_system_prompt(order: dict[str, Any]) -> str:
    return (
        "You are an outbound pizza ordering voice agent.\n\n"
        f"ORDER JSON:\n{json.dumps(order, indent=2)}\n\n"
        "IMPORTANT CALL STRATEGY\n"
        "1) IVR NAVIGATION PHASE\n"
        "- Be extremely literal.\n"
        "- If IVR asks digits, say only digits needed.\n"
        "- If IVR asks name, callback, zip, confirmation, provide exact minimal answer only.\n"
        "- No extra words.\n\n"
        "2) HOLD PHASE\n"
        "- Stay silent during hold music/silence.\n"
        "- Speak only after a live human greeting.\n\n"
        "3) HUMAN ORDER PHASE\n"
        "- Place order exactly from ORDER JSON.\n"
        "- Pizza is mandatory.\n"
        "- Use acceptable_topping_subs when needed.\n"
        "- Reject no_go_toppings.\n"
        "- Side: first choice, then backups in order.\n"
        "- Drink: optional per budget and skip_if_over_budget.\n"
        "- Collect per-item prices, total, delivery time, and order number.\n"
        "- Provide special_instructions before ending.\n"
        "- If unclear, ask brief clarifying question.\n\n"
        "BEHAVIORAL RULES\n"
        "- Phone-friendly, concise, natural tone with humans.\n"
        "- Never reveal internal reasoning.\n"
        "- Never invent unavailable options.\n"
    )


@app.post("/call")
async def create_call(order: OrderModel) -> dict[str, str]:
    if not BASE_URL:
        raise HTTPException(status_code=500, detail="BASE_URL is missing")
    if not TWILIO_FROM_NUMBER:
        raise HTTPException(status_code=500, detail="TWILIO_FROM_NUMBER is missing")

    ws_base = BASE_URL.replace("https://", "wss://").replace("http://", "ws://")
    stream_url = f"{ws_base}/media-stream"
    twiml = f"<Response><Connect><Stream url=\"{stream_url}\"/></Connect></Response>"
    call = twilio_client.calls.create(
        to=to_e164(order.phone_number),
        from_=TWILIO_FROM_NUMBER,
        twiml=twiml,
    )
    pretty_log("CALL_CREATED", call_sid=call.sid, to=to_e164(order.phone_number))

    CALL_STORE[call.sid] = {
        "call_sid": call.sid,
        "stream_sid": None,
        "phase": "ivr_or_hold",
        "order": order.model_dump(),
        "result": {
            "outcome": "unknown",
            "items_ordered": {
                "pizza": deepcopy(order.pizza.model_dump()),
                "side": deepcopy(order.side.model_dump()),
                "drink": deepcopy(order.drink.model_dump()),
            },
            "prices": {"pizza": None, "side": None, "drink": None},
            "total": None,
            "delivery_time": None,
            "order_number": None,
        },
    }
    return {"call_sid": call.sid}


@app.get("/call/{call_sid}")
async def get_call(call_sid: str) -> dict[str, Any]:
    state = CALL_STORE.get(call_sid)
    if not state:
        raise HTTPException(status_code=404, detail="call not found")
    return state


@app.websocket("/media-stream")
async def media_stream(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        pretty_log("WS_ACCEPTED")
        connected_raw = None
        start_raw = None

        while True:
            raw = await websocket.receive()
            payload = json.loads(raw.get("text", "{}"))
            event = payload.get("event")
            pretty_log("WS_FRAME", ws_event=event)
            if event == "connected":
                connected_raw = raw
            elif event == "start":
                start_raw = raw
                break

        start_data = json.loads(start_raw.get("text", "{}"))
        call_sid = start_data.get("start", {}).get("callSid")
        stream_sid = start_data.get("start", {}).get("streamSid")
        if not call_sid or not stream_sid:
            pretty_log("WS_INVALID_START", call_sid=call_sid, stream_sid=stream_sid)
            await websocket.close(code=1003)
            return
        if call_sid not in CALL_STORE:
            pretty_log("UNKNOWN_CALL_SID", call_sid=call_sid)
            await websocket.close(code=1008)
            return

        call_state = CALL_STORE[call_sid]
        call_state["stream_sid"] = stream_sid
        order_data = call_state["order"]
        pretty_log("CALL_BOUND", call_sid=call_sid, stream_sid=stream_sid)

        consumed_frames = [frame for frame in [connected_raw, start_raw] if frame is not None]
        original_receive = websocket.receive
        replay_done = False

        async def patched_receive():
            nonlocal replay_done
            if not replay_done and consumed_frames:
                frame = consumed_frames.pop(0)
                if not consumed_frames:
                    replay_done = True
                return frame
            return await original_receive()

        websocket.receive = patched_receive

        serializer = TwilioFrameSerializer(
            stream_sid=stream_sid,
            call_sid=call_sid,
            account_sid=TWILIO_ACCOUNT_SID,
            auth_token=TWILIO_AUTH_TOKEN,
        )
        transport = FastAPIWebsocketTransport(
            websocket=websocket,
            params=FastAPIWebsocketParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                audio_in_sample_rate=8000,
                audio_out_sample_rate=8000,
                vad_enabled=True,
                vad_analyzer=SileroVADAnalyzer(),
                vad_audio_passthrough=True,
                serializer=serializer,
            ),
        )

        stt = DeepgramSTTService(
            api_key=DEEPGRAM_API_KEY,
            live_options=LiveOptions(
                model="nova-2-phonecall",
                encoding="mulaw",
                sample_rate=8000,
                channels=1,
                interim_results=True,
                utterance_end_ms=1000,
            ),
        )
        llm = OpenAILLMService(
            api_key=OPENAI_API_KEY,
            settings=OpenAILLMService.Settings(model="gpt-4o"),
        )
        tts = ElevenLabsTTSService(
            api_key=ELEVENLABS_API_KEY,
            voice_id=ELEVENLABS_VOICE_ID,
            output_format="ulaw_8000",
            sample_rate=8000,
        )

        context = LLMContext(messages=[{"role": "system", "content": build_system_prompt(order_data)}])
        context_aggregator = LLMContextAggregatorPair(context)
        ivr_orchestrator = IvrOrchestrator(call_state=call_state, twilio=twilio_client)

        pipeline = Pipeline(
            [
                transport.input(),
                stt,
                ivr_orchestrator,
                context_aggregator.user(),
                llm,
                tts,
                transport.output(),
                context_aggregator.assistant(),
            ]
        )
        task = PipelineTask(
            pipeline,
            params=PipelineParams(
                audio_in_sample_rate=8000,
                audio_out_sample_rate=8000,
                allow_interruptions=True,
            ),
        )
        
        async def ivr_fallback_driver() -> None:
            try:
                pretty_log("IVR_FALLBACK_DRIVER_START", call_sid=call_sid)
                await asyncio.sleep(1.0)
                await asyncio.to_thread(
                    twilio_client.calls(call_sid).update,
                    twiml="<Response><Play digits=\"1\"/></Response>",
                )
                pretty_log("IVR_FALLBACK_DTMF", call_sid=call_sid, digits="1")

                await asyncio.sleep(2.0)
                await task.queue_frame(TTSSpeakFrame(order_data["customer_name"]))
                pretty_log("IVR_FALLBACK_TTS", call_sid=call_sid, text=order_data["customer_name"])

                await asyncio.sleep(2.0)
                await asyncio.to_thread(
                    twilio_client.calls(call_sid).update,
                    twiml=f"<Response><Play digits=\"{order_data['phone_number']}\"/></Response>",
                )
                pretty_log("IVR_FALLBACK_DTMF", call_sid=call_sid, digits=order_data["phone_number"])

                await asyncio.sleep(2.0)
                zip_code = extract_zip_from_address(order_data["delivery_address"])
                if zip_code:
                    await task.queue_frame(TTSSpeakFrame(zip_code))
                    pretty_log("IVR_FALLBACK_TTS", call_sid=call_sid, text=zip_code)

                await asyncio.sleep(2.0)
                await task.queue_frame(TTSSpeakFrame("yes"))
                pretty_log("IVR_FALLBACK_TTS", call_sid=call_sid, text="yes")
                call_state["phase"] = "hold"
                pretty_log("PHASE_CHANGE", call_sid=call_sid, phase="hold")
                pretty_log("IVR_FALLBACK_DRIVER_END", call_sid=call_sid)
            except Exception as fallback_exc:
                pretty_log("IVR_FALLBACK_DRIVER_ERROR", call_sid=call_sid, error=fallback_exc)

        fallback_task = asyncio.create_task(ivr_fallback_driver())

        @transport.event_handler("on_client_disconnected")
        async def on_client_disconnected(transport, client):
            call_state["phase"] = "completed_or_disconnected"
            if not fallback_task.done():
                fallback_task.cancel()
            await task.queue_frame(EndFrame())
            pretty_log("CALL_DISCONNECTED", call_sid=call_sid)
            print(json.dumps(call_state["result"]))

        runner = PipelineRunner()
        pretty_log("PIPELINE_START", call_sid=call_sid)
        await runner.run(task)
        pretty_log("PIPELINE_END", call_sid=call_sid)
    except Exception as exc:
        print(f"media_stream error: {exc}")
        traceback.print_exc()
        await websocket.close(code=1011)
