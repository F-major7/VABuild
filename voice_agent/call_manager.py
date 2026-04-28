from __future__ import annotations

import asyncio
import base64
import json
from datetime import datetime, timezone
from typing import Any

from fastapi import WebSocket
from starlette.websockets import WebSocketState

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import EndFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
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

from .config import Settings
from .deepgram_client import DeepgramClient
from .elevenlabs_client import ElevenLabsClient
from .ivr_state_machine import IvrStateMachine
from .logger import get_logger
from .models import CallStatusResponse, OrderModel
from .twilio_client import TwilioClient


logger = get_logger("voice_agent.call_manager")


class CallManager:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._twilio = TwilioClient(settings)
        self._deepgram_by_call: dict[str, DeepgramClient] = {}
        self._elevenlabs = ElevenLabsClient(settings)
        self._state_machines: dict[str, IvrStateMachine] = {}
        self._call_states: dict[str, dict[str, Any]] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def initiate_call(self, order: OrderModel) -> str:
        call_sid = await self._twilio.create_outbound_call(order.phone_number)
        self._state_machines[call_sid] = IvrStateMachine(order)
        self._locks[call_sid] = asyncio.Lock()
        self._call_states[call_sid] = {
            "call_sid": call_sid,
            "order": order,
            "stream_sid": None,
            "ivr_state": "WELCOME",
            "retry_count": 0,
            "phase": "ivr",
            "collected": {},
            "logs": [],
        }
        self._log_event(call_sid, "call_initiated", {"to": order.phone_number})
        return call_sid

    async def get_call_status(self, call_sid: str) -> CallStatusResponse:
        call_state = self._call_states.get(call_sid)
        if not call_state:
            raise KeyError(f"Call not found for sid={call_sid}")
        return CallStatusResponse(
            call_sid=call_sid,
            ivr_state=call_state["ivr_state"],
            retry_count=call_state["retry_count"],
            phase=call_state["phase"],
            collected=call_state["collected"],
            logs=call_state["logs"],
        )

    async def handle_media_stream(self, websocket: WebSocket) -> None:
        await websocket.accept()
        call_sid: str | None = None
        stream_sid: str | None = None
        deepgram: DeepgramClient | None = None
        transcript_task: asyncio.Task | None = None
        hold_event: asyncio.Event | None = None
        handoff_to_human = False
        try:
            while True:
                receive_task = asyncio.create_task(websocket.receive_json())
                wait_tasks = {receive_task}
                if hold_event is not None:
                    wait_tasks.add(asyncio.create_task(hold_event.wait()))
                done, pending = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)

                if hold_event is not None and hold_event.is_set():
                    handoff_to_human = True
                    for task in pending:
                        task.cancel()
                    break

                if receive_task not in done:
                    for task in pending:
                        task.cancel()
                    continue

                frame = receive_task.result()
                event_type = frame.get("event")
                if event_type == "start":
                    call_sid = frame["start"]["callSid"]
                    stream_sid = frame["start"]["streamSid"]
                    hold_event = asyncio.Event()
                    deepgram = DeepgramClient(self._settings)
                    await deepgram.connect()
                    self._deepgram_by_call[call_sid] = deepgram
                    if call_sid in self._call_states:
                        self._call_states[call_sid]["stream_sid"] = stream_sid
                        self._call_states[call_sid]["transcript_buffer"] = []
                        self._call_states[call_sid]["hold_event"] = hold_event
                    transcript_task = asyncio.create_task(
                        self._consume_deepgram_transcripts(call_sid, stream_sid, websocket, deepgram)
                    )
                    self._log_event(call_sid, "media_stream_started", {"stream_sid": stream_sid})
                elif event_type == "media":
                    if deepgram is None:
                        continue
                    payload = frame["media"]["payload"]
                    audio_bytes = base64.b64decode(payload)
                    await deepgram.send_audio(audio_bytes)
                elif event_type == "stop":
                    if call_sid:
                        self._log_event(call_sid, "media_stream_stopped", {})
                    break

            if handoff_to_human and call_sid and stream_sid:
                if transcript_task:
                    transcript_task.cancel()
                    transcript_task = None
                if deepgram:
                    await deepgram.close()
                    deepgram = None
                if call_sid in self._deepgram_by_call:
                    del self._deepgram_by_call[call_sid]
                await self._run_human_conversation(call_sid, stream_sid, websocket)
        finally:
            if transcript_task:
                transcript_task.cancel()
            if deepgram:
                await deepgram.close()
            if call_sid and call_sid in self._deepgram_by_call:
                del self._deepgram_by_call[call_sid]
            # Avoid raising if the websocket is already closed by transport/pipeline.
            if (
                websocket.client_state != WebSocketState.DISCONNECTED
                and websocket.application_state != WebSocketState.DISCONNECTED
            ):
                await websocket.close()

    async def _consume_deepgram_transcripts(
        self,
        call_sid: str,
        stream_sid: str,
        websocket: WebSocket,
        deepgram: DeepgramClient,
    ) -> None:
        async for event in deepgram.transcripts():
            event_type = event.get("type")
            if event_type == "transcript":
                transcript_buffer = self._call_states[call_sid].setdefault("transcript_buffer", [])
                transcript_buffer.append(event.get("text", ""))
            elif event_type == "utterance_end":
                transcript_buffer = self._call_states[call_sid].setdefault("transcript_buffer", [])
                utterance = " ".join(text for text in transcript_buffer if text).strip()
                transcript_buffer.clear()
                if utterance:
                    await self._handle_ivr_prompt(call_sid, stream_sid, websocket, utterance)

    async def _handle_ivr_prompt(
        self, call_sid: str, stream_sid: str, websocket: WebSocket, transcript: str
    ) -> None:
        lock = self._locks[call_sid]
        async with lock:
            state_machine = self._state_machines[call_sid]
            transition = state_machine.transition_for_prompt(transcript)
            if transition is None:
                failed = state_machine.register_reprompt()
                self._call_states[call_sid]["retry_count"] = state_machine.retry_count
                self._log_event(
                    call_sid,
                    "ivr_reprompt_detected",
                    {"transcript": transcript, "retry_count": state_machine.retry_count},
                )
                if failed:
                    self._call_states[call_sid]["phase"] = "failed"
                    self._call_states[call_sid]["ivr_state"] = state_machine.state.value
                    self._log_event(call_sid, "ivr_failed", {"reason": "max_retries_exceeded"})
                    await self._twilio.end_call(call_sid)
                return

            previous_state = self._call_states[call_sid]["ivr_state"]
            self._call_states[call_sid]["ivr_state"] = transition.next_state.value
            self._call_states[call_sid]["retry_count"] = state_machine.retry_count
            self._log_event(
                call_sid,
                "ivr_transition",
                {
                    "from": previous_state,
                    "to": transition.next_state.value,
                    "matched_prompt": transition.matched_prompt,
                    "action_type": transition.action_type,
                },
            )

            if transition.action_type == "dtmf" and transition.action_value:
                await websocket.send_json(
                    {
                        "event": "send-digits",
                        "streamSid": stream_sid,
                        "sendDigits": transition.action_value,
                    }
                )
                self._log_event(call_sid, "dtmf_sent", {"digits": transition.action_value})
            elif transition.action_type == "tts" and transition.action_value:
                audio_bytes = await self._elevenlabs.synthesize_mulaw_8khz(transition.action_value)
                encoded = base64.b64encode(audio_bytes).decode("utf-8")
                await websocket.send_json(
                    {
                        "event": "media",
                        "streamSid": stream_sid,
                        "media": {"payload": encoded},
                    }
                )
                self._log_event(call_sid, "tts_sent", {"text": transition.action_value})

            if transition.terminal:
                self._call_states[call_sid]["phase"] = "hold"
                self._log_event(call_sid, "phase_updated", {"phase": "hold"})
                hold_event = self._call_states[call_sid].get("hold_event")
                if hold_event:
                    hold_event.set()

    def _log_event(self, call_sid: str, event_name: str, data: dict[str, Any]) -> None:
        payload = {"event": event_name, **data}
        timestamp = datetime.now(timezone.utc).isoformat()
        if call_sid in self._call_states:
            self._call_states[call_sid]["logs"].append({"timestamp": timestamp, **payload})
            if event_name == "phase_updated" and data.get("phase") == "hold":
                hold_event = self._call_states[call_sid].get("hold_event")
                if hold_event:
                    hold_event.set()
        logger.info(
            event_name,
            extra={"event_data": {"call_sid": call_sid, "timestamp": timestamp, **payload}},
        )

    async def _run_human_conversation(self, call_sid: str, stream_sid: str, websocket: WebSocket) -> None:
        order = self._call_states[call_sid]["order"]
        order_json = order.model_dump() if hasattr(order, "model_dump") else order

        system_prompt = f"""
You are an AI agent that just finished navigating a pizza restaurant IVR.
You are now on hold waiting for a human employee to pick up.

ORDER DATA:
{json.dumps(order_json, indent=2)}

PHASE 2 - HOLD:
Stay completely silent. Do not say anything.
Wait for a human to greet you before speaking.
Hold music or silence may be present - ignore it.
If you hear any intelligible human speech, start speaking.
Respond when you hear a clear human greeting like
"hello", "thanks for calling", "what can I get you", etc.

PHASE 3 - HUMAN CONVERSATION:
Once human picks up, place the order naturally and conversationally.

ORDERING RULES:
- Order in strict sequence: pizza first, then side, then drink
- Do NOT list the entire order at once
- Start with pizza only
- Wait for the employee to ask what else ("anything else?", etc.) before moving to side
- After side, wait again for a follow-up prompt before moving to drink
- If employee does not ask for next item yet, do not volunteer it
- Order pizza with exact size, crust, and toppings from order data
- If a topping is unavailable, ONLY accept from acceptable_topping_subs
- NEVER accept no_go_toppings under any circumstances even if offered
- Order side first_choice first. If unavailable try backup_options in order.
- If all sides unavailable and if_all_unavailable is "skip", skip the side
- Track running total as prices are given
- Order drink first_choice. If skip_if_over_budget is true and drink
  would push total over budget_max, skip the drink
- Push for exact prices if employee gives vague answers
- Get exact delivery time, not ranges
- Get order confirmation number
- Deliver special_instructions word for word before hanging up
- Before saying goodbye, ask if they need anything else
- Say goodbye naturally and end the call

SPEAKING STYLE:
- Sound like a real caller, not robotic
- Use short natural fillers occasionally, such as "umm", "uhh", "hmm", "got it", "okay"
- Do not overuse fillers; keep speech concise and clear
- Keep turns short and phone-friendly

HANGUP OUTCOMES:
When call ends, you must have one of these outcomes:
- completed: pizza confirmed, prices collected, total + delivery time +
  order number received, special instructions delivered
- nothing_available: pizza itself cannot be ordered
- over_budget: pizza + side already exceeds budget_max
- detected_as_bot: employee suspects you are a bot

After the call ends print this JSON to stdout:
{{
  "outcome": "completed|nothing_available|over_budget|detected_as_bot",
  "pizza": {{"description": "...", "substitutions": {{}}, "price": 0.0}},
  "side": {{"description": "...", "original": "...", "price": 0.0}},
  "drink": {{"description": "...", "price": 0.0}},
  "total": 0.0,
  "delivery_time": "...",
  "order_number": "...",
  "special_instructions_delivered": true
}}
"""

        serializer = TwilioFrameSerializer(
            stream_sid=stream_sid,
            call_sid=call_sid,
            account_sid=self._settings.twilio_account_sid,
            auth_token=self._settings.twilio_auth_token,
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
            api_key=self._settings.deepgram_api_key,
            live_options=LiveOptions(
                model="nova-2-phonecall",
                encoding="linear16",
                sample_rate=8000,
                channels=1,
                interim_results=True,
                utterance_end_ms=1000,
            ),
        )

        llm = OpenAILLMService(
            api_key=self._settings.openai_api_key,
            model="gpt-4o",
        )

        tts = ElevenLabsTTSService(
            api_key=self._settings.elevenlabs_api_key,
            voice_id=self._settings.elevenlabs_voice_id,
            output_format="ulaw_8000",
            sample_rate=8000,
        )

        context = LLMContext(messages=[{"role": "system", "content": system_prompt}])
        vad = SileroVADAnalyzer(sample_rate=8000)
        context_aggregator = LLMContextAggregatorPair(
            context, user_params=LLMUserAggregatorParams(vad_analyzer=vad)
        )

        pipeline = Pipeline(
            [
                transport.input(),
                stt,
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

        @transport.event_handler("on_client_disconnected")
        async def on_disconnected(transport, client):
            await task.queue_frame(EndFrame())
            self._log_event(call_sid, "human_phase_completed", {})

        self._log_event(call_sid, "human_phase_started", {"stream_sid": stream_sid})
        runner = PipelineRunner(handle_sigint=False)
        await runner.run(task)
