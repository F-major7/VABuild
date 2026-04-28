from __future__ import annotations

import asyncio
import base64
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import WebSocket
from starlette.websockets import WebSocketState

from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import EndFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.serializers.twilio import TwilioFrameSerializer
from pipecat.services.deepgram.stt import DeepgramSTTService, LiveOptions
from pipecat.services.elevenlabs.tts import ElevenLabsTTSService
from pipecat.services.llm_service import FunctionCallParams
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
from .llm_ivr import LlmIvrDriver
from .logger import get_logger
from .metrics import calls_total, human_phase_duration, ivr_duration, ivr_retries_total
from .models import CallStatusResponse, OrderModel
from .twilio_client import TwilioClient


logger = get_logger("voice_agent.call_manager")


class CallManager:
    """Orchestrates the full lifecycle of an outbound pizza ordering call."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._twilio = TwilioClient(settings)
        self._deepgram_by_call: dict[str, DeepgramClient] = {}
        self._elevenlabs = ElevenLabsClient(settings)
        self._llm_ivr: dict[str, LlmIvrDriver] = {}
        self._call_states: dict[str, dict[str, Any]] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def initiate_call(self, order: OrderModel) -> str:
        """Dial the restaurant, initialise per-call state, return call_sid."""
        call_sid = await self._twilio.create_outbound_call(order.phone_number)
        self._llm_ivr[call_sid] = LlmIvrDriver(order, self._settings)
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
        """Return current phase, IVR state, retry count, and event log for a call."""
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
        """Drive the Twilio media-stream WebSocket through IVR then human conversation phases."""
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
                        self._call_states[call_sid]["ivr_start"] = time.monotonic()
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
            if call_sid:
                self._write_call_log(call_sid)
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
            driver = self._llm_ivr[call_sid]
            transition = await driver.decide_action(transcript)
            if transition is None:
                failed = driver.register_reprompt()
                self._call_states[call_sid]["retry_count"] = driver.retry_count
                self._log_event(
                    call_sid,
                    "ivr_reprompt_detected",
                    {"transcript": transcript, "retry_count": driver.retry_count},
                )
                ivr_retries_total.labels(state=driver.state).inc()
                if failed:
                    self._call_states[call_sid]["phase"] = "failed"
                    self._call_states[call_sid]["ivr_state"] = driver.state
                    self._log_event(call_sid, "ivr_failed", {"reason": "max_retries_exceeded"})
                    await self._twilio.end_call(call_sid)
                return

            previous_state = self._call_states[call_sid]["ivr_state"]
            self._call_states[call_sid]["ivr_state"] = transition.next_state.value
            self._call_states[call_sid]["retry_count"] = driver.retry_count
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
                try:
                    audio_bytes = await self._elevenlabs.synthesize_mulaw_8khz(transition.action_value)
                    encoded = base64.b64encode(audio_bytes).decode("utf-8")
                    await websocket.send_json(
                        {
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {"payload": encoded},
                        }
                    )
                    self._log_event(call_sid, "tts_sent", {"text": transition.action_value, "bytes": len(audio_bytes)})
                except Exception as exc:
                    logger.error(
                        "ivr_tts_error",
                        extra={"event_data": {"call_sid": call_sid, "text": transition.action_value, "error": str(exc)}},
                    )
                    self._log_event(call_sid, "ivr_tts_error", {"text": transition.action_value, "error": str(exc)})

            if transition.terminal:
                self._call_states[call_sid]["phase"] = "hold"
                ivr_start = self._call_states[call_sid].get("ivr_start")
                if ivr_start is not None:
                    ivr_duration.observe(time.monotonic() - ivr_start)
                self._log_event(call_sid, "phase_updated", {"phase": "hold"})
                hold_event = self._call_states[call_sid].get("hold_event")
                if hold_event:
                    hold_event.set()

    def _write_call_log(self, call_sid: str) -> None:
        """Write a structured call log file to logs/ after a call ends."""
        state = self._call_states.get(call_sid, {})
        result: dict[str, Any] = state.get("result", {})
        order: OrderModel | None = state.get("order")
        events: list[dict[str, Any]] = state.get("logs", [])

        outcome = result.get("outcome") or state.get("phase", "disconnected")
        raw_id = result.get("order_number") or call_sid[-6:]
        identifier = re.sub(r"[^a-zA-Z0-9]", "", raw_id)[:12]

        now = datetime.now(timezone.utc)
        filename = f"{now.strftime('%Y-%m-%d_%H%M')}_{outcome}_{identifier}.json"

        summary: dict[str, Any] = {
            "outcome": outcome,
            "call_sid": call_sid,
            "started_at": events[0]["timestamp"] if events else now.isoformat(),
            "ended_at": now.isoformat(),
            **({"customer": order.customer_name, "phone": order.phone_number} if order else {}),
            **{k: v for k, v in result.items() if k != "outcome"},
        }

        log_events = [
            {"time": e.get("timestamp", "")[11:19], **{k: v for k, v in e.items() if k != "timestamp"}}
            for e in events
        ]

        logs_dir = Path(__file__).resolve().parent.parent / "logs"
        logs_dir.mkdir(exist_ok=True)
        (logs_dir / filename).write_text(
            json.dumps({"summary": summary, "events": log_events}, indent=2, default=str)
        )
        logger.info("call_log_written", extra={"event_data": {"call_sid": call_sid, "file": filename}})

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
        order: OrderModel = self._call_states[call_sid]["order"]
        order_json = order.model_dump()

        system_prompt = f"""You are an AI agent placing a pizza delivery order over the phone.
You have just navigated the restaurant's automated IVR and are now on hold waiting for a human employee.

ORDER:
{json.dumps(order_json, indent=2)}

=== PHASE: ON HOLD ===
Stay completely silent. Do not speak under any circumstances.
Ignore hold music, beeps, and silence.
Only begin speaking when you hear a clear human greeting such as
"hello", "thanks for calling", "what can I get for you", or similar.

=== PHASE: ORDERING ===
Once a human picks up, place the order using the rules below.

SEQUENCE — strictly one item at a time:
1. Order the pizza first. State size, crust, and toppings.
2. Wait for the employee to ask "anything else?" or similar before mentioning the side.
3. After the side is confirmed, wait for another "anything else?" prompt before ordering the drink.
   If adding the drink would push the running total over {order_json['budget_max']} and
   skip_if_over_budget is true, skip the drink entirely.

PIZZA SUBSTITUTIONS:
- If a topping is unavailable, only accept substitutions from: {order_json['pizza']['acceptable_topping_subs']}
- Never accept any of the following even if offered: {order_json['pizza']['no_go_toppings']}
- If the employee offers a no-go topping, politely decline and ask for an alternative.

SIDES:
- Try first_choice first. If unavailable, try backup_options in order.
- If all options are unavailable and if_all_unavailable is "skip", skip the side.

BUDGET:
- Keep a running total as each item is priced.
- If pizza + side already exceeds {order_json['budget_max']}, outcome is over_budget — end the call.

PRICES AND DELIVERY:
- If the employee gives a vague price ("about thirty"), ask for the exact amount.
- If the employee gives a time range ("35-40 minutes"), ask which one it will be.

ORDER NUMBER AND SPECIAL INSTRUCTIONS:
- Get the order confirmation number before delivering special instructions.
- Deliver this word for word: "{order_json['special_instructions']}"

SPEAKING STYLE:
- Sound like a real person, not a robot.
- Use occasional natural fillers: "umm", "uhh", "got it", "okay". Do not overuse them.
- Keep turns short and phone-friendly.

ENDING THE CALL:
You MUST call the complete_order tool before hanging up. Do not end the conversation without calling it.
Outcomes:
- completed: order confirmed, all prices collected, delivery time and order number received,
  special instructions delivered
- nothing_available: the pizza itself cannot be ordered in any form
- over_budget: pizza + side already exceed budget_max
- detected_as_bot: employee suspects you are not a human caller
"""

        complete_order_schema = FunctionSchema(
            name="complete_order",
            description=(
                "Call this when the ordering conversation is finished and you are ready to hang up. "
                "This is the only exit from the call — you must call it."
            ),
            properties={
                "outcome": {
                    "type": "string",
                    "enum": ["completed", "nothing_available", "over_budget", "detected_as_bot"],
                    "description": "Result of the ordering attempt.",
                },
                "pizza_description": {"type": "string", "description": "Final pizza as ordered."},
                "pizza_substitutions": {
                    "type": "object",
                    "description": "Map of original topping to substituted topping.",
                },
                "pizza_price": {"type": "number"},
                "side_description": {"type": "string", "description": "Side item as ordered, or empty string if skipped."},
                "side_original": {"type": "string", "description": "Side first_choice from the order."},
                "side_price": {"type": "number"},
                "drink_description": {"type": "string", "description": "Drink as ordered, or empty string if skipped."},
                "drink_price": {"type": "number"},
                "total": {"type": "number", "description": "Sum of all items actually ordered."},
                "delivery_time": {"type": "string", "description": "Exact delivery time quoted by the restaurant."},
                "order_number": {"type": "string", "description": "Confirmation number from the restaurant."},
                "special_instructions_delivered": {
                    "type": "boolean",
                    "description": "Whether special_instructions were read to the employee.",
                },
            },
            required=[
                "outcome",
                "pizza_description",
                "pizza_price",
                "total",
                "delivery_time",
                "order_number",
                "special_instructions_delivered",
            ],
        )

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
            output_format="pcm_16000",
            sample_rate=8000,
        )

        context = LLMContext(
            messages=[{"role": "system", "content": system_prompt}],
            tools=ToolsSchema(standard_tools=[complete_order_schema]),
        )
        context_aggregator = LLMContextAggregatorPair(context)

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

        async def handle_complete_order(params: FunctionCallParams) -> None:
            result = dict(params.arguments)
            self._call_states[call_sid]["result"] = result
            print(json.dumps(result, indent=2), flush=True)
            self._log_event(call_sid, "complete_order", result)
            calls_total.labels(outcome=result.get("outcome", "unknown")).inc()
            human_start = self._call_states[call_sid].get("human_start")
            if human_start is not None:
                human_phase_duration.observe(time.monotonic() - human_start)
            await params.result_callback({"status": "ok"})
            await task.queue_frame(EndFrame())

        llm.register_function("complete_order", handle_complete_order)

        @transport.event_handler("on_client_disconnected")
        async def on_disconnected(transport, client):
            await task.queue_frame(EndFrame())
            self._log_event(call_sid, "human_phase_completed", {})

        self._call_states[call_sid]["human_start"] = time.monotonic()
        self._log_event(call_sid, "human_phase_started", {"stream_sid": stream_sid})
        self._call_states[call_sid]["phase"] = "human_conversation"
        runner = PipelineRunner(handle_sigint=False)
        await runner.run(task)
