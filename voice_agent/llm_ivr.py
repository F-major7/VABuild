from __future__ import annotations

import json
import time
from typing import Any

from openai import AsyncOpenAI

from .config import Settings
from .ivr_state_machine import IvrState, StateTransitionResult
from .logger import get_logger
from .metrics import llm_response_latency
from .models import OrderModel

logger = get_logger("voice_agent.llm_ivr")

_TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "press_digit",
            "description": "Press a digit or sequence of digits on the phone keypad.",
            "parameters": {
                "type": "object",
                "properties": {
                    "digit": {
                        "type": "string",
                        "description": "Digit or digit sequence to send (e.g. '1' or '5551234567').",
                    }
                },
                "required": ["digit"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "speak_value",
            "description": "Speak a value aloud into the phone (name, zip code, or confirmation).",
            "parameters": {
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "Text to speak.",
                    }
                },
                "required": ["text"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "enter_hold",
            "description": "Signal that the IVR is placing you on hold or transferring to a human.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
]

_SYSTEM_PROMPT_TEMPLATE = """\
You are navigating an automated IVR phone menu to place a pizza delivery order.
You will receive transcripts of what the IVR says.
Call exactly one tool per transcript based on what the IVR is asking.

Order details for reference:
- Customer name: {customer_name}
- Callback number: {phone_number}
- Delivery zip code: {zip_code}

Rules:
- press_digit("1") when the IVR offers a delivery option (e.g. "press 1 for delivery")
- speak_value("{customer_name}") when the IVR asks for the name on the order
- press_digit("{phone_number}") when the IVR asks for a callback or phone number
- speak_value("{zip_code}") when the IVR asks for a delivery zip code
- speak_value("yes") when the IVR asks you to confirm the order details
- enter_hold() when the IVR says it is placing you on hold or connecting to a team member

Respond only via tool calls, never with plain text.
"""


class LlmIvrDriver:
    """LLM-driven IVR navigator using OpenAI function calling."""

    MAX_RETRIES = 3

    def __init__(self, order: OrderModel, settings: Settings) -> None:
        self._order = order
        self._client = AsyncOpenAI(api_key=settings.openai_api_key)
        self._state = IvrState.WELCOME
        self._retry_count = 0
        self._system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(
            customer_name=order.customer_name,
            phone_number=order.phone_number,
            zip_code=order.extract_zip_code(),
        )

    @property
    def state(self) -> str:
        return self._state.value

    @property
    def retry_count(self) -> int:
        return self._retry_count

    async def decide_action(self, transcript: str) -> StateTransitionResult | None:
        """Send transcript to LLM, parse tool call, return StateTransitionResult."""
        start = time.monotonic()
        try:
            response = await self._client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": self._system_prompt},
                    {"role": "user", "content": transcript},
                ],
                tools=_TOOLS,
                tool_choice="required",
            )
        except Exception as exc:
            logger.error(
                "ivr_llm_error",
                extra={"event_data": {"error": str(exc), "state": self._state.value}},
            )
            return None

        latency_s = time.monotonic() - start
        llm_response_latency.observe(latency_s)
        latency_ms = int(latency_s * 1000)
        choice = response.choices[0]

        if not choice.message.tool_calls:
            logger.warning(
                "ivr_llm_no_tool_call",
                extra={"event_data": {"transcript": transcript, "state": self._state.value}},
            )
            return None

        tool_call = choice.message.tool_calls[0]
        name = tool_call.function.name
        args: dict[str, Any] = json.loads(tool_call.function.arguments)

        logger.info(
            "ivr_llm_tool_call",
            extra={
                "event_data": {
                    "tool": name,
                    "args": args,
                    "latency_ms": latency_ms,
                    "state": self._state.value,
                }
            },
        )

        result = self._map_tool_call(name, args)
        if result is None:
            logger.warning(
                "ivr_llm_state_mismatch",
                extra={"event_data": {"tool": name, "args": args, "state": self._state.value}},
            )
        return result

    def register_reprompt(self) -> bool:
        """Increment retry count; return True if max retries exceeded."""
        self._retry_count += 1
        return self._retry_count >= self.MAX_RETRIES

    def _map_tool_call(self, name: str, args: dict[str, Any]) -> StateTransitionResult | None:
        if name == "enter_hold":
            self._state = IvrState.HOLD
            self._retry_count = 0
            return StateTransitionResult(
                next_state=IvrState.HOLD,
                action_type="none",
                action_value=None,
                matched_prompt="enter_hold",
                terminal=True,
            )

        if name == "press_digit":
            digit = str(args.get("digit", ""))
            if self._state == IvrState.WELCOME:
                self._state = IvrState.NAME
                self._retry_count = 0
                return StateTransitionResult(
                    next_state=IvrState.NAME,
                    action_type="dtmf",
                    action_value=digit,
                    matched_prompt="press_digit",
                )
            if self._state == IvrState.PHONE:
                self._state = IvrState.ZIP
                self._retry_count = 0
                return StateTransitionResult(
                    next_state=IvrState.ZIP,
                    action_type="dtmf",
                    action_value=digit,
                    matched_prompt="press_digit",
                )

        if name == "speak_value":
            text = str(args.get("text", ""))
            if self._state == IvrState.NAME:
                self._state = IvrState.PHONE
                self._retry_count = 0
                return StateTransitionResult(
                    next_state=IvrState.PHONE,
                    action_type="tts",
                    action_value=text,
                    matched_prompt="speak_value",
                )
            if self._state == IvrState.ZIP:
                self._state = IvrState.CONFIRM
                self._retry_count = 0
                return StateTransitionResult(
                    next_state=IvrState.CONFIRM,
                    action_type="tts",
                    action_value=text,
                    matched_prompt="speak_value",
                )
            if self._state == IvrState.CONFIRM:
                self._state = IvrState.HOLD
                self._retry_count = 0
                return StateTransitionResult(
                    next_state=IvrState.HOLD,
                    action_type="tts",
                    action_value=text,
                    matched_prompt="speak_value",
                    terminal=True,
                )

        return None
