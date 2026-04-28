from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable

from .models import OrderModel


class IvrState(str, Enum):
    WELCOME = "WELCOME"
    NAME = "NAME"
    PHONE = "PHONE"
    ZIP = "ZIP"
    CONFIRM = "CONFIRM"
    HOLD = "HOLD"


@dataclass(slots=True)
class StateTransitionResult:
    next_state: IvrState
    action_type: str
    action_value: str | None
    matched_prompt: str
    terminal: bool = False


class IvrStateMachine:
    MAX_RETRIES = 3

    PROMPT_KEYWORDS: dict[IvrState, tuple[str, ...]] = {
        IvrState.WELCOME: (
            "press 1 for delivery",
            "press one for delivery",
            "press 2 for carryout",
            "press two for carryout",
        ),
        IvrState.NAME: ("say the name for the order", "name for the order"),
        IvrState.PHONE: ("10-digit callback", "ten-digit callback", "callback number"),
        IvrState.ZIP: ("delivery zip code", "say your delivery zip"),
        IvrState.CONFIRM: ("is that correct", "say yes to confirm"),
        IvrState.HOLD: ("please hold", "connect you to a team member"),
    }

    def __init__(self, order: OrderModel):
        self.order = order
        self.state = IvrState.WELCOME
        self.retry_count = 0

    def transition_for_prompt(self, transcript: str) -> StateTransitionResult | None:
        normalized = transcript.lower().strip()
        if not normalized:
            return None

        current_prompt_matched = any(
            keyword in normalized for keyword in self.PROMPT_KEYWORDS[self.state]
        )
        if not current_prompt_matched:
            hold_prompt_matched = any(
                keyword in normalized for keyword in self.PROMPT_KEYWORDS[IvrState.HOLD]
            )
            if hold_prompt_matched:
                self.state = IvrState.HOLD
                self.retry_count = 0
                return StateTransitionResult(
                    next_state=IvrState.HOLD,
                    action_type="none",
                    action_value=None,
                    matched_prompt=normalized,
                    terminal=True,
                )
            return None

        self.retry_count = 0
        if self.state == IvrState.WELCOME:
            self.state = IvrState.NAME
            return StateTransitionResult(
                next_state=self.state,
                action_type="dtmf",
                action_value="1",
                matched_prompt=normalized,
            )

        if self.state == IvrState.NAME:
            self.state = IvrState.PHONE
            return StateTransitionResult(
                next_state=self.state,
                action_type="tts",
                action_value=self.order.customer_name,
                matched_prompt=normalized,
            )

        if self.state == IvrState.PHONE:
            self.state = IvrState.ZIP
            return StateTransitionResult(
                next_state=self.state,
                action_type="dtmf",
                action_value=self.order.phone_number,
                matched_prompt=normalized,
            )

        if self.state == IvrState.ZIP:
            self.state = IvrState.CONFIRM
            return StateTransitionResult(
                next_state=self.state,
                action_type="tts",
                action_value=self.order.extract_zip_code(),
                matched_prompt=normalized,
            )

        if self.state == IvrState.CONFIRM:
            self.state = IvrState.HOLD
            return StateTransitionResult(
                next_state=self.state,
                action_type="tts",
                action_value="yes",
                matched_prompt=normalized,
                terminal=True,
            )

        return StateTransitionResult(
            next_state=IvrState.HOLD,
            action_type="none",
            action_value=None,
            matched_prompt=normalized,
            terminal=True,
        )

    def register_reprompt(self) -> bool:
        self.retry_count += 1
        return self.retry_count >= self.MAX_RETRIES

    def expected_keywords(self) -> Iterable[str]:
        return self.PROMPT_KEYWORDS[self.state]
