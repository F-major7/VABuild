import re
from typing import Any

from pydantic import BaseModel, Field, field_validator


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

    @field_validator("delivery_address")
    @classmethod
    def ensure_zip_present(cls, value: str) -> str:
        if not re.search(r"\b\d{5}\b", value):
            raise ValueError("delivery_address must include a 5-digit zip code")
        return value

    def extract_zip_code(self) -> str:
        match = re.search(r"\b(\d{5})\b", self.delivery_address)
        if not match:
            raise ValueError("zip code could not be extracted from delivery_address")
        return match.group(1)


class CallCreateResponse(BaseModel):
    call_sid: str
    status: str


class CallStatusResponse(BaseModel):
    call_sid: str
    ivr_state: str
    retry_count: int
    phase: str
    collected: dict[str, Any]
    logs: list[dict[str, Any]]
