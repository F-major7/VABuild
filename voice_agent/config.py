from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    twilio_account_sid: str = Field(alias="TWILIO_ACCOUNT_SID")
    twilio_auth_token: str = Field(alias="TWILIO_AUTH_TOKEN")
    twilio_from_number: str = Field(alias="TWILIO_FROM_NUMBER")
    deepgram_api_key: str = Field(alias="DEEPGRAM_API_KEY")
    openai_api_key: str = Field(alias="OPENAI_API_KEY")
    elevenlabs_api_key: str = Field(alias="ELEVENLABS_API_KEY")
    elevenlabs_voice_id: str = Field(alias="ELEVENLABS_VOICE_ID")
    anthropic_api_key: str = Field(alias="ANTHROPIC_API_KEY")
    anthropic_model: str = Field(default="claude-haiku-4-5-20251001", alias="ANTHROPIC_MODEL")
    base_url: str = Field(alias="BASE_URL")

    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parent / ".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
