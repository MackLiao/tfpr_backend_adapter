from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    config_path: str
    hf_token: str | None = None
    cors_origins: list[str] = ["http://localhost:5173"]
    page_size_default: int = 100
    page_size_max: int = 10000

    model_config = {"env_prefix": "TFBP_"}


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
