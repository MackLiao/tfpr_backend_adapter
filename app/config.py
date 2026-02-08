from __future__ import annotations

from functools import lru_cache
import os

from huggingface_hub import HfFolder
from pydantic import Field
from pydantic_settings import BaseSettings


def _default_hf_token() -> str | None:
    """Resolve HF token from generic env var or local Hugging Face auth cache."""
    return os.getenv("HF_TOKEN") or HfFolder.get_token()


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    config_path: str
    hf_token: str | None = Field(default_factory=_default_hf_token)
    cors_origins: list[str] = ["http://localhost:5173"]
    page_size_default: int = 100
    page_size_max: int = 10000

    model_config = {"env_prefix": "TFBP_"}


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
