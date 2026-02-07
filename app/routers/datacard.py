from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from tfbpapi import DataCard

from app.config import Settings, get_settings

router = APIRouter(prefix="/datacard", tags=["datacard"])


def _get_datacard(repo_id: str, settings: Settings = Depends(get_settings)) -> DataCard:
    """Create a DataCard for the given repository."""
    return DataCard(repo_id, token=settings.hf_token)


@router.get("/{repo_id:path}/configs")
def list_configs(repo_id: str, settings: Settings = Depends(get_settings)) -> list[dict]:
    """List dataset configurations for a repository."""
    card = _get_datacard(repo_id, settings)
    configs = card.configs
    return [
        {
            "name": cfg.name,
            "default": cfg.default,
            "data_type": cfg.data_type.value if cfg.data_type else None,
        }
        for cfg in configs
    ]


@router.get("/{repo_id:path}/features/{config_name}")
def get_features(
    repo_id: str,
    config_name: str,
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    """Get feature definitions for a dataset configuration."""
    card = _get_datacard(repo_id, settings)
    features = card.get_features(config_name)
    # Convert FeatureInfo objects to dicts for JSON serialization
    return {
        name: feat.model_dump() if hasattr(feat, "model_dump") else feat
        for name, feat in features.items()
    }


@router.get("/{repo_id:path}/conditions/{config_name}")
def get_conditions(
    repo_id: str,
    config_name: str,
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    """Get experimental conditions for a dataset configuration."""
    card = _get_datacard(repo_id, settings)
    return card.get_experimental_conditions(config_name)
