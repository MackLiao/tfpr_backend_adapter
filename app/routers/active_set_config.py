from __future__ import annotations

from functools import lru_cache
import threading
from pathlib import Path

import yaml
from fastapi import APIRouter, Depends, Request
from tfbpapi import VirtualDB
from tfbpapi.fetchers import HfDataCardFetcher, HfSizeInfoFetcher

from app.config import Settings, get_settings
from app.dataset_catalog import (
    DATASET_CATALOG,
    DATASET_CATALOG_BY_ID,
    MANAGED_DATASET_KEYS,
    DatasetCatalogItem,
    SupplementalDatasetConfig,
)
from app.dependencies import get_vdb, get_vdb_lock
from app.schemas import (
    ActiveSetConfigSyncRequest,
    ActiveSetConfigSyncResponse,
    DatasetCatalogEntry,
)

router = APIRouter(tags=["active-set-config"])


@lru_cache(maxsize=64)
def _cached_datacard_raw(repo_id: str, token: str | None) -> dict:
    try:
        return HfDataCardFetcher(token=token).fetch(repo_id)
    except Exception:
        return {}


@lru_cache(maxsize=64)
def _cached_size_raw(repo_id: str, token: str | None) -> dict:
    try:
        return HfSizeInfoFetcher(token=token).fetch(repo_id)
    except Exception:
        return {}


@lru_cache(maxsize=64)
def _config_columns(repo_id: str, token: str | None) -> dict[str, list[str]]:
    raw = _cached_datacard_raw(repo_id, token)
    configs = raw.get("configs", []) if isinstance(raw, dict) else []
    by_config: dict[str, list[str]] = {}

    for config in configs:
        if not isinstance(config, dict):
            continue
        config_name = config.get("config_name")
        if not isinstance(config_name, str):
            continue

        dataset_info = config.get("dataset_info")
        features = dataset_info.get("features", []) if isinstance(dataset_info, dict) else []
        columns: list[str] = []
        for feature in features:
            if not isinstance(feature, dict):
                continue
            name = feature.get("name")
            if isinstance(name, str):
                columns.append(name)

        by_config[config_name] = columns

    return by_config


@lru_cache(maxsize=64)
def _config_size_map(repo_id: str, token: str | None) -> dict[str, tuple[int | None, int | None]]:
    raw = _cached_size_raw(repo_id, token)
    if not isinstance(raw, dict):
        return {}

    size_node = raw.get("size", raw)
    configs = size_node.get("configs", []) if isinstance(size_node, dict) else []
    by_config: dict[str, tuple[int | None, int | None]] = {}

    for config in configs:
        if not isinstance(config, dict):
            continue
        config_name = config.get("config")
        if not isinstance(config_name, str):
            continue
        rows = config.get("num_rows")
        estimated = config.get("estimated_num_rows")
        columns = config.get("num_columns")
        by_config[config_name] = (
            rows if isinstance(rows, int) else (estimated if isinstance(estimated, int) else None),
            columns if isinstance(columns, int) else None,
        )

    return by_config


def _active_catalog_ids(vdb: VirtualDB) -> set[str]:
    active_pairs = set(vdb._db_name_map.values())
    return {
        item.id
        for item in DATASET_CATALOG
        if (item.repo_id, item.config_name) in active_pairs
    }


def _load_metadata_config(path: Path) -> dict:
    if not path.exists():
        return {"repositories": {}}

    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    if not isinstance(payload, dict):
        raise ValueError("VirtualDB config file must contain a YAML dictionary")

    repositories = payload.get("repositories")
    if repositories is None:
        payload["repositories"] = {}
    elif not isinstance(repositories, dict):
        raise ValueError("'repositories' in VirtualDB config must be a dictionary")

    return payload


def _prune_managed_dataset_entries(payload: dict) -> None:
    repositories = payload.setdefault("repositories", {})
    for repo_id in list(repositories.keys()):
        repo_cfg = repositories[repo_id]
        if not isinstance(repo_cfg, dict):
            continue

        dataset_cfg = repo_cfg.get("dataset")
        if not isinstance(dataset_cfg, dict):
            continue

        for config_name in list(dataset_cfg.keys()):
            if (repo_id, config_name) in MANAGED_DATASET_KEYS:
                del dataset_cfg[config_name]

        if not dataset_cfg:
            repo_cfg.pop("dataset", None)

        if not repo_cfg:
            del repositories[repo_id]


def _dataset_config_entry(
    *,
    db_name: str,
    sample_id_field: str,
) -> dict:
    return {
        "db_name": db_name,
        "sample_id": {
            "field": sample_id_field,
        },
    }


def _append_selected_datasets(payload: dict, selected: list[DatasetCatalogItem]) -> None:
    repositories = payload.setdefault("repositories", {})

    for item in selected:
        repo_cfg = repositories.setdefault(item.repo_id, {})
        dataset_cfg = repo_cfg.setdefault("dataset", {})
        dataset_cfg[item.config_name] = _dataset_config_entry(
            db_name=item.db_name,
            sample_id_field=item.sample_id_field,
        )
        for supplemental in item.supplemental_configs:
            _append_supplemental_dataset(
                dataset_cfg=dataset_cfg,
                supplemental=supplemental,
            )


def _append_supplemental_dataset(
    *,
    dataset_cfg: dict,
    supplemental: SupplementalDatasetConfig,
) -> None:
    dataset_cfg[supplemental.config_name] = _dataset_config_entry(
        db_name=supplemental.db_name,
        sample_id_field=supplemental.sample_id_field,
    )


def _write_metadata_config(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False)


@router.get("/dataset-catalog", response_model=list[DatasetCatalogEntry])
def dataset_catalog(
    vdb: VirtualDB = Depends(get_vdb),
    settings: Settings = Depends(get_settings),
) -> list[DatasetCatalogEntry]:
    active_ids = _active_catalog_ids(vdb)

    result: list[DatasetCatalogEntry] = []
    for item in DATASET_CATALOG:
        columns = _config_columns(item.repo_id, settings.hf_token).get(item.config_name, [])
        estimated_rows, num_columns = _config_size_map(item.repo_id, settings.hf_token).get(
            item.config_name, (None, None)
        )

        result.append(
            DatasetCatalogEntry(
                id=item.id,
                name=item.name,
                repo_id=item.repo_id,
                config_name=item.config_name,
                db_name=item.db_name,
                sample_id_field=item.sample_id_field,
                estimated_rows=estimated_rows,
                num_columns=num_columns if num_columns is not None else (len(columns) or None),
                column_names=columns,
                selectable=item.selectable,
                is_active=item.id in active_ids,
                unsupported_reason=item.unsupported_reason,
            )
        )

    return result


@router.post("/active-set/sync-config", response_model=ActiveSetConfigSyncResponse)
def sync_active_set_config(
    body: ActiveSetConfigSyncRequest,
    request: Request,
    settings: Settings = Depends(get_settings),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> ActiveSetConfigSyncResponse:
    selected_items: list[DatasetCatalogItem] = []
    seen: set[str] = set()

    for dataset_id in body.dataset_ids:
        if dataset_id in seen:
            continue
        seen.add(dataset_id)

        item = DATASET_CATALOG_BY_ID.get(dataset_id)
        if item is None:
            raise ValueError(f"Unknown dataset id: {dataset_id}")

        if not item.selectable:
            reason = item.unsupported_reason or "This dataset is not selectable"
            raise ValueError(f"Dataset '{dataset_id}' is not selectable: {reason}")

        selected_items.append(item)

    config_path = Path(settings.config_path)

    payload = _load_metadata_config(config_path)
    _prune_managed_dataset_entries(payload)
    _append_selected_datasets(payload, selected_items)
    _write_metadata_config(config_path, payload)

    with lock:
        request.app.state.vdb = VirtualDB(settings.config_path, token=settings.hf_token)

    active_ids = [item.id for item in selected_items]

    return ActiveSetConfigSyncResponse(
        config_path=str(config_path),
        active_dataset_ids=active_ids,
        active_dataset_count=len(active_ids),
    )
