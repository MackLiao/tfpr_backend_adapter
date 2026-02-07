from __future__ import annotations

import threading

from fastapi import APIRouter, Depends
from tfbpapi import VirtualDB

from app.dependencies import get_vdb, get_vdb_lock
from app.schemas import DatasetInfo, HealthResponse

router = APIRouter(tags=["discovery"])


@router.get("/health", response_model=HealthResponse)
def health_check(
    vdb: VirtualDB = Depends(get_vdb),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> HealthResponse:
    """Check if the API is up and VirtualDB has views registered."""
    with lock:
        tables = vdb.tables()
    return HealthResponse(status="ok", tables_registered=len(tables))


@router.get("/tables", response_model=list[str])
def list_tables(
    vdb: VirtualDB = Depends(get_vdb),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> list[str]:
    """List all registered SQL view names."""
    with lock:
        return vdb.tables()


@router.get("/datasets", response_model=list[DatasetInfo])
def list_datasets(
    vdb: VirtualDB = Depends(get_vdb),
) -> list[DatasetInfo]:
    """List all datasets with metadata (db_name, repo, comparative status)."""
    results: list[DatasetInfo] = []
    for db_name, (repo_id, config_name) in vdb._db_name_map.items():
        results.append(
            DatasetInfo(
                db_name=db_name,
                repo_id=repo_id,
                config_name=config_name,
                is_comparative=vdb._is_comparative(repo_id, config_name),
            )
        )
    return results


@router.get("/common-fields", response_model=list[str])
def get_common_fields(
    vdb: VirtualDB = Depends(get_vdb),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> list[str]:
    """Get field names shared across all primary _meta views."""
    with lock:
        return vdb.get_common_fields()
