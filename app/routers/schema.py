from __future__ import annotations

import threading

from fastapi import APIRouter, Depends
from tfbpapi import VirtualDB

from app.dependencies import get_vdb, get_vdb_lock
from app.schemas import ColumnInfo

router = APIRouter(tags=["schema"])


@router.get("/schema/{table}", response_model=list[ColumnInfo])
def describe_table(
    table: str,
    vdb: VirtualDB = Depends(get_vdb),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> list[ColumnInfo]:
    """Get column names and types for a table/view."""
    with lock:
        df = vdb.describe(table)
    return [
        ColumnInfo(column_name=row["column_name"], column_type=row["column_type"])
        for _, row in df.iterrows()
    ]


@router.get("/fields/{table}", response_model=list[str])
def get_fields(
    table: str,
    vdb: VirtualDB = Depends(get_vdb),
    lock: threading.Lock = Depends(get_vdb_lock),
) -> list[str]:
    """Get column names for a table/view."""
    with lock:
        return vdb.get_fields(table)
