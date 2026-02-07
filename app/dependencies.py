from __future__ import annotations

import threading

from fastapi import Request
from tfbpapi import VirtualDB


def get_vdb(request: Request) -> VirtualDB:
    """FastAPI dependency that provides the shared VirtualDB instance."""
    return request.app.state.vdb


def get_vdb_lock(request: Request) -> threading.Lock:
    """FastAPI dependency that provides the VirtualDB threading lock."""
    return request.app.state.vdb_lock
