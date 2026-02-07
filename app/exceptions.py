from __future__ import annotations

import logging

import duckdb
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from tfbpapi.errors import DataCardError, DataCardValidationError, HfDataFetchError

logger = logging.getLogger(__name__)


def register_exception_handlers(app: FastAPI) -> None:
    """Register global exception handlers that map SDK errors to HTTP responses."""

    @app.exception_handler(FileNotFoundError)
    async def _file_not_found(request: Request, exc: FileNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(ValueError)
    async def _value_error(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    @app.exception_handler(duckdb.Error)
    async def _duckdb_error(request: Request, exc: duckdb.Error) -> JSONResponse:
        return JSONResponse(
            status_code=400, content={"detail": f"SQL error: {exc}"}
        )

    @app.exception_handler(DataCardError)
    async def _datacard_error(request: Request, exc: DataCardError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(DataCardValidationError)
    async def _datacard_validation_error(
        request: Request, exc: DataCardValidationError
    ) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(HfDataFetchError)
    async def _hf_fetch_error(request: Request, exc: HfDataFetchError) -> JSONResponse:
        logger.error("HuggingFace fetch error: %s", exc)
        return JSONResponse(
            status_code=502, content={"detail": f"HuggingFace error: {exc}"}
        )
