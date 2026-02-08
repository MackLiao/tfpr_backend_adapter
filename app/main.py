from __future__ import annotations

import logging
import threading
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from tfbpapi import VirtualDB

from app.config import get_settings
from app.exceptions import register_exception_handlers
from app.routers import active_set_config, analysis, datacard, discovery, query, schema

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Initialize VirtualDB at startup, tear down on shutdown."""
    settings = get_settings()
    logger.info("Initializing VirtualDB from %s", settings.config_path)
    vdb = VirtualDB(settings.config_path, token=settings.hf_token)
    app.state.vdb = vdb
    app.state.vdb_lock = threading.Lock()
    yield


def create_app() -> FastAPI:
    """Application factory."""
    settings = get_settings()

    app = FastAPI(
        title="TFBP API",
        description="REST API for Transcription Factor Binding & Perturbation data",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    register_exception_handlers(app)

    app.include_router(discovery.router, prefix="/api/v1")
    app.include_router(active_set_config.router, prefix="/api/v1")
    app.include_router(schema.router, prefix="/api/v1")
    app.include_router(query.router, prefix="/api/v1")
    app.include_router(datacard.router, prefix="/api/v1")
    app.include_router(analysis.router, prefix="/api/v1")

    return app


app = create_app()
