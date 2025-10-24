"""FastAPI application entry point for analytics services."""
from __future__ import annotations

import logging
from typing import Dict

from fastapi import Depends, FastAPI, HTTPException

from .config import AppConfig, get_config
from .db import close_db_pool, get_pool, init_db_pool
from .scheduler import SchedulerManager

LOGGER = logging.getLogger(__name__)

app = FastAPI(title="BF1942 Analytics", version="0.1.0")


async def get_app_config() -> AppConfig:
    return get_config()


async def get_db_pool_dependency():
    try:
        return get_pool()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.on_event("startup")
async def on_startup() -> None:
    """Initialise resources on startup."""

    config = get_config()
    LOGGER.info("Starting analytics application")

    pool = await init_db_pool(config)
    scheduler_manager = SchedulerManager(config, pool)
    scheduler_manager.start()

    app.state.config = config
    app.state.pool = pool
    app.state.scheduler_manager = scheduler_manager


@app.on_event("shutdown")
async def on_shutdown() -> None:
    """Tear down resources on shutdown."""

    scheduler_manager: SchedulerManager | None = getattr(
        app.state, "scheduler_manager", None
    )
    if scheduler_manager:
        scheduler_manager.shutdown()

    await close_db_pool()
    LOGGER.info("Analytics application shut down")


@app.get("/health", tags=["monitoring"])
async def health(pool=Depends(get_db_pool_dependency)) -> Dict[str, str]:
    """Return a simple health indicator."""

    if pool is None:
        raise HTTPException(status_code=503, detail="Database pool unavailable")
    return {"status": "ok"}


@app.get("/metrics", tags=["monitoring"])
async def metrics() -> Dict[str, object]:
    """Return scheduler metrics and configuration highlights."""

    scheduler_manager: SchedulerManager | None = getattr(
        app.state, "scheduler_manager", None
    )
    if scheduler_manager is None:
        raise HTTPException(status_code=503, detail="Scheduler not available")

    config: AppConfig = app.state.config
    snapshot = scheduler_manager.snapshot()
    return {
        "log_level": config.log_level,
        "database": {
            "host": config.database.host,
            "database": config.database.database,
            "pool_min_size": config.database.min_size,
            "pool_max_size": config.database.max_size,
        },
        "scheduler": snapshot,
    }


@app.get("/config", tags=["monitoring"], include_in_schema=False)
async def config_endpoint(config: AppConfig = Depends(get_app_config)) -> AppConfig:
    """Expose configuration details for diagnostics."""

    return config


__all__ = ["app"]
