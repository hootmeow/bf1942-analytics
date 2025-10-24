"""FastAPI application entry point for analytics services."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder

from .config import AppConfig, get_config
from .db import close_db_pool, get_pool, init_db_pool
from .migrations import apply_sql_directory
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


async def _fetch_all(pool, query: str, *args) -> List[Dict[str, object]]:
    """Execute a query returning all rows as JSON-serialisable dictionaries."""

    async with pool.acquire() as connection:
        records = await connection.fetch(query, *args)
    return jsonable_encoder([dict(record) for record in records])


async def _fetch_one(pool, query: str, *args) -> Dict[str, object] | None:
    """Execute a query returning a single row or ``None``."""

    async with pool.acquire() as connection:
        record = await connection.fetchrow(query, *args)
    if record is None:
        return None
    return jsonable_encoder(dict(record))


@app.on_event("startup")
async def on_startup() -> None:
    """Initialise resources on startup."""

    config = get_config()
    LOGGER.info("Starting analytics application")

    pool = await init_db_pool(config)
    await apply_sql_directory(pool)
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


@app.get("/analytics/players/metrics", tags=["analytics"])
async def player_metrics(
    player_id: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
    pool=Depends(get_db_pool_dependency),
):
    """Expose player performance metrics sourced from materialized views."""

    if pool is None:
        raise HTTPException(status_code=503, detail="Database pool unavailable")

    if player_id:
        record = await _fetch_one(
            pool,
            "SELECT * FROM mv_player_advanced_metrics WHERE player_id = $1",
            player_id,
        )
        if record is None:
            raise HTTPException(status_code=404, detail="Player not found")
        return record

    return await _fetch_all(
        pool,
        """
        SELECT *
        FROM mv_player_advanced_metrics
        ORDER BY total_score DESC
        LIMIT $1
        """,
        limit,
    )


@app.get("/analytics/players/{player_id}/maps", tags=["analytics"])
async def player_map_breakdowns(
    player_id: str,
    limit: int = Query(50, ge=1, le=500),
    pool=Depends(get_db_pool_dependency),
):
    """Return per-map and mod splits for a specific player."""

    if pool is None:
        raise HTTPException(status_code=503, detail="Database pool unavailable")

    records = await _fetch_all(
        pool,
        """
        SELECT *
        FROM mv_player_map_mod_breakdowns
        WHERE player_id = $1
        ORDER BY sessions_played DESC
        LIMIT $2
        """,
        player_id,
        limit,
    )
    if not records:
        raise HTTPException(status_code=404, detail="Player map splits not found")
    return records


@app.get("/analytics/players/{player_id}/heatmap", tags=["analytics"])
async def player_heatmap(
    player_id: str,
    limit: int = Query(200, ge=1, le=1000),
    pool=Depends(get_db_pool_dependency),
):
    """Return hourly session heatmap buckets for a player."""

    if pool is None:
        raise HTTPException(status_code=503, detail="Database pool unavailable")

    records = await _fetch_all(
        pool,
        """
        SELECT *
        FROM player_session_heatmaps
        WHERE player_id = $1
        ORDER BY hour_bucket DESC
        LIMIT $2
        """,
        player_id,
        limit,
    )
    if not records:
        raise HTTPException(status_code=404, detail="Player heatmap not found")
    return records


@app.get("/analytics/servers/population", tags=["analytics"])
async def server_population_trends(
    server_id: str | None = None,
    since: datetime | None = None,
    limit: int = Query(168, ge=1, le=2000),
    pool=Depends(get_db_pool_dependency),
):
    """Return population trend metrics for servers."""

    if pool is None:
        raise HTTPException(status_code=503, detail="Database pool unavailable")

    conditions: List[str] = []
    params: List[object] = []
    if server_id:
        params.append(server_id)
        conditions.append(f"server_id = ${len(params)}")
    if since:
        params.append(since)
        conditions.append(f"hour_bucket >= ${len(params)}")

    where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    order_clause = " ORDER BY hour_bucket DESC"

    if not conditions:
        params.append(limit)
        query = (
            "SELECT * FROM mv_server_population_trends"
            f"{where_clause}{order_clause} LIMIT ${len(params)}"
        )
    else:
        query = f"SELECT * FROM mv_server_population_trends{where_clause}{order_clause}"

    return await _fetch_all(pool, query, *params)


@app.get("/analytics/servers/rotation", tags=["analytics"])
async def server_rotation_statistics(
    server_id: str | None = None,
    limit: int = Query(200, ge=1, le=1000),
    pool=Depends(get_db_pool_dependency),
):
    """Return per-server map rotation analytics."""

    if pool is None:
        raise HTTPException(status_code=503, detail="Database pool unavailable")

    if server_id:
        return await _fetch_all(
            pool,
            """
            SELECT *
            FROM mv_server_rotation_statistics
            WHERE server_id = $1
            ORDER BY rounds_played DESC
            LIMIT $2
            """,
            server_id,
            limit,
        )

    return await _fetch_all(
        pool,
        """
        SELECT *
        FROM mv_server_rotation_statistics
        ORDER BY rounds_played DESC
        LIMIT $1
        """,
        limit,
    )


@app.get("/analytics/servers/pings", tags=["analytics"])
async def server_ping_distributions(
    server_id: str | None = None,
    since: datetime | None = None,
    limit: int = Query(90, ge=1, le=1000),
    pool=Depends(get_db_pool_dependency),
):
    """Return ping distribution summaries per server."""

    if pool is None:
        raise HTTPException(status_code=503, detail="Database pool unavailable")

    conditions: List[str] = []
    params: List[object] = []
    if server_id:
        params.append(server_id)
        conditions.append(f"server_id = ${len(params)}")
    if since:
        params.append(since)
        conditions.append(f"day_bucket >= ${len(params)}")

    where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    order_clause = " ORDER BY day_bucket DESC"

    if not conditions:
        params.append(limit)
        query = (
            "SELECT * FROM mv_server_ping_distributions"
            f"{where_clause}{order_clause} LIMIT ${len(params)}"
        )
    else:
        query = f"SELECT * FROM mv_server_ping_distributions{where_clause}{order_clause}"

    return await _fetch_all(pool, query, *params)


@app.get("/analytics/servers/{server_id}/leaderboard", tags=["analytics"])
async def server_leaderboard(
    server_id: str,
    limit: int = Query(50, ge=1, le=500),
    pool=Depends(get_db_pool_dependency),
):
    """Return per-server leaderboards sorted by score."""

    if pool is None:
        raise HTTPException(status_code=503, detail="Database pool unavailable")

    records = await _fetch_all(
        pool,
        """
        SELECT *
        FROM mv_server_leaderboards
        WHERE server_id = $1
        ORDER BY score_rank ASC
        LIMIT $2
        """,
        server_id,
        limit,
    )
    if not records:
        raise HTTPException(status_code=404, detail="Server leaderboard not found")
    return records


@app.get("/analytics/global/activity", tags=["analytics"])
async def global_activity(pool=Depends(get_db_pool_dependency)) -> Dict[str, object]:
    """Return the latest global activity rollup."""

    if pool is None:
        raise HTTPException(status_code=503, detail="Database pool unavailable")

    record = await _fetch_one(
        pool,
        "SELECT * FROM mv_global_activity_rollups",
    )
    if record is None:
        raise HTTPException(status_code=404, detail="Global activity rollup unavailable")
    return record


@app.get("/analytics/global/daily", tags=["analytics"])
async def global_daily_trends(
    since: datetime | None = None,
    limit: int = Query(30, ge=1, le=365),
    pool=Depends(get_db_pool_dependency),
):
    """Return historical daily global metrics."""

    if pool is None:
        raise HTTPException(status_code=503, detail="Database pool unavailable")

    if since is not None:
        return await _fetch_all(
            pool,
            """
            SELECT *
            FROM mv_global_daily_trends
            WHERE day_bucket >= $1
            ORDER BY day_bucket DESC
            """,
            since,
        )

    return await _fetch_all(
        pool,
        """
        SELECT *
        FROM mv_global_daily_trends
        ORDER BY day_bucket DESC
        LIMIT $1
        """,
        limit,
    )


@app.get("/analytics/global/regions", tags=["analytics"])
async def global_region_activity(
    region: str | None = None,
    since: datetime | None = None,
    limit: int = Query(90, ge=1, le=1000),
    pool=Depends(get_db_pool_dependency),
):
    """Return aggregated regional activity statistics."""

    if pool is None:
        raise HTTPException(status_code=503, detail="Database pool unavailable")

    conditions: List[str] = []
    params: List[object] = []
    if region:
        params.append(region)
        conditions.append(f"region = ${len(params)}")
    if since:
        params.append(since)
        conditions.append(f"day_bucket >= ${len(params)}")

    where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    order_clause = " ORDER BY day_bucket DESC"

    if not conditions:
        params.append(limit)
        query = (
            "SELECT * FROM mv_global_region_activity"
            f"{where_clause}{order_clause} LIMIT ${len(params)}"
        )
    else:
        query = f"SELECT * FROM mv_global_region_activity{where_clause}{order_clause}"

    return await _fetch_all(pool, query, *params)


@app.get("/config", tags=["monitoring"], include_in_schema=False)
async def config_endpoint(config: AppConfig = Depends(get_app_config)) -> AppConfig:
    """Expose configuration details for diagnostics."""

    return config


__all__ = ["app"]
