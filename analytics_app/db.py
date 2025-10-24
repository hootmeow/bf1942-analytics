"""Database connection management for the analytics app."""
from __future__ import annotations

import asyncpg
from asyncpg.pool import Pool

from .config import AppConfig, get_config

_POOL: Pool | None = None


async def init_db_pool(config: AppConfig | None = None) -> Pool:
    """Initialise the global asyncpg pool."""

    global _POOL
    if _POOL is not None:
        return _POOL

    if config is None:
        config = get_config()

    _POOL = await asyncpg.create_pool(
        dsn=config.database.dsn,
        min_size=config.database.min_size,
        max_size=config.database.max_size,
    )
    return _POOL


async def close_db_pool() -> None:
    """Close the global asyncpg pool."""

    global _POOL
    if _POOL is not None:
        await _POOL.close()
        _POOL = None


def get_pool() -> Pool:
    """Return the cached asyncpg pool or raise a runtime error."""

    if _POOL is None:
        raise RuntimeError("Database pool has not been initialised.")
    return _POOL


__all__ = ["init_db_pool", "close_db_pool", "get_pool"]
