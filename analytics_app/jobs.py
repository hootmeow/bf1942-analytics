"""Scheduled jobs executed by the analytics scheduler."""
from __future__ import annotations

import logging
from typing import Iterable

from asyncpg.pool import Pool

from .config import SchedulerConfig

LOGGER = logging.getLogger(__name__)


async def refresh_materialized_views(pool: Pool, views: Iterable[str]) -> None:
    """Refresh configured materialized views."""

    async with pool.acquire() as connection:
        for view in views:
            quoted_view = await connection.fetchval("SELECT quote_ident($1)", view)
            LOGGER.info("Refreshing materialized view: %s", view)
            await connection.execute(
                f"REFRESH MATERIALIZED VIEW CONCURRENTLY {quoted_view};"
            )


async def run_retention_procedure(pool: Pool, scheduler_config: SchedulerConfig) -> None:
    """Execute a retention maintenance stored procedure if configured."""

    if not scheduler_config.retention_procedure:
        LOGGER.debug("No retention procedure configured; skipping job.")
        return

    procedure = scheduler_config.retention_procedure
    async with pool.acquire() as connection:
        quoted_procedure = await connection.fetchval(
            "SELECT quote_ident($1)", procedure
        )
        LOGGER.info("Running retention procedure: %s", procedure)
        await connection.execute(f"CALL {quoted_procedure}();")


async def run_partition_procedure(pool: Pool, scheduler_config: SchedulerConfig) -> None:
    """Execute a partition maintenance stored procedure if configured."""

    if not scheduler_config.partition_procedure:
        LOGGER.debug("No partition procedure configured; skipping job.")
        return

    procedure = scheduler_config.partition_procedure
    async with pool.acquire() as connection:
        quoted_procedure = await connection.fetchval(
            "SELECT quote_ident($1)", procedure
        )
        LOGGER.info("Running partition procedure: %s", procedure)
        await connection.execute(f"CALL {quoted_procedure}();")


__all__ = [
    "refresh_materialized_views",
    "run_partition_procedure",
    "run_retention_procedure",
]
