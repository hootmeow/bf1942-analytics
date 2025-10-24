"""Scheduled jobs executed by the analytics scheduler."""
from __future__ import annotations

import logging
from pathlib import Path

from asyncpg.pool import Pool

from .config import SchedulerConfig
from .sql_jobs import (
    execute_sql_job,
    load_sql_jobs,
    record_job_result,
)

LOGGER = logging.getLogger(__name__)


async def run_sql_refresh_jobs(pool: Pool, scheduler_config: SchedulerConfig) -> None:
    """Execute SQL refresh jobs discovered from the configured directory."""

    sql_directory = Path(scheduler_config.sql_jobs_directory)
    definitions = load_sql_jobs(sql_directory)
    if not definitions:
        LOGGER.debug("No SQL jobs discovered in directory: %s", sql_directory)
        return

    async with pool.acquire() as connection:
        for definition in definitions:
            result = await execute_sql_job(connection, definition)
            try:
                await record_job_result(connection, result)
            except Exception:  # pragma: no cover - persistence failure
                LOGGER.exception(
                    "Failed to persist analytics job metrics for %s",
                    definition.name,
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
    "run_sql_refresh_jobs",
    "run_partition_procedure",
    "run_retention_procedure",
]
