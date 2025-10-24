"""Scheduler management using APScheduler."""
from __future__ import annotations

import logging
from typing import Any, Dict

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from .config import AppConfig
from .jobs import (
    refresh_materialized_views,
    run_partition_procedure,
    run_retention_procedure,
)

LOGGER = logging.getLogger(__name__)


class SchedulerManager:
    """Wrapper that manages the lifecycle of the APScheduler instance."""

    def __init__(self, config: AppConfig, pool) -> None:
        self._config = config
        self._pool = pool
        self._scheduler = AsyncIOScheduler()

    @property
    def scheduler(self) -> AsyncIOScheduler:
        return self._scheduler

    def start(self) -> None:
        """Start the scheduler with configured jobs."""

        scheduler_conf = self._config.scheduler
        LOGGER.info("Starting scheduler with configuration: %s", scheduler_conf)

        self._scheduler.add_job(
            refresh_materialized_views,
            trigger=IntervalTrigger(seconds=scheduler_conf.refresh_interval_seconds),
            args=[self._pool, scheduler_conf.views_to_refresh],
            id="refresh_materialized_views",
            replace_existing=True,
        )

        self._scheduler.add_job(
            run_retention_procedure,
            trigger=IntervalTrigger(seconds=scheduler_conf.retention_interval_seconds),
            args=[self._pool, scheduler_conf],
            id="retention_maintenance",
            replace_existing=True,
        )

        self._scheduler.add_job(
            run_partition_procedure,
            trigger=IntervalTrigger(seconds=scheduler_conf.partition_interval_seconds),
            args=[self._pool, scheduler_conf],
            id="partition_maintenance",
            replace_existing=True,
        )

        self._scheduler.start()

    def shutdown(self) -> None:
        """Shutdown the scheduler."""

        if self._scheduler.running:
            LOGGER.info("Shutting down scheduler")
            self._scheduler.shutdown(wait=False)

    def snapshot(self) -> Dict[str, Any]:
        """Return a snapshot of scheduler jobs for diagnostics."""

        jobs = [
            {
                "id": job.id,
                "next_run_time": job.next_run_time.isoformat()
                if job.next_run_time
                else None,
                "trigger": str(job.trigger),
            }
            for job in self._scheduler.get_jobs()
        ]
        return {"jobs": jobs}


__all__ = ["SchedulerManager"]
