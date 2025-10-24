"""Application configuration utilities."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from functools import lru_cache
from typing import List


@dataclass(frozen=True)
class DatabaseConfig:
    """Configuration for the asyncpg connection pool."""

    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = "postgres"
    database: str = "postgres"
    min_size: int = 1
    max_size: int = 10

    @property
    def dsn(self) -> str:
        """Return a DSN string assembled from the configuration."""

        return (
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/"
            f"{self.database}"
        )


@dataclass(frozen=True)
class SchedulerConfig:
    """Configuration for APScheduler jobs."""

    refresh_interval_seconds: int = 300
    retention_interval_seconds: int = 3600
    partition_interval_seconds: int = 86400
    views_to_refresh: List[str] = field(
        default_factory=lambda: ["mv_player_advanced_stats"]
    )
    retention_procedure: str | None = None
    partition_procedure: str | None = None
    sql_jobs_directory: str = "sql/analytics"


@dataclass(frozen=True)
class AppConfig:
    """Top-level configuration for the analytics application."""

    log_level: str = "INFO"
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)


def _int_from_env(key: str, default: int) -> int:
    value = os.getenv(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:  # pragma: no cover - defensive branch
        raise ValueError(f"Environment variable {key} must be an integer") from exc


def _list_from_env(key: str, default: List[str]) -> List[str]:
    value = os.getenv(key)
    if value is None:
        return default
    values = [item.strip() for item in value.split(",") if item.strip()]
    return values or default


@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    """Load configuration from environment variables with defaults."""

    default_db = DatabaseConfig()
    database = DatabaseConfig(
        host=os.getenv("DB_HOST", default_db.host),
        port=_int_from_env("DB_PORT", default_db.port),
        user=os.getenv("DB_USER", default_db.user),
        password=os.getenv("DB_PASSWORD", default_db.password),
        database=os.getenv("DB_NAME", default_db.database),
        min_size=_int_from_env("DB_POOL_MIN_SIZE", default_db.min_size),
        max_size=_int_from_env("DB_POOL_MAX_SIZE", default_db.max_size),
    )

    default_scheduler = SchedulerConfig()
    scheduler = SchedulerConfig(
        refresh_interval_seconds=_int_from_env(
            "REFRESH_INTERVAL_SECONDS", default_scheduler.refresh_interval_seconds
        ),
        retention_interval_seconds=_int_from_env(
            "RETENTION_INTERVAL_SECONDS", default_scheduler.retention_interval_seconds
        ),
        partition_interval_seconds=_int_from_env(
            "PARTITION_INTERVAL_SECONDS", default_scheduler.partition_interval_seconds
        ),
        views_to_refresh=_list_from_env(
            "ANALYTICS_VIEWS_TO_REFRESH", default_scheduler.views_to_refresh
        ),
        retention_procedure=os.getenv(
            "RETENTION_PROCEDURE", default_scheduler.retention_procedure or ""
        )
        or None,
        partition_procedure=os.getenv(
            "PARTITION_PROCEDURE", default_scheduler.partition_procedure or ""
        )
        or None,
        sql_jobs_directory=os.getenv(
            "ANALYTICS_SQL_DIRECTORY", default_scheduler.sql_jobs_directory
        ),
    )

    default_app = AppConfig()
    config = AppConfig(
        log_level=os.getenv("LOG_LEVEL", default_app.log_level),
        database=database,
        scheduler=scheduler,
    )

    logging.basicConfig(level=config.log_level.upper())
    logging.getLogger(__name__).debug("Configuration loaded: %s", config)

    return config


__all__ = [
    "AppConfig",
    "DatabaseConfig",
    "SchedulerConfig",
    "get_config",
]
