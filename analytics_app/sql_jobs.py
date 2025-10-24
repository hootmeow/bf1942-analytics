"""Utilities for loading and executing analytics SQL job definitions."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

from asyncpg.connection import Connection
from datetime import datetime, timezone

LOGGER = logging.getLogger(__name__)

DEFAULT_SQL_DIRECTORY = Path(__file__).resolve().parent.parent / "sql" / "analytics"


@dataclass(frozen=True)
class SqlJobDefinition:
    """Structured metadata extracted from a SQL definition file."""

    name: str
    job_type: str
    object_name: str
    refresh_sql: str
    source_file: Path
    description: str | None


@dataclass(frozen=True)
class SqlJobResult:
    """Represents execution metrics for a SQL job invocation."""

    definition: SqlJobDefinition
    success: bool
    duration_ms: float
    rows_affected: int | None
    message: str | None
    started_at: datetime
    finished_at: datetime


def _parse_metadata(lines: Iterable[str]) -> Dict[str, str]:
    metadata: Dict[str, str] = {}
    current_key: str | None = None
    for raw_line in lines:
        line = raw_line.strip()
        if not line.startswith("--"):
            break
        content = line[2:].strip()
        if not content:
            continue
        if content.startswith("@"):
            if " " in content:
                key, value = content[1:].split(" ", 1)
            else:
                key, value = content[1:], ""
            metadata[key] = value.strip()
            current_key = key
            continue
        if content.startswith("|") and current_key:
            metadata[current_key] = metadata.get(current_key, "") + "\n" + content[1:].lstrip()
    return metadata


def load_sql_jobs(directory: Path | None = None) -> List[SqlJobDefinition]:
    """Discover SQL job definitions from the configured directory."""

    sql_dir = directory or DEFAULT_SQL_DIRECTORY
    if not sql_dir.exists():
        LOGGER.warning("SQL job directory does not exist: %s", sql_dir)
        return []

    definitions: List[SqlJobDefinition] = []
    for path in sorted(sql_dir.glob("*.sql")):
        text = path.read_text()
        metadata = _parse_metadata(text.splitlines())
        name = metadata.get("name")
        job_type = metadata.get("type")
        object_name = metadata.get("object")
        refresh_sql = metadata.get("refresh_sql")
        description = metadata.get("description") or None

        if not all([name, job_type, object_name]):
            LOGGER.debug("Skipping SQL file without required metadata: %s", path)
            continue

        if job_type == "materialized_view" and not refresh_sql:
            refresh_sql = f"REFRESH MATERIALIZED VIEW CONCURRENTLY {object_name}"
        elif not refresh_sql:
            LOGGER.debug("Skipping SQL file without refresh SQL metadata: %s", path)
            continue

        definitions.append(
            SqlJobDefinition(
                name=name,
                job_type=job_type,
                object_name=object_name,
                refresh_sql=refresh_sql,
                source_file=path,
                description=description,
            )
        )

    return definitions


async def execute_sql_job(connection: Connection, definition: SqlJobDefinition) -> SqlJobResult:
    """Execute the refresh statement for the given SQL job."""

    start_time = time.perf_counter()
    started_at = datetime.now(timezone.utc)
    rows_affected: int | None = None
    message: str | None = None
    success = True

    LOGGER.info(
        "Executing analytics SQL job '%s' (%s)",
        definition.name,
        definition.job_type,
    )

    try:
        result = await connection.execute(definition.refresh_sql)
        message = result
        if result.startswith("INSERT"):
            try:
                rows_affected = int(result.split()[-1])
            except (ValueError, IndexError):
                rows_affected = None
    except Exception as exc:  # pragma: no cover - requires database failure
        LOGGER.exception("SQL job failed: %s", definition.name)
        success = False
        message = str(exc)

    duration_ms = (time.perf_counter() - start_time) * 1000
    finished_at = datetime.now(timezone.utc)

    return SqlJobResult(
        definition=definition,
        success=success,
        duration_ms=duration_ms,
        rows_affected=rows_affected,
        message=message,
        started_at=started_at,
        finished_at=finished_at,
    )


async def record_job_result(connection: Connection, result: SqlJobResult) -> None:
    """Persist execution metrics to analytics_job_runs."""

    await connection.execute(
        """
        INSERT INTO analytics_job_runs (
            job_name,
            job_type,
            object_name,
            source_file,
            refresh_sql,
            started_at,
            finished_at,
            duration_ms,
            rows_affected,
            success,
            message
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """,
        result.definition.name,
        result.definition.job_type,
        result.definition.object_name,
        str(result.definition.source_file),
        result.definition.refresh_sql,
        result.started_at,
        result.finished_at,
        result.duration_ms,
        result.rows_affected,
        result.success,
        result.message,
    )


__all__ = [
    "SqlJobDefinition",
    "SqlJobResult",
    "execute_sql_job",
    "load_sql_jobs",
    "record_job_result",
]
