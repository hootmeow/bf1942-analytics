"""Lightweight SQL migration tooling for analytics definitions."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, List

from asyncpg import Connection
from asyncpg.pool import Pool

LOGGER = logging.getLogger(__name__)

MIGRATIONS_TABLE = "analytics_sql_migrations"


def _default_sql_directory() -> Path:
    return Path(__file__).resolve().parent.parent / "sql" / "analytics"


def _split_sql_statements(sql: str) -> List[str]:
    """Split SQL content into executable statements.

    The helper intentionally ignores SQL comments and expects statements to be
    terminated with a semicolon at the end of a line."""

    statements: List[str] = []
    buffer: List[str] = []

    for raw_line in sql.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        if stripped.startswith("--"):
            # Metadata and documentation comments are ignored by the executor.
            continue

        buffer.append(raw_line)
        if stripped.endswith(";"):
            statement = "\n".join(buffer).rstrip().rstrip(";")
            if statement:
                statements.append(statement)
            buffer = []

    # Handle final statement without a terminating semicolon.
    if buffer:
        statement = "\n".join(buffer).strip()
        if statement:
            statements.append(statement)

    return statements


async def _ensure_migrations_table(connection: Connection) -> None:
    await connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
            filename TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )


async def _run_statements(connection: Connection, statements: Iterable[str]) -> None:
    for statement in statements:
        stripped = statement.strip()
        if not stripped:
            continue
        LOGGER.debug("Executing SQL statement: %s", stripped.split("\n", 1)[0])
        await connection.execute(statement)


async def apply_sql_directory(pool: Pool, directory: Path | None = None) -> None:
    """Apply SQL files in the given directory in lexicographical order."""

    sql_dir = directory or _default_sql_directory()
    if not sql_dir.exists():
        LOGGER.warning("SQL directory does not exist: %s", sql_dir)
        return

    files = sorted(path for path in sql_dir.iterdir() if path.suffix == ".sql")
    if not files:
        LOGGER.info("No SQL files discovered in directory: %s", sql_dir)
        return

    async with pool.acquire() as connection:
        await _ensure_migrations_table(connection)
        for path in files:
            filename = path.name
            already_applied = await connection.fetchval(
                f"SELECT 1 FROM {MIGRATIONS_TABLE} WHERE filename = $1",
                filename,
            )
            if already_applied:
                LOGGER.debug("Skipping previously applied SQL file: %s", filename)
                continue

            LOGGER.info("Applying SQL file: %s", filename)
            sql_text = path.read_text()
            statements = _split_sql_statements(sql_text)
            async with connection.transaction():
                await _run_statements(connection, statements)
                await connection.execute(
                    f"INSERT INTO {MIGRATIONS_TABLE} (filename) VALUES ($1)",
                    filename,
                )


__all__ = ["apply_sql_directory"]
