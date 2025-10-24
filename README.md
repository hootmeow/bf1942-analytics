# BF1942 Analytics Engine

This repository contains the standalone analytics worker and API that extends the
`bf1942-ingest` pipeline. It is designed to keep heavy reporting jobs out of the
ingest loop by delegating PostgreSQL refreshes, retention routines, and derived
metrics to a dedicated FastAPI service backed by `asyncpg` and APScheduler.

The analytics application is SQL-first: every materialized view or roll-up that
feeds the player, server, and global dashboards is versioned in
[`sql/analytics/`](sql/analytics/), while the Python service only coordinates
refreshes, runs maintenance procedures, and exposes REST endpoints.

---

## Table of contents

1. [Server prerequisites](#server-prerequisites)
2. [Clone the repository](#clone-the-repository)
3. [Create the Python environment](#create-the-python-environment)
4. [Provision PostgreSQL](#provision-postgresql)
5. [Configure the analytics app](#configure-the-analytics-app)
6. [Prime the analytics schema](#prime-the-analytics-schema)
7. [Run the service](#run-the-service)
8. [Background execution with systemd](#background-execution-with-systemd)
9. [Verifying the API](#verifying-the-api)
10. [Maintaining SQL analytics jobs](#maintaining-sql-analytics-jobs)

---

## Server prerequisites

The instructions below target Ubuntu **24.04 LTS** (the latest 24.x release at
the time of writing). Adjust package names if you are using a different
distribution.

Install the base toolchain, Python runtime, and PostgreSQL client headers:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip build-essential libpq-dev \
    postgresql postgresql-contrib git
```

> **Why the extra packages?**
> - `python3-venv` allows you to create an isolated virtual environment for
>   the analytics application.
> - `libpq-dev` and `build-essential` ensure that the `asyncpg` wheel can build
>   if precompiled wheels are unavailable for your platform.
> - `postgresql` installs the server locally (skip if you use a managed
>   database).

---

## Clone the repository

```bash
cd /opt
sudo git clone https://github.com/your-org/bf1942-analytics.git
sudo chown -R "$USER":"$USER" bf1942-analytics
cd bf1942-analytics
```

If you maintain the ingest engine and analytics engine in the same host, ensure
both repositories live under a shared parent directory for easier configuration
management.

---

## Create the Python environment

Create and activate a dedicated virtual environment (adjust the path if you
prefer a different location):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

The dependency footprint is intentionally small:

- `asyncpg` – asynchronous PostgreSQL driver
- `fastapi` – API framework used by `analytics_app.main`
- `uvicorn[standard]` – ASGI server for local and production deployments
- `apscheduler` – lightweight scheduler that drives refresh and maintenance jobs

> **Tip:** When activating the environment automatically for your user, append
> `source /opt/bf1942-analytics/.venv/bin/activate` to your shell profile.

---

## Provision PostgreSQL

The analytics engine **reuses** the database provisioned by the
`bf1942-ingest` service. Ensure the ingest stack is installed and has already
created the `bf1942_db` database (or update the name in the commands below).

With PostgreSQL installed locally, create a role for the analytics service and
grant it the permissions required to refresh materialized views and execute
maintenance procedures:

```bash
sudo -u postgres psql <<'SQL'
DO
$$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'bf1942_analytics') THEN
        CREATE ROLE bf1942_analytics WITH LOGIN PASSWORD 'change-me';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE bf1942_db TO bf1942_analytics;
\c bf1942_db
GRANT USAGE ON SCHEMA public TO bf1942_analytics;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO bf1942_analytics;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO bf1942_analytics;
SQL
```

If you use a managed PostgreSQL instance, create the role through your provider
and apply the same grants.

Record the connection details (`host`, `port`, `database`, `user`, `password`) –
you will reference them in the configuration step.

---

## Configure the analytics app

The analytics service is configured via environment variables consumed by
[`analytics_app.config.get_config`](analytics_app/config.py). Create an
`.env` file in the project root (or use systemd `Environment=` directives) with
values tailored to your infrastructure:

```bash
cat <<'ENV' > .env
# Database connection
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=bf1942_db
DB_USER=bf1942_analytics
DB_PASSWORD=change-me
DB_POOL_MIN_SIZE=2
DB_POOL_MAX_SIZE=10

# Scheduler tuning (seconds)
REFRESH_INTERVAL_SECONDS=300
RETENTION_INTERVAL_SECONDS=3600
PARTITION_INTERVAL_SECONDS=86400

# Optional: override which SQL jobs to refresh automatically
# ANALYTICS_VIEWS_TO_REFRESH=mv_player_summaries,mv_server_population_trends

# Optional: stored procedures to invoke for retention/partition jobs
# RETENTION_PROCEDURE=prune_old_sessions
# PARTITION_PROCEDURE=refresh_server_partitions

LOG_LEVEL=INFO
ENV
```

When running the service manually, export these values in your shell or rely on
a process manager (systemd, Docker, etc.) to load them.

---

## Prime the analytics schema

All materialized views, helper tables, and job metadata live under
[`sql/analytics/`](sql/analytics/). On startup the application calls
[`analytics_app.migrations.apply_sql_directory`](analytics_app/migrations.py) to
apply any new SQL files in lexicographical order.

For a manual bootstrap (useful when seeding a fresh database from an admin
shell), activate the virtual environment and run the small helper script below.
The configuration loader now reads values from `.env` automatically, so there is
no need to export each variable by hand:

```bash
source .venv/bin/activate
python - <<'PY'
import asyncio
from analytics_app.config import get_config
from analytics_app.db import init_db_pool, close_db_pool
from analytics_app.migrations import apply_sql_directory

async def main():
    config = get_config()
    pool = await init_db_pool(config)
    try:
        await apply_sql_directory(pool)
    finally:
        await close_db_pool()

asyncio.run(main())
PY
```

This ensures tables like `analytics_job_runs` and materialized views such as
`mv_player_advanced_metrics`, `mv_server_population_trends`, and
`mv_global_activity_rollups` exist before the scheduler issues refreshes.

---

## Run the service

With the environment configured and migrations applied, launch the FastAPI app
using Uvicorn:

```bash
source .venv/bin/activate
export $(grep -v '^#' .env | xargs)
uvicorn analytics_app.main:app --host 0.0.0.0 --port 8000
```

During startup the service will:

1. Open an `asyncpg` pool using `analytics_app.db.init_db_pool`.
2. Apply any pending SQL files through `analytics_app.migrations.apply_sql_directory`.
3. Schedule recurring jobs (refresh materialized views, run retention/partition
   procedures) via `analytics_app.scheduler.SchedulerManager`.

Keep the process attached to your terminal for initial verification. Once you
confirm the endpoints work, consider running it in the background with systemd.

---

## Background execution with systemd

Create a dedicated service unit that activates the virtual environment, loads
the `.env` file, and supervises Uvicorn. Replace `/opt/bf1942-analytics` with
the actual clone path.

```bash
sudo tee /etc/systemd/system/bf1942-analytics.service > /dev/null <<'UNIT'
[Unit]
Description=Battlefield 1942 Analytics API
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/bf1942-analytics
EnvironmentFile=/opt/bf1942-analytics/.env
ExecStart=/opt/bf1942-analytics/.venv/bin/uvicorn analytics_app.main:app \
    --host 0.0.0.0 --port 8000
Restart=on-failure
User=www-data
Group=www-data

[Install]
WantedBy=multi-user.target
UNIT
```

Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now bf1942-analytics.service
sudo systemctl status bf1942-analytics.service
```

Logs stream through the journal. To inspect them:

```bash
journalctl -u bf1942-analytics.service -f
```

Adjust the `User`/`Group` directives to match the service account you prefer.
Grant that account database access and read permissions on the repository.

---

## Verifying the API

Once the service is running, confirm the health and analytics endpoints respond
correctly:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/metrics
curl http://localhost:8000/analytics/players/metrics?limit=5
curl http://localhost:8000/analytics/servers/population?server_id=your-server-id
curl http://localhost:8000/analytics/global/activity
```

The `/metrics` route exposes scheduler state (job IDs, next run times) and the
connection pool configuration, which is useful when tuning job intervals.

If you observe `503` errors, verify that the database credentials are correct
and the target materialized views exist. The app automatically re-applies SQL
files on restart, so adding a new analytics view is as simple as dropping a new
`NNN_description.sql` file under `sql/analytics/` and redeploying.

---

## Maintaining SQL analytics jobs

Each SQL file in [`sql/analytics/`](sql/analytics/) carries a metadata header
that the scheduler reads via `analytics_app.sql_jobs.load_sql_jobs`. Key fields:

- `@name` – Logical identifier shown in the `analytics_job_runs` table.
- `@type` – Usually `materialized_view`, but can be a custom label (e.g.
  `aggregation`).
- `@object` – Database object that the job refreshes.
- `@refresh_sql` – Optional override; defaults to `REFRESH MATERIALIZED VIEW
  CONCURRENTLY <object>` when omitted for materialized views.
- `@description` – Free-form documentation, supports multi-line values using
  `-- |` prefixes.

When you add or modify a SQL definition:

1. Create a new file with a monotonically increasing prefix (e.g.
   `008_new_metric.sql`).
2. Commit the change to version control.
3. Restart the analytics service or call the helper script from the
   "[Prime the analytics schema](#prime-the-analytics-schema)" section. The
   migrations helper tracks applied filenames in the `analytics_sql_migrations`
   table to avoid re-running the same file twice.

The scheduler writes execution metrics into `analytics_job_runs`, including
success/failure flags, durations, row counts, and the originating SQL file.
Leverage this table to build internal dashboards or alerts around job health.

---

## Next steps

- Integrate the analytics API with your frontend to render player, server, and
  global dashboards.
- Expand the SQL library with additional derived metrics or retention routines.
- Wire alerting (Prometheus, Grafana, PagerDuty) around the `/metrics` endpoint
  or the `analytics_job_runs` history to catch regressions early.

Happy fragging!
