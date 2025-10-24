-- @name server_uptime
-- @type materialized_view
-- @object mv_server_uptime
-- @description Hourly uptime metrics derived from server_snapshots samples.
-- @indexes CREATE INDEX IF NOT EXISTS idx_mv_server_uptime_server_hour ON mv_server_uptime (server_id, hour_bucket);
-- @refresh_sql REFRESH MATERIALIZED VIEW CONCURRENTLY mv_server_uptime;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_server_uptime AS
WITH snapshots AS (
    SELECT
        server_id,
        snapshot_time,
        status,
        LAG(snapshot_time) OVER (PARTITION BY server_id ORDER BY snapshot_time) AS previous_snapshot
    FROM server_snapshots
)
SELECT
    server_id,
    DATE_TRUNC('hour', snapshot_time) AS hour_bucket,
    COUNT(*) FILTER (WHERE status = 'online') AS online_samples,
    COUNT(*) FILTER (WHERE status <> 'online') AS offline_samples,
    SUM(
        EXTRACT(EPOCH FROM (snapshot_time - previous_snapshot))
    ) FILTER (
        WHERE status = 'online' AND previous_snapshot IS NOT NULL
    )::BIGINT AS online_seconds
FROM snapshots
GROUP BY server_id, DATE_TRUNC('hour', snapshot_time);

CREATE INDEX IF NOT EXISTS idx_mv_server_uptime_server_hour
    ON mv_server_uptime (server_id, hour_bucket);
