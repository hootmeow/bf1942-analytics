-- @name server_snapshot_rollups
-- @type incremental_insert
-- @object server_snapshot_rollups
-- @description Hourly JSONB rollups persisted for downstream APIs.
-- @indexes CREATE UNIQUE INDEX IF NOT EXISTS idx_server_snapshot_rollups_pk ON server_snapshot_rollups (bucket_start, server_id);
-- @refresh_sql INSERT INTO server_snapshot_rollups (bucket_start, bucket_end, server_id, payload, updated_at)
-- |SELECT
-- |    agg.bucket AS bucket_start,
-- |    agg.bucket + INTERVAL '1 hour' AS bucket_end,
-- |    agg.server_id,
-- |    jsonb_build_object(
-- |        'samples', agg.samples,
-- |        'average_players', agg.avg_players,
-- |        'peak_players', agg.peak_players,
-- |        'map_samples', agg.map_samples,
-- |        'mod_samples', agg.mod_samples
-- |    ) AS payload,
-- |    NOW() AS updated_at
-- |FROM (
-- |    SELECT
-- |        bucket,
-- |        server_id,
-- |        COUNT(*) AS samples,
-- |        AVG(player_count)::NUMERIC AS avg_players,
-- |        MAX(player_count) AS peak_players,
-- |        (
-- |            SELECT jsonb_object_agg(map_name, sample_count)
-- |            FROM (
-- |                SELECT map_name, COUNT(*) AS sample_count
-- |                FROM snapshots s2
-- |                WHERE s2.server_id = s1.server_id
-- |                  AND s2.bucket = s1.bucket
-- |                GROUP BY map_name
-- |            ) ms
-- |        ) AS map_samples,
-- |        (
-- |            SELECT jsonb_object_agg(mod_name, sample_count)
-- |            FROM (
-- |                SELECT mod_name, COUNT(*) AS sample_count
-- |                FROM snapshots s3
-- |                WHERE s3.server_id = s1.server_id
-- |                  AND s3.bucket = s1.bucket
-- |                GROUP BY mod_name
-- |            ) mm
-- |        ) AS mod_samples
-- |    FROM (
-- |        SELECT
-- |            DATE_TRUNC('hour', snapshot_time) AS bucket,
-- |            server_id,
-- |            COALESCE(map_name, payload ->> 'map') AS map_name,
-- |            COALESCE(mod_name, payload ->> 'mod') AS mod_name,
-- |            COALESCE((payload ->> 'player_count')::INT, 0) AS player_count
-- |        FROM server_snapshots
-- |    ) s1
-- |    GROUP BY bucket, server_id
-- |) agg
-- |WHERE agg.bucket >= COALESCE((SELECT MAX(bucket_start) FROM server_snapshot_rollups), '1970-01-01'::TIMESTAMPTZ)
-- |ON CONFLICT (bucket_start, server_id) DO UPDATE
-- |SET
-- |    payload = EXCLUDED.payload,
-- |    bucket_end = EXCLUDED.bucket_end,
-- |    updated_at = NOW();

CREATE TABLE IF NOT EXISTS server_snapshot_rollups (
    bucket_start TIMESTAMPTZ NOT NULL,
    bucket_end TIMESTAMPTZ NOT NULL,
    server_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT server_snapshot_rollups_pk PRIMARY KEY (bucket_start, server_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_server_snapshot_rollups_pk
    ON server_snapshot_rollups (bucket_start, server_id);
