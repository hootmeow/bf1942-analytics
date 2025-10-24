-- @name map_mod_trends
-- @type materialized_view
-- @object mv_map_mod_trends
-- @description Daily map and mod popularity metrics with player counts.
-- @indexes CREATE INDEX IF NOT EXISTS idx_mv_map_mod_trends_bucket ON mv_map_mod_trends (day_bucket, map_name, mod_name);
-- @refresh_sql REFRESH MATERIALIZED VIEW CONCURRENTLY mv_map_mod_trends;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_map_mod_trends AS
SELECT
    DATE_TRUNC('day', (to_jsonb(ss) ->> 'snapshot_time')::TIMESTAMPTZ) AS day_bucket,
    COALESCE(to_jsonb(ss) ->> 'map_name', to_jsonb(ss) ->> 'map', 'unknown') AS map_name,
    COALESCE(to_jsonb(ss) ->> 'mod_name', to_jsonb(ss) ->> 'mod', 'unknown') AS mod_name,
    COUNT(*) AS samples,
    AVG((to_jsonb(ss) ->> 'player_count')::NUMERIC) AS average_players,
    MAX((to_jsonb(ss) ->> 'player_count')::INT) AS peak_players
FROM server_snapshots ss
GROUP BY 1, 2, 3;

CREATE INDEX IF NOT EXISTS idx_mv_map_mod_trends_bucket
    ON mv_map_mod_trends (day_bucket, map_name, mod_name);
