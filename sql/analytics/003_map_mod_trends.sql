-- @name map_mod_trends
-- @type materialized_view
-- @object mv_map_mod_trends
-- @description Daily map and mod popularity metrics with player counts.
-- @indexes CREATE INDEX IF NOT EXISTS idx_mv_map_mod_trends_bucket ON mv_map_mod_trends (day_bucket, map_name, mod_name);
-- @refresh_sql REFRESH MATERIALIZED VIEW CONCURRENTLY mv_map_mod_trends;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_map_mod_trends AS
SELECT
    DATE_TRUNC('day', snapshot_time) AS day_bucket,
    COALESCE(map_name, payload ->> 'map') AS map_name,
    COALESCE(mod_name, payload ->> 'mod') AS mod_name,
    COUNT(*) AS samples,
    AVG((payload ->> 'player_count')::NUMERIC) AS average_players,
    MAX((payload ->> 'player_count')::INT) AS peak_players
FROM server_snapshots
GROUP BY DATE_TRUNC('day', snapshot_time), COALESCE(map_name, payload ->> 'map'), COALESCE(mod_name, payload ->> 'mod');

CREATE INDEX IF NOT EXISTS idx_mv_map_mod_trends_bucket
    ON mv_map_mod_trends (day_bucket, map_name, mod_name);
