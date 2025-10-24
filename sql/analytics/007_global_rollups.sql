-- @name global_activity_rollups
-- @type materialized_view
-- @object mv_global_activity_rollups
-- @description Live global player, map, mod, and regional rollups derived from the latest server snapshots.
-- @indexes CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_global_activity_rollups_singleton ON mv_global_activity_rollups ((true));
-- @refresh_sql REFRESH MATERIALIZED VIEW CONCURRENTLY mv_global_activity_rollups;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_global_activity_rollups AS
WITH latest_snapshots AS (
    SELECT DISTINCT ON (ss.server_id)
        ss.server_id,
        (to_jsonb(ss) ->> 'snapshot_time')::TIMESTAMPTZ AS snapshot_time,
        COALESCE((to_jsonb(ss) ->> 'player_count')::INT, 0) AS player_count,
        COALESCE(to_jsonb(ss) ->> 'map_name', to_jsonb(ss) ->> 'map', 'unknown') AS map_name,
        COALESCE(to_jsonb(ss) ->> 'mod_name', to_jsonb(ss) ->> 'mod', 'unknown') AS mod_name,
        COALESCE(
            to_jsonb(ss) ->> 'country',
            to_jsonb(ss) ->> 'country_code',
            to_jsonb(ss) ->> 'region',
            to_jsonb(ss) ->> 'geo_country',
            'unknown'
        ) AS region,
        to_jsonb(ss) ->> 'ip' AS ip_address
    FROM server_snapshots ss
    ORDER BY ss.server_id, (to_jsonb(ss) ->> 'snapshot_time')::TIMESTAMPTZ DESC
),
global_counts AS (
    SELECT
        SUM(player_count) AS live_players,
        AVG(player_count)::NUMERIC AS average_players_per_server,
        MAX(player_count) AS peak_server_players
    FROM latest_snapshots
),
map_rankings AS (
    SELECT
        map_name,
        SUM(player_count) AS concurrent_players,
        RANK() OVER (ORDER BY SUM(player_count) DESC NULLS LAST) AS map_rank
    FROM latest_snapshots
    GROUP BY map_name
),
mod_rankings AS (
    SELECT
        mod_name,
        SUM(player_count) AS concurrent_players,
        RANK() OVER (ORDER BY SUM(player_count) DESC NULLS LAST) AS mod_rank
    FROM latest_snapshots
    GROUP BY mod_name
),
regional_activity AS (
    SELECT
        region,
        SUM(player_count) AS concurrent_players
    FROM latest_snapshots
    GROUP BY region
)
SELECT
    gc.live_players,
    gc.average_players_per_server,
    gc.peak_server_players,
    (SELECT jsonb_agg(jsonb_build_object('map', map_name, 'players', concurrent_players, 'rank', map_rank) ORDER BY map_rank)
     FROM map_rankings) AS live_map_leaderboard,
    (SELECT jsonb_agg(jsonb_build_object('mod', mod_name, 'players', concurrent_players, 'rank', mod_rank) ORDER BY mod_rank)
     FROM mod_rankings) AS live_mod_leaderboard,
    (SELECT jsonb_object_agg(region, concurrent_players) FROM regional_activity) AS regional_player_counts
FROM global_counts gc;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_global_activity_rollups_singleton
    ON mv_global_activity_rollups ((true));

-- @name global_daily_trends
-- @type materialized_view
-- @object mv_global_daily_trends
-- @description Historical daily player, map, and mod trends with rolling windows.
-- @indexes CREATE INDEX IF NOT EXISTS idx_mv_global_daily_trends_day ON mv_global_daily_trends (day_bucket);
-- @refresh_sql REFRESH MATERIALIZED VIEW CONCURRENTLY mv_global_daily_trends;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_global_daily_trends AS
WITH daily_snapshots AS (
    SELECT
        DATE_TRUNC('day', (to_jsonb(ss) ->> 'snapshot_time')::TIMESTAMPTZ) AS day_bucket,
        SUM(COALESCE((to_jsonb(ss) ->> 'player_count')::INT, 0)) AS total_players,
        AVG(COALESCE((to_jsonb(ss) ->> 'player_count')::INT, 0))::NUMERIC AS average_players,
        MAX(COALESCE((to_jsonb(ss) ->> 'player_count')::INT, 0)) AS peak_players
    FROM server_snapshots ss
    GROUP BY 1
),
ranked_days AS (
    SELECT
        ds.*,
        SUM(total_players) OVER (
            ORDER BY day_bucket
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS seven_day_players,
        AVG(average_players) OVER (
            ORDER BY day_bucket
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_average_players
    FROM daily_snapshots ds
),
map_popularity AS (
    SELECT
        DATE_TRUNC('day', (to_jsonb(ss) ->> 'snapshot_time')::TIMESTAMPTZ) AS day_bucket,
        COALESCE(to_jsonb(ss) ->> 'map_name', to_jsonb(ss) ->> 'map', 'unknown') AS map_name,
        SUM(COALESCE((to_jsonb(ss) ->> 'player_count')::INT, 0)) AS player_minutes
    FROM server_snapshots ss
    GROUP BY 1, 2
),
map_rankings AS (
    SELECT
        mp.day_bucket,
        mp.map_name,
        mp.player_minutes,
        RANK() OVER (
            PARTITION BY mp.day_bucket
            ORDER BY mp.player_minutes DESC NULLS LAST
        ) AS map_rank
    FROM map_popularity mp
),
mod_popularity AS (
    SELECT
        DATE_TRUNC('day', (to_jsonb(ss) ->> 'snapshot_time')::TIMESTAMPTZ) AS day_bucket,
        COALESCE(to_jsonb(ss) ->> 'mod_name', to_jsonb(ss) ->> 'mod', 'unknown') AS mod_name,
        SUM(COALESCE((to_jsonb(ss) ->> 'player_count')::INT, 0)) AS player_minutes
    FROM server_snapshots ss
    GROUP BY 1, 2
),
mod_rankings AS (
    SELECT
        mp.day_bucket,
        mp.mod_name,
        mp.player_minutes,
        RANK() OVER (
            PARTITION BY mp.day_bucket
            ORDER BY mp.player_minutes DESC NULLS LAST
        ) AS mod_rank
    FROM mod_popularity mp
)
SELECT
    rd.day_bucket,
    rd.total_players,
    rd.average_players,
    rd.peak_players,
    rd.seven_day_players,
    rd.rolling_average_players,
    (SELECT jsonb_agg(jsonb_build_object('map', map_name, 'player_minutes', player_minutes, 'rank', map_rank) ORDER BY map_rank)
     FROM map_rankings mr
     WHERE mr.day_bucket = rd.day_bucket) AS map_leaderboard,
    (SELECT jsonb_agg(jsonb_build_object('mod', mod_name, 'player_minutes', player_minutes, 'rank', mod_rank) ORDER BY mod_rank)
     FROM mod_rankings mm
     WHERE mm.day_bucket = rd.day_bucket) AS mod_leaderboard
FROM ranked_days rd;

CREATE INDEX IF NOT EXISTS idx_mv_global_daily_trends_day
    ON mv_global_daily_trends (day_bucket);

-- @name global_region_activity
-- @type materialized_view
-- @object mv_global_region_activity
-- @description Regional activity aggregates keyed by inferred server country codes.
-- @indexes CREATE INDEX IF NOT EXISTS idx_mv_global_region_activity_region_day ON mv_global_region_activity (region, day_bucket);
-- @refresh_sql REFRESH MATERIALIZED VIEW CONCURRENTLY mv_global_region_activity;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_global_region_activity AS
WITH region_snapshots AS (
    SELECT
        DATE_TRUNC('day', (to_jsonb(ss) ->> 'snapshot_time')::TIMESTAMPTZ) AS day_bucket,
        COALESCE(
            to_jsonb(ss) ->> 'country',
            to_jsonb(ss) ->> 'country_code',
            to_jsonb(ss) ->> 'region',
            to_jsonb(ss) ->> 'geo_country',
            'unknown'
        ) AS region,
        COALESCE((to_jsonb(ss) ->> 'player_count')::INT, 0) AS player_count
    FROM server_snapshots ss
),
region_totals AS (
    SELECT
        day_bucket,
        region,
        SUM(player_count) AS total_players,
        AVG(player_count)::NUMERIC AS average_players,
        MAX(player_count) AS peak_players
    FROM region_snapshots
    GROUP BY day_bucket, region
)
SELECT
    rt.day_bucket,
    rt.region,
    rt.total_players,
    rt.average_players,
    rt.peak_players,
    SUM(rt.total_players) OVER (
        PARTITION BY rt.region
        ORDER BY rt.day_bucket
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_weekly_players
FROM region_totals rt;

CREATE INDEX IF NOT EXISTS idx_mv_global_region_activity_region_day
    ON mv_global_region_activity (region, day_bucket);
