-- @name server_population_trends
-- @type materialized_view
-- @object mv_server_population_trends
-- @description Hourly server population trends with rolling averages and uptime context.
-- @indexes CREATE INDEX IF NOT EXISTS idx_mv_server_population_trends_server_hour ON mv_server_population_trends (server_id, hour_bucket);
-- @refresh_sql REFRESH MATERIALIZED VIEW CONCURRENTLY mv_server_population_trends;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_server_population_trends AS
WITH raw_snapshots AS (
    SELECT
        ss.server_id,
        ss.snapshot_time,
        DATE_TRUNC('hour', ss.snapshot_time) AS hour_bucket,
        COALESCE(ss.player_count, (ss.payload ->> 'player_count')::INT, 0) AS player_count,
        COALESCE(ss.status, ss.payload ->> 'status', 'unknown') AS status
    FROM server_snapshots ss
),
hourly AS (
    SELECT
        server_id,
        hour_bucket,
        AVG(player_count)::NUMERIC AS average_players,
        MAX(player_count) AS peak_players,
        MIN(player_count) AS minimum_players,
        COUNT(*) FILTER (WHERE status = 'online') AS online_samples,
        COUNT(*) AS total_samples
    FROM raw_snapshots
    GROUP BY server_id, hour_bucket
),
rolling AS (
    SELECT
        h.*,
        AVG(average_players) OVER (
            PARTITION BY h.server_id
            ORDER BY h.hour_bucket
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_24h_avg,
        AVG(average_players) OVER (
            PARTITION BY h.server_id
            ORDER BY h.hour_bucket
            ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_avg
    FROM hourly h
),
daily_uptime AS (
    SELECT
        server_id,
        DATE_TRUNC('day', hour_bucket) AS day_bucket,
        SUM(online_samples)::NUMERIC / NULLIF(SUM(online_samples + offline_samples), 0) AS uptime_ratio
    FROM mv_server_uptime
    GROUP BY server_id, DATE_TRUNC('day', hour_bucket)
)
SELECT
    r.server_id,
    r.hour_bucket,
    r.average_players,
    r.peak_players,
    r.minimum_players,
    r.online_samples,
    r.total_samples,
    COALESCE(r.online_samples::NUMERIC / NULLIF(r.total_samples, 0), 0) AS hour_uptime_ratio,
    r.rolling_24h_avg,
    r.rolling_7d_avg,
    (r.rolling_24h_avg - r.rolling_7d_avg) AS population_trend,
    du.uptime_ratio AS daily_uptime_ratio
FROM rolling r
LEFT JOIN daily_uptime du
    ON du.server_id = r.server_id
   AND du.day_bucket = DATE_TRUNC('day', r.hour_bucket);

CREATE INDEX IF NOT EXISTS idx_mv_server_population_trends_server_hour
    ON mv_server_population_trends (server_id, hour_bucket);

-- @name server_rotation_statistics
-- @type materialized_view
-- @object mv_server_rotation_statistics
-- @description Map rotation frequencies and round pacing metrics per server.
-- @indexes CREATE INDEX IF NOT EXISTS idx_mv_server_rotation_statistics_server_map ON mv_server_rotation_statistics (server_id, map_name, mod_name);
-- @refresh_sql REFRESH MATERIALIZED VIEW CONCURRENTLY mv_server_rotation_statistics;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_server_rotation_statistics AS
WITH round_data AS (
    SELECT
        r.server_id,
        r.map_name,
        r.mod_name,
        r.started_at,
        r.ended_at,
        EXTRACT(EPOCH FROM (COALESCE(r.ended_at, NOW()) - r.started_at)) AS duration_seconds,
        LAG(r.map_name) OVER (PARTITION BY r.server_id ORDER BY r.started_at) AS previous_map,
        LEAD(r.map_name) OVER (PARTITION BY r.server_id ORDER BY r.started_at) AS next_map,
        ROW_NUMBER() OVER (PARTITION BY r.server_id ORDER BY r.started_at DESC) AS recent_round_rank
    FROM rounds r
)
SELECT
    server_id,
    map_name,
    mod_name,
    COUNT(*) AS rounds_played,
    AVG(duration_seconds)::NUMERIC / 60.0 AS average_round_minutes,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_seconds) AS median_round_seconds,
    SUM(CASE WHEN previous_map IS DISTINCT FROM map_name THEN 1 ELSE 0 END) AS map_entry_count,
    SUM(CASE WHEN next_map IS DISTINCT FROM map_name THEN 1 ELSE 0 END) AS map_exit_count,
    MAX(recent_round_rank) AS rounds_recorded
FROM round_data
GROUP BY server_id, map_name, mod_name;

CREATE INDEX IF NOT EXISTS idx_mv_server_rotation_statistics_server_map
    ON mv_server_rotation_statistics (server_id, map_name, mod_name);

-- @name server_ping_distributions
-- @type materialized_view
-- @object mv_server_ping_distributions
-- @description Ping distribution percentiles derived from player session telemetry.
-- @indexes CREATE INDEX IF NOT EXISTS idx_mv_server_ping_distributions_server_day ON mv_server_ping_distributions (server_id, day_bucket);
-- @refresh_sql REFRESH MATERIALIZED VIEW CONCURRENTLY mv_server_ping_distributions;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_server_ping_distributions AS
WITH session_timings AS (
    SELECT
        ps.server_id,
        COALESCE(
            NULLIF(to_jsonb(ps) ->> 'session_start', '')::TIMESTAMPTZ,
            NULLIF(to_jsonb(ps) ->> 'session_start_at', '')::TIMESTAMPTZ,
            NULLIF(to_jsonb(ps) ->> 'session_started_at', '')::TIMESTAMPTZ,
            NULLIF(to_jsonb(ps) ->> 'session_begin', '')::TIMESTAMPTZ,
            NULLIF(to_jsonb(ps) ->> 'session_begin_at', '')::TIMESTAMPTZ,
            NULLIF(to_jsonb(ps) ->> 'start_time', '')::TIMESTAMPTZ,
            NULLIF(to_jsonb(ps) ->> 'started_at', '')::TIMESTAMPTZ,
            NULLIF(to_jsonb(ps) ->> 'created_at', '')::TIMESTAMPTZ
        ) AS session_start,
        COALESCE(ps.average_ping_ms, ps.avg_ping_ms, ps.max_ping_ms) AS ping_sample
    FROM player_sessions ps
)
SELECT
    st.server_id,
    DATE_TRUNC('day', st.session_start) AS day_bucket,
    COUNT(*) AS samples,
    PERCENTILE_CONT(ARRAY[0.1, 0.25, 0.5, 0.75, 0.9]) WITHIN GROUP (ORDER BY st.ping_sample) AS ping_percentiles,
    AVG(st.ping_sample) AS average_ping_ms
FROM session_timings st
WHERE st.ping_sample IS NOT NULL
GROUP BY st.server_id, DATE_TRUNC('day', st.session_start);

CREATE INDEX IF NOT EXISTS idx_mv_server_ping_distributions_server_day
    ON mv_server_ping_distributions (server_id, day_bucket);

-- @name server_leaderboards
-- @type materialized_view
-- @object mv_server_leaderboards
-- @description Per-server leaderboards across kills, score, and win rates.
-- @indexes CREATE INDEX IF NOT EXISTS idx_mv_server_leaderboards_server_rank ON mv_server_leaderboards (server_id, score_rank, kill_rank);
-- @refresh_sql REFRESH MATERIALIZED VIEW CONCURRENTLY mv_server_leaderboards;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_server_leaderboards AS
WITH session_source AS (
    SELECT
        COALESCE(
            to_jsonb(ps) ->> 'player_id',
            to_jsonb(ps) ->> 'player_guid',
            to_jsonb(ps) ->> 'player_hash',
            to_jsonb(ps) ->> 'player_name'
        ) AS player_id,
        starts.session_start AS session_start_at,
        normalized.session_end_at,
        durations.session_seconds_played,
        ps.id,
        ps.server_id,
        COALESCE(
            NULLIF(to_jsonb(ps) ->> 'round_id', ''),
            NULLIF(to_jsonb(ps) ->> 'round_guid', ''),
            NULLIF(to_jsonb(ps) ->> 'round_hash', '')
        )::rounds.id%TYPE AS round_id,
        ps.team,
        ps.map_name,
        ps.mod_name,
        ps.kills,
        ps.deaths,
        ps.score,
        ps.average_ping_ms,
        ps.avg_ping_ms,
        ps.max_ping_ms
    FROM player_sessions ps
        CROSS JOIN LATERAL (
            SELECT
                COALESCE(
                    NULLIF(to_jsonb(ps) ->> 'session_start', '')::TIMESTAMPTZ,
                    NULLIF(to_jsonb(ps) ->> 'session_start_at', '')::TIMESTAMPTZ,
                    NULLIF(to_jsonb(ps) ->> 'session_started_at', '')::TIMESTAMPTZ,
                    NULLIF(to_jsonb(ps) ->> 'session_begin', '')::TIMESTAMPTZ,
                    NULLIF(to_jsonb(ps) ->> 'session_begin_at', '')::TIMESTAMPTZ,
                    NULLIF(to_jsonb(ps) ->> 'start_time', '')::TIMESTAMPTZ,
                    NULLIF(to_jsonb(ps) ->> 'started_at', '')::TIMESTAMPTZ,
                    NULLIF(to_jsonb(ps) ->> 'created_at', '')::TIMESTAMPTZ
                ) AS session_start
        ) starts
        CROSS JOIN LATERAL (
            SELECT
                COALESCE(
                    NULLIF(to_jsonb(ps) ->> 'session_end', '')::TIMESTAMPTZ,
                    NULLIF(to_jsonb(ps) ->> 'session_end_at', '')::TIMESTAMPTZ,
                    NULLIF(to_jsonb(ps) ->> 'session_finished_at', '')::TIMESTAMPTZ,
                    CASE
                        WHEN starts.session_start IS NOT NULL
                             AND NULLIF(to_jsonb(ps) ->> 'session_duration_seconds', '') IS NOT NULL THEN
                            starts.session_start
                            + MAKE_INTERVAL(secs => (to_jsonb(ps) ->> 'session_duration_seconds')::DOUBLE PRECISION)
                        WHEN starts.session_start IS NOT NULL
                             AND NULLIF(to_jsonb(ps) ->> 'duration_seconds', '') IS NOT NULL THEN
                            starts.session_start
                            + MAKE_INTERVAL(secs => (to_jsonb(ps) ->> 'duration_seconds')::DOUBLE PRECISION)
                        WHEN starts.session_start IS NOT NULL
                             AND NULLIF(to_jsonb(ps) ->> 'seconds_played', '') IS NOT NULL THEN
                            starts.session_start
                            + MAKE_INTERVAL(secs => (to_jsonb(ps) ->> 'seconds_played')::DOUBLE PRECISION)
                    END
                ) AS session_end_at
        ) normalized
        CROSS JOIN LATERAL (
            SELECT
                COALESCE(
                    GREATEST(
                        COALESCE(
                            NULLIF(to_jsonb(ps) ->> 'session_duration_seconds', '')::DOUBLE PRECISION,
                            NULLIF(to_jsonb(ps) ->> 'duration_seconds', '')::DOUBLE PRECISION,
                            NULLIF(to_jsonb(ps) ->> 'seconds_played', '')::DOUBLE PRECISION,
                            CASE
                                WHEN normalized.session_end_at IS NOT NULL
                                     AND starts.session_start IS NOT NULL THEN
                                    EXTRACT(EPOCH FROM (normalized.session_end_at - starts.session_start))
                                WHEN starts.session_start IS NOT NULL THEN
                                    EXTRACT(EPOCH FROM (NOW() - starts.session_start))
                            END
                        ),
                        60.0
                    ),
                    60.0
                ) AS session_seconds_played
        ) durations
    WHERE COALESCE(
        to_jsonb(ps) ->> 'player_id',
        to_jsonb(ps) ->> 'player_guid',
        to_jsonb(ps) ->> 'player_hash',
        to_jsonb(ps) ->> 'player_name'
    ) IS NOT NULL
),
session_metrics AS (
    SELECT
        ss.server_id,
        ss.player_id,
        ss.kills,
        ss.deaths,
        ss.score,
        ss.session_start_at,
        ss.session_end_at,
        CASE
            WHEN COALESCE(r.winning_team, ss.team) IS NULL THEN NULL
            WHEN ss.team IS NULL THEN NULL
            WHEN COALESCE(r.winning_team, ss.team) = ss.team THEN 1
            ELSE 0
        END AS win_flag
    FROM session_source ss
    LEFT JOIN rounds r ON r.id = ss.round_id::rounds.id%TYPE
),
player_totals AS (
    SELECT
        server_id,
        player_id,
        SUM(kills) AS total_kills,
        SUM(deaths) AS total_deaths,
        SUM(score) AS total_score,
        SUM(COALESCE(win_flag, 0))::NUMERIC / NULLIF(COUNT(*), 0) AS win_rate,
        MAX(COALESCE(session_end_at, session_start_at)) AS last_seen_at
    FROM session_metrics
    GROUP BY server_id, player_id
),
ranked AS (
    SELECT
        pt.*,
        RANK() OVER (PARTITION BY pt.server_id ORDER BY pt.total_score DESC) AS score_rank,
        RANK() OVER (PARTITION BY pt.server_id ORDER BY pt.total_kills DESC) AS kill_rank,
        RANK() OVER (PARTITION BY pt.server_id ORDER BY pt.win_rate DESC) AS win_rate_rank
    FROM player_totals pt
)
SELECT
    server_id,
    player_id,
    total_kills,
    total_deaths,
    total_score,
    win_rate,
    last_seen_at,
    score_rank,
    kill_rank,
    win_rate_rank
FROM ranked;

CREATE INDEX IF NOT EXISTS idx_mv_server_leaderboards_server_rank
    ON mv_server_leaderboards (server_id, score_rank, kill_rank);
