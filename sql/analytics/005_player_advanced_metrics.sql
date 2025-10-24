-- @name player_advanced_metrics
-- @type materialized_view
-- @object mv_player_advanced_metrics
-- @description Comprehensive player performance metrics including rolling form and loyalty scores.
-- @indexes CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_player_advanced_metrics_player ON mv_player_advanced_metrics (player_id);
-- @refresh_sql REFRESH MATERIALIZED VIEW CONCURRENTLY mv_player_advanced_metrics;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_player_advanced_metrics AS
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
        ps.round_id,
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
        ss.player_id,
        ss.id AS session_id,
        ss.server_id,
        ss.round_id,
        ss.session_start_at,
        ss.session_end_at,
        ss.session_seconds_played AS seconds_played,
        ss.kills,
        ss.deaths,
        ss.score,
        ss.team,
        COALESCE(r.winning_team, ss.team) AS inferred_winning_team,
        COALESCE(r.map_name, ss.map_name) AS map_name,
        COALESCE(r.mod_name, ss.mod_name) AS mod_name,
        CASE
            WHEN COALESCE(r.winning_team, ss.team) IS NULL THEN NULL
            WHEN ss.team IS NULL THEN NULL
            WHEN COALESCE(r.winning_team, ss.team) = ss.team THEN 1
            ELSE 0
        END AS win_flag,
        (ss.kills::NUMERIC / NULLIF(ss.deaths, 0)) AS session_kd,
        (ss.kills::NUMERIC / NULLIF(ss.session_seconds_played / 60.0, 0)) AS kills_per_minute,
        (ss.score::NUMERIC / NULLIF(ss.session_seconds_played / 60.0, 0)) AS score_per_minute
    FROM session_source ss
    LEFT JOIN rounds r ON r.id = ss.round_id
),
ranked_sessions AS (
    SELECT
        sm.*,
        ROW_NUMBER() OVER (PARTITION BY sm.player_id ORDER BY sm.session_start_at DESC) AS session_rank,
        AVG(sm.session_kd) OVER (
            PARTITION BY sm.player_id
            ORDER BY sm.session_start_at DESC
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS rolling_kd_ratio,
        AVG(COALESCE(sm.win_flag, 0)::NUMERIC) OVER (
            PARTITION BY sm.player_id
            ORDER BY sm.session_start_at DESC
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS rolling_win_rate,
        AVG(sm.kills_per_minute) OVER (
            PARTITION BY sm.player_id
            ORDER BY sm.session_start_at DESC
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS rolling_kpm,
        AVG(sm.score_per_minute) OVER (
            PARTITION BY sm.player_id
            ORDER BY sm.session_start_at DESC
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS rolling_spm
    FROM session_metrics sm
),
recent_form AS (
    SELECT
        player_id,
        MAX(rolling_kd_ratio) FILTER (WHERE session_rank = 1) AS recent_kd_ratio,
        MAX(rolling_win_rate) FILTER (WHERE session_rank = 1) AS recent_win_rate,
        MAX(rolling_kpm) FILTER (WHERE session_rank = 1) AS recent_kpm,
        MAX(rolling_spm) FILTER (WHERE session_rank = 1) AS recent_spm,
        MAX(session_start_at) AS last_session_start,
        MAX(session_end_at) AS last_session_end
    FROM ranked_sessions
    GROUP BY player_id
),
team_time AS (
    SELECT
        player_id,
        team,
        SUM(seconds_played) AS team_seconds
    FROM session_metrics
    GROUP BY player_id, team
),
team_rankings AS (
    SELECT
        tt.player_id,
        tt.team,
        tt.team_seconds,
        SUM(tt.team_seconds) OVER (PARTITION BY tt.player_id) AS total_seconds,
        ROW_NUMBER() OVER (
            PARTITION BY tt.player_id
            ORDER BY tt.team_seconds DESC NULLS LAST, tt.team
        ) AS team_rank
    FROM team_time tt
),
team_loyalty AS (
    SELECT
        player_id,
        MAX(team) FILTER (WHERE team_rank = 1) AS preferred_team,
        MAX(CASE WHEN team_rank = 1 THEN team_seconds::NUMERIC / NULLIF(total_seconds, 0) END) AS team_loyalty_ratio,
        jsonb_object_agg(team, CASE WHEN total_seconds = 0 THEN NULL ELSE team_seconds::NUMERIC / NULLIF(total_seconds, 0) END) FILTER (WHERE team IS NOT NULL) AS team_distribution
    FROM team_rankings
    GROUP BY player_id
),
aggregates AS (
    SELECT
        sm.player_id,
        COUNT(*) AS sessions_played,
        SUM(sm.seconds_played)::BIGINT AS total_seconds_played,
        SUM(sm.kills) AS total_kills,
        SUM(sm.deaths) AS total_deaths,
        SUM(sm.score) AS total_score,
        SUM(sm.kills)::NUMERIC / NULLIF(SUM(sm.deaths), 0) AS kill_death_ratio,
        SUM(sm.kills)::NUMERIC / NULLIF(SUM(sm.seconds_played) / 60.0, 0) AS kills_per_minute,
        SUM(sm.score)::NUMERIC / NULLIF(SUM(sm.seconds_played) / 60.0, 0) AS score_per_minute,
        SUM(COALESCE(sm.win_flag, 0))::NUMERIC / NULLIF(COUNT(sm.session_id), 0) AS win_rate,
        MAX(COALESCE(sm.session_end_at, sm.session_start_at)) AS last_seen_at
    FROM session_metrics sm
    GROUP BY sm.player_id
),
baseline AS (
    SELECT
        AVG(sm.kills_per_minute) AS average_kpm,
        COALESCE(NULLIF(STDDEV_POP(sm.kills_per_minute), 0), 1) AS stddev_kpm
    FROM session_metrics sm
)
SELECT
    agg.player_id,
    agg.sessions_played,
    agg.total_seconds_played,
    agg.total_kills,
    agg.total_deaths,
    agg.total_score,
    agg.kill_death_ratio,
    agg.kills_per_minute,
    agg.score_per_minute,
    agg.win_rate,
    recent.recent_kd_ratio,
    recent.recent_win_rate,
    recent.recent_kpm,
    recent.recent_spm,
    recent.last_session_start,
    recent.last_session_end,
    agg.last_seen_at,
    loyalty.preferred_team,
    loyalty.team_loyalty_ratio,
    loyalty.team_distribution,
    GREATEST(0, LEAST(100, ROUND(50 + 15 * ((agg.kills_per_minute - baseline.average_kpm) / baseline.stddev_kpm)))) AS aggressiveness_score
FROM aggregates agg
LEFT JOIN recent_form recent ON recent.player_id = agg.player_id
LEFT JOIN team_loyalty loyalty ON loyalty.player_id = agg.player_id
CROSS JOIN baseline;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_player_advanced_metrics_player
    ON mv_player_advanced_metrics (player_id);

-- @name player_map_mod_breakdowns
-- @type materialized_view
-- @object mv_player_map_mod_breakdowns
-- @description Per-player map and mod performance splits with tempo metrics.
-- @indexes CREATE INDEX IF NOT EXISTS idx_mv_player_map_mod_breakdowns_player_map ON mv_player_map_mod_breakdowns (player_id, map_name, mod_name);
-- @refresh_sql REFRESH MATERIALIZED VIEW CONCURRENTLY mv_player_map_mod_breakdowns;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_player_map_mod_breakdowns AS
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
        ps.round_id,
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
        ss.player_id,
        COALESCE(r.map_name, ss.map_name) AS map_name,
        COALESCE(r.mod_name, ss.mod_name) AS mod_name,
        ss.session_seconds_played AS seconds_played,
        ss.kills,
        ss.deaths,
        ss.score,
        CASE
            WHEN COALESCE(r.winning_team, ss.team) IS NULL THEN NULL
            WHEN ss.team IS NULL THEN NULL
            WHEN COALESCE(r.winning_team, ss.team) = ss.team THEN 1
            ELSE 0
        END AS win_flag
    FROM session_source ss
    LEFT JOIN rounds r ON r.id = ss.round_id
)
SELECT
    player_id,
    map_name,
    mod_name,
    COUNT(*) AS sessions_played,
    SUM(seconds_played)::BIGINT AS total_seconds_played,
    SUM(kills) AS total_kills,
    SUM(deaths) AS total_deaths,
    SUM(score) AS total_score,
    SUM(kills)::NUMERIC / NULLIF(SUM(seconds_played) / 60.0, 0) AS kills_per_minute,
    SUM(score)::NUMERIC / NULLIF(SUM(seconds_played) / 60.0, 0) AS score_per_minute,
    SUM(COALESCE(win_flag, 0))::NUMERIC / NULLIF(COUNT(*), 0) AS win_rate
FROM session_metrics
GROUP BY player_id, map_name, mod_name;

CREATE INDEX IF NOT EXISTS idx_mv_player_map_mod_breakdowns_player_map
    ON mv_player_map_mod_breakdowns (player_id, map_name, mod_name);

-- @name player_session_heatmaps
-- @type incremental_insert
-- @object player_session_heatmaps
-- @description Hourly player session heatmaps including activity weights.
-- @indexes CREATE UNIQUE INDEX IF NOT EXISTS idx_player_session_heatmaps_pk ON player_session_heatmaps (player_id, hour_bucket);
-- @refresh_sql INSERT INTO player_session_heatmaps (player_id, hour_bucket, sessions_started, seconds_played, updated_at)
-- |SELECT
-- |    agg.player_id,
-- |    agg.hour_bucket,
-- |    agg.sessions_started,
-- |    agg.seconds_played,
-- |    NOW() AS updated_at
-- |FROM (
-- |    SELECT
-- |        COALESCE(
-- |            to_jsonb(ps) ->> 'player_id',
-- |            to_jsonb(ps) ->> 'player_guid',
-- |            to_jsonb(ps) ->> 'player_hash',
-- |            to_jsonb(ps) ->> 'player_name'
-- |        ) AS player_id,
-- |        DATE_TRUNC(
-- |            'hour',
-- |            COALESCE(
-- |                NULLIF(to_jsonb(ps) ->> 'session_start', '')::TIMESTAMPTZ,
-- |                NULLIF(to_jsonb(ps) ->> 'session_start_at', '')::TIMESTAMPTZ,
-- |                NULLIF(to_jsonb(ps) ->> 'session_started_at', '')::TIMESTAMPTZ,
-- |                NULLIF(to_jsonb(ps) ->> 'session_begin', '')::TIMESTAMPTZ,
-- |                NULLIF(to_jsonb(ps) ->> 'session_begin_at', '')::TIMESTAMPTZ,
-- |                NULLIF(to_jsonb(ps) ->> 'start_time', '')::TIMESTAMPTZ,
-- |                NULLIF(to_jsonb(ps) ->> 'started_at', '')::TIMESTAMPTZ,
-- |                NULLIF(to_jsonb(ps) ->> 'created_at', '')::TIMESTAMPTZ
-- |            )
-- |        ) AS hour_bucket,
-- |        COUNT(*) AS sessions_started,
-- |        SUM(session_seconds_played)::BIGINT AS seconds_played
-- |    FROM player_sessions ps
-- |    GROUP BY player_id,
-- |        DATE_TRUNC(
-- |            'hour',
-- |            COALESCE(
-- |                NULLIF(to_jsonb(ps) ->> 'session_start', '')::TIMESTAMPTZ,
-- |                NULLIF(to_jsonb(ps) ->> 'session_start_at', '')::TIMESTAMPTZ,
-- |                NULLIF(to_jsonb(ps) ->> 'session_started_at', '')::TIMESTAMPTZ,
-- |                NULLIF(to_jsonb(ps) ->> 'session_begin', '')::TIMESTAMPTZ,
-- |                NULLIF(to_jsonb(ps) ->> 'session_begin_at', '')::TIMESTAMPTZ,
-- |                NULLIF(to_jsonb(ps) ->> 'start_time', '')::TIMESTAMPTZ,
-- |                NULLIF(to_jsonb(ps) ->> 'started_at', '')::TIMESTAMPTZ,
-- |                NULLIF(to_jsonb(ps) ->> 'created_at', '')::TIMESTAMPTZ
-- |            )
-- |        )
-- |) agg
-- |WHERE agg.hour_bucket >= COALESCE((SELECT MAX(hour_bucket) FROM player_session_heatmaps), '1970-01-01'::TIMESTAMPTZ)
-- |ON CONFLICT (player_id, hour_bucket) DO UPDATE
-- |SET
-- |    sessions_started = EXCLUDED.sessions_started,
-- |    seconds_played = EXCLUDED.seconds_played,
-- |    updated_at = NOW();

CREATE TABLE IF NOT EXISTS player_session_heatmaps (
    player_id TEXT NOT NULL,
    hour_bucket TIMESTAMPTZ NOT NULL,
    sessions_started INTEGER NOT NULL,
    seconds_played BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT player_session_heatmaps_pk PRIMARY KEY (player_id, hour_bucket)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_player_session_heatmaps_pk
    ON player_session_heatmaps (player_id, hour_bucket);
