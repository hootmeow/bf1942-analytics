-- @name player_summaries
-- @type materialized_view
-- @object mv_player_summaries
-- @description Aggregated player performance metrics across recorded sessions.
-- @indexes CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_player_summaries_player ON mv_player_summaries (player_id);
-- @refresh_sql REFRESH MATERIALIZED VIEW CONCURRENTLY mv_player_summaries;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_player_summaries AS
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
)
SELECT
    ss.player_id,
    COUNT(*) AS sessions_played,
    SUM(ss.session_seconds_played)::BIGINT AS seconds_played,
    SUM(ss.kills) AS total_kills,
    SUM(ss.deaths) AS total_deaths,
    SUM(ss.score) AS total_score,
    (SUM(ss.kills)::NUMERIC / NULLIF(SUM(ss.deaths), 0)) AS kill_death_ratio,
    MAX(COALESCE(ss.session_end_at, ss.session_start_at)) AS last_seen_at
FROM session_source ss
GROUP BY ss.player_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_player_summaries_player
    ON mv_player_summaries (player_id);
