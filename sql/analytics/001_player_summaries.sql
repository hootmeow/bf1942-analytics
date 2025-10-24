-- @name player_summaries
-- @type materialized_view
-- @object mv_player_summaries
-- @description Aggregated player performance metrics across recorded sessions.
-- @indexes CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_player_summaries_player ON mv_player_summaries (player_id);
-- @refresh_sql REFRESH MATERIALIZED VIEW CONCURRENTLY mv_player_summaries;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_player_summaries AS
WITH session_source AS (
    SELECT *
    FROM (
        SELECT
            COALESCE(
                to_jsonb(ps) ->> 'player_id',
                to_jsonb(ps) ->> 'player_guid',
                to_jsonb(ps) ->> 'player_hash',
                to_jsonb(ps) ->> 'player_name'
            ) AS player_id,
            ps.*
        FROM player_sessions ps
    ) enriched
    WHERE enriched.player_id IS NOT NULL
)
SELECT
    ss.player_id,
    COUNT(*) AS sessions_played,
    SUM(EXTRACT(EPOCH FROM (COALESCE(ss.session_end, NOW()) - ss.session_start)))::BIGINT AS seconds_played,
    SUM(ss.kills) AS total_kills,
    SUM(ss.deaths) AS total_deaths,
    SUM(ss.score) AS total_score,
    (SUM(ss.kills)::NUMERIC / NULLIF(SUM(ss.deaths), 0)) AS kill_death_ratio,
    MAX(COALESCE(ss.session_end, ss.session_start)) AS last_seen_at
FROM session_source ss
GROUP BY ss.player_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_player_summaries_player
    ON mv_player_summaries (player_id);
