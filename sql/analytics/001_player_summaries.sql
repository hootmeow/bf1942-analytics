-- @name player_summaries
-- @type materialized_view
-- @object mv_player_summaries
-- @description Aggregated player performance metrics across recorded sessions.
-- @indexes CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_player_summaries_player ON mv_player_summaries (player_id);
-- @refresh_sql REFRESH MATERIALIZED VIEW CONCURRENTLY mv_player_summaries;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_player_summaries AS
SELECT
    ps.player_id,
    COUNT(*) AS sessions_played,
    SUM(EXTRACT(EPOCH FROM (COALESCE(ps.session_end, NOW()) - ps.session_start)))::BIGINT AS seconds_played,
    SUM(ps.kills) AS total_kills,
    SUM(ps.deaths) AS total_deaths,
    SUM(ps.score) AS total_score,
    (SUM(ps.kills)::NUMERIC / NULLIF(SUM(ps.deaths), 0)) AS kill_death_ratio,
    MAX(COALESCE(ps.session_end, ps.session_start)) AS last_seen_at
FROM player_sessions ps
GROUP BY ps.player_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_player_summaries_player
    ON mv_player_summaries (player_id);
