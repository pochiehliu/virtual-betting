WITH last_season_game AS(
    SELECT g_id, game_time, t_id_home, t_id_away
    FROM game
    WHERE (date_part('month', game_time) > 8 AND date_part('year', game_time) = 2018) OR
          (date_part('month', game_time) <= 8 AND date_part('year', game_time) = 2019)
    ORDER BY game_time
),
team_last_season AS (
    SELECT *
    FROM last_season_game AS L
    WHERE t_id_home = {tid} OR t_id_away = {tid}
),
team_win_lose AS (
    SELECT  t_id_home AS H, t_id_away AS A,
            home_q1_score + home_q2_score + home_q3_score + home_q4_score -
            away_q1_score - away_q2_score - away_q3_score - away_q4_score +
            coalesce(home_ot_score,0) - coalesce(away_ot_score,0) > 0 AS HA
    FROM game_stats AS G, team_last_season AS T
    WHERE G.g_id = T.g_id
)
SELECT ROUND(CAST(CAST(COUNT( CASE WHEN ((H = {tid} AND HA=TRUE) OR (A = {tid} AND HA = FALSE)) THEN 1 END) AS FLOAT) * 100 /
       CAST (count(*) AS FLOAT) AS NUMERIC), 3) AS R
FROM team_win_lose;

SELECT ROUND(CAST(CAST(SUM(field_goals_made) AS FLOAT) * 100 /
       CAST (SUM(field_goal_attempts) AS FLOAT) AS NUMERIC), 3) AS FGP
FROM player_game_stats WHERE t_id = {tid};

SELECT ROUND(CAST(CAST(SUM(three_pointers_made) AS FLOAT) * 100 /
       CAST (SUM(three_point_attempts) AS FLOAT) AS NUMERIC), 3) AS FGP
FROM player_game_stats WHERE t_id = {tid};

SELECT ROUND(CAST((SUM(offensive_rebounds)::FLOAT +
        SUM(defensive_rebounds)::FLOAT) / (
          SELECT COUNT(DISTINCT g_id)::FLOAT
          FROM player_game_stats
          WHERE t_id = {tid}) AS NUMERIC), 3) AS R_per_game
FROM player_game_stats WHERE t_id = {tid};


SELECT ROUND(CAST((SUM(turnovers)::FLOAT) / (
          SELECT COUNT(DISTINCT g_id)::FLOAT
          FROM player_game_stats
          WHERE t_id = {tid}) AS NUMERIC), 3) AS T_per_game
FROM player_game_stats WHERE t_id = {tid};
