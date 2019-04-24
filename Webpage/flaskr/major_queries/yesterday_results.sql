WITH yest_games AS (
  SELECT *
  FROM game
  WHERE game_time::date = (current_timestamp at time zone 'EDT')::date - 1
  ORDER BY game_time
),

yest_games_team AS (
  SELECT Y.g_id, T1.name AS H_name, T2.name AS A_name
  FROM yest_games AS Y, team AS T1, team AS T2
  WHERE Y.t_id_home = T1.t_id
  AND Y.t_id_away = T2.t_id
)

SELECT H_name,
    home_q1_score, home_q2_score, home_q3_score, home_q4_score, home_ot_score,
    home_q1_score + home_q2_score + home_q3_score + home_q4_score + COALESCE(home_ot_score, 0)  AS H_score,
    A_name,
    away_q1_score,away_q2_score,away_q3_score,away_q4_score,away_ot_score,
    away_q1_score + away_q2_score + away_q3_score + away_q4_score + COALESCE(away_ot_score, 0)  AS A_score
FROM yest_games_team, game_stats
WHERE yest_games_team.g_id = game_stats.g_id;
