WITH y_g AS (
    SELECT *
    FROM game
    WHERE game_time >= current_date - 1 AND game_time < current_date),
y_g_t AS (
    SELECT Y.g_id, T1.name AS H_name, T2.name AS A_name
    FROM y_g AS Y, team AS T1, team AS T2
    WHERE Y.t_id_home = T1.t_id AND Y.t_id_away = T2.t_id
),
y_g_t_s AS(
    SELECT H_name,
        home_q1_score,home_q2_score,home_q3_score,home_q4_score,home_ot_score,
        home_q1_score+home_q2_score+home_q3_score+home_q4_score+home_ot_score AS H_score,
        A_name,
        away_q1_score,away_q2_score,away_q3_score,away_q4_score,away_ot_score,
        away_q1_score+away_q2_score+away_q3_score+away_q4_score+away_ot_score AS A_score
    FROM y_g_t AS Y, game_stats AS GS
    WHERE Y.g_id = GS.g_id
)
SELECT * FROM y_g_t_s
