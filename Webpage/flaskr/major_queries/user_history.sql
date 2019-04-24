WITH bet_info AS (
    SELECT P.bet_time, M.o_id, M.g_id, M.odds_side,
           M.odds_payout, M.odds_line,
           P.bet_size, BT.name AS BET_TYPE
    FROM place_bet AS P, make_odds AS M, bet_type AS BT
    WHERE P.o_id = M.o_id
    AND P.u_id = {user}
    AND BT.bt_id = M.bt_id
    ORDER BY bet_time DESC
),

team_info AS (
    SELECT B.o_id, T1.name AS HOME, T2.name AS AWAY
    FROM bet_info AS B, game AS G, team AS T1, team AS T2
    WHERE G.g_id = B.g_id
    AND G.g_id IN (SELECT g_id FROM bet_info)
    AND G.t_id_home = T1.t_id
    AND G.t_id_away = T2.t_id
),

game_info AS (
    SELECT GS.g_id,
    home_q1_score + home_q2_score + home_q3_score + home_q4_score + COALESCE(home_ot_score, 0) AS H,
    away_q1_score + away_q2_score + away_q3_score + away_q4_score + COALESCE(away_ot_score, 0) AS V,
    home_q1_score + home_q2_score + home_q3_score + home_q4_score + COALESCE(home_ot_score, 0) +
    away_q1_score + away_q2_score + away_q3_score + away_q4_score + COALESCE(away_ot_score, 0) AS OU
    FROM game_stats AS GS
    WHERE GS.g_id IN (SELECT g_id FROM bet_info)
),

OU AS (
    SELECT B.bet_time, T.HOME, T.AWAY, G.H AS H_score, G.V AS V_score, B.bet_size, B.odds_side,
           B.BET_TYPE, B.odds_line, B.odds_payout,
           CASE
             WHEN (
               (G.OU > B.odds_line AND B.odds_side = 'O') OR
               (G.OU < B.odds_line AND B.odds_side = 'U')
             ) = TRUE
             THEN 'WON'
             WHEN (G.OU = B.odds_line)
             THEN 'TIED'
             ELSE 'LOST'
           END AS WIN_LOST
    FROM bet_info AS B, game_info AS G, team_info AS T
    WHERE B.g_id = G.g_id
    AND (B.odds_side = 'O' OR B.odds_side = 'U')
    AND B.o_id = T.o_id
),

HV AS (
    SELECT B.bet_time, T.HOME, T.AWAY, G.H AS H_score, G.V AS V_score, B.bet_size, B.odds_side,
           B.BET_TYPE, B.odds_line, B.odds_payout,
           CASE
             WHEN (
               ((G.H - G.V + B.odds_line) > 0 AND B.odds_side = 'H') OR
               ((G.H-G.V - B.odds_line) < 0 AND B.odds_side = 'V')
             ) = TRUE
             THEN 'WON'
             WHEN (
               ((G.H-G.V + B.odds_line) = 0 AND B.odds_side = 'H') OR
               ((G.H-G.V - B.odds_line) = 0 AND B.odds_side = 'V')
             ) = TRUE
             THEN 'TIED'
             ELSE 'LOST'
           END AS WIN_LOST
    FROM bet_info AS B, game_info AS G, team_info AS T
    WHERE B.g_id = G.g_id AND (B.odds_side = 'H' OR B.odds_side = 'V') AND B.o_id = T.o_id
),

INCOMPLETE AS (
    SELECT B.bet_time, T.HOME, T.AWAY,
      0 AS H_score, 0 AS V_score,
      B.bet_size, B.odds_side,
      B.BET_TYPE, B.odds_line, B.odds_payout,
      'PENDING' AS WIN_LOST
    FROM bet_info AS B, team_info AS T
    WHERE B.g_id NOT IN (SELECT g_id FROM game_info) AND B.o_id = T.o_id
)

SELECT *
FROM INCOMPLETE
UNION
SELECT *
FROM OU
UNION
SELECT *
FROM HV
ORDER BY bet_time DESC;
