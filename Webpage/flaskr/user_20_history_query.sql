-- but for the user page, I want the bet history table of their last 20 (max) bets
-- date
-- away_team name
-- home_team name
-- away final score
-- home final score
-- 6) bet type name (can get from bet_type table) should be either moneyline, over under or pointspread
-- 7) results = {WIN, LOSS,  PENDING}

%%sql
%%sql
WITH bet_info AS(
    SELECT P.bet_time, M.o_id, M.g_id, M.odds_side, M.odds_payout, M.odds_line, 
           P.bet_size, BT.name AS BET_TYPE
    FROM place_bet AS P, make_odds AS M, bet_type AS BT
    WHERE P.o_id = M.o_id AND P.u_id = 1 AND BT.bt_id = M.bt_id
    ORDER BY bet_time DESC
    LIMIT 20),
team_info AS(
    SELECT B.o_id, T1.name AS Home, T2.name AS AWAY
    FROM bet_info AS B, game AS G, team AS T1, team AS T2
    WHERE G.g_id = B.g_id AND G.g_id IN (SELECT g_id FROM bet_info) AND
          G.t_id_home = T1.t_id AND G.t_id_away = T2.t_id
),
game_info AS(
    SELECT GS.g_id, 
    home_q1_score+home_q2_score+home_q3_score+home_q4_score+coalesce(home_ot_score,0) AS H,
    away_q1_score+away_q2_score+away_q3_score+away_q4_score+coalesce(away_ot_score,0) AS V,
    home_q1_score+home_q2_score+home_q3_score+home_q4_score+coalesce(home_ot_score,0)+
    away_q1_score+away_q2_score+away_q3_score+away_q4_score+coalesce(away_ot_score,0) AS OU
    FROM game_stats AS GS
    WHERE GS.g_id IN (SELECT g_id FROM bet_info)),
OU AS (
    SELECT B.bet_time, T.HOME, T.AWAY, G.H AS H_score, G.V AS V_score, B.bet_size, 
           B.BET_TYPE, B.odds_side,
           CASE WHEN (G.OU > B.odds_line AND B.odds_side = 'O') = TRUE THEN 'WIN'
                ELSE 'LOST' END AS WIN_LOST
    FROM bet_info AS B, game_info AS G, team_info AS T
    WHERE B.g_id = G.g_id AND (B.odds_side = 'O' OR B.odds_side = 'U') AND B.o_id = T.o_id
),
HV AS(
    SELECT B.bet_time, T.HOME, T.AWAY, G.H AS H_score, G.V AS V_score, B.bet_size, 
           B.BET_TYPE, B.odds_side,
           CASE WHEN (G.H-G.V > B.odds_line AND B.odds_side = 'H') = TRUE THEN 'WIN'
                ELSE 'LOST' END AS WIN_LOST
    FROM bet_info AS B, game_info AS G, team_info AS T
    WHERE B.g_id = G.g_id AND (B.odds_side = 'H' OR B.odds_side = 'V') AND B.o_id = T.o_id
),
INCOMPLETE AS (
    SELECT B.bet_time, T.HOME, T.AWAY, 0 AS H_score, 0 AS V_score, B.bet_size, 
           B.BET_TYPE, B.odds_side, 'PENDING' AS WIN_LOST
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
ORDER BY bet_time DESC