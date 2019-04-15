WITH bet_info AS(
    SELECT M.o_id, M.g_id, M.sb_id, M.bt_id, M.odds_side, M.odds_payout, M.odds_line, P.bet_size
    FROM place_bet AS P, make_odds AS M
    WHERE P.o_id = M.o_id AND P.u_id = 1),
game_info AS(
    SELECT g_id,
    home_q1_score+home_q2_score+home_q3_score+home_q4_score+coalesce(home_ot_score,0) AS H,
    away_q1_score+away_q2_score+away_q3_score+away_q4_score+coalesce(away_ot_score,0) AS V,
    home_q1_score+home_q2_score+home_q3_score+home_q4_score+coalesce(home_ot_score,0)+
    away_q1_score+away_q2_score+away_q3_score+away_q4_score+coalesce(away_ot_score,0) AS OU
    FROM game_stats AS G
    WHERE G.g_id IN (SELECT g_id FROM bet_info)),
OU AS (
    SELECT B.o_id, B.bet_size, B.odds_payout,
           G.OU > B.odds_line AS O_or_H_flag,
           B.odds_side = 'O' AS bet_O_or_H_flag
    FROM bet_info AS B, game_info AS G
    WHERE B.g_id = G.g_id AND (B.odds_side = 'O' OR B.odds_side = 'U')),
HV AS(
    SELECT B.o_id, B.bet_size, B.odds_payout,
           G.H-G.V > B.odds_line AS O_or_H_flag,
           B.odds_side = 'H' AS bet_O_or_H_flag
    FROM bet_info AS B, game_info AS G
    WHERE B.g_id = G.g_id AND (B.odds_side = 'H' OR B.odds_side = 'V')
)
SELECT *
FROM OU
UNION
SELECT *
FROM HV
