-----------------------------------------------------------------------------------
-- GIVEN u_id, find the betted game is complete
-- first WITH, find given user
-- second WITH, find all betted g_id
-- third step find all completed game the user bet
WITH bet_user AS (
    SELECT u_id
    FROM users
    WHERE u_id = 1),
bet_info AS(
    SELECT M.g_id
    FROM place_bet AS P, make_odds AS M, bet_user
    WHERE P.o_id = M.o_id AND P.u_id = bet_user.u_id)
SELECT g_id
FROM game
WHERE g_id IN (SELECT g_id FROM bet_info) AND g_id IN (SELECT g_id FROM game_stats)
-----------------------------------------------------------------------------------
-- GIVEN u_id, find the betted game is NOT complete
-- first WITH, find given user, i think we can skip 1st table due to known u_id
-- second WITH, find all betted g_id
-- third step find all completed game the user bet
WITH bet_user AS (
    SELECT u_id
    FROM users
    WHERE u_id = 1),
bet_info AS(
    SELECT M.g_id
    FROM place_bet AS P, make_odds AS M, bet_user
    WHERE P.o_id = M.o_id AND P.u_id = bet_user.u_id)
SELECT g_id
FROM game AS G
WHERE G.g_id IN (SELECT g_id FROM bet_info) AND G.g_id NOT IN (SELECT g_id FROM game_stats)
---------------------------------------------------------------------------------------------
-- find all yesterday game
SELECT *
FROM game
WHERE game_time >= TIMESTAMP 'yesterday' AND
      game_time < TIMESTAMP 'today'
----------------------------------------------------------------------------------------------
-- outer method, find all users
# outer: extract all user
SELECT DISTINCT u_id
FROM users
---------------------------------------------------------------------------------------------
-- Inner method for calculating balance (known u_id)
-- assume given u_id = 1
-- 1st WITH: find user bet info
-- 2nd WITH: find and calculate the actual scores
-- 3rd WITH: find and compare the OU bet
-- 4th WITH: find and compare the HV bet
-- O_or_H_flag: actual score is greater than odds_line
-- bet_O_or_H_flag: people bet on Over/Home or not
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
---------------------------------------------------------------------------------------------
-- not complete game balance
-- assume known u_id = 1
-- 1st with: find not complete game
-- 2nd with: find corresponding bet
WITH game_info AS (
      SELECT g_id
      FROM game
      WHERE g_id NOT IN (SELECT g_id FROM game_stats)),
bet_info AS (
      SELECT M.o_id, P.bet_size
      FROM place_bet AS P, make_odds AS M
      WHERE P.o_id = M.o_id AND M.g_id IN ( SELECT g_id FROM game_info) AND P.u_id = 1)
SELECT *
FROM bet_info
-----------------------------------------------------------------------------------------
-- Given a team_id
-- We need to get their SEASON long
-- 1) win percentage
SELECT count(CASE WHEN WIN_FLAG THEN 1 END) as w, count(*) as t ,
       CAST(count(CASE WHEN WIN_FLAG THEN 1 END) AS FLOAT) / CAST(count(*) AS FLOAT) AS R
FROM (SELECT t_id_home = 0 AND
            (home_q1_score + home_q2_score + home_q3_score + home_q4_score -
             away_q1_score - away_q2_score - away_q3_score - away_q4_score +
             coalesce(home_ot_score,0) - coalesce(away_ot_score,0)) >0 AS WIN_FLAG
      FROM game, game_stats
      WHERE (game.t_id_home = 0 OR game.t_id_away = 0) AND game.g_id = game_stats.g_id) AS win

-- 2) field goal percentage (sum FG / sum FGA)
SELECT SUM(field_goals_made) AS FG, SUM(field_goal_attempts)  AS FGA,
       CAST(SUM(field_goals_made) AS FLOAT) / CAST(SUM(field_goal_attempts) AS FLOAT) AS FGP
FROM game, player_game_stats
WHERE (game.t_id_home = 0 OR game.t_id_away = 0) AND
       game.g_id = player_game_stats.g_id AND
       player_game_stats.t_id = 0

-- 3) 3 point percentage (sum TP / sum TPA)
SELECT SUM(three_pointers_made) AS TP, SUM(three_point_attempts)  AS TPA,
       CAST(SUM(three_pointers_made) AS FLOAT) / CAST(SUM(three_point_attempts) AS FLOAT) AS TPP
FROM game, player_game_stats
WHERE (game.t_id_home = 0 OR game.t_id_away = 0) AND
       game.g_id = player_game_stats.g_id AND
       player_game_stats.t_id = 0

-- 4) rebounds per game
SELECT (SUM(offensive_rebounds)+ SUM(defensive_rebounds))/ AVG(C) AS R_per_game
FROM game, player_game_stats,
     (SELECT COUNT(*) AS C FROM game WHERE game.t_id_home = 1 OR game.t_id_away = 1) AS Total
WHERE (game.t_id_home = 1 OR game.t_id_away = 1) AND
       game.g_id = player_game_stats.g_id AND
       player_game_stats.t_id = 1

--5) turnovers per game

SELECT SUM(turnovers) / AVG(C) AS T_per_game
FROM game, player_game_stats,
     (SELECT COUNT(*) AS C FROM game WHERE game.t_id_home = 1 OR game.t_id_away = 1) AS Total
WHERE (game.t_id_home = 1 OR game.t_id_away = 1) AND
       game.g_id = player_game_stats.g_id AND
       player_game_stats.t_id = 1
