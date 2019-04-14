-- from previous results, separate the game is completed or not
-- To know whether they're complete, you have to check if the game_id exists in "game_stats" table
-- completed game

SELECT *
FROM (SELECT * FROM place_bet WHERE u_id = given_u_id) AS u_bet, make_odds, 
     (SELECT g_id FROM game_stats) AS game
WHERE u_bet.o_id = make_odds.o_id AND make_odds.g_id IN game

-- not complete game
SELECT *
FROM (SELECT * FROM place_bet WHERE u_id = given_u_id) AS u_bet, make_odds,
     (SELECT g_id FROM game_stats) AS game
WHERE u_bet.o_id = make_odds.o_id AND make_odds.g_id NOT IN game

-- you have to take a person betting history and calculate their balance
-- is get the sum of all bet_size values for the bets of this user and subtract this value

-- 1st OV
SELECT U_bet.bet_size * U_bet.odds_payout
FROM (SELECT *
      FROM place_bet AS P, make_odds AS M
      WHERE P.u_id = given_u_id AND
            M.o_id = P.o_id AND
            (M.odds_side = 'O' or M.odds_side = 'U') ) AS U_bet,
     (SELECT (home_q1_score + home_q1_score + home_q1_score + home_q1_score +
              away_q1_score + away_q1_score + away_q1_score + away_q1_score ) AS total, g_id
      FROM game_stats ) AS G
WHERE U_bet.g_id = G.g_id AND
      ((M.odds_side = 'O' AND G.total > U_bet.odds_line) OR
       (M.odds_side = 'U' AND G.total < U_bet.odds_line);

# 1st OV tie
SELECT U_bet.bet_size * U_bet.odds_payout
FROM (SELECT *
      FROM place_bet AS P, make_odds AS M
      WHERE P.u_id = given_u_id AND
            M.o_id = P.o_id AND
            (M.odds_side = 'O' or M.odds_side = 'U') ) AS U_bet,
     (SELECT (home_q1_score + home_q1_score + home_q1_score + home_q1_score +
              away_q1_score + away_q1_score + away_q1_score + away_q1_score ) AS total, g_id
      FROM game_stats ) AS G
WHERE U_bet.g_id = G.g_id AND
      ((M.odds_side = 'O' AND G.total = U_bet.odds_line) OR
       (M.odds_side = 'U' AND G.total = U_bet.odds_line);


# 2nd HV
SELECT U_bet.bet_size * U_bet.odds_payout
FROM (SELECT *
      FROM place_bet AS P, make_odds AS M
      WHERE P.u_id = given_u_id AND
            M.o_id = P.o_id AND
            (M.odds_side = 'O' or M.odds_side = 'U')) AS U_bet,
     (SELECT (home_q1_score + home_q1_score + home_q1_score + home_q1_score ) AS home,
             (away_q1_score + away_q1_score + away_q1_score + away_q1_score ) AS away, g_id
      FROM game_stats ) AS G
WHERE U_bet.g_id = G.g_id AND
      ((M.odds_side = 'H' AND G.home - G.away > U_bet.odds_line) OR
       (M.odds_side = 'V' AND G.away - G.home > U_bet.odds_line));

# 2nd HV tie
SELECT U_bet.bet_size * U_bet.odds_payout
FROM (SELECT *
      FROM place_bet AS P, make_odds AS M
      WHERE P.u_id = given_u_id AND
            M.o_id = P.o_id AND
            (M.odds_side = 'O' or M.odds_side = 'U')) AS U_bet,
     (SELECT (home_q1_score + home_q1_score + home_q1_score + home_q1_score ) AS home,
             (away_q1_score + away_q1_score + away_q1_score + away_q1_score ) AS away, g_id
      FROM game_stats ) AS G
WHERE U_bet.g_id = G.g_id AND
      ((M.odds_side = 'H' AND G.home - G.away = U_bet.odds_line) OR
       (M.odds_side = 'V' AND G.away - G.home = U_bet.odds_line))


# Given a team_id
# We need to get their SEASON long
# 1) win percentage
SELECT count(CASE WHEN WIN_FLAG THEN 1 END) as w, count(*) as t ,
       CAST(count(CASE WHEN WIN_FLAG THEN 1 END) AS FLOAT) / CAST(count(*) AS FLOAT) AS R
FROM (SELECT t_id_home = 0 AND
            (home_q1_score + home_q2_score + home_q3_score + home_q4_score -
             away_q1_score - away_q2_score - away_q3_score - away_q4_score +
             coalesce(home_ot_score,0) - coalesce(away_ot_score,0)) >0 AS WIN_FLAG
      FROM game, game_stats
      WHERE (game.t_id_home = 0 OR game.t_id_away = 0) AND game.g_id = game_stats.g_id)as win

# 2) field goal percentage (sum FG / sum FGA)
SELECT SUM(field_goals_made) AS FG, SUM(field_goal_attempts)  AS FGA,
       CAST(SUM(field_goals_made) AS FLOAT) / CAST(SUM(field_goal_attempts) AS FLOAT) AS FGP
FROM game, player_game_stats
WHERE (game.t_id_home = 0 OR game.t_id_away = 0) AND
       game.g_id = player_game_stats.g_id AND
       player_game_stats.t_id = 0

# 3) 3 point percentage (sum TP / sum TPA)
SELECT SUM(three_pointers_made) AS TP, SUM(three_point_attempts)  AS TPA,
       CAST(SUM(three_pointers_made) AS FLOAT) / CAST(SUM(three_point_attempts) AS FLOAT) AS TPP
FROM game, player_game_stats
WHERE (game.t_id_home = 0 OR game.t_id_away = 0) AND
       game.g_id = player_game_stats.g_id AND
       player_game_stats.t_id = 0

# 4) rebounds per game
SELECT (SUM(offensive_rebounds)+ SUM(defensive_rebounds))/ AVG(C) AS R_per_game
FROM game, player_game_stats,
     (SELECT COUNT(*) AS C FROM game WHERE game.t_id_home = 1 OR game.t_id_away = 1) AS Total
WHERE (game.t_id_home = 1 OR game.t_id_away = 1) AND
       game.g_id = player_game_stats.g_id AND
       player_game_stats.t_id = 1

# 5) turnovers per game

SELECT SUM(turnovers) / AVG(C) AS T_per_game
FROM game, player_game_stats,
     (SELECT COUNT(*) AS C FROM game WHERE game.t_id_home = 1 OR game.t_id_away = 1) AS Total
WHERE (game.t_id_home = 1 OR game.t_id_away = 1) AND
       game.g_id = player_game_stats.g_id AND
       player_game_stats.t_id = 1
