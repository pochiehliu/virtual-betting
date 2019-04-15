SELECT COUNT(CASE WHEN WIN_FLAG THEN 1 END) AS w, COUNT(*) AS t ,
       CAST(COUNT(CASE WHEN WIN_FLAG THEN 1 END) AS FLOAT) / CAST(COUNT(*) AS FLOAT) AS R
FROM (SELECT t_id_home = 0 AND
            (home_q1_score + home_q2_score + home_q3_score + home_q4_score -
             away_q1_score - away_q2_score - away_q3_score - away_q4_score +
             coalesce(home_ot_score, 0) - coalesce(away_ot_score, 0)) > 0 AS WIN_FLAG
      FROM game, game_stats
      WHERE (game.t_id_home = {tid} OR game.t_id_away = {tid}) AND game.g_id = game_stats.g_id) AS win;

SELECT SUM(field_goals_made) AS FG, SUM(field_goal_attempts)  AS FGA,
       CAST(SUM(field_goals_made) AS FLOAT) / CAST(SUM(field_goal_attempts) AS FLOAT) AS FGP
FROM game, player_game_stats
WHERE (game.t_id_home = {tid} OR game.t_id_away = {tid}) AND
       game.g_id = player_game_stats.g_id AND
       player_game_stats.t_id = {tid};

SELECT SUM(three_pointers_made) AS TP, SUM(three_point_attempts)  AS TPA,
       CAST(SUM(three_pointers_made) AS FLOAT) / CAST(SUM(three_point_attempts) AS FLOAT) AS TPP
FROM game, player_game_stats
WHERE (game.t_id_home = {tid} OR game.t_id_away = {tid}) AND
       game.g_id = player_game_stats.g_id AND
       player_game_stats.t_id = {tid};

SELECT (SUM(offensive_rebounds)+ SUM(defensive_rebounds))/ AVG(C) AS R_per_game
FROM game, player_game_stats,
     (SELECT COUNT(*) AS C FROM game WHERE game.t_id_home = {tid} OR game.t_id_away = {tid}) AS Total
WHERE (game.t_id_home = {tid} OR game.t_id_away = {tid}) AND
       game.g_id = player_game_stats.g_id AND
       player_game_stats.t_id = {tid};

SELECT SUM(turnovers) / AVG(C) AS T_per_game
FROM game, player_game_stats,
     (SELECT COUNT(*) AS C FROM game WHERE game.t_id_home = {tid} OR game.t_id_away = {tid}) AS Total
WHERE (game.t_id_home = {tid} OR game.t_id_away = {tid}) AND
       game.g_id = player_game_stats.g_id AND
       player_game_stats.t_id = {tid};
