-- Three queries

-- 1
-- Gets the total amount of points scored by LeBron James
-- in October of 2018

SELECT SUM(pgs.points) AS Points
FROM player_game_stats AS pgs
WHERE pgs.p_id = (
  SELECT p.p_id
  FROM player AS p
  WHERE p.first_name = 'LeBron'
  AND p.last_name = 'James')
AND pgs.g_id IN (
  SELECT g.g_id
  FROM game AS g
  WHERE EXTRACT(MONTH FROM g.game_time) = 10
  AND EXTRACT(YEAR FROM g.game_time) = 2018
);

-- 2
-- Gets the match outcomes of all the matches that
-- Peter Grantcharov bet on:
SELECT *
FROM game
WHERE game.g_id IN (
  SELECT mo.g_id
  FROM make_odds AS mo
  WHERE mo.o_id IN (

  )
)
