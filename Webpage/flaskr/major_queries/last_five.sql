
WITH last_five AS (
  SELECT *
  FROM game_stats AS gs
  WHERE gs.g_id IN (
    SELECT g.g_id
    FROM game AS g
    WHERE (g.t_id_away = {a} AND g.t_id_home = {h})
    OR (g.t_id_away = {h} AND g.t_id_home = {a})
  )
  ORDER BY gs.g_id DESC
  LIMIT 5
)

SELECT *
FROM last_five INNER JOIN game
ON last_five.g_id = game.g_id
;
