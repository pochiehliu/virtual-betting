-- Three queries

-- 1
-- Gets the match outcomes of all the matches that
-- Peter Grantcharov bet on, along with relevant
-- information to determine bet outcomes.
SELECT
tmp3.name AS Type,
tmp3.odds_side AS Side,
tmp3.bet_size AS Amount,
tmp3.odds_payout AS Odds,
tmp3.odds_line AS Cover,
g.game_time AS Game_Date,
g.home_q1_score +
g.home_q2_score +
g.home_q3_score +
g.home_q4_score +
g.home_ot_score AS Home_Score,
g.away_q1_score +
g.away_q2_score +
g.away_q3_score +
g.away_q4_score +
g.away_ot_score AS Away_Score
FROM (
  SELECT *
  FROM (
    SELECT *
    FROM (
      SELECT pb.o_id AS o_id, pb.bet_size AS bet_size
      FROM place_bet AS pb
      WHERE pb.u_id = (
        SELECT u.u_id
        FROM users AS u
        WHERE u.first_name = 'Peter'
        AND u.last_name = 'Grantcharov'
      )
    ) AS tmp1 INNER JOIN make_odds AS mo
    ON tmp1.o_id = mo.o_id
  ) AS tmp2 INNER JOIN bet_type AS bt
  ON tmp2.bt_id = bt.bt_id
) as tmp3 INNER JOIN game as g
ON tmp3.g_id = g.g_id;

-- 2
-- Get instances of players who have had 40 point games
SELECT p.first_name, p.last_name, tmp2.points, tmp2.game_time
FROM player AS p RIGHT OUTER JOIN (
  SELECT tmp1.p_id, tmp1.points, g.game_time
  FROM (
    SELECT pgs.p_id, pgs.points, pgs.g_id
    FROM player_game_stats AS pgs
    WHERE pgs.points > 39) AS tmp1
  INNER JOIN game AS g
  ON tmp1.g_id = g.g_id
) AS tmp2
ON p.p_id = tmp2.p_id

-- 3
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
