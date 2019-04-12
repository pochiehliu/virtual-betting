CREATE TABLE player (
	p_id int SERIAL PRIMARY KEY,
	first_name text NOT NULL,
	last_name text NOT NULL
);

CREATE TABLE team (
	t_id int PRIMARY KEY,
	name text NOT NULL,
	short VARCHAR (3) NOT NULL,
	sbr_name text
);

CREATE TABLE users (
	u_id SERIAL int PRIMARY KEY ,
	username text UNIQUE NOT NULL,
	first_name text NOT NULL,
	last_name text NOT NULL,
	password text NOT NULL,
	balance float NOT NULL
);

CREATE TABLE sportsbook (
	sb_id int PRIMARY KEY,
	name text NOT NULL
);

CREATE TABLE bet_type (
	bt_id int PRIMARY KEY,
	name text NOT NULL
);

CREATE TABLE game (
	g_id VARCHAR (12) PRIMARY KEY,
	game_time timestamp NOT NULL,
	t_id_home int REFERENCES team (t_id),
	t_id_away int REFERENCES team (t_id),
	CHECK (t_id_home != t_id_away)
);

CREATE TABLE game_stats(
	g_id VARCHAR (12) REFERENCES game (g_id),
	PRIMARY KEY (g_id),
	home_q1_score int NOT NULL,
	home_q2_score int NOT NULL,
	home_q3_score int NOT NULL,
	home_q4_score int NOT NULL,
	away_q1_score int NOT NULL,
	away_q2_score int NOT NULL,
	away_q3_score int NOT NULL,
	away_q4_score int NOT NULL,
	home_ot_score int,
	away_ot_score int,
	CHECK (
		home_q1_score >= 0 AND
		home_q2_score >= 0 AND
		home_q3_score >= 0 AND
		home_q4_score >= 0 AND
		away_q1_score >= 0 AND
		away_q2_score >= 0 AND
		away_q3_score >= 0 AND
		away_q4_score >= 0
  ),
	CHECK (
		home_ot_score >= 0 OR
		home_ot_score IS NULL
  ),
	CHECK (
		away_ot_score >= 0 OR
		away_ot_score IS NULL
  )
);

CREATE TABLE player_game_stats (
	g_id VARCHAR (12) REFERENCES game (g_id) ON DELETE NO ACTION,
	p_id int REFERENCES player (p_id) ON DELETE NO ACTION,
	PRIMARY KEY (g_id, p_id),
	t_id int REFERENCES team (t_id),
	minutes_played float NOT NULL,
	field_goals_made int NOT NULL,
	field_goal_attempts int NOT NULL,
	three_pointers_made int NOT NULL,
	three_point_attempts int NOT NULL,
	free_throws_made int NOT NULL,
	free_throw_attempts int NOT NULL,
	offensive_rebounds int NOT NULL,
	defensive_rebounds int NOT NULL,
	assists int NOT NULL,
	steals int NOT NULL,
	blocks int NOT NULL,
	turnovers int NOT NULL,
	personal_fouls int NOT NULL,
	points int NOT NULL,
	plus_minus int NOT NULL,
	offensive_rebound_percentage float NOT NULL,
	defensive_rebound_percentage float NOT NULL,
	total_rebound_percentage float NOT NULL,
	assist_percentage float NOT NULL,
	steal_percentage float NOT NULL,
	block_percentage float NOT NULL,
	turnover_percentage float NOT NULL,
	usage_percentage float NOT NULL,
	offensive_rating int NOT NULL,
	defensive_rating int NOT NULL,
	CHECK (
		field_goals_made <= field_goal_attempts AND
		three_pointers_made <= three_point_attempts AND
		free_throws_made <= free_throw_attempts
	),
	CHECK (
		minutes_played >= 0 AND
		field_goals_made >= 0 AND
		field_goal_attempts >= 0 AND
		three_pointers_made >= 0 AND
		three_point_attempts >= 0 AND
		free_throws_made >= 0 AND
		free_throw_attempts >= 0 AND
		offensive_rebounds >= 0 AND
		defensive_rebounds >= 0 AND
		steals >= 0 AND
		blocks >= 0 AND
		turnovers >= 0 AND
		personal_fouls >= 0 AND
		points >= 0 AND
		offensive_rebound_percentage >= 0 AND
		offensive_rebound_percentage <= 100 AND
		defensive_rebound_percentage >= 0 AND
		defensive_rebound_percentage <= 100 AND
		total_rebound_percentage >= 0 AND
		total_rebound_percentage <= 100 AND
		assist_percentage >= 0 AND
		assist_percentage <= 100 AND
		steal_percentage >= 0 AND
		steal_percentage <= 100 AND
		block_percentage >= 0 AND
		block_percentage <= 100 AND
		turnover_percentage >= 0 AND
		turnover_percentage <= 100 AND
		usage_percentage >= 0 AND
		usage_percentage <= 100 AND
		offensive_rating >= 0 AND
		defensive_rating >= 0
	)
);

CREATE TABLE make_odds (
	o_id int SERIAL PRIMARY KEY,
	g_id VARCHAR (12) REFERENCES game (g_id),
	sb_id int REFERENCES sportsbook (sb_id) ON DELETE NO ACTION,
	bt_id int REFERENCES bet_type (bt_id) ON DELETE NO ACTION,
	odds_time timestamp NOT NULL,
	odds_side char NOT NULL,
	CHECK (
		odds_side = 'H' OR
		odds_side = 'V' OR
		odds_side = 'O' OR
		odds_side = 'U'
	),
	UNIQUE (g_id, sb_id, bt_id, odds_time, odds_side),
	odds_payout float NOT NULL,
	CHECK (odds_payout >= 1),
	odds_line float NOT NULL
);

CREATE TABLE place_bet(
	o_id int REFERENCES make_odds (o_id),
	bet_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	u_id int REFERENCES users (u_id) ON DELETE NO ACTION,
	bet_size decimal(12, 2) NOT NULL,
	CHECK (bet_size > 0),
	PRIMARY KEY (o_id, bet_time, u_id)
);
