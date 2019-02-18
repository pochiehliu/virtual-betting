-- stand alone tables
CREATE TABLE player (
	p_id int PRIMARY KEY,
	first_name text NOT NULL,
	last_name text NOT NULL,
	dob date NOT NULL,
	posit text NOT NULL,
	CHECK (posit = 'PG' OR
		   posit = 'SG' OR
		   posit = 'C' OR
		   posit = 'PF' OR
		   posit = 'SF'),
	shooting_hand char NOT NULL,
	CHECK (shooting_hand = 'L' OR shooting_hand = 'R')
);


CREATE TABLE team (
	t_id int PRIMARY KEY,
	name text NOT NULL,
	city text NOT NULL
);


CREATE TABLE arena (
	a_id int PRIMARY KEY,
	name text NOT NULL,
	capacity int NOT NULL,
	CHECK (capacity > 0)
);


CREATE TABLE users (
	u_id int PRIMARY KEY,
	name text NOT NULL
);


CREATE TABLE referee (
	r_id int PRIMARY KEY,
	name text NOT NULL
);


CREATE TABLE sportsbook (
	sb_id int PRIMARY KEY,
	name text NOT NULL
);


CREATE TABLE game(
	g_id int PRIMARY KEY,
    game_time timestamp NOT NULL,
    -- ARENA
	a_id int REFERENCES arena (a_id),
	attendance int NOT NULL,
	--- REF
	r_id int REFERENCES referee (r_id),
	-- HOME & AWAY
	t_id_home int REFERENCES team (t_id),
	t_id_away int REFERENCES team (t_id),
	CHECK (t_id_home != t_id_away),
	home_q1_score int NOT NULL,
	home_q2_score int NOT NULL,
	home_q3_score int NOT NULL,
	home_q4_score int NOT NULL,
	away_q1_score int NOT NULL,
	away_q2_score int NOT NULL,
	away_q3_score int NOT NULL,
	away_q4_score int NOT NULL,
	home_ot_score int NOT NULL,
	away_ot_score int NOT NULL,
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
	-- which game
	g_id int REFERENCES game (g_id) ON DELETE CASCADE,
	-- which player
	p_id int REFERENCES player (p_id),
	PRIMARY KEY (g_id, p_id),
	-- which team
	t_id int REFERENCES team (t_id),
	-- stats
	Minutes_played int NOT NULL,
  	Field_goals_made int NOT NULL,
  	Field_goal_attempts int NOT NULL, 
  	Three_pointers_made int NOT NULL,
  	Three_point_attempts int NOT NULL,
  	Free_throws_made int NOT NULL,
  	Free_throw_attempts int NOT NULL,
  	Offensive_rebounds int NOT NULL,
  	Defensive_rebounds int NOT NULL,
  	Assists int NOT NULL,
  	Steals int NOT NULL,
  	Blocks int NOT NULL,
  	Turnovers int NOT NULL,
  	Personal_fouls int NOT NULL,
  	Points int NOT NULL,
  	Plus_minus int NOT NULL,
  	Offensive_rebound_percentage int NOT NULL,
  	Defensive_rebound_percentage int NOT NULL,
  	Total_rebound_percentage int NOT NULL,
  	Assist_percentage int NOT NULL, 
  	Steal_percentage int NOT NULL,
  	Block_percentage int NOT NULL,
  	Turnover_percentage int NOT NULL,
  	Usage_percentage int NOT NULL,
  	Offensive_rating int NOT NULL,
  	Defensive_rating int NOT NULL,
  	CHECK (
  	    Field_goals_made <= Field_goal_attempts AND
  	    Three_pointers_made <= Three_point_attempts AND
  	    Free_throws_made <= Free_throw_attempts
  	    ),
  	CHECK (
  	    Minutes_played >= 0 AND
  	    Field_goals_made >= 0 AND
  	    Field_goal_attempts >= 0 AND
  	    Three_pointers_made >= 0 AND
  	    Three_point_attempts >= 0 AND
  	    Free_throws_made >= 0 AND
  	    Free_throw_attempts >= 0 AND
  	    Offensive_rebounds >= 0 AND
  	    Defensive_rebounds >= 0 AND
  	    Steals >= 0 AND
  	    Blocks >= 0 AND
  	    Turnovers >= 0 AND
  	    Personal_fouls >= 0 AND
  	    Points >= 0 AND
  	    Offensive_rebound_percentage >= 0 AND
  	    Defensive_rebound_percentage >= 0 AND
  	    Total_rebound_percentage >= 0 AND
  	    Assist_percentage >= 0 AND
  	    Steal_percentage >= 0 AND
        Block_percentage >= 0 AND
  	    Turnover_percentage >= 0 AND
  	    Usage_percentage >= 0 AND
  	    Offensive_rating >= 0 AND
  	    Defensive_rating >= 0
  	    )
);

-- sportsbook table
CREATE TABLE moneyline (
	ml_id int PRIMARY KEY,
	g_id int REFERENCES game (g_id),
	sb_id int REFERENCES sportsbook (sb_id),
	odds_time timestamp NOT NULL,
	-- moneyline
	h_money_line int NOT NULL,
	a_money_line int NOT NULL,
);

CREATE TABLE spread (
	sp_id int PRIMARY KEY,
	g_id int REFERENCES game (g_id),
	sb_id int REFERENCES sportsbook (sb_id),
	odds_time timestamp NOT NULL,
	-- moneyline
	h_spread int NOT NULL,
	a_spread int NOT NULL,
	spread_value int,
);

CREATE TABLE overunder (
	ou_id int PRIMARY KEY,
	g_id int REFERENCES game (g_id),
	sb_id int REFERENCES sportsbook (sb_id),
	odds_time timestamp NOT NULL,
	-- moneyline
	over_line int,
	under_line int,
	over_under_value int
);

CREATE TABLE place_bet(
	b_id int PRIMARY KEY,
	u_id int REFERENCES users (u_id) ON DELETE CASCADE,
	g_id int REFERENCES game (g_id) ON DELETE CASCADE,
	sb_id int REFERENCES sportsbook (sb_id) ON DELETE CASCADE,
	bet_time timestamp NOT NULL,
	-- what type of bet
	bet_type text NOT NULL,
	CHECK (bet_type = 'overunder' or 
		   bet_type = 'spread' or
		   bet_type = 'moneyline' or
		   bet_type = 'multi')
);

CREATE TABLE moneyline_bet (
	m_b_id PRIMARY KEY,
	b_id int REFERENCES place_bet (b_id),
	bet_type text REFERENCES place_bet (bet_type),
	CHECK (bet_type = 'moneyline' or bet_type = 'multi'),
	bet_h_money_line int,
	bet_a_money_line int,
	bet_money_line_value int
);

CREATE TABLE spread_bet (
	s_b_id PRIMARY KEY,
	b_id int REFERENCES place_bet (b_id),
	bet_type text REFERENCES place_bet (bet_type),
	CHECK (bet_type = 'spread' or bet_type = 'multi'),
	bet_h_spread int,
	bet_a_spread int,
	bet_spread_value int
);

CREATE TABLE ou_bet (
	ou_b_id PRIMARY KEY,
	b_id int REFERENCES place_bet (b_id),
	bet_type text REFERENCES place_bet (bet_type),
	CHECK (bet_type = 'overunder' or bet_type = 'multi'),
	bet_over_line int,
	bet_under_line int,
	bet_over_under_value int
)