-- stand alone table
CREATE TABLE player(
	p_id int PRIMARY KEY,
	first_name text NOT NULL,
	last_name text NOT NULL,
	-- IS-A
	position text NOT NULL,
	CHECK (position = 'PG' OR 
		   position = 'SG' OR
		   position = 'C' OR
		   position = 'PF' OR
		   position = 'SF'),
	-- IS-A shooting hand 
	shooting_hand text NOT NULL,
	CHECK (shooting_hand = 'L' OR shooting_hand = 'R')
);

CREATE TABLE team(
	t_id int PRIMARY KEY,
	name text UNIQUE NOT NULL,
	city text UNIQUE NOT NULL
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

CREATE TABLE sportbook (
	sb_id int PRIMARY KEY,
	name text NOT NULL
);

-- aggreage by matches
CREATE TABLE matches(
	m_id int PRIMARY KEY,
    match_time timestamp NOT NULL,
	a_id int REFERENCES arena (a_id),
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
	home_extend_score int NOT NULL,
	away_extend_score int NOT NULL,
	CHECK (
		home_q1_score >= 0 AND
		home_q2_score >= 0 AND
		home_q3_score >= 0 AND
		home_q4_score >= 0 AND
		home_extend_score >= 0 AND
		away_q1_score >= 0 AND
		away_q2_score >= 0 AND
		away_q3_score >= 0 AND
		away_q4_score >= 0 AND
		away_extend_score >= 0
		)
);

CREATE TABLE match_referee (
	m_id int REFERENCES match (m_id) ON DELETE CASCADE,
	r_id int REFERENCES referee (r_id),
	PRIMARY KEY (m_id, r_id)
);

CREATE TABLE match_player_stats (
	m_id int REFERENCES match (m_id) ON DELETE CASCADE,
	p_id int REFERENCES player (p_id),
	PRIMARY KEY (m_id, p_id),
	t_id int REFERENCES team (t_id),
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
  	Defensive_rating int NOT NULL
);

-- sportbook attr table
CREATE TABLE sb_match_odds(
	m_id int REFERENCES match (m_id) ON DELETE CASCADE,
	sb_id int REFERENCES sportbook (sb_id) ON DELETE CASCADE,
    odds_time timestamp, -- sportbook will update odds from timet to time
	PRIMARY KEY (m_id, sb_id, odds_time), 
	h_money_line int,
	a_money_line int,
	h_spread int,
	a_spread int,
	over_line int,
	under_line int,
	spread_value int,
	over_under_value int
);

-- user bet table
CREATE TABLE place_bet(
	b_id int PRIMARY KEY,
	u_id int REFERENCES users (u_id) ON DELETE CASCADE,
	m_id int REFERENCES matches (m_id) ON DELETE CASCADE,
	sb_id int REFERENCES sportbook (sb_id) ON DELETE CASCADE,
	bet_time timestamp NOT NULL,
	bet_h_money_line int,
	bet_a_money_line int,
	bet_h_spread int,
	bet_a_spread int,
	bet_over_line int,
	bet_under_line int,
	bet_spread_value int,
	bet_over_under_value int
);