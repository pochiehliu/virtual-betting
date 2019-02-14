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
)

-- aggreage by match

CREATE TABLE matches(
	m_id int PRIMARY KEY,
	match_date date NOT NULL,
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
	away_extend_score int NOT NULL
);


CREATE TABLE match_referee (
	m_id int REFERENCES match (m_id) ON DELETE CASCADE,
	r_id int REFERENCES referee (r_id),
	PRIMARY KEY (m_id, r_id)
);

CREATE TABLE match_player_stats (
	m_id int REFERENCES match (m_id) ON DELETE CASCADE,
	p_id int REFERENCES player (p_id),
	PRIMARY KEY (m_id, p_id)
	t_id int REFERENCES team (t_id),
	Minutes_played int,
  	Field_goals_made int,
  	Field_goal_attempts int, 
  	Three_pointers_made int,
  	Three_point_attempts int,
  	Free_throws_made int,
  	Free_throw_attempts int,
  	Offensive_rebounds int,
  	Defensive_rebounds int,
  	Assists int,
  	Steals int,
  	Blocks int,
  	Turnovers int,
  	Personal_fouls int,
  	Points int,
  	Plus_minus int,
  	Offensive_rebound_percentage int,
  	Defensive_rebound_percentage int,
  	Total_rebound_percentage int,
  	Assist_percentage int, 
  	Steal_percentage int,
  	Block_percentage int,
  	Turnover_percentage int,
  	Usage_percentage int,
  	Offensive_rating int,
  	Defensive_rating int
);

-- sportbook attr table
CREATE TABLE sb_match_odds(
	m_id REFERENCES match (m_id) ON DELETE CASCADE,
	sb_id REFERENCES sportbook (sb_id) ON DELETE CASCADE,
	PRIMARY KEY (m_id, sb_id),
	h_money_line int NOT NULL,
	a_money_line int NOT NULL,
	h_spread int NOT NULL,
	a_spread int NOT NULL,
	over_line int NOT NULL,
	under_line int NOT NULL,
	spread_value int NOT NULL,
	over_under_value int NOT NULL
);

-- user bet table
CREATE TABLE place_bet(
	b_id int PRIMARY KEY,
	u_id int REFERENCES user (u_id) ON DELETE CASCADE,
	m_id int REFERENCES match (m_id) ON DELETE CASCADE,
	sb_id int REFERENCES sportbook (sb_id) ON DELETE CASCADE,
	bet_h_money_line int,
	bet_a_money_line int,
	bet_h_spread int,
	bet_a_spread int,
	bet_over_line int,
	bet_under_line int,
	bet_spread_value int,
	bet_over_under_value int
);