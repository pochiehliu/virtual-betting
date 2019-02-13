-- stand alone table
CREATE TABLE player(
	p_id int PRIMARY KEY,
	name text NOT NULL
);

CREATE TABLE team(
	t_id int PRIMARY KEY,
	name text UNIQUE NOT NULL
);

CREATE TABLE arena (
	a_id int PRIMARY KEY,
	name text NOT NULL,
	capacity int NOT NULL,
	CHECK (capacity > 0)
);

CREATE TABLE match(
	m_id int PRIMARY KEY,
	match_data date NOT NULL
);

CREATE TABLE user (
	u_id int PRIMARY KEY,
	name text NOT NULL,
	join_date date NOT NULL
);

CREATE TABLE referee (
	r_id int PRIMARY KEY,
	name text NOT NULL
);

CREATE TABLE sportbook (
	sb_id int PRIMARY KEY,
	name text NOT NULL
)

-- RELATIONSHIP TABLE
CREATE TABLE sb_match_odds(
	m_id REFERENCES match (m_id) ON DELETE CASCADE,
	sb_id REFERENCES sportbook (sb_id) ON DELETE CASCADE,
	PRIMARY KEY (m_id, sb_id),
	h_odds int NOT NULL,
	h_lines int NOT NULL,
	a_odds int NOT NULL,
	a_lines int NOT NULL
);

CREATE TABLE place_bet(
	b_id int PRIMARY KEY,
	u_id int REFERENCES user (u_id) ON DELETE CASCADE,
	m_id int REFERENCES match (m_id) ON DELETE CASCADE,
	sb_id int REFERENCES sportbook (sb_id) ON DELETE CASCADE,
	bet_how_much_on_lines int,
	bet_how_much_on_spreads int 
);


CREATE TABLE match_arena (
	m_id int REFERENCES match (m_id) ON DELETE CASCADE,
	a_id int REFERENCES arena (a_id),
	attendance int NOT NULL,
	PRIMARY KEY (m_id)
);

CREATE TABLE match_statistic (
	m_id int REFERENCES match (m_id) ON DELETE CASCADE,
	home_game_statistics int,
	away_game_statistics int,
	PRIMARY KEY (m_id)
);

CREATE TABLE match_referee (
	m_id REFERENCES match (m_id) ON DELETE CASCADE,
	r_id REFERENCES referee (r_id),
	PRIMARY KEY (m_id)
);

CREATE TABLE team_player_match_tri_relationship (
	ma_id REFERENCES match (m_id) ON DELETE CASCADE,
	away_t_id REFERENCES team (t_id),
	home_t_id REFERENCES team (t_id),
	CHECK (away_team_id != home_team_id),
	h_p1_id NOT NULL REFERENCES player (p_id),
	h_p2_id NOT NULL REFERENCES player (p_id),
	h_p3_id NOT NULL REFERENCES player (p_id),
	h_p4_id NOT NULL REFERENCES player (p_id),
	h_p5_id NOT NULL REFERENCES player (p_id),
	h_p6_id REFERENCES player (p_id),
	a_p1_id NOT NULL REFERENCES player (p_id),
	a_p2_id NOT NULL REFERENCES player (p_id),
	a_p3_id NOT NULL REFERENCES player (p_id),
	a_p4_id NOT NULL REFERENCES player (p_id),
	a_p5_id NOT NULL REFERENCES player (p_id),
	a_p6_id REFERENCES player (p_id),
	CHECK (all 12 players are unique)
);


-- this table is redundent because we can extract 
-- this info from match table
CREATE TABLE player_in_team(
	p_id int NOT NULL REFERENCES player (p_id),
	current_t_id int NOT NULL REFERENCES team (t_id),
	previous_t_id int REFERENCES team (t_id),
	CHECK (current_t_id != previous_t_id),
	PRIMARY KEY (p_id, current_t_id)
);











