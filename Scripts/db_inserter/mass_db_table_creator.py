"""
This file takes all the existing data from each individual
CSV file, and merges them to create the CSV files that can
be directly imported into the postgresql database.
"""

import pandas as pd
import os
from scraping.merger import merge


DATA_DIR = './../../Data/'


def team():
    bask_ref = merge(DATA_DIR + 'bask_ref_csvs/', 'game')
    sbr = merge(DATA_DIR, 'sbr_team')

    def get_short(long):
        return bask_ref.loc[bask_ref.home_name == long].iloc[0].game_id[-3:]

    def get_med(long):
        for t in sbr.home.unique():
            if t.split()[-1] in long:
                return t
        return 'NULL'

    team_df = pd.DataFrame(data={'name': bask_ref.home_name.unique()})
    team_df['short'] = team_df[['name']].applymap(get_short)
    team_df['sbr_name'] = team_df[['name']].applymap(get_med)

    team_df.to_csv(DATA_DIR + 'db_inserts/team.csv', index_label='t_id')


def player():
    bask_ref = merge(DATA_DIR + 'bask_ref_csvs/', 'player')
    names = bask_ref.name.unique()
    first = pd.Series(names).apply(lambda n: n.split()[0])
    last = pd.Series(names).apply(lambda n: ' '.join(n.split()[1:]))
    player_df = pd.DataFrame(data={'first_name': first, 'last_name': last})
    player_df.to_csv(DATA_DIR + 'db_inserts/player.csv', index_label='p_id')


def users():
    pass


def sportsbook():
    pass


def bet_type():
    pass


def game():
    teams = pd.read_csv(DATA_DIR + 'db_inserts/team.csv')
    teams = dict(zip(teams.name, teams.t_id))
    bask_ref = merge(DATA_DIR + 'bask_ref_csvs/', 'game')

    def get_t_id(name):
        return teams[name]

    game_df = pd.DataFrame(data={'g_id': bask_ref.game_id,
                                 'game_time': bask_ref.date.apply(lambda d: d.replace(',', ''))})
    game_df[['t_id_home', 't_id_away']] = bask_ref[['home_name', 'away_name']].applymap(get_t_id)

    scores = ["home_q1_score", "home_q2_score", "home_q3_score", "home_q4_score",
              "away_q1_score", "away_q2_score", "away_q3_score", "away_q4_score",
              "home_ot_score", "away_ot_score"]

    game_df[scores] = bask_ref[['home_q1', 'home_q2', 'home_q3', 'home_q4',
                                'away_q1', 'away_q2', 'away_q3', 'away_q4',
                                'home_ot', 'away_ot']]
    game_df.home_ot_score[game_df.home_ot_score == 0] = 'NULL'
    game_df.away_ot_score[game_df.away_ot_score == 0] = 'NULL'

    game_df.to_csv(DATA_DIR + 'db_inserts/game.csv', index=False)


def player_game_stats():
    players = pd.read_csv(DATA_DIR + 'db_inserts/player.csv')
    players = dict(zip(players.first_name + ' ' + players.last_name, players.p_id))
    teams = pd.read_csv(DATA_DIR + 'db_inserts/team.csv')
    teams = dict(zip(teams.name, teams.t_id))
    bask_ref = merge(DATA_DIR + 'bask_ref_csvs/', 'player')

    def get_p_id(name):
        return players[name]

    def get_t_id(name):
        return teams[name]

    def get_mp(m):
        return int(m.split(':')[0]) + int(m.split(':')[1])/60

    player_df = pd.DataFrame(data={'g_id': bask_ref.game_id})
    player_df[['p_id']] = bask_ref[['name']].applymap(get_p_id)
    player_df[['t_id']] = bask_ref[['team']].applymap(get_t_id)
    player_df[['minutes_played']] = bask_ref[['mp']].applymap(get_mp)

    stat = ["field_goals_made", "field_goal_attempts", "three_pointers_made",
            "three_point_attempts", "free_throws_made", "free_throw_attempts",
            "offensive_rebounds", "defensive_rebounds", "assists", "steals",
            "blocks", "turnovers", "personal_fouls", "points", "plus_minus",
            "offensive_rebound_percentage", "defensive_rebound_percentage",
            "total_rebound_percentage", "assist_percentage", "steal_percentage",
            "block_percentage", "turnover_percentage", "usage_percentage",
            "offensive_rating", "defensive_rating"]
    player_df[stat] = bask_ref[['fg', 'fga', 'tp', 'tpa', 'ft', 'fta',
                                'orb', 'drb', 'ast', 'stl', 'blk', 'tov',
                                'pf', 'pts', 'pm', 'orbp', 'drbp', 'trbp',
                                'astp', 'stlp', 'blkp', 'tovp', 'usgp',
                                'ortg', 'drtg']]
    player_df.fillna(0, inplace=True)
    player_df.to_csv(DATA_DIR + 'db_inserts/player_game_stats.csv', index=False)


"""
CREATE TABLE make_odds (
    o_id int PRIMARY KEY,
    g_id VARCHAR (12) REFERENCES game (g_id) ON DELETE NO ACTION,
    sb_id int REFERENCES sportsbook (sb_id) ON DELETE NO ACTION,
    bt_id int REFERENCES bet_type (bt_id) ON DELETE NO ACTION,
    odds_time timestamp NOT NULL, -- sportsbook will update odds intermittently
    odds_side char NOT NULL,
    CHECK (
        odds_side = 'H' OR
        odds_side = 'V' OR
        odds_side = 'O' OR
        odds_side = 'U'
    ),
    UNIQUE (g_id, sb_id, bt_id, odds_time, odds_side),
    odds_payout float NOT NULL,
    CHECK (odds_payout >= 1), -- will use decimal odds
    odds_line float NOT NULL
);
"""


def make_odds():
    # TODO
    pass


def place_bet():
    pass


def main():
    pass


if __name__ == '__main__':
    os.chdir('Scripts/db_inserter/')
    main()


