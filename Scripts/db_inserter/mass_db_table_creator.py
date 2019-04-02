"""
This file takes all the existing data from each individual
CSV file, and merges them to create the CSV files that can
be directly imported into the postgresql database.
"""

import pandas as pd
import numpy as np
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


def make_odds():
    sbr_bets = merge(DATA_DIR + 'sbr_csvs/', '')
    sbr_names = merge(DATA_DIR, 'sbr_team').drop(['time'], axis=1)
    sbr = pd.merge(left=sbr_bets, right=sbr_names, how='inner', on=['date', 'game_num'], validate='m:1')
    bask_ref = pd.read_csv(DATA_DIR + 'db_inserts/game.csv')[['g_id', 'game_time']]
    teams = pd.read_csv(DATA_DIR + 'db_inserts/team.csv')
    teams = dict(zip(teams.sbr_name, teams.short))

    # functions that decode the betting lines, returns as string tuple (payout, odds_line)
    def get_total(line, pay):
        if line == '-':
            return np.nan
        sign = '-' if '-' in line else ('+' if '-' in line else np.nan)
        if pd.isnull(sign):
            return np.nan
        return sign + line.split(sign)[1] if pay else line.split(sign)[0].replace('½', '.5')

    def get_spread(line, pay):
        if line == '-':
            return np.nan
        sign = '-' if '-' in line[1:] else ('+' if '-' in line[1:] else np.nan)
        if pd.isnull(sign):
            return np.nan
        return sign + line[1:].split(sign)[1] if pay else line[0] + line[1:].split(sign)[0].replace('½', '.5')

    def get_ml(line, pay):
        if line == '-':
            return np.nan
        else:
            return line if pay else 0

    def odds_convert(line):
        if pd.isnull(line):
            return line
        else:
            line = int(line)
            return round(100 / (line * -1) + 1, 3) if line < 0 else round((line / 100) + 1, 3)

    def get_lines(l, pay):
        stat = l.odds_payout if pay else l.odds_line
        return get_ml(stat, pay) if l.bt_id == 1 else (get_total(stat, pay) if l.bt_id == 2 else get_spread(stat, pay))

    def get_bt_id(b):
        return 3 if b == 'p' else (2 if b == 't' else 1)

    def get_short(n):
        if n.home not in ['Charlotte', 'Brooklyn', 'New Orleans']:
            return teams[n.home]
        else:
            if n.home == 'Charlotte':
                return 'CHA' if n.date < 20140701 else 'CHO'
            elif n.home == 'Brooklyn':
                return 'NJN' if n.date < 20120701 else 'BRK'
            else:
                return 'NOP' if n.date > 20130701 else ('NOK' if n.date < 20070701 else 'NOH')

    sbr['short'] = sbr.apply(lambda x: get_short(x), axis=1)
    sbr['g_id'] = sbr.date.astype(str) + '0' + sbr.short
    sbr = pd.merge(left=sbr, right=bask_ref, how='inner', on=['g_id'], validate='m:1')

    # stacking arrays from scratch
    sb_id = pd.Series(np.repeat(1, 147663 * 2))
    for i in range(2, 11):
        sb_id = sb_id.append(pd.Series(np.repeat(i, 147663 * 2)))

    bets = sbr.bet.append(sbr.bet)
    times = sbr.game_time.append(sbr.game_time)
    for i in range(2, 20):
        bets = bets.append(sbr.bet)
        times = times.append(sbr.game_time)

    lines = sbr.ab1.append(sbr.hb1)
    side = pd.Series(np.repeat('V', 147663)).append(pd.Series(np.repeat('H', 147663)))
    for i in range(2, 11):
        for s in ['ab', 'hb']:
            lines = lines.append(sbr[s + str(i)])
            side = side.append(pd.Series(np.repeat('V' if s == 'ab' else 'H', 147663)))

    odds_df = pd.DataFrame(data={'sb_id': sb_id.values,
                                 'bt_id': bets.values,
                                 'odds_time': times.values,
                                 'odds_side': side.values,
                                 'odds_payout': lines.values,
                                 'odds_line': lines.values})

    odds_df.bt_id = odds_df[['bt_id']].applymap(get_bt_id).bt_id
    odds_df.odds_payout = odds_df.apply(lambda x: get_lines(x, True), axis=1)
    odds_df.odds_line = odds_df.apply(lambda x: get_lines(x, False), axis=1)
    odds_df.dropna(how='any', inplace=True)
    odds_df.odds_payout = odds_df.odds_payout.map(odds_convert)
    odds_df.reset_index(drop=True, inplace=True)
    odds_df.to_csv(DATA_DIR + 'db_inserts/make_odds.csv', index_label='o_id')


def place_bet():
    pass


def main():
    team()
    player()
    game()
    player_game_stats()
    make_odds()


if __name__ == '__main__':
    os.chdir('Scripts/db_inserter/')
    main()


