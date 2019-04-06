"""
This file takes all the existing data from each individual
CSV file, and merges them to create the CSV files that can
be directly imported into the postgresql database.

This saves the trouble of having to make INSERT statements
for every previously scraped entry.

Creates the following tables:
    1. team
    2. player
    3. game
    4. game_stats
    5. player_game_stats
    6. make_odds

Takes about 30 seconds to run for ALL CSVs. Puts all CSVs in
'./../../Data/db_inserts/'
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


def player(update_db=False, names=None):
    if not update_db:
        names = merge(DATA_DIR + 'bask_ref_csvs/', 'player').name.unique()
    first = pd.Series(names).apply(lambda n: n.split()[0])
    last = pd.Series(names).apply(lambda n: ' '.join(n.split()[1:]))
    player_df = pd.DataFrame(data={'first_name': first, 'last_name': last})
    if not update_db:
        player_df.to_csv(DATA_DIR + 'db_inserts/player.csv', index_label='p_id')
    else:
        return player_df


def game(update_db=False, games=None):
    if not update_db:
        games = merge(DATA_DIR + 'bask_ref_csvs/', 'game')
    teams = pd.read_csv(DATA_DIR + 'db_inserts/team.csv')
    teams = dict(zip(teams.name, teams.t_id))

    gdf = pd.DataFrame(data={'g_id': games.game_id,
                             'game_time': games.date.apply(lambda d: d.replace(',', ''))})
    gdf[['t_id_home', 't_id_away']] = games[['home_name', 'away_name']].applymap(lambda x: teams[x])

    if not update_db:
        gdf.to_csv(DATA_DIR + 'db_inserts/game.csv', index=False)
    else:
        return gdf


def game_stats(update_db=False, games=None):
    if not update_db:
        games = merge(DATA_DIR + 'bask_ref_csvs/', 'game')

    gdf = pd.DataFrame(data={'g_id': games.game_id,
                             'home_q1_score': games.home_q1,
                             'home_q2_score': games.home_q2,
                             'home_q3_score': games.home_q3,
                             'home_q4_score': games.home_q4,
                             'away_q1_score': games.away_q1,
                             'away_q2_score': games.away_q2,
                             'away_q3_score': games.away_q3,
                             'away_q4_score': games.away_q4,
                             'home_ot_score': games.home_ot,
                             'away_ot_score': games.away_ot})

    gdf.loc[(gdf.home_ot_score == 0) & (gdf.away_ot_score == 0), ['home_ot_score', 'away_ot_score']] = 'NULL'
    if not update_db:
        gdf.to_csv(DATA_DIR + 'db_inserts/game_stats.csv', index=False)
    else:
        return gdf


def player_game_stats(update_db=False, player_stats=None):
    if not update_db:
        player_stats = merge(DATA_DIR + 'bask_ref_csvs/', 'player')
    players = pd.read_csv(DATA_DIR + 'db_inserts/player.csv')
    players = dict(zip(players.first_name + ' ' + players.last_name, players.p_id))
    teams = pd.read_csv(DATA_DIR + 'db_inserts/team.csv')
    teams = dict(zip(teams.name, teams.t_id))

    pdf = pd.DataFrame(data={'g_id': player_stats.game_id})
    pdf['p_id'] = player_stats.name.map(lambda p_name: players[p_name])
    pdf['t_id'] = player_stats.team.map(lambda t_name: teams[t_name])
    pdf['minutes_played'] = player_stats.mp.map(lambda x: int(x.split(':')[0]) + int(x.split(':')[1])/60)

    stat = ["field_goals_made", "field_goal_attempts", "three_pointers_made",
            "three_point_attempts", "free_throws_made", "free_throw_attempts",
            "offensive_rebounds", "defensive_rebounds", "assists", "steals",
            "blocks", "turnovers", "personal_fouls", "points", "plus_minus",
            "offensive_rebound_percentage", "defensive_rebound_percentage",
            "total_rebound_percentage", "assist_percentage", "steal_percentage",
            "block_percentage", "turnover_percentage", "usage_percentage",
            "offensive_rating", "defensive_rating"]

    pdf[stat] = player_stats[['fg', 'fga', 'tp', 'tpa', 'ft', 'fta',
                                'orb', 'drb', 'ast', 'stl', 'blk', 'tov',
                                'pf', 'pts', 'pm', 'orbp', 'drbp', 'trbp',
                                'astp', 'stlp', 'blkp', 'tovp', 'usgp',
                                'ortg', 'drtg']]
    pdf.fillna(0, inplace=True)

    if not update_db:
        pdf.to_csv(DATA_DIR + 'db_inserts/player_game_stats.csv', index=False)
    else:
        return pdf


def make_odds(update_db=False, sbr_bets=None, sbr_names=None):
    if not update_db:
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

    def fix_side(x):
        if x.bt_id != 2:
            return x.odds_side
        else:
            return 'O' if x.odds_side == 'V' else 'U'

    sbr['short'] = sbr.apply(lambda x: get_short(x), axis=1)
    sbr['g_id'] = sbr.date.astype(str) + '0' + sbr.short
    sbr = pd.merge(left=sbr, right=bask_ref, how='inner', on=['g_id'], validate='m:1')
    sbr = sbr.loc[sbr.length == 'full']

    # stacking arrays from scratch
    sb_id = pd.Series(np.repeat(1, len(sbr) * 2))
    lines = sbr.ab1.append(sbr.hb1)
    side = pd.Series(np.repeat('V', len(sbr))).append(pd.Series(np.repeat('H', len(sbr))))
    g_id = sbr.g_id.append(sbr.g_id)
    bets = sbr.bet.append(sbr.bet)
    times = sbr.game_time.append(sbr.game_time)
    for i in range(2, 11):
        sb_id = sb_id.append(pd.Series(np.repeat(i, len(sbr) * 2)))
        for s in ['ab', 'hb']:
            lines = lines.append(sbr[s + str(i)])
            side = side.append(pd.Series(np.repeat('V' if s == 'ab' else 'H', len(sbr))))
            bets = bets.append(sbr.bet)
            times = times.append(sbr.game_time)
            g_id = g_id.append(sbr.g_id)

    odds_df = pd.DataFrame(data={'g_id': g_id.values,
                                 'sb_id': sb_id.values,
                                 'bt_id': bets.values,
                                 'odds_time': times.values,
                                 'odds_side': side.values,
                                 'odds_payout': lines.values,
                                 'odds_line': lines.values})

    odds_df.bt_id = odds_df.bt_id.map(lambda b: 3 if b == 'p' else (2 if b == 't' else 1))
    odds_df.odds_payout = odds_df.apply(lambda x: get_lines(x, True), axis=1)
    odds_df.odds_line = odds_df.apply(lambda x: get_lines(x, False), axis=1)
    odds_df.dropna(how='any', inplace=True)
    odds_df.odds_payout = odds_df.odds_payout.map(odds_convert)
    odds_df.reset_index(drop=True, inplace=True)
    odds_df.odds_side = odds_df.apply(lambda x: fix_side(x), axis=1)

    if not update_db:
        odds_df.to_csv(DATA_DIR + 'db_inserts/make_odds.csv', index_label='o_id')
    else:
        return odds_df


def main():
    # team()
    player()
    game()
    game_stats()
    # player_game_stats()
    # make_odds()


if __name__ == '__main__':
    if 'README.md' in os.listdir('.'):
        os.chdir('Scripts/db_inserter/')
    main()


