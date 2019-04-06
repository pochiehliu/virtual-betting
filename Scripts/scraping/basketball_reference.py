"""
This program will scrape basketball reference and create a player
data base with game stats for all players in all games. This is then
saved to a CSV file.

It must be run from command line and given argument of either 'full'
or 'update', where the former will do a complete scraping from Oct
2000 to present, and update will only get data from days since our
most recent data.

NOTE - The naming convention for the CSV files is "*_<MONTH><YEAR>",
       where * is either 'player' or 'game', MONTH is the three
       letter abbreviation, and YEAR is in the format YYYY.
"""
import pandas as pd
import numpy as np
from multiprocessing import Pool
import sys
import datetime as dt
import calendar
from scraping.general_tools import *

# CONSTANTS
DATA_LOC = './../../Data/bask_ref_csvs/'  # where to put data

PLAYER_COLS = ['game_id', 'date', 'season', 'team', 'opp', 'starting_five', 'name', 'mp',
               'fg', 'fga', 'fgp', 'tp', 'tpa', 'tpp', 'ft', 'fta', 'ftp',
               'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'pm',
               'tsp', 'efgp', 'tpar', 'ftr', 'orbp', 'drbp', 'trbp',
               'astp', 'stlp', 'blkp', 'tovp', 'usgp', 'ortg', 'drtg'
               ]
GAME_COLS = ['game_id', 'date', 'season', 'arena', 'away_name', 'home_name', 'att',
             'pace', 'ref1', 'ref2', 'ref3',
             'away_q1', 'away_q2', 'away_q3', 'away_q4', 'away_ot', 'away_final',
             'away_ortg', 'away_drtg',
             'home_q1', 'home_q2', 'home_q3', 'home_q4', 'home_ot', 'home_final',
             'home_ortg', 'home_drtg'
             ]


def merge_ot(score_list):
    """
    Adjusts a team's score list so overtime columns are merged together.
    :param score_list:
    :return: list of length 6 (q1-q4, OT, final)
    """
    if len(score_list) == 5:
        return score_list[:4] + ['0'] + [score_list[4]]
    else:
        new_list = score_list[:4]
        ot_sum = 0
        for ot in score_list[4:-1]:
            ot_sum += int(ot)
        return new_list + [str(ot_sum), score_list[-1]]


def get_available_games(season, month):
    # link for monthly page
    month_link = "https://www.basketball-reference.com/leagues/NBA_" + str(season) + "_games-" + month + ".html"
    month_page = get_page(month_link)

    # ensures link is valid
    if month_page is None:
        print("Failed for month link: {}".format(month_link))
        return None

    g_id = away = home = game_time = np.array([])
    for game in month_page.find_all('td', {'data-stat': "visitor_team_name"}):
        info = game['csk'].split('.')
        g_id = np.append(g_id, info[1])
        away = np.append(away, info[0])
        home = np.append(home, info[1][-3:])

    for game in month_page.find_all('tbody')[0].find_all('tr'):
        cut = game.get_text('csk').split('csk')
        t = cut[1][:-1] + ' PM '
        day = cut[0].split(', ')
        day = calendar.month_name[list(calendar.month_abbr).index(day[1][:3])] + ' ' + day[1][4:] + ' ' + day[2]
        game_time = np.append(game_time, t + day)

    return pd.DataFrame(data={'g_id': g_id,
                              'game_time': game_time,
                              't_id_home': home,
                              't_id_away': away})


def get_quarter_scores(line):
    """
    Gets the quarter scores of both teams, with overtime scores clumped into
    a single OT value after calling helper functino, merge_ot().
    :param line:
    :return: list of two lists [away_list, home_list]
    """
    num_quarters = int(list(line.children)[5].split('colspan="')[1][0]) - 1
    all_scores = []
    for team in ['away', 'home']:
        shift = 0 if team == 'away' else 1
        scores = []
        for quarter in range(num_quarters):
            scores.append(list(line.children)[5].split("'center\'>")[quarter +
                                                                     1 +
                                                                     shift *
                                                                     (num_quarters + 1)].split('<')[0])
        all_scores.append(merge_ot(scores +
                                   [list(line.children)[5].split("<strong>")[shift + 1].split('<')[0]]))
    return all_scores


def get_basic_game_info(box_page):
    """
    Gets date, arena, attendance, and team names
    :param box_page:
    :return: dictionary of basic game data
    """
    date = list(box_page.find_all(class_="scorebox_meta")[0].children)[1].get_text()
    arena = list(box_page.find_all(class_="scorebox_meta")[0].children)[2].get_text()
    away_name = box_page.find_all('title')[0].get_text().split(' at ')[0]
    home_name = box_page.find_all('title')[0].get_text().split(' at ')[1].split(' Box')[0]
    att = list(box_page.children)[3].get_text().split('Attendance:')[1].split('\n')[0].strip('\xa0')
    return {'date': date, 'arena': arena, 'away_name': away_name, 'home_name': home_name, 'att': att}


def get_refs(box_page):
    """
    Gets the three referees from the game; fills slots with 'MISSING' if there are
    less than three referees found.
    :param box_page:
    :return: list of three referee names
    """
    refs = list(box_page.children)[3].get_text().split('Officials:')[1].split('\n')[0].strip('\xa0').split(', ')
    if len(refs) != 3:
        new = []
        for i in range(3):
            try:
                new.append(refs[i])
            except IndexError:
                new.append('MISSING')
        return new
    return refs


def get_player_stats(bas, adv, num, g_id, basics, season, team):
    """
    Gets the stats for individual player
    :param bas: beautiful soup object, contains location of basic stats
    :param adv: beautiful soup object, contains location of advanced stats
    :param num: integer indicating player's location in soup
    :param g_id: game id in format YYYYMMDD0<HOME TEAM ABBREVIATION>
    :param basics: dictionary of general stats acquired in get_game_stats()
    :param season: int, season (year that championship is played in)
    :param team: string, either 'home' or 'away'
    :return: list of stats for player or None if there are no stats
    """
    # fill in first few values on in player line
    player_line = [g_id, basics['date'], season,
                   basics['away_name'] if team == 'away' else basics['home_name'],
                   basics['away_name'] if team == 'home' else basics['home_name'],
                   1 if num < 11 else 0]

    player_bas_stats, player_adv_stats = bas[num], adv[num]

    try:
        if list(player_bas_stats.children)[1].get_text()[0] not in '1234567890':
            if list(player_bas_stats.children)[1].get_text() == 'Did Not Play':
                player_line = player_line + [list(player_bas_stats.children)[0].get_text(),
                                             '0:00'] + np.zeros(33).tolist()
            else:
                player_line = None
        else:
            for stat in range(21):  # basic stats
                player_line.append(list(player_bas_stats.children)[stat].get_text())
            for stat in range(2, 16):  # advanced stats
                player_line.append(list(player_adv_stats.children)[stat].get_text())
    except IndexError:
        player_line = None

    return player_line


def get_game_stats(g_id, player_df, game_df, season):
    """
    Updates the player and game data frames with stats from a single game
    :param g_id: game id in format <YYYYMMDD>0<HOME TEAM ABBREVIATION>
    :param player_df: player data frame
    :param game_df: game data frame
    :param season: int, season (year that championship is played in)
    :return: player_df, game_df, boolean of whether month is completed
    """
    box_link = 'https://www.basketball-reference.com/boxscores/' + g_id + '.html'
    box_page = get_page(box_link)

    # all links are valid, so failing would be result of not having reached this date's games yet
    if box_page is None:
        return player_df, game_df, True

    # get basic information
    basics = get_basic_game_info(box_page)
    refs = get_refs(box_page)

    # define deeper tag; get score lines and pace
    deep = list(list(list(list(list(box_page.children)[3].children)[3].children)[1].children)[9].children)
    score_line = get_quarter_scores(deep[17])
    away_line, home_line = score_line[0], score_line[1]
    pace = list(deep[19].children)[5].split('pace" >')[2].split('<')[0]

    # beginning of full game line (row to be inserted to data base)
    full_game_line = [g_id, basics['date'], season, basics['arena'], basics['away_name'],
                      basics['home_name'], basics['att'], pace] + refs + away_line

    # get stats for home and away teams; paired with their index in the html
    for team, i in [('away', 23), ('home', 27)]:
        # define locations for basic and advanced stats
        bas = list(list(list(list(list(deep[i].children)[3].children)[1].children)[1].children)[6].children)
        adv = list(list(list(list(list(deep[i + 2].children)[3].children)[1].children)[1].children)[6].children)

        # loop every player
        for num in range(len(bas) - 1):
            if num % 2 == 0 or num == 11:  # skip empty locations
                continue

            # get player stats
            player_line = get_player_stats(bas, adv, num, g_id, basics, season, team)

            # add player stats to player_df if he exits
            if player_line is None:
                continue
            else:
                player_df.loc[len(player_df)] = player_line

        # get team stats
        team_adv_stats = list(list(list(list(deep[i + 2].children)[3].children)[1].children)[1].children)[8]
        for stat in range(14, 16):
            full_game_line.append(list(list(team_adv_stats.children)[0].children)[stat].get_text())
        full_game_line = full_game_line + home_line if team == 'away' else full_game_line

    # add team stats to game data frame
    game_df.loc[len(game_df)] = full_game_line

    return player_df, game_df, False


def get_month_stats(season, month):
    """
    Gets full player and game data frames for entire month by looping games
    in the month and calling get_game_stats() for each game.
    :param season: int, season (year that championship is played in)
    :param month: string, full month name in lower case
    :return: None if month page doesn't exist, otherwise nothing
    """
    player_df = pd.DataFrame(columns=PLAYER_COLS)
    game_df = pd.DataFrame(columns=GAME_COLS)
    mon = month[:3].capitalize()
    szn = season - 1 if mon in ['Oct', 'Nov', 'Dec'] else season
    try:
        completed = pd.read_csv(DATA_LOC + "game_" + mon + str(szn) + ".csv", header=0).game_id.values
    except FileNotFoundError:
        completed = []

    # link for monthly page
    month_link = "https://www.basketball-reference.com/leagues/NBA_" + str(season) + "_games-" + month + ".html"
    month_page = get_page(month_link)

    # ensures link is valid
    if month_page is None:
        print("failed for month link: {}".format(month_link))
        return None

    # loop every game for this month; finished = True when data reached without box scores
    finished = False
    for game in month_page.find_all('th', csk=True):
        g_id = game['csk']
        if not finished:
            if g_id in completed:
                continue
            else:
                player_df, game_df, finished = get_game_stats(g_id, player_df, game_df, season)

    # save monthly results as CSVs
    player_df.to_csv(DATA_LOC + "player_" + mon + str(szn) + ".csv", mode='a', index_label="Index")
    game_df.to_csv(DATA_LOC + "game_" + mon + str(szn) + ".csv", mode='a', index_label="Index")
    print('Completed: {m} of {y}'.format(m=month.capitalize(), y=szn))


def main(season):
    """
    Loops through each month of the season to get the relevant data.
    :param season:
    :return:
    """
    months = ["october", "november", "december", "january",
              "february", "march", "april", "may", 'june']

    for month in months:
        get_month_stats(season, month)


if __name__ == '__main__':
    if 'README.md' in os.listdir('.'):
        os.chdir('Scripts/scraping/')

    args = sys.argv

    if len(args) == 1:
        print("Must supply argument")
        print("Argument must be either:")
        print('      1) "full"; downloads all data from 2001 season to present')
        print('      2) "update"; downloads data that is not yet archived')

    elif args[1] not in ['full', 'update']:
        print('Argument must be either "full" or "update"')

    elif args[1] == 'full':
        now = dt.datetime.now()
        nba_szn = now.year + 1 if now.month >= 10 else now.year

        for half in [np.arange(2001, 2012), np.arange(2012, nba_szn + 1)]:
            pool = Pool(processes=len(half))
            pool.map(main, half)

    elif args[1] == 'update':
        last_date = get_last_date(DATA_LOC)  # in format YYYYMM
        now = dt.datetime.now()
        cur_month = '0' + str(now.month) if now.month < 10 else str(now.month)
        current = int(str(now.year) + cur_month)  # in format YYYYMM

        while last_date <= current:
            last_month = calendar.month_name[int(str(last_date)[4:])].lower()
            last_season = int(str(last_date)[:4]) + 1 if last_month in ['october',
                                                                        'november',
                                                                        'december'] else int(str(last_date)[:4])
            get_month_stats(last_season, last_month)
            last_date = last_date + 1 if last_month != 'december' else int(str(int(str(last_date)[:4]) + 1) + '01')
