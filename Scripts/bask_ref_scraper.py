"""
This program will scrape basketball reference and create a player
date base with game stats for all players in all games. This is then
saved to a CSV file.
NOTE - The naming convention for the CSV files is "*_<MONTH><YEAR>",
       where * is either 'player' or 'game', MONTH is the three
       letter abbreviation, and YEAR is in the format YYYY.
"""
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
import os
import sys
import datetime as dt
import time
import calendar


def get_page(link):
    """
    Gets the page content from a given link; gives it a second try
    in case internet connection is lost for a moment to result in
    timeout error.
    :param link:
    :return: page content, or None if the link doesn't work
    """
    try:
        page = requests.get(link)
    except TimeoutError:
        page = requests.get(link)
    if page.status_code != 200:
        return None
    return BeautifulSoup(page.content, 'html.parser')


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
    Gets the three referees from the game; fills slots with NaN if there are
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


def get_last():
    """
    Finds last month where basketball-reference has been scraped.
    :return: integer of format YYYYMM
    """

    files = os.listdir('./../Data/bask_ref_csvs/')
    max_date = 200000

    for file in files:
        if file.startswith('game'):
            year = file[-8:-4]
            month = time.strptime(file[-11:-8], '%b').tm_mon
            month = '0' + str(month) if month < 10 else str(month)
            combo = int(year + month)

            # if it's the most recent month
            if combo > max_date:
                max_date = combo

    return max_date


def game_looper(game, player_df, game_df, season):
    box_link = 'https://www.basketball-reference.com/boxscores/' + game['csk'] + '.html'
    box_page = get_page(box_link)

    # all links are valid, so failing would be result of not having reached this date yet
    if box_page is None:
        print("No game played at link: {}".format(box_link))
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
    full_game_line = [game['csk'], basics['date'], season, basics['arena'],
                      basics['away_name'], basics['home_name'], basics['att'], pace] + refs + away_line

    # get stats for home and away teams; paired with their index in the html
    for team, i in [('away', 23), ('home', 27)]:
        # define locations for basic and advanced stats
        bas = list(list(list(list(list(deep[i].children)[3].children)[1].children)[1].children)[6].children)
        adv = list(list(list(list(list(deep[i + 2].children)[3].children)[1].children)[1].children)[6].children)

        for num in range(len(bas) - 1):    # loops every player
            if num % 2 == 0 or num == 11:  # skip empty locations
                continue

            # fill in first few values on in player line
            player_line = [game['csk'], basics['date'], season,
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
                        continue
                else:
                    for stat in range(21):  # basic stats
                        player_line.append(list(player_bas_stats.children)[stat].get_text())
                    for stat in range(2, 16): # advanced stats
                        player_line.append(list(player_adv_stats.children)[stat].get_text())
            except IndexError:
                continue
            player_df.loc[len(player_df)] = player_line

        team_adv_stats = list(list(list(list(deep[i + 2].children)[3].children)[1].children)[1].children)[8]
        for stat in range(14, 16):
            full_game_line.append(list(list(team_adv_stats.children)[0].children)[stat].get_text())
        full_game_line = full_game_line + home_line if team == 'away' else full_game_line

    game_df.loc[len(game_df)] = full_game_line

    return player_df, game_df


def month_looper(season, month, update):
    if update:
        mon = month[:3].capitalize()
        year = season - 1 if mon in ['Oct', 'Nov', 'Dec'] else season
        player_df = pd.read_csv('./../Data/bask_ref_csvs/player_' + mon + str(year) + '.csv',
                                header=0,
                                index_col='Index')
        game_df = pd.read_csv('./../Data/bask_ref_csvs/game_' + mon + str(year) + '.csv',
                              header=0,
                              index_col='Index')
    else:
        # define data frames to store the player and game data in
        player_cols = ['game_id', 'date', 'season', 'team', 'opp', 'starting_five', 'name', 'mp',
                       'fg', 'fga', 'fgp', 'tp', 'tpa', 'tpp', 'ft', 'fta', 'ftp',
                       'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'pm',
                       'tsp', 'efgp', 'tpar', 'ftr', 'orbp', 'drbp', 'trbp',
                       'astp', 'stlp', 'blkp', 'tovp', 'usgp', 'ortg', 'drtg'
                       ]
        game_cols = ['game_id', 'date', 'season', 'arena', 'away_name', 'home_name', 'pace',
                     'attendance', 'ref1', 'ref2', 'ref3',
                     'away_q1', 'away_q2', 'away_q3', 'away_q4', 'away_ot', 'away_final',
                     'away_ortg', 'away_drtg',
                     'home_q1', 'home_q2', 'home_q3', 'home_q4', 'home_ot', 'home_final',
                     'home_ortg', 'home_drtg'
                     ]
        player_df = pd.DataFrame(columns=player_cols)
        game_df = pd.DataFrame(columns=game_cols)

    # link for monthly page
    month_link = "https://www.basketball-reference.com/leagues/NBA_" + str(season) + "_games-" + month + ".html"
    month_page = get_page(month_link)

    # ensures link is valid
    if month_page is None:
        print("failed for month link: {}".format(month_link))
        return None

    # loop every game for this month
    completed = False
    for game in month_page.find_all('th', csk=True):
        if not completed:
            if len(game_df) > 0 and int(game['csk'][:8]) <= int(game_df.game_id.iloc[-1][:8]):
                continue
            else:
                player_df, game_df, completed = game_looper(game, player_df, game_df, season)

    # save monthly results as CSVs
    mon = month[:3].capitalize()
    szn = season - 1 if mon in ['Oct', 'Nov', 'Dec'] else season

    player_df.to_csv("./../Data/bask_ref_csvs/player_" + mon + str(szn) + ".csv", index_label="Index")
    game_df.to_csv("./../Data/bask_ref_csvs/game_" + mon + str(szn) + ".csv", index_label="Index")
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
        month_looper(season, month)


if __name__ == '__main__':
    if 'README.md' in os.listdir('.'):
        os.chdir('Scripts/')

    args = sys.argv

    if len(args) == 1:
        print("Must supply argument")
        print("Argument must be either:")
        print('      1) "full"; downloads all data from 2001 to present')
        print('      2) "update"; downloads data that is not yet archived')

    elif args[1] not in ['full', 'update']:
        print('Argument must be either "full" or "update"')

    elif args[1] == 'full':
        now = dt.datetime.now()
        nba_szn = now.year + 1 if now.month >= 10 else now.year

        half_1 = np.arange(2001, 2012)
        half_2 = np.arange(2012, nba_szn + 1)
        for half in [half_1, half_2]:
            pool = Pool(processes=len(half))
            pool.map(main, half)

    elif args[1] == 'update':
        last_date = get_last()    # in format YYYYMM

        now = dt.datetime.now()
        cur_year = str(now.year)
        cur_month = '0' + str(now.month) if now.month < 10 else str(now.month)
        current = int(cur_year + cur_month)

        first = True    # so that month_looper knows not to re-scrape games from last scraped month
        while last_date <= current:
            last_month = calendar.month_name[int(str(last_date)[4:])].lower()
            last_season = int(str(last_date)[:4]) + 1 if last_month in ['october',
                                                                        'november',
                                                                        'december'] else int(str(last_date)[:4])
            month_looper(last_season, last_month, first)

            last_date = last_date + 1 if last_month != 'december' else int(str(int(str(last_date)[:4]) + 1) + '01')
            first = False




