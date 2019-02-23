"""
This program will scrape basketball reference and create a player
date base with game stats for all players in all games. This is then
saved to a CSV file.
"""
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
import os


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


def fix_scores(score_list):
    """
    Adjusts a team's score list so overtime columns are added together.
    :param score_list:
    :return: list of length 5 (q1-q4, final)
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
    a single OT value
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
        all_scores.append(fix_scores(scores +
                                     [list(line.children)[5].split("<strong>")[shift + 1].split('<')[0]]))
    return all_scores


def get_basic_game_info(box_page):
    """
    Gets some basic game data
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
    less than three found.
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


def main(season):
    """
    Takes season as only arg, then loops through each month of the season
    and gets the relevant data. This is then printed to two separate CSVs
    for player data and general game data.
    :param season:
    :return:
    """
    for month in ["october", "november", "december", "january", "february", "march", "april", "may", 'june']:
        # define data frames to store the data in
        player_df = pd.DataFrame(columns=['game_id', 'date', 'season', 'team', 'opp', 'starting_five', 'name', 'mp',
                                          'fg', 'fga', 'fgp', 'tp', 'tpa', 'tpp', 'ft', 'fta', 'ftp',
                                          'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'pm',
                                          'tsp', 'efgp', 'tpar', 'ftr', 'orbp', 'drbp', 'trbp',
                                          'astp', 'stlp', 'blkp', 'tovp', 'usgp', 'ortg', 'drtg'
                                          ])
        game_df = pd.DataFrame(columns=['game_id', 'date', 'season', 'arena', 'away_name', 'home_name', 'pace',
                                        'attendance', 'ref1', 'ref2', 'ref3',
                                        'away_q1', 'away_q2', 'away_q3', 'away_q4', 'away_ot', 'away_final',
                                        'away_ortg', 'away_drtg',
                                        'home_q1', 'home_q2', 'home_q3', 'home_q4', 'home_ot', 'home_final',
                                        'home_ortg', 'home_drtg'
                                        ])
        # link for monthly page
        month_link = "https://www.basketball-reference.com/leagues/NBA_" + str(season) + "_games-" + month + ".html"
        month_page = get_page(month_link)

        # ensures link is valid
        if month_page is None:
            print("failed for link: {}".format(month_link))
            continue

        # loop every game
        for game in month_page.find_all('th', csk=True):
            box_link = 'https://www.basketball-reference.com/boxscores/' + game['csk'] + '.html'
            box_page = get_page(box_link)

            # all links are valid, so failing would be result of not having reached this date yet
            if box_page is None:
                print("No game played at link: {}".format(box_link))
                break

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

            # get stats for home and away teams
            for team in ['away', 'home']:
                idx = 23 if team == 'away' else 27
                bas = list(list(list(list(list(deep[idx].children)[3].children)[1].children)[1].children)[6].children)
                adv = list(list(list(list(list(deep[idx + 2].children)[3].children)[1].children)[1].children)[6].children)

                for num in range(len(bas) - 1):  # loops every player
                    if num % 2 == 0 or num == 11:  # empty locations
                        continue

                    player_bas_stats, player_adv_stats = bas[num], adv[num]
                    player_line = [game['csk'], basics['date'], season,
                                   basics['away_name'] if team == 'away' else basics['home_name'],
                                   basics['away_name'] if team == 'home' else basics['home_name'],
                                   1 if num < 11 else 0]
                    try:
                        if list(player_bas_stats.children)[1].get_text()[0] not in '1234567890':
                            if list(player_bas_stats.children)[1].get_text() == 'Did Not Play':
                                player_line = player_line + [list(player_bas_stats.children)[0].get_text(),
                                                             '0:00'] + np.zeros(33).tolist()
                            else:
                                continue
                        else:
                            for stat in range(21):
                                player_line.append(list(player_bas_stats.children)[stat].get_text())
                            for stat in range(2, 16):
                                player_line.append(list(player_adv_stats.children)[stat].get_text())
                    except IndexError:
                        continue
                    player_df.loc[len(player_df)] = player_line

                team_adv_stats = list(list(list(list(deep[idx + 2].children)[3].children)[1].children)[1].children)[8]
                for stat in range(14, 16):
                    full_game_line.append(list(list(team_adv_stats.children)[0].children)[stat].get_text())

                full_game_line = full_game_line + home_line if team == 'away' else full_game_line
            try:
                game_df.loc[len(game_df)] = full_game_line
            except ValueError:
                print('error for {y}/{m}'.format(y=season, m=month))
                print(full_game_line)
                print('only has {} cols'.format(len(full_game_line)))

        # save monthly results as CSVs
        player_df.to_csv("./../Data/bask_ref_csvs/player_db_" + str(season) + '_' + month + ".csv", index_label="Index")
        game_df.to_csv("./../Data/bask_ref_csvs/game_db_" + str(season) + '_' + month + ".csv", index_label="Index")
        print('Completed: {m} {y}'.format(m=month, y=season))


if __name__ == '__main__':
    seasons = np.arange(2001, 2019)
    pool = Pool(processes=len(seasons))
    pool.map(main, seasons)


def get_missing():
    """
    Little function use to get list of missing months after a botched
    run.
    :return: list of missing months in format '<season>_<month>'
    """
    files = os.listdir('./../Data/bask_ref_csvs/')
    missing = []
    for year in np.arange(2001, 2019):
        for month in ["october", "november", "december", "january",
                      "february", "march", "april", "may", 'june']:
            if 'game_db_' + str(year) + '_' + month + '.csv' in files:
                if 'player_db_' + str(year) + '_' + month + '.csv' in files:
                    continue
            missing.append(str(year) + '_' + month)
    return missing



