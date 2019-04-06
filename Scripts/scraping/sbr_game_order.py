"""
This script will get team names, game times, and game number
(as listed on www.sportsbookreview.com) for all NBA matches.
This data is dumped to a CSV called: 'sbr_team_list.csv'

It must be run from command line and given argument of either 'full'
or 'update', where the former will do a complete scraping for all
dates that have had betting data scraped from SBR, while 'update'
will only get data from days that have had betting data scraped
but have not had games added to sbr_team_list.csv.

When running from command line, run from Betting/ directory.
"""
import pandas as pd
import numpy as np
import sys
from scraping.merger import merge
from scraping.general_tools import *

# URL to base website of scraping
BASE_URL = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/pointspread/'
# Location of scraped SBR files, needed to determine where to start scrape update from
SBR_PATH = './../../Data/sbr_csvs/'
# Location of where to store file
STORE_PATH = './../../Data/'


def get_basics(date):
    """
    Gets the team names and game times for given date.
    :param date:
    :return: dictionary with team names and game times
    """
    page = get_page(BASE_URL + '?date=' + str(date))

    teams = list(page.find_all(class_='_3O1Gx'))
    teams = [teams[i].get_text() for i in range(len(teams))]
    game_times = list(page.find_all(class_='_1t1eJ'))
    game_times = [game_times[i].get_text().split('H2H')[0] for i in range(len(game_times))]

    return teams, game_times


def main(status):
    if status == 'update':
        sbr_basic = pd.read_csv(STORE_PATH + 'sbr_team_list.csv', header=0, index_col='Index')
        completed = sbr_basic.date.unique()
    else:
        sbr_basic = pd.DataFrame(columns=['date', 'time', 'game_num', 'away', 'home'])
        completed = np.array([])

    # gets the dates that have had betting data scraped
    date_list = merge(SBR_PATH, '').date.unique()
    date_list.sort()

    # loops every date that we need that is not already completed
    for date in set(date_list) - set(completed):
        teams, game_times = get_basics(date)
        for game in range(len(game_times)):
            entry = [date, game_times[game], game, teams[game * 2], teams[game * 2 + 1]]
            sbr_basic.loc[len(sbr_basic)] = entry

    sbr_basic.to_csv(STORE_PATH + 'sbr_team_list.csv', index_label='Index')


if __name__ == '__main__':
    if 'README.md' in os.listdir('.'):
        os.chdir('Scripts/scraping/')

    args = sys.argv

    if len(args) == 1:
        print("Must supply argument")
        print("Argument must be either:")
        print('      1) "full"; downloads all data from 2006 season to present')
        print('      2) "update"; downloads data that is not yet archived')

    elif args[1] not in ['full', 'update']:
        print('Argument must be either "full" or "update"')

    elif args[1] in ['full', 'update']:
        main(args[1])

    else:
        print('Unexpected error')
