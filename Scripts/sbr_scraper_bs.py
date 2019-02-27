"""
This script will get team names, game times, and game number
(as listed on www.sportsbookreview.com) for all NBA matches.
This data is dumped to a CSV.
"""
import pandas as pd
from bs4 import BeautifulSoup
import requests
import os
import datetime as dt

BASE_URL = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/pointspread/'


def get_page(link):
    """
    Gets the page content from a given link; tries again in case
    connection is lost for a moment to result in timeout error.
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


def get_dates():
    """
    Function that gets a pandas series of all the dates to be
    searched.
    :return: pandas series with dates of games
    """
    loc = './../Data/bask_ref_csvs/'
    file_list = [file for file in os.listdir(loc) if file.startswith('game')]
    all_csv = []
    for file in file_list:
        all_csv.append(pd.read_csv(loc + file, index_col='Index', header=0))
    df = pd.concat(all_csv, ignore_index=True, sort=False)
    df.date = df.date.apply(lambda x: dt.datetime.strptime(x, "%I:%M %p, %B %d, %Y"))
    dates = df.date.apply(lambda x: str(x.year) + (str(x.month) if x.month > 9 else '0' + str(x.month)) + (str(x.day) if x.day > 9 else '0' + str(x.day)))
    dates = dates.loc[dates.astype(int) > 20061000]
    return dates.unique()


def get_basics(date):
    """
    Gets the team names and game times for given date.
    :param date:
    :return: dictionary with team names and game lines
    """
    page = get_page(BASE_URL + '?date=' + date)

    teams = list(page.find_all(class_='_3O1Gx'))
    teams = [teams[i].get_text() for i in range(len(teams))]
    game_times = list(page.find_all(class_='_1t1eJ'))
    game_times = [game_times[i].get_text().split('H2H')[0] for i in range(len(game_times))]

    return teams, game_times


def main():
    sbr_basic = pd.DataFrame(columns=['date', 'time', 'game_num', 'away', 'home'])
    date_list = get_dates()
    date_list.sort()

    for date in date_list:
        teams, game_times = get_basics(date)
        for game in range(len(game_times)):
            entry = [date, game_times[game], game, teams[game * 2], teams[game * 2 + 1]]
            sbr_basic.loc[len(sbr_basic)] = entry

    sbr_basic.to_csv('./../Data/sbr_team_list.csv', index_label='Index')


if __name__ == '__main__':
    if 'README.md' in os.listdir('.'):
        os.chdir('Scripts/')
    main()


