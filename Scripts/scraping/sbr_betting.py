"""
This program will scrape all HISTORIC NBA betting data from
www.sportsbookreview.com using Selenium.

This is not the scraper that will be used for daily line
updates.

The files are saved as CSV in a folder for each individual
month.
"""
import pandas as pd
import numpy as np
from multiprocessing import Pool
import sys
import calendar
from scraping.merger import merge
from scraping.general_tools import *

# CONSTANTS
BASE_URL = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/'
BET_COLUMN_NAMES = ['date', 'bet', 'length', 'game_num', 'aw', 'hw', 'ao', 'ho',
                    'ab1', 'hb1', 'ab2', 'hb2', 'ab3', 'hb3', 'ab4', 'hb4',
                    'ab5', 'hb5', 'ab6', 'hb6', 'ab7', 'hb7', 'ab8', 'hb8',
                    'ab9', 'hb9', 'ab10', 'hb10']

# Location of scraped SBR files, needed to determine where to start scrape update from
SBR_PATH = './../../Data/sbr_csvs/'


def get_faulty_dates():
    """
    Function that gets a numpy array of faulty dates
    :return: numpy array with dates of games in format 'YYYYMMDD'
    """
    lines = [line.rstrip('\n') for line in open('Misc/Output.txt', 'r')]
    dates = np.array([])
    for line in lines:
        if line.startswith('Could not get'):
            dates = np.append(dates, line.split(' ')[-1])
    return dates.astype(int)


def update_bet_df(df, date, lists,
                  lengths=('', 'first', 'sec')):
    """
    Updates the data frame storing all the data after each day.
    :param df:
    :param date:
    :param lists: list of lists attained by day_loop()
    :param lengths: tuple with all length types enclosed in lists
    :return: updated data frame
    """
    bet_types = ['p', 'm', 't']
    for bet_type in bet_types:
        for length_type in lengths:
            lst = lists.pop(0)
            for game in range(int(len(lst) / 24)):
                df.loc[len(df)] = [date,
                                   bet_type,
                                   'full' if length_type == '' else length_type,
                                   game] + lst[(game * 24):((game + 1) * 24)]
    return df


def check_data(full_list, date):
    entry_counts = [len(x) for x in full_list]  # list with number of entries for from each page
    uniques = len(set(entry_counts))            # = 1 if all pages give same number of data entries
    status = None
    if uniques != 1:
        if uniques > 1:                         # unequal number of entries for different pages,
            status = "slow load"
        elif uniques < 1:
            status = "no load"
    else:
        if 0 in set(entry_counts):
            status = 'no data'
        elif __name__ == '__main__' and int(len(full_list[0]) / 24) != GAME_COUNTS[date]:
            status = 'unexpected game count'
        else:
            status = 'pass'
    return status


def scrape(driver, date, sleep=0.35, run=1,
           lengths=('', '1st-half/', '2nd-half/')):
    """
    For a give day, will return a list of lists, where each individual list
    contains data for a bet type + length type combination on given date.
    :param driver: chrome web driver
    :param date: in format 'YYYYMMDD'
    :param sleep: to ensure page loads, needs a sleep ~ 0.2-0.5, varies
                  based on internet connection and number of games
    :param run: run number
    :param lengths: tuple with the game lengths to scrape, default is all available
    :return: list of lists
    """
    full_list = []
    bet_types = ['pointspread/', 'money-line/', 'totals/']
    for bet_type in bet_types:
        for length in lengths:      # blank length is for full game
            driver.get(BASE_URL + bet_type + length + '?date=' + date)

            time.sleep(sleep)

            singles = [t.text for t in driver.find_elements_by_class_name('_1QEDd')]
            full_list.append(singles)

    status = check_data(full_list, date)

    """
    Three cases:
        1) pass; then return data
        2) fail for second time; return data if it's because of unexpected game
           count, otherwise don't return anything
        3) fail first time; return data if only one game off expected, otherwise try
           again with increased sleep to hopefully solve slow loading problem
    """
    if status == 'pass':
        pass

    elif run == 2:
        text_file = open('Misc/Output.txt', "a")
        print('Issue on {d} because: {s}'.format(d=date, s=status), file=text_file)
        text_file.close()

        if status == 'unexpected game count':
            text_file = open('Misc/Output.txt', "a")
            print("Got {g} instead of {e}".format(e=GAME_COUNTS[date], g=int(len(full_list[0]) / 24)), file=text_file)
            text_file.close()
        else:
            return None
    else:
        if status == 'unexpected game count' and GAME_COUNTS[date] - int(len(full_list[0]) / 24) == 1:
            pass
        else:
            full_list = scrape(driver, date, sleep=1, run=2, lengths=lengths)
    return full_list


def day_caller(month, first=False):
    """
    Will initialize a data frame to store data, initialize a
    web driver to scrape data, call day for given month linearly.
    :param month: as string in format YYYYMM
    :param first: boolean, indicates whether month being scraped
                  has been scraped previously (and therefore
                  needs just to be appended instead of built
                  from scratch).
    :return: nothing
    """
    y = month[:4]
    m = calendar.month_abbr[int(month[4:])]

    if not first:
        df = pd.DataFrame(columns=BET_COLUMN_NAMES)
    else:
        df = pd.read_csv(SBR_PATH + m + y + '.csv', header=0, index_col='Index')

    # get driver
    driver = get_driver('Misc/chromedriver', headless=True)

    days_in_month = [x for x in DATE_LIST if x[:6] == month]

    # loop days in the month; skip if already scraped
    for date in days_in_month:
        if first and int(date) in COMPLETED:
            continue

        # scrape data
        list_of_lists = scrape(driver, date, 0.35, 1)

        # paste data to data frame
        if list_of_lists is not None:
            df = update_bet_df(df, date, list_of_lists)
        else:
            text_file = open('Misc/Output.txt', "a")
            print('Skipped {d} because None was returned.'.format(d=date), file=text_file)
            text_file.close()
    driver.quit()

    # dump to csv
    df.to_csv(SBR_PATH + m + y + '.csv', index_label='Index')
    print("Completed: {m} of {y}".format(m=m, y=y))


def main(arg):
    """
    - Gets all dates to scrape on SBR (format: YYYYMMDD)
    - Moves through years sequentially
    - For given year, will scrape each month in parallel
    :return:
    """
    if arg == 'full':
        for year in range(2006, 2020):
            months = pd.Series([date[:6] for date in DATE_LIST if int(date[:4]) == year]).unique()
            pool = Pool(processes=len(months))
            pool.map(day_caller, months)

    elif arg == 'update':
        last = get_last_date(SBR_PATH)
        months = pd.Series([date[:6] for date in DATE_LIST]).unique().astype(int)
        months = np.sort(months[months >= last])
        for month in months:
            first = True if month == last else False
            day_caller(str(month), first)


if __name__ == '__main__':
    if 'README.md' in os.listdir('.'):
        os.chdir('Scripts/scraping/')

    # Lists of games
    ALL_GAME_DATES = merge(SBR_PATH + '../bask_ref_csvs/', 'game').game_id.apply(lambda row: row[:8])
    DATE_LIST = ALL_GAME_DATES.unique()                           # dates that there are NBA stats for
    GAME_COUNTS = ALL_GAME_DATES.groupby(ALL_GAME_DATES).count()  # Used to find how many games to expect on SBR
    COMPLETED = date_list = merge(SBR_PATH, '').date.unique()     # dates we shouldn't re-scrape

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
