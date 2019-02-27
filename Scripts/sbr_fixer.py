"""
This program will fix the dates that sbr_scraper_sel.py
was not able to scrape on first run; fetches these dates
from output file produced by sbr_scraper.sel.py.
"""
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd
from multiprocessing import Pool
import time
import os
import datetime as dt
import numpy as np
import calendar

# global variable
BASE_URL = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/'


def get_faulty_dates():
    """
    Function that gets a pandas series of all the dates to be
    searched.
    :return: numpy array with dates of games in format 'YYYYMMDD'
    """
    lines = [line.rstrip('\n') for line in open("./../Data/sbr_csvs/Output.txt", "r")]
    dates = np.array([])
    for line in lines:
        if line.startswith('Could not get'):
            dates = np.append(dates, line.split(' ')[-1])
    return dates.astype(int)


def update(df, date, lists):
    """
    Updates the data frame storing all the data after each day.
    :param df:
    :param date: as integer
    :param lists: list of lists attained by day_loop()
    :return: updated data frame
    """
    top_slice = df.loc[df.date < date].copy()
    mid_slice = df.loc[df.date == date].copy()
    bottom_slice = df.loc[df.date > date].copy()

    for bet_type in ['p', 'm', 't']:
        for length_type in ['full', 'first', 'sec']:
            lst = lists.pop(0)
            for game in range(int(len(lst) / 24)):
                mid_slice.loc[len(mid_slice)] = [date, bet_type, length_type, game] + lst[(game * 24):((game + 1) * 24)]
    df = pd.concat([top_slice, mid_slice, bottom_slice], ignore_index=True, sort=False)
    return df


def day_loop(driver, date, sleep, run):
    """
    For a give day, will return a list of lists, where each individual list
    contains data for a bet type + length type combination on given date.
    :param driver: chromedriver
    :param date: in format 'YYYYMMDD'
    :param sleep: to ensure page loads, needs a sleep
    :param run: a counter to limit number of re-attempts for a failing page
    :return: list of lists
    """
    # list that will hold the entry count for bet type/length type combination;
    # should equal (10 (# books) + 2 (wager + opener)) * n_games * 2 (teams per game)
    game_check = []

    full_list = []
    for bet_type in ['pointspread/', 'money-line/', 'totals/']:
        for length_type in ['', '1st-half/', '2nd-half/']:      # blank length is for full game
            driver.get(BASE_URL + bet_type + length_type + '?date=' + str(date))

            time.sleep(sleep)

            test = driver.find_elements_by_class_name('_1QEDd')
            singles = [t.text for t in test]
            game_check.append(len(singles))
            full_list.append(singles)

    # checks that everything was scraped correctly
    uniques = len(set(game_check))  # = 1 if all pages give same number of data entries
    if uniques != 1:
        if uniques > 1:  # unequal number of entries for different pages,
            if run < 3:    # need to re-run with a longer sleep interval.
                text_file = open("./../Data/sbr_csvs/Output.txt", "a")
                print('\nGoing round {}'.format(run + 2), file=text_file)
                text_file.close()
                full_list = day_loop(driver, date, sleep + 0.15, run + 1)
            else:
                text_file = open("./../Data/sbr_csvs/Output.txt", "a")
                print('\nCould not get match entries for {}'.format(date),
                      file=text_file)
                text_file.close()
                return None
        elif 0 in set(game_check):
            text_file = open("./../Data/sbr_csvs/Output.txt", "a")
            print('\nNo games on date: {}'.format(date), file=text_file)
            text_file.close()
            return None
    return full_list


def day_caller(date):
    """
    Will open CSV containing the passed date, initialize a
    webdriver to scrape data, dump corrected data CSV.
    :param date: individual date
    :return:
    """
    year = str(date)[:4]
    month_name = calendar.month_abbr[int(str(date)[4:6])]

    df = pd.read_csv('./../Data/sbr_csvs/' + month_name + year + '.csv', header=0, index_col='Index')

    # configure the driver
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome('./../Misc/chromedriver', options=options)
    driver.set_window_position(0, 0)
    driver.set_window_size(1500, 950)

    # loop every day for the month
    list_of_lists = day_loop(driver, date, 0.2, 0)

    if list_of_lists is not None:
        df = update(df, date, list_of_lists)

    # push monthly data frame to a csv
    df.to_csv('./../Data/sbr_csvs/' + month_name + year + '.csv', index_label='Index')
    print("Completed {}".format(date))


def main():
    """
    - Gets all dates to scrape on SBR (format: YYYYMMDD)
    - Moves through years sequentially
    - For given year, will scrape each month in parallel
    :return:
    """
    dates = get_faulty_dates()
    dates.sort()

    for date in dates:
        day_caller(date)


if __name__ == '__main__':
    main()






