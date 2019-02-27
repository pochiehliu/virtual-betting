"""
This program will scrape all NBA betting data from
www.sportsbookreview.com using Selenium.
The files are saved as CSV in a folder for each individual
month.
"""
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd
from multiprocessing import Pool
import time
import os
import datetime as dt
import calendar

# global variable
BASE_URL = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/'


def get_dates(unique):
    """
    Function that gets a pandas series of all the dates to be
    searched.
    :return: numpy array with dates of games in format 'YYYYMMDD'
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
    return dates.unique() if unique else dates.groupby(dates).count()


def get_driver():
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome('./../Misc/chromedriver', options=options)
    driver.set_window_position(0, 0)
    driver.set_window_size(1500, 950)
    return driver


def update(df, date, lists):
    """
    Updates the data frame storing all the data after each day.
    :param df:
    :param date:
    :param lists: list of lists attained by day_loop()
    :return: updated data frame
    """
    for bet_type in ['p', 'm', 't']:
        for length_type in ['full', 'first', 'sec']:
            lst = lists.pop(0)
            for game in range(int(len(lst) / 24)):
                df.loc[len(df)] = [date, bet_type, length_type, game] + lst[(game * 24):((game + 1) * 24)]
    return df


def check_data(full_list, date):
    entry_counts = [len(x) for x in full_list]  # list with number of entries for from each page
    uniques = len(set(entry_counts))  # = 1 if all pages give same number of data entries
    status = None
    if uniques != 1:
        if uniques > 1:  # unequal number of entries for different pages,
            status = "slow load"
        elif uniques < 1:
            status = "no load"
    else:
        if 0 in set(entry_counts):
            status = 'no data'
        elif int(len(full_list[0]) / 24) != expected_counts[date]:
            status = 'unexpected game count'
        else:
            status = 'pass'
    return status


def scrape(driver, date, sleep, run):
    """
    For a give day, will return a list of lists, where each individual list
    contains data for a bet type + length type combination on given date.
    :param driver: chromedriver
    :param date: in format 'YYYYMMDD'
    :param sleep: to ensure page loads, needs a sleep
    :param run: run number
    :return: list of lists
    """
    full_list = []
    for bet_type in ['pointspread/', 'money-line/', 'totals/']:
        for length_type in ['', '1st-half/', '2nd-half/']:      # blank length is for full game
            driver.get(BASE_URL + bet_type + length_type + '?date=' + date)

            time.sleep(sleep)

            test = driver.find_elements_by_class_name('_1QEDd')
            singles = [t.text for t in test]
            full_list.append(singles)

    status = check_data(full_list, date)

    if status == 'pass':
        pass
    elif run == 2:
        text_file = open('./../Data/Output.txt', "a")
        print('Issue on {d} because: {s}'.format(d=date, s=status), file=text_file)
        text_file.close()
        if status == 'unexpected game count':
            text_file = open('./../Data/Output.txt', "a")
            print("Should've had {e} but got {g} games".format(e=expected_counts[date],
                                                               g=int(len(full_list[0]) / 24)),
                  file=text_file)
            text_file.close()
        else:
            return None
    else:
        if status == 'unexpected game count' and expected_counts[date] - int(len(full_list[0]) / 24) == 1:
            pass
        else:
            full_list = scrape(driver, date, 1, 2)
    return full_list


def day_caller(month):
    """
    Will initialize a data frame to store data, initialize a
    webdriver to scrape data, call day for given month linearly.
    :param month: as string in format YYYYMM
    :return:
    """
    start = dt.datetime.now()
    column_names = ['date', 'bet', 'length', 'game_num', 'aw', 'hw', 'ao', 'ho',
                    'ab1', 'hb1', 'ab2', 'hb2', 'ab3', 'hb3', 'ab4', 'hb4',
                    'ab5', 'hb5', 'ab6', 'hb6', 'ab7', 'hb7', 'ab8', 'hb8',
                    'ab9', 'hb9', 'ab10', 'hb10']
    df = pd.DataFrame(columns=column_names)

    # get driver
    driver = get_driver()

    days_in_month = [x for x in dates if x[:6] == month]

    for date in days_in_month:
        # scrape data
        list_of_lists = scrape(driver, date, 0.35, 1)

        # paste data to data frame
        if list_of_lists is not None:
            df = update(df, date, list_of_lists)
        else:
            text_file = open('./../Data/Output.txt', "a")
            print('Skipped {d} because None was returned.'.format(d=date), file=text_file)
            text_file.close()

    driver.quit()

    # dump to csv
    y = month[:4]
    m = calendar.month_abbr[int(month[4:])]
    df.to_csv('./../Data/sbr_csvs/' + m + y + '.csv', index_label='Index')
    end = dt.datetime.now()

    print("Did {d} in {t} for {g} games.".format(d=month, t=end-start, g=int(len(df)/9)))


def main():
    """
    - Gets all dates to scrape on SBR (format: YYYYMMDD)
    - Moves through years sequentially
    - For given year, will scrape each month in parallel
    :return:
    """
    for year in range(2006, 2020):
        months = pd.Series([x[:6] for x in dates if int(x[:4]) == year]).unique()
        pool = Pool(processes=len(months))
        pool.map(day_caller, months)


if __name__ == '__main__':
    if 'README.md' in os.listdir('.'):
        os.chdir('Scripts/')

    # get all unique dates for which we need to scrape data
    dates = get_dates(unique=True)
    dates.sort()

    # get the expected number of games for each date counts
    expected_counts = get_dates(unique=False)

    main()

