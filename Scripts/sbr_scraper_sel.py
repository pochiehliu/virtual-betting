from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd
from multiprocessing import Pool
import time
import os
import datetime as dt
# global variable
base_url = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/'


def get_dates():
    """
    Function that gets a pandas series of all the dates to be
    searched.
    :return: pandas series with dates of games
    """
    loc = './../Data/bask_ref_csvs/'
    file_list = [file for file in os.listdir(loc) if not file.startswith(('.', 'full')) and file.startswith('game')]
    all_csv = []
    for file in file_list:
        all_csv.append(pd.read_csv(loc + file, index_col='Index', header=0))
    df = pd.concat(all_csv, ignore_index=True, sort=False)
    df.date = df.date.apply(lambda x: dt.datetime.strptime(x, "%I:%M %p, %B %d, %Y"))
    dates = df.date.apply(lambda x: str(x.year) + (str(x.month) if x.month > 9 else '0' + str(x.month)) + (str(x.day) if x.day > 9 else '0' + str(x.day)))
    dates = dates.loc[dates.astype(int) > 20061000]
    return dates.unique()


def day_loop(driver, date, sleep, run):
    # list that will hold the length of the amount of entries for bet length type
    game_check = []

    full_list = []
    for bet_type in ['pointspread/', 'money-line/', 'totals/']:
        for length_type in ['', '1st-half/', '2nd-half/']:      # blank length is for full game
            driver.get(base_url + bet_type + length_type + '?date=' + date)
            time.sleep(sleep)

            test = driver.find_elements_by_class_name('_1QEDd')
            singles = [t.text for t in test]
            game_check.append(len(singles))
            full_list.append(singles)

    if 0 in set(game_check):
        print('No games on date: {}'.format(date))
        return None
    elif len(set(game_check)) > 1:  # unequal number of entries for different pages
        if run < 3:
            full_list = day_loop(driver, date, sleep * 2, run + 1)
        else:
            print('Could not get match entries for {}'.format(date))
            return None
    return full_list


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


def main():
    column_list = ['date', 'bet', 'length', 'game_num', 'aw', 'hw', 'ao', 'ho',
                   'ab1', 'hb1', 'ab2', 'hb2', 'ab3', 'hb3', 'ab4', 'hb4',
                   'ab5', 'hb5', 'ab6', 'hb6', 'ab7', 'hb7', 'ab8', 'hb8',
                   'ab9', 'hb9', 'ab10', 'hb10']
    df = pd.DataFrame(columns=column_list)
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome('./../Misc/chromedriver', options=options)
    driver.set_window_position(0, 0)
    driver.set_window_size(1500, 950)

    dates = get_dates()
    dates.sort()
    start = time.time()

    for date in dates[:5]:
        list_of_lists = day_loop(driver, date, .2, 0)
        df = update(df, date, list_of_lists)
        print('did day {}'.format(date))
    end = time.time()

    print("5 days took {}s".format(end - start))
    df.to_csv('./../Data/sbr_betting_data.csv', index_label='Index')
    driver.quit()


if __name__ == '__main__':
    main()






