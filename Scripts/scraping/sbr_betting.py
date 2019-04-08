"""
This object will scrape HISTORIC NBA betting data from
www.sportsbookreview.com using Selenium.

The files are saved as CSV in a folder for each individual
month when called from command line with argument of "full"
or "update".
"""
import pandas as pd
import numpy as np
from multiprocessing import Pool
import sys
import calendar
from scraping.merger import merge
from scraping.general_tools import *
from db_inserter.Logger import Logger

# CONSTANTS
BET_COLUMN_NAMES = ['date', 'bet', 'length', 'game_num', 'aw', 'hw', 'ao', 'ho',
                    'ab1', 'hb1', 'ab2', 'hb2', 'ab3', 'hb3', 'ab4', 'hb4',
                    'ab5', 'hb5', 'ab6', 'hb6', 'ab7', 'hb7', 'ab8', 'hb8',
                    'ab9', 'hb9', 'ab10', 'hb10']

# Location of scraped SBR files, needed to determine where to start scrape update from
SBR_PATH = './Data/sbr_csvs/'
BASK_REF_PATH = './Data/bask_ref_csvs/'
SCRAPING_PATH = './Scripts/scraping/'


class ScrapeSession:
    BASE_URL = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/'
    ALL_GAME_DATES = merge(BASK_REF_PATH, 'game').game_id.apply(lambda row: row[:8])
    UNIQUE_DATES = ALL_GAME_DATES.unique()
    COMPLETED = merge(SBR_PATH, '').date.unique()  # dates we shouldn't re-scrape

    def __init__(self):
        self.logger = Logger('sbr_scraping', SCRAPING_PATH + 'Misc/')
        self.lengths = ['', '1st-half/', '2nd-half/']  # site has full game URLs as blank
        self.bet_types = ['pointspread/', 'money-line/', 'totals/']

    def full_scrape(self, years=np.arange(2006, 2020)):
        for year in years:
            months = pd.Series([date[:6] for date in self.UNIQUE_DATES if int(date[:4]) == year]).unique()
            pool = Pool(processes=len(months))
            pool.map(self.month_scraper, months)

    def update_scrape(self):
        last = get_last_date(BASK_REF_PATH)
        months = pd.Series([date[:6] for date in self.UNIQUE_DATES]).unique().astype(int)
        months = np.sort(months[months >= last])
        for month in months:
            self.month_scraper(str(month))

    def month_scraper(self, month):
        days_in_month = self._get_month_game_days(month)
        month_df = self.day_scraper(days_in_month)
        self._dump_to_csv(month_df, month)

    def day_scraper(self, days_in_month):
        """
        :param days_in_month: list of dates (strings) in format YYYYMMDD
        :return: data frame with betting data from days in this list
        """
        driver = get_driver(SCRAPING_PATH + 'Misc/chromedriver', headless=True)
        df = pd.DataFrame(columns=BET_COLUMN_NAMES)
        for date in days_in_month:
            scraped_data = self.scrape(driver, date)
            if scraped_data is not None:
                df = self.update_bet_df(df, date, scraped_data)
        driver.quit()
        return df

    def scrape(self, driver, date, sleep=0.35, run=1):
        """
        For a give day, will return a list of lists, where each individual list
        contains data for a bet type + length type combination on given date.
        :param driver: chrome web driver
        :param date: in format 'YYYYMMDD'
        :param sleep: to ensure page loads, needs a sleep ~ 0.2-0.5, varies
                      based on internet connection and number of games
        :param run: run number
        :return: list of lists
        """
        all_data = []
        for bet_type in self.bet_types:
            for length in self.lengths:
                driver.get(self.BASE_URL + bet_type + length + '?date=' + date)
                time.sleep(sleep)
                page_data = [el.text for el in driver.find_elements_by_class_name('_1QEDd')]
                all_data.append(page_data)

        status = self.check_data(all_data, date, run)

        if status == 'pass':
            return all_data
        elif status == 're-run':
            return self.scrape(driver, date, 1, 2)
        else:
            return None

    def check_data(self, full_list, date, run):
        unique_entry_counts = len(set([len(x) for x in full_list]))  # = 1 if all pages give same number of data entries
        if unique_entry_counts == 1:
            return 'pass'
        elif unique_entry_counts == 0:
            self.logger.log('Found no games on {d}'.format(d=date))
        else:
            if run == 2:
                self.logger.log('Slow load on {d}'.format(d=date))
            else:
                return 're-run'
        return None

    def update_bet_df(self, df, date, lists):
        """
        Updates the data frame storing all the data after each day.
        :param df:
        :param date:
        :param lists: list of lists attained by day_loop()
        :return: updated data frame
        """
        for bet_type in self.bet_types:
            for length_type in self.lengths:
                lst = lists.pop(0)
                for game in range(int(len(lst) / 24)):
                    df.loc[len(df)] = [date,
                                       bet_type[0],
                                       'full' if length_type == '' else length_type,
                                       game] + lst[(game * 24):((game + 1) * 24)]
        return df

    def _get_month_game_days(self, month):
        return [x for x in self.UNIQUE_DATES if x[:6] == month and int(x) not in self.COMPLETED]

    def _dump_to_csv(self, df, month):
        year, month_abbrev = self._get_year_month(month)
        df.to_csv(SBR_PATH + month_abbrev + year + '.csv', mode='a+', index_label='Index')

    @staticmethod
    def _get_year_month(month):
        return month[:4], calendar.month_abbr[int(month[4:])]


def main(arg):
    """
    - Gets all dates to scrape on SBR (format: YYYYMMDD)
    - Moves through years sequentially
    - For given year, will scrape each month in parallel
    :return:
    """
    scraper = ScrapeSession()
    scraper.full_scrape() if arg == 'full' else scraper.update_scrape()


def _arg_parse(args):
    if len(args) == 1:
        print("Must supply argument of either:")
        print('      1) "full"; downloads all data from 2006 season to present')
        print('      2) "update"; downloads data that is not yet archived')
    elif args[1] not in ['full', 'update']:
        print('Argument must be either "full" or "update"')
    else:
        main(args[1])


if __name__ == '__main__':
    _arg_parse(sys.argv)

