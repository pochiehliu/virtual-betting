"""
This class can get team names, game times, and game number
(as listed on www.sportsbookreview.com) for all NBA matches.
This data is dumped to a CSV called: 'sbr_team_list.csv'

When running from command line, run from top directory.
"""
import pandas as pd
import numpy as np
import sys
from scraping.merger import merge
from scraping.general_tools import *

SBR_PATH = './Data/sbr_csvs/'
DATA_PATH = './Data/'
SBR_GAME_ORDER_COLS = ['date', 'time', 'game_num', 'away', 'home']


class GameOrderScraper:
    BASE_URL = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/pointspread/?date='

    def __init__(self):
        self.betting_dates = merge(SBR_PATH, '').date.unique()
        self.completed_dates = np.array([])

    def full_scrape(self):
        self._call_day_scraper()

    def update_scrape(self):
        self.completed_dates = self._get_completed_dates()
        self._call_day_scraper()

    def _call_day_scraper(self):
        dates = set(self.betting_dates) - set(self.completed_dates)
        new_data = self.day_scraper(dates)
        self._dump_to_csv(new_data)

    def day_scraper(self, dates):
        df = pd.DataFrame(columns=SBR_GAME_ORDER_COLS)
        for date in dates:
            teams, game_times = self._scrape(date)
            for idx, game in enumerate(game_times):
                entry = [date, game, idx, teams[idx * 2], teams[idx * 2 + 1]]
                df.loc[len(df)] = entry
        return df

    def _scrape(self, date):
        page = get_page(self.BASE_URL + str(date))

        teams = list(page.find_all(class_='_3O1Gx'))
        teams = [team.get_text() for idx, team in enumerate(teams)]
        game_times = list(page.find_all(class_='_1t1eJ'))
        game_times = [game_time.get_text().split('H2H')[0] for idx, game_time in enumerate(game_times)]

        return teams, game_times

    @staticmethod
    def _dump_to_csv(df):
        df.to_csv(DATA_PATH + 'sbr_team_list.csv', mode='a+', index_label='Index')

    @staticmethod
    def _get_completed_dates():
        if 'sbr_team_list.csv' in os.listdir(DATA_PATH):
            return pd.read_csv(DATA_PATH + 'sbr_team_list.csv', header=0, index_col='Index').date.unique()
        else:
            return np.array([])


def main(arg):
    scraper = GameOrderScraper()
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

