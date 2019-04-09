"""
This program will scrape basketball reference and create a player
data base with game stats for all players in all games. This is then
saved to a CSV file.

It must be run from command line and given argument of either 'full'
or 'update', where the former will do a complete scraping from Oct
2000 to present, and update will only get data from days since our
most recent data.

NOTE - The naming convention for the CSV files is "*_<MONTH><YEAR>",
       where * is either 'player' or 'game', MONTH is the three
       letter abbreviation, and YEAR is in the format YYYY.
"""
import pandas as pd
import numpy as np
from multiprocessing import Pool
import sys
import calendar
from scraping.general_tools import *
from db_inserter.Logger import Logger
from db_inserter.update_db import get_date, increment_date

# CONSTANTS
BASK_REF_PATH = './Data/bask_ref_csvs/'
SCRAPING_PATH = './Scripts/scraping/'

PLAYER_COLS = ['game_id', 'date', 'season', 'team', 'opp', 'starting_five', 'name', 'mp',
               'fg', 'fga', 'fgp', 'tp', 'tpa', 'tpp', 'ft', 'fta', 'ftp',
               'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'pm',
               'tsp', 'efgp', 'tpar', 'ftr', 'orbp', 'drbp', 'trbp',
               'astp', 'stlp', 'blkp', 'tovp', 'usgp', 'ortg', 'drtg']

GAME_COLS = ['game_id', 'date', 'season', 'arena', 'away_name', 'home_name', 'att',
             'pace', 'ref1', 'ref2', 'ref3',
             'away_q1', 'away_q2', 'away_q3', 'away_q4', 'away_ot', 'away_final',
             'away_ortg', 'away_drtg',
             'home_q1', 'home_q2', 'home_q3', 'home_q4', 'home_ot', 'home_final',
             'home_ortg', 'home_drtg']


class BaskRefScraper:
    month_link = "https://www.basketball-reference.com/leagues/NBA_"
    box_link = 'https://www.basketball-reference.com/boxscores/'
    months = ["october", "november", "december", "january", "february", "march", "april", "may", "june"]

    def __init__(self):
        self.logger = Logger('bask_ref_scraping', SCRAPING_PATH + 'Misc/')
        self.season = None
        self.month = None
        self.tag = None

        self.month_page = None
        self.game_id = None
        self.box_page = None
        self.game_tables = None

        self.player_df = pd.DataFrame(columns=PLAYER_COLS)
        self.game_df = pd.DataFrame(columns=GAME_COLS)
        self.completed = []
        self.last_date = get_date(day=False)
        self.current_date = get_date(day=False)

    def full_scrape(self):
        # TODO find a way to pool this process
        for season in np.arange(2001, 2020):
            for month in self.months:
                self.set_season_month(season, month, fix_season=True)
                self._scrape_month()
                self.clear_dfs()

    def update_scrape(self):
        while self.last_date <= self.current_date:
            self.set_season_month(self.last_date[:4], self.last_date[4:], fix_season=False)

            if "game_" + self.tag + ".csv" in os.listdir(BASK_REF_PATH):
                self.completed = pd.read_csv("game_" + self.tag + ".csv").game_id.values

            self._scrape_month()

            self.clear_dfs()
            self.last_date = increment_date(self.last_date)

    def _scrape_month(self):
        self.month_page = self.set_month_page()
        self.get_month_stats()
        self._dump_to_csv()
        print('Completed: {m} of {y}'.format(m=self.month, y=self.season))

    def set_month_page(self):
        link = self.month_link + self.season + "_games-" + self.month + ".html"
        return get_page(link)

    def _get_box_page(self, game_id):
        link = self.box_link + game_id + '.html'
        return get_page(link)

    def _get_team_ids(self):
        games = self.month_page.find_all('td', {'data-stat': "visitor_team_name"})
        g_id = [game['csk'][:3] for game in games]
        away = [game['csk'][4:-3] for game in games]
        home = [game['csk'][-3:] for game in games]
        return g_id, away, home

    def _get_game_times(self):
        games = self.month_page.find_all('tbody')[0].find_all('tr')
        return [self._read_time(game) for game in games]

    def get_available_games(self):
        if self.month_page is None:
            self.logger.log("No games for {m} in {s}".format(m=self.month, s=self.season))
            return pd.DataFrame()
        else:
            g_id, away, home = self._get_team_ids()
            game_time = self._get_game_times()

        return pd.DataFrame(data={'g_id': g_id,
                                  'game_time': game_time,
                                  't_id_home': home,
                                  't_id_away': away})

    def _get_quarter_scores(self):
        """
        :return: tuple of two lists (away_list, home_list)
        """
        com_soup = BeautifulSoup(self.box_page.find_all(text=lambda text: isinstance(text, Comment))[15], 'html')
        scores = [score.text for score in com_soup.find_all('td', class_=True)]
        return self._merge_ot(scores[:len(scores)//2]), self._merge_ot(scores[len(scores)//2:])

    def _get_basic_info(self):
        """
        Gets date, arena, attendance, and team names
        """
        date = list(self.box_page.find_all(class_="scorebox_meta")[0].children)[1].get_text()
        arena = list(self.box_page.find_all(class_="scorebox_meta")[0].children)[2].get_text()
        away_name = self.box_page.find_all('title')[0].get_text().split(' at ')[0]
        home_name = self.box_page.find_all('title')[0].get_text().split(' at ')[1].split(' Box')[0]
        att = list(self.box_page.children)[3].get_text().split('Attendance:')[1].split('\n')[0].strip('\xa0')
        refs = self._get_refs()
        away_line, home_line = self._get_quarter_scores()
        pace = self._get_pace()
        return {'date': date, 'arena': arena, 'away_name': away_name, 'home_name': home_name, 'att': att,
                'ref': refs, 'away_line': away_line, 'home_line': home_line, 'pace': pace}

    def _get_refs(self):
        """
        Gets the three referees from the game; fills slots with 'MISSING' if there are
        less than three referees found.
        """
        refs = list(self.box_page.children)[3].get_text().split('Officials:')[1].split('\n')[0].strip('\xa0')
        return refs.split(', ') + np.repeat('MISSING', 3 - len(refs.split(', '))).tolist()

    def _get_pace(self):
        com_soup = BeautifulSoup(self.box_page.find_all(text=lambda text: isinstance(text, Comment))[16], 'html')
        return com_soup.find('td', class_=True).get_text()

    def _loop_players(self, basic_table, advanced_table):
        basic = [self._get_player_stats(player, 'basic') for player in self._valid_player(basic_table)]
        advanced = [self._get_player_stats(player, 'advanced') for player in self._valid_player(advanced_table)]

        for player in zip(basic, advanced):
            self.player_df.loc[len(self.player_df)] = list(player)

    def _get_player_stats(self, player, kind):
        if self._valid_stat_line_check(player, kind):
            if kind == 'basic':
                if len(list(player)) == 21:
                    player_stats = [list(player)[x].get_text() for x in range(21)]
                else:
                    player_stats = [list(player)[0].get_text()] + list(np.zeros(20))
            else:
                if len(list(player)) == 16:
                    player_stats = [list(player)[x].get_text() for x in range(2, 16)]
                else:
                    player_stats = list(np.zeros(14))
        return player_stats

    def _get_game_stats(self):
        """
        Updates the player and game data frames with stats from a single game
        :return: player_df, game_df, boolean of whether month is completed
        """
        basics = self._get_basic_info()

        self.game_tables = self.box_page.find_all('table')

        self._loop_players(self.game_tables[0], self.game_tables[1])  # away players
        self._loop_players(self.game_tables[2], self.game_tables[3])  # home players

        team_stats = [self._get_team_stats(table) for table in (self.game_tables[1], self.game_tables[3])]

        full_game_line = [self.game_id,
                          basics['date'],
                          self.season,
                          basics['arena'],
                          basics['away_name'],
                          basics['home_name'],
                          basics['att'],
                          basics['pace'],
                          basics['refs'],
                          basics['away_line']
                          ] + team_stats[0] + [basics['home_line']] + team_stats[1]

        self.game_df.loc[len(self.game_df)] = full_game_line

    def get_month_stats(self, games=None):
        """
        Updates full player and game data frames for entire month by looping
        games in the month and calling get_game_stats() for each game.
        """
        if not games:
            games = [g['csk'] for g in self.month_page.find_all('th', csk=True) if g['csk'] not in self.completed]

        for game in games:
            self.game_id = game
            self.box_page = self._get_box_page(game)
            self._get_game_stats()

    def _dump_to_csv(self):
        self.player_df.to_csv(BASK_REF_PATH + "player_" + self.tag + ".csv", mode='a+', index_label="Index")
        self.game_df.to_csv(BASK_REF_PATH + "game_" + self.tag + ".csv", mode='a+', index_label="Index")

    def set_season_month(self, year, month, fix_season=False):
        if len(str(month)) > 2:
            self.month = month
        else:
            self.month = calendar.month_name[int(month)].lower()

        if fix_season:
            self.season = int(year)
        else:
            self.season = int(year) if month not in ["october", "november", "december"] else int(year) - 1

        self.tag = self.month[:3].title() + self.season

    def clear_dfs(self):
        self.player_df = pd.DataFrame(columns=PLAYER_COLS)
        self.game_df = pd.DataFrame(columns=GAME_COLS)

    @staticmethod
    def _read_time(game):
        cut = game.get_text('csk').split('csk')
        day_time = cut[1][:-1]
        day = cut[0].split(', ')
        day = calendar.month_name[list(calendar.month_abbr).index(day[1][:3])] + ' ' + day[1][4:] + ' ' + day[2]
        return day_time + ' PM ' + day

    @staticmethod
    def _merge_ot(score_list):
        if len(score_list) == 5:
            return score_list[:4] + ['0'] + [score_list[4]]
        else:
            ot_sum = [str(np.array(score_list[4:-1]).astype(int).sum())]
            return score_list[:4] + ot_sum + score_list[-1:]

    @staticmethod
    def _get_team_stats(table):
        return table.find_all('tfoot')[0].get_text(';').split(';')[-3:-1]

    @staticmethod
    def _valid_stat_line_check(player, kind):
        max_stats = 21 if kind == 'basic' else 16
        if len(player) == max_stats or player.get_text(';').split(';')[1][0] in '1234567890:':
            return True
        elif player.get_text(';').split(';')[1] == 'Did Not Play':
            return True
        return False

    @staticmethod
    def _valid_player(table):
        rows = table.find_all('tr')
        for idx, row in rows:
            if idx > 2 and idx != 7 and idx != len(rows) - 1:
                yield row


def main(arg):
    scraper = BaskRefScraper()
    if arg == 'full':
        scraper.full_scrape()
    elif arg == 'update':
        scraper.last_date = get_last_date(BASK_REF_PATH)
        scraper.update_scrape()


if __name__ == '__main__':
    main(arg_parse(sys.argv))
