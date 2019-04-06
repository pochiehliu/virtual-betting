import pandas as pd
from scraping.general_tools import *
from scraping.sbr_betting import scrape

BASE_URL = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/'




"""
COLUMN_NAMES = ['date', 'bet', 'length', 'game_num', 'aw', 'hw', 'ao', 'ho',
                'ab1', 'hb1', 'ab2', 'hb2', 'ab3', 'hb3', 'ab4', 'hb4',
                'ab5', 'hb5', 'ab6', 'hb6', 'ab7', 'hb7', 'ab8', 'hb8',
                'ab9', 'hb9', 'ab10', 'hb10']

df = pd.DataFrame(columns=COLUMN_NAMES)

driver = get_driver('../scraping/Misc/chromedriver', headless=True)
list_of_lists = scrape(driver, date, 0.35, 1)
driver.get(BASE_URL + bet_type + length_type + '?date=' + date)
"""


