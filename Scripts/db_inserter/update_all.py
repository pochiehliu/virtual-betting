"""
This file will update all existing CSV files to populate
them with new data since the last scraping. Takes about
10-15 seconds per day since last scraping.

TODO: update database with newly scraped data; will do so by calling update_db.py
"""

import os

if __name__ == '__main__':
    os.chdir('Scripts/scraping')

    os.system('python basketball_reference.py update')
    os.system('python sbr_betting.py update')
    os.system('python sbr_game_order.py update')



