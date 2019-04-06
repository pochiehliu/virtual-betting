"""
This file will update all existing CSV files to populate
them with new data since the last scraping. Takes about
10-15 seconds per day since last scraping.

Should be run from either the main betting directory or
the directory of the file.
"""

import os

if __name__ == '__main__':
    if 'README.md' in os.listdir('.'):
        os.chdir('Scripts/scraping/')
    else:
        os.chdir('./../scraping/')
    os.system('python basketball_reference.py update')
    os.system('python sbr_betting.py update')
    os.system('python sbr_game_order.py update')



