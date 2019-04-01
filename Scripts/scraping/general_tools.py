from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os
import time

"""
Beautiful Soup Tools
"""


def get_page(link):
    """
    Gets the page content from a given link; gives it a second try
    in case internet connection is lost for a moment to result in
    timeout error.
    :param link:
    :return: page content, or None if the link doesn't work
    """
    try:
        page = requests.get(link)
    except TimeoutError:
        page = requests.get(link)
    if page.status_code != 200:
        return None
    return BeautifulSoup(page.content, 'html.parser')


"""
Selenium Tools
"""


def get_driver(driver_loc, headless):
    options = Options()
    if headless:
        options.add_argument("--headless")
    driver = webdriver.Chrome(driver_loc, options=options)
    driver.set_window_position(0, 0)
    driver.set_window_size(1500, 950)
    return driver


"""
Other Tools
"""


def get_last_date(loc):
    """
    Finds last month where basketball-reference has been scraped.
    :return: integer of format YYYYMM
    """
    files = os.listdir(loc)
    max_date = 200000

    for file in files:
        if not file.startswith('.'):
            year = file[-8:-4]
            month = time.strptime(file[-11:-8], '%b').tm_mon
            month = '0' + str(month) if month < 10 else str(month)
            combo = int(year + month)

            # if it's the most recent month
            if combo > max_date:
                max_date = combo

    return max_date



