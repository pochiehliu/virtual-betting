from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
import pandas as pd
from multiprocessing import Pool

options = Options()
# options.add_extension('Misc/Adblock-Plus_v3.4.2.crx')   # remove when actually doing scraping
# options.add_argument("--headless")

driver = webdriver.Chrome('Misc/chromedriver', options=options)
base_url = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/'
bet_types = ['pointspread/', 'money-line/', 'totals/']
length_types = ['', '1st-half/', '2nd-half/']   # blank length is for full game

bet_type = 'pointspread/'
length_type = ''
date = '20161031'
day_link = base_url + bet_type + length_type + '?date=' + date
driver.get(day_link)

test = driver.find_elements_by_class_name('_3Nv_7')






# driver.find_element_by_xpath('//*[@id="schedule"]/tbody').text.split('\n')