"""
NOT COMPLETE
This script will get all relevant data that can be scraped from the source
code from sports book review.
"""
import pandas as pd
from bs4 import BeautifulSoup
import requests

base_url = 'https://www.sportsbookreview.com/betting-odds/nba-basketball/'
bet_types = ['pointspread/', 'money-line/', 'totals/']
length_types = ['', '1st-half/', '2nd-half/']   # blank length is for full game


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


def get_stats(page):
    """
    Gets the opening lines and the wager percentages.
    :param page:
    :return: dictionary with opener line and wagers
    """
    opener = list(page.find_all(class_='opener'))
    opener = [opener[i].get_text() for i in range(len(opener))]

    wagers = [int(opener[i].strip('%')) / 100 for i in range(len(opener)) if (i + 1) % 6 in [1, 2]]
    opener = [opener[i] for i in range(len(opener)) if (i + 1) % 6 in [3, 5]]

    return {'opener': opener, 'wagers': wagers}


def get_basics(page):
    """
    Gets the team names and game times.
    :param page:
    :return: dictionary with team names and game lines
    """
    teams = list(page.find_all(class_='_3O1Gx'))
    teams = [teams[i].get_text() for i in range(len(teams))]

    game_times = list(page.find_all(class_='_1t1eJ'))
    game_times = [game_times[i].get_text().split('H2H')[0] for i in range(len(game_times))]

    return {'teams': teams, 'game_times': game_times}


def looping(date):
    """
    Loops the 9 pages for a given day (3 bet types x 3 length types).

    :param date:
    :return: dictionary with information to be printed to the
    """
    row = {}
    for bet_type in bet_types:
        for length_type in length_types:
            day_link = base_url + bet_type + length_type + '?date=' + date
            page = get_page(day_link)
            stats = get_stats(page)

            if bet_type == 'pointspread/' and length_type == '':
                basics = get_basics(page)
                row = {**row, **basics}

            row[bet_type + length_type + 'opener'] = stats['opener']
            row[bet_type + length_type + 'wagers'] = stats['wagers']
    return row


def main():
    sbr_basic = pd.DataFrame(columns=['date', 'time', 'away', 'home',
                                      'p_fg_w', 'p_1st_w', 'p_2nd_w',
                                      'p_fg', 'p_1st', 'p_2nd',
                                      'm_fg_w', 'm_1st_w', 'm_2nd_w',
                                      'm_fg', 'm_1st', 'm_2nd',
                                      't_fg_w', 't_1st_w', 't_2nd_w',
                                      't_fg', 't_1st', 't_2nd'
                                      ])
    date_list = ['20161031']    # list of all dates that t

    for date in date_list:
        row = looping(date)
        for game in range(len(row['game_times'])):
            entry = [date, row['game_times'][game], row['teams'][game * 2], row['teams'][game * 2 + 1]]

            for bet_type in bet_types:
                for length_type in length_types:
                    for data in ['wagers', 'opener']:
                        entry.append(row[bet_type + length_type + data][game * 2 + 1])
            print(len(entry))
            sbr_basic.loc[sbr_basic.shape[0]] = entry
    sbr_basic.to_csv('./../Data/opener_and_wagers.csv', index_label='Index')


if __name__ == '__main__':
    main()


