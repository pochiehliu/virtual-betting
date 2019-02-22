"""
This program will scrape basketball reference and create two primary data bases:
        1) player date base: with game stats for all players in all games
        2) game data base: with team stats for all games
    In addition to these two individual master data bases, the individual data
    bases for the corresponding months are also saved as CSVs.
"""
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests


def get_page(link):
    """
    Gets the page content from a given link
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


def fix_scores(score_list):
    """
    Adjusts a team's score list overtime columns are added together.
    :param score_list:
    :return: list of length 5 (q1-q4, final)
    """
    if len(score_list) == 5:
        return score_list[:4] + ['0'] + [score_list[4]]
    else:
        new_list = score_list[:4]
        ot_sum = 0
        for ot in score_list[4:-1]:
            ot_sum += int(ot)
        return new_list + [str(ot_sum), score_list[-1]]


def get_quarter_scores(line):
    """
    Gets the quarter scores of both teams, with overtime scores clumped into
    :param line:
    :return: list of two lists [away_list, home_list]
    """
    num_quarters = int(list(line.children)[5].split('colspan="')[1][0]) - 1
    all_scores = []
    for team in ['away', 'home']:
        shift = 0 if team == 'away' else 1
        scores = []
        for quarter in range(num_quarters):
            scores.append(list(line.children)[5].split("'center\'>")[quarter + 1 + shift * (num_quarters + 1)].split('<')[0])
        all_scores.append(fix_scores(scores + [list(line.children)[5].split("<strong>")[shift + 1].split('<')[0]]))
    return all_scores


def main(season, month):
    # define data frames to store the data in
    player_df = pd.DataFrame(columns=['game_id', 'date', 'season', 'team', 'opp', 'starting_five', 'name', 'mp',
                                      'fg', 'fga', 'fgp', 'tp', 'tpa', 'tpp', 'ft', 'fta', 'ftp',
                                      'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'pm',
                                      'tsp', 'efgp', 'tpar', 'ftr', 'orbp', 'drbp', 'trbp',
                                      'astp', 'stlp', 'blkp', 'tovp', 'usgp', 'ortg', 'drtg'])
    game_df = pd.DataFrame(columns=['game_id', 'date', 'season', 'arena', 'away_name', 'home_name', 'pace',
                                    'away_q1', 'away_q2', 'away_q3', 'away_q4', 'away_ot', 'away_final',
                                    'away_fg', 'away_fga', 'away_fgp', 'away_tp', 'away_tpa', 'away_tpp',
                                    'away_ft', 'away_fta', 'away_ftp', 'away_orb', 'away_drb', 'away_trb',
                                    'away_ast', 'away_stl', 'away_blk', 'away_tov', 'away_pf', 'away_tsp',
                                    'away_efgp', 'away_tpar', 'away_ftr', 'away_orbp', 'away_drbp', 'away_trbp',
                                    'away_astp', 'away_stlp', 'away_blkp', 'away_tovp', 'away_usgp', 'away_ortg',
                                    'away_drtg',
                                    'home_q1', 'home_q2', 'home_q3', 'home_q4', 'home_ot', 'home_final',
                                    'home_fg', 'home_fga', 'home_fgp', 'home_tp', 'home_tpa', 'home_tpp',
                                    'home_ft', 'home_fta', 'home_ftp', 'home_orb', 'home_drb', 'home_trb',
                                    'home_ast', 'home_stl', 'home_blk', 'home_tov', 'home_pf', 'home_tsp',
                                    'home_efgp', 'home_tpar', 'home_ftr', 'home_orbp', 'home_drbp', 'home_trbp',
                                    'home_astp', 'home_stlp', 'home_blkp', 'home_tovp', 'home_usgp', 'home_ortg',
                                    'home_drtg'])
    # link for monthly page
    month_link = "https://www.basketball-reference.com/leagues/NBA_" + str(season) + "_games-" + month + ".html"
    month_page = get_page(month_link)

    # ensures link is valid
    if month_page is None:
        print("failed for link: {}".format(month_link))
        return None

    # loop every game
    for game in month_page.find_all('th', csk=True):
        box_link = 'https://www.basketball-reference.com/boxscores/' + game['csk'] + '.html'
        box_page = get_page(box_link)

        # all links are valid, so failing would be result of not having reached this date yet
        if box_page is None:
            print("No game played at link: {}".format(box_link))
            break

        # get basic information
        away_name = box_page.find_all('title')[0].get_text().split(' at ')[0]
        home_name = box_page.find_all('title')[0].get_text().split(' at ')[1].split(' Box')[0]
        date = list(box_page.find_all(class_="scorebox_meta")[0].children)[1].get_text()
        arena = list(box_page.find_all(class_="scorebox_meta")[0].children)[2].get_text()

        # define deeper tag; get score lines and pace
        deep = list(list(list(list(list(box_page.children)[3].children)[3].children)[1].children)[9].children)
        score_line = get_quarter_scores(deep[17])
        away_line, home_line = score_line[0], score_line[1]
        pace = list(deep[19].children)[5].split('pace" >')[2].split('<')[0]

        # beginning of full game line
        full_game_line = [game['csk'], date, season, arena, away_name, home_name, pace] + away_line

        # loops once for home and away team
        for team in ['away', 'home']:
            idx = 23 if team == 'away' else 27
            bas = list(list(list(list(list(deep[idx].children)[3].children)[1].children)[1].children)[6].children)
            adv = list(list(list(list(list(deep[idx + 2].children)[3].children)[1].children)[1].children)[6].children)

            for num in range(len(bas) - 1):
                if num % 2 == 0:
                    continue
                if num == 11:
                    continue

                player_bas_stats, player_adv_stats = bas[num], adv[num]
                player_line = [game['csk'], date, season,
                               away_name if team == 'away' else home_name,
                               away_name if team == 'home' else home_name,
                               1 if num < 11 else 0]
                try:
                    if list(player_bas_stats.children)[1].get_text()[0] not in '1234567890':
                        if list(player_bas_stats.children)[1].get_text() == 'Did Not Play':
                            player_line = player_line + [list(player_bas_stats.children)[0].get_text(),
                                                         '0:00'] + np.zeros(33).tolist()
                        else:
                            continue
                    else:
                        for stat in range(21):
                            player_line.append(list(player_bas_stats.children)[stat].get_text())
                        for stat in range(2, 16):
                            player_line.append(list(player_adv_stats.children)[stat].get_text())
                except IndexError:
                    continue
                player_df.loc[len(player_df)] = player_line
            team_bas_stats = list(list(list(list(deep[idx].children)[3].children)[1].children)[1].children)[8]
            team_adv_stats = list(list(list(list(deep[idx + 2].children)[3].children)[1].children)[1].children)[8]

            for stat in range(2, 19):
                full_game_line.append(list(list(team_bas_stats.children)[0].children)[stat].get_text())
            for stat in range(2, 16):
                full_game_line.append(list(list(team_adv_stats.children)[0].children)[stat].get_text())
            full_game_line = full_game_line + home_line if team == 'away' else full_game_line
        game_df.loc[len(game_df)] = full_game_line

    # save monthly results as CSVs
    player_df.to_csv("Data/NBA/Player/player_db_" + str(season) + '_' + month + ".csv", index_label="Index")
    game_df.to_csv("Data/NBA/Game/game_db_" + str(season) + '_' + month + ".csv", index_label="Index")
    print('Completed: {m} {y}'.format(m=month, y=season))


if __name__ == '__main__':
    for szn in np.arange(2001, 2020):
        for mon in ["october", "november", "december", "january", "february", "march", "april", "may", 'june']:
            main(szn, mon)
