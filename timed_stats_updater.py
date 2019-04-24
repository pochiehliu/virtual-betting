"""
This script will update the basketball gama data.
Currently set to run overnight.
"""
import time
from pytz import timezone
from datetime import datetime, timedelta
import datetime as dt
from sqlalchemy import *
from sqlalchemy import exc
from scraping.basketball_reference import *
from scraping.sbr_betting import *
from scraping.sbr_game_order import *
from db_inserter.table_transformer import *
from dateutil.relativedelta import *
from db_inserter.Logger import Logger

"""
GENERAL PURPOSE METHODS
"""


def select_all(table_name):
    return pd.read_sql("""SELECT * FROM {};""".format(table_name), conn)


def insert_to_db(table_name, row, columns=''):
    """
    :param table_name: name of table in database
    :param row: data frame row to be inserted
    :param columns: list of column names to insert into
    :return:
    """
    values = str(tuple(row))
    columns = str(tuple(columns)).replace("'", '') if isinstance(columns, list) else columns
    return """INSERT INTO """ + table_name + columns + """ VALUES """ + values


def get_date(day=False):
    current = datetime.now(timezone('US/Eastern')) + dt.timedelta(days=0 if day else 1)
    return current.strftime('%Y%m%d') if day else current.strftime('%Y%m')


def increment_date(date, day=False):
    date = dt.datetime.strptime(date, '%Y%m%d') if day else dt.datetime.strptime(date, '%Y%m')
    delta = dt.timedelta(days=1) if day else relativedelta(months=1)
    return (date + delta).strftime('%Y%m%d') if day else (date + delta).strftime('%Y%m')


"""
UPDATES GAME TABLE
"""


def _get_team_dict():
    team_id_map = select_all('team')
    return dict(zip(team_id_map.short, team_id_map.t_id))


def _get_new_games(last_month):
    scraper = BaskRefScraper()
    scraper.set_season_month(last_month[:4], last_month[4:], fix_season=False)
    scraper.set_month_page()
    return scraper.get_available_games()


def _update_game(game):
    return "UPDATE game SET game_time = '{gt}' WHERE g_id = '{g_id}';".format(gt=game.game_time,
                                                                              g_id=game.g_id)


def _insert_new_games(new_games, stored_games):
    # inserts / updates games in game table
    for idx, game in new_games.iterrows():
        match = stored_games.loc[stored_games.g_id == game.g_id]
        if len(match) == 0:
            conn.execute(insert_to_db(table_name='game', row=game))
        elif pd.to_datetime(game.game_time) != match.iloc[0].game_time:
            conn.execute(_update_game(game))


def _remove_old_games(stored_games, new_games):
    min_date = new_games.min()
    stored_subset = stored_games[stored_games > min_date]

    for game in set(stored_subset) - set(new_games):
        conn.execute("""DELETE FROM game WHERE g_id = '{}';""".format(game))


def update_game_table():
    team_id_map = _get_team_dict()
    stored_games = select_all('game ORDER BY g_id')
    last_month = stored_games.g_id.values[-1][:6]
    current_month = get_date(day=False)

    while last_month <= current_month:
        new_games = _get_new_games(last_month)
        new_games.loc[:, ['t_id_home', 't_id_away']] = new_games.iloc[:, -2:].applymap(lambda x: team_id_map[x])
        _insert_new_games(new_games, stored_games)
        _remove_old_games(stored_games.g_id.values, new_games.g_id.values)
        last_month = increment_date(last_month, day=False)


"""
UPDATES GAME_STATS TABLE
UPDATES PLAYER TABLE
UPDATES PLAYER_GAME_STATS TABLE

SHOULD BE RUN ONCE A DAY (OVERNIGHT OPTIMAL)
"""


def _get_pdf_gdf(missing_games):
    scraper = BaskRefScraper()
    scraper.get_month_stats(missing_games)
    return scraper.player_df, scraper.game_df


def _update_game_stats_table(game_df):
    for idx, game in transform_game_stats(game_df).iterrows():
        conn.execute(insert_to_db(table_name='game_stats', row=game))


def _update_player_table(player_df):
    done_players = select_all('player')
    done_players = set(done_players.first_name + ' ' + done_players.last_name)

    for idx, player in transform_player(player_df).iterrows():
        name = player.first_name + ' ' + player.last_name
        if name not in done_players:
            conn.execute(insert_to_db(table_name='player', row=player[1:]))


def _update_player_game_stats(player_df):
    players = select_all('player')
    teams = select_all('team')

    for idx, player in transform_player_game_stats(player_df, players, teams).iterrows():
        if idx % 50 == 0:
            conn.execute(insert_to_db(table_name='player_game_stats', row=player))


def update_stats_tables():
    all_games = select_all('game').g_id.values
    done_games = select_all('game_stats').g_id.values
    missing_games = np.sort(np.array(list(set(all_games).difference(set(done_games)))))

    player_df, game_df = _get_pdf_gdf(missing_games)

    _update_game_stats_table(game_df)
    _update_player_table(player_df)
    _update_player_game_stats(player_df)


"""
INFINITE FOR LOOP :)
"""


logger = Logger('bask_ref_log', '.')
while 1:
    try:
        with open('./Webpage/flaskr/.DBurl.txt') as file:
            DB_URL = file.readline()
        cur_time = datetime.now(timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S")
        engine = create_engine(DB_URL)
        conn = engine.connect()
        update_game_table()
        update_stats_tables()
        conn.close()
        logger.log('Successful bask ref scrape at {}'.format(cur_time))

    except Exception:
        logger.log('Scraping error for {}'.format(cur_time))

    dt_stats = datetime.now(timezone('US/Eastern')) + timedelta(days=1)
    dt_stats = dt_stats.replace(hour=6)

    while datetime.now(timezone('US/Eastern')) < dt_stats:
        time.sleep(3600)
