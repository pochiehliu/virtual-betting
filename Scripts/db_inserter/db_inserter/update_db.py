"""
This script will connect to the data base, find the most
up-to-date data, and update with new data.
"""
import datetime as dt
from sqlalchemy import *
from sqlalchemy import exc
from scraping.basketball_reference import *
from scraping.sbr_betting import *
from scraping.sbr_game_order import *
from db_inserter.table_transformer import *
from dateutil.relativedelta import *


DB_USER = "pdg2116"
DB_PASSWORD = "f5ih31DBMB"
DB_SERVER = "w4111.cisxo09blonu.us-east-1.rds.amazonaws.com"
DB_URL = "postgresql://" + DB_USER + ":" + DB_PASSWORD + "@" + DB_SERVER + "/w4111"

engine = create_engine(DB_URL)

"""
GENERAL PURPOSE METHODS
"""


def select_all(table_name):
    return pd.read_sql("""SELECT * FROM """ + table_name + ";", engine)


def insert_to_db(table_name, row, columns=''):
    """
    :param table_name: name of table in database
    :param row: data frame row to be inserted
    :param columns: list of column names to insert into
    :return:
    """
    values = str(tuple(row))
    columns = str(tuple(columns)).replace("'", '')
    return """INSERT INTO """ + columns + table_name + """ VALUES """ + values


def get_date(day=False):
    current = dt.datetime.now() + dt.timedelta(days=0 if day else 1)
    return current.strftime('%Y%m%d') if day else current.strftime('%Y%m')


def increment_date(date, day=False):
    date = dt.datetime.strptime(date, '%Y%m%d') if day else dt.datetime.strptime(date, '%Y%m')
    delta = dt.timedelta(days=1) if day else relativedelta(months=1)
    return (date + delta).strftime('%Y%m%d') if day else (date + delta).strftime('%Y%m')


"""
UPDATES GAME TABLE

THIS SHOULD BE CALLED ONCE A DAY
"""


def _get_team_dict():
    team_id_map = select_all('team')
    return dict(zip(team_id_map.short, team_id_map.t_id))


def _get_new_games(last_month):
    scraper = BaskRefScraper()
    scraper.set_season_month(last_month[:4], last_month[4:], fix_season=False)
    scraper.set_month_page()
    return scraper.get_available_games()


def _insert_new_games(new_games, done_games):
    for idx, game in new_games.iterrows():
        if game.g_id not in done_games:
            engine.execute(insert_to_db(table_name='game', row=game))


def update_game_table():
    team_id_map = _get_team_dict()
    done_games = select_all('game ORDER BY g_id').g_id.values
    last_month = done_games[-1][:6]
    current_month = get_date(day=False)

    while last_month <= current_month:
        new_games = _get_new_games(last_month)
        new_games.loc[:, ['t_id_home', 't_id_away']] = new_games.iloc[:, -2:].applymap(lambda x: team_id_map[x])
        _insert_new_games(new_games, done_games)
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
        engine.execute(insert_to_db(table_name='game_stats', row=game))


def _update_player_table(player_df):
    done_players = select_all('player')
    done_players = set(done_players.first_name + ' ' + done_players.last_name)

    for idx, player in transform_player(player_df).iterrows():
        if player not in done_players:
            engine.execute(insert_to_db(table_name='player', row=player))


def _update_player_game_stats(player_df):
    players = select_all('player')
    teams = select_all('team')

    for idx, player in transform_player_game_stats(player_df, players, teams).iterrows():
        engine.execute(insert_to_db(table_name='player_game_stats', row=player))


def update_stats_tables():
    all_games = select_all('game').g_id.values
    done_games = select_all('game_stats').g_id.values
    missing_games = np.array(list(set(all_games).difference(set(done_games))))

    player_df, game_df = _get_pdf_gdf(missing_games)

    _update_game_stats_table(game_df)
    _update_player_table(player_df)
    _update_player_game_stats(player_df)


"""
UPDATES MAKE_ODDS TABLE

SHOULD BE CALLED INTERMITTENTLY (FEW TIMES/DAY)

TAKES A GOOD 30 SEC
"""


def _get_odds(date):
    scraper = ScrapeSession()
    scraper.lengths = ['']  # because we only want full game betting odds
    return scraper.day_scraper(date)


def _get_team_order(date):
    scraper = GameOrderScraper()
    return scraper.day_scraper(date)


def update_make_odds():
    date = get_date(day=True)
    odds = _get_odds(date)
    game_order = _get_team_order(date)

    games = select_all('game')
    teams = select_all('team')

    make_odds = transform_make_odds(odds, game_order, games, teams)
    make_odds.loc[:, 'odds_time'] = str(dt.datetime.now())
    for idx, line in make_odds.iterrows():
        insert_to_db(table_name='make_odds', row=line[1:], columns=make_odds.columns[1:])


