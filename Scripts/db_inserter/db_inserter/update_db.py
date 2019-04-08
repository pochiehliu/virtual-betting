"""
This script will connect to the data base, find the most
up-to-date data, and update with new data.
"""
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
DATABASEURI = "postgresql://" + DB_USER + ":" + DB_PASSWORD + "@" + DB_SERVER + "/w4111"

engine = create_engine(DATABASEURI)

"""
GENERAL PURPOSE METHODS
"""


def select_all(table):
    return pd.read_sql("""SELECT * FROM """ + table + ";", engine)


def insert_to_db(table, row, columns=''):
    """
    :param table: name of table in database
    :param row: data frame row to be inserted
    :param columns: list of column names to insert into
    :return:
    """
    values = str(tuple(row))
    columns = str(tuple(columns)).replace("'", '')
    return """INSERT INTO """ + columns + table + """ VALUES """ + values


"""
UPDATES GAME TABLE

THIS SHOULD BE CALLED ONCE A DAY
"""


def _get_team_dict():
    team_id_map = select_all('team')
    return dict(zip(team_id_map.short, team_id_map.t_id))


def _get_date(day=False):
    current = dt.datetime.now() + dt.timedelta(days=0 if day else 1)
    return current.strftime('%Y%m%d') if day else current.strftime('%Y%m')


def _increment_date(date, day=False):
    date = dt.datetime.strptime(date, '%Y%m%d') if day else dt.datetime.strptime(date, '%Y%m')
    delta = dt.timedelta(days=1) if day else relativedelta(months=1)
    return (date + delta).strftime('%Y%m%d') if day else (date + delta).strftime('%Y%m')


def update_game_table():
    team_id_map = _get_team_dict()
    done_games = pd.read_sql("""SELECT * FROM game ORDER BY g_id;""", engine).g_id.values
    last_month = done_games[-1][:6]
    current_month = _get_date(day=False)

    while last_month <= current_month:
        new_games = get_available_games(int(last_month[:4]), calendar.month_name[int(last_month[4:6])].lower())
        new_games.loc[:, ['t_id_home', 't_id_away']] = new_games.iloc[:, -2:].applymap(lambda x: team_id_map[x])
        for idx, game in new_games.iterrows():
            if game.g_id not in done_games:
                engine.execute(insert_to_db(table='game', row=game))
        last_month = _increment_date(last_month, day=False)


"""
UPDATES GAME_STATS TABLE
UPDATES PLAYER TABLE
UPDATES PLAYER_GAME_STATS TABLE

SHOULD BE RUN ONCE A DAY (OVERNIGHT OPTIMAL)
"""


def _get_pdf_gdf(missing_games):
    player_df = pd.DataFrame(columns=PLAYER_COLS)
    game_df = pd.DataFrame(columns=GAME_COLS)

    for game_id in missing_games:
        player_df, game_df, finished = get_game_stats(game_id, player_df, game_df)

    return player_df, game_df


def _update_game_stats_table(game_df):
    for idx, game in transform_game_stats(game_df).iterrows():
        engine.execute(insert_to_db(table='game_stats', row=game))


def _update_player_table(player_df):
    done_players = select_all('player')
    done_players = set(done_players.first_name + ' ' + done_players.last_name)

    for idx, player in transform_player(player_df).iterrows():
        if player not in done_players:
            engine.execute(insert_to_db(table='player', row=player))


def _update_player_game_stats(player_df):
    # transformer needs player_stats, players, teams
    players = select_all('player')
    teams = select_all('team')

    for idx, player in transform_player_game_stats(player_df, players, teams).iterrows():
        engine.execute(insert_to_db(table='player_game_stats', row=player))


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
    date = _get_date(day=True)
    odds = _get_odds(date)
    game_order = _get_team_order(date)

    games = select_all('game')
    teams = select_all('team')

    make_odds = transform_make_odds(odds, game_order, games, teams)
    make_odds.loc[:, 'odds_time'] = str(dt.datetime.now())
    for idx, line in make_odds.iterrows():
        insert_to_db(table='make_odds', row=line[1:], columns=make_odds.columns[1:])


