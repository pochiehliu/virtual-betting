import time
from datetime import datetime, timedelta
import datetime as dt
from sqlalchemy import *
from sqlalchemy import exc
from scraping.basketball_reference import *
from scraping.sbr_betting import *
from scraping.sbr_game_order import *
from db_inserter.table_transformer import *
from dateutil.relativedelta import *

"""
GENERAL PURPOSE METHODS
"""


def select_all(table_name):
    return pd.read_sql("""SELECT * FROM """ + table_name + ";", conn)


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
    current = dt.datetime.now() + dt.timedelta(days=0 if day else 1)
    return current.strftime('%Y%m%d') if day else current.strftime('%Y%m')


def increment_date(date, day=False):
    date = dt.datetime.strptime(date, '%Y%m%d') if day else dt.datetime.strptime(date, '%Y%m')
    delta = dt.timedelta(days=1) if day else relativedelta(months=1)
    return (date + delta).strftime('%Y%m%d') if day else (date + delta).strftime('%Y%m')


"""
UPDATES MAKE_ODDS TABLE

SHOULD BE CALLED INTERMITTENTLY (FEW TIMES/DAY)

TAKES A GOOD 30 SEC
"""


def _get_odds(date):
    scraper = ScrapeSession()
    scraper.lengths = ['']  # because we only want full game betting odds
    return scraper.day_scraper([date])


def _get_team_order(date):
    scraper = GameOrderScraper()
    return scraper.day_scraper([date])


def update_make_odds():
    date = get_date(day=True)
    odds = _get_odds(date)
    game_order = _get_team_order(date)

    games = select_all('game')
    teams = select_all('team')

    make_odds = transform_make_odds(odds, game_order, games, teams)
    make_odds.loc[:, 'odds_time'] = str(dt.datetime.now())

    for idx, line in make_odds.iterrows():
        conn.execute(insert_to_db(table_name='make_odds', row=line[1:], columns=make_odds.columns[1:].tolist()))


"""
UPDATE BET DISPLAY CSV FILE
"""


def store_betting_data():
    lower = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    upper = (dt.datetime.now() + dt.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    bounds = """game_time > '""" + lower + "' AND game_time < '" + upper
    games_statement = """SELECT * FROM game WHERE {b}';""".format(b=bounds)
    df = clean_display_data(pd.read_sql(games_statement, conn))
    df.to_csv('Webpage/flaskr/betting_data.csv')


def clean_display_data(game_df):
    teams = select_all('team')
    teams = dict(zip(teams.t_id, teams.name))
    game_df[['t_id_home', 't_id_away']] = game_df.iloc[:, -2:].applymap(lambda x: teams[x])
    g_id = game_df.g_id
    game_times = game_df.game_time.map(lambda x: dt.datetime.strftime(x, '%c')).values
    away_team = game_df.t_id_away
    home_team = game_df.t_id_home
    away_ml = [get_best_bet(x, bt_id=1, side='V') for x in game_df.g_id.values]
    home_ml = [get_best_bet(x, bt_id=1, side='H') for x in game_df.g_id.values]
    away_ps = [get_best_bet(x, bt_id=3, side='V') for x in game_df.g_id.values]
    home_ps = [get_best_bet(x, bt_id=3, side='H') for x in game_df.g_id.values]
    over = [get_best_bet(x, bt_id=2, side='O') for x in game_df.g_id.values]
    under = [get_best_bet(x, bt_id=2, side='U') for x in game_df.g_id.values]
    df = pd.DataFrame(data={'game_times': game_times,
                            'g_id': g_id,
                            'away_team': away_team,
                            'home_team': home_team,
                            'away_ml_oid': [x[0] if x is not None else "-" for x in away_ml],
                            'away_ml_pay': [x[2] if x is not None else "-" for x in away_ml],
                            'home_ml_oid': [x[0] if x is not None else "-" for x in home_ml],
                            'home_ml_pay': [x[2] if x is not None else "-" for x in home_ml],
                            'away_ps_oid': [x[0] if x is not None else "-" for x in away_ps],
                            'away_ps_line': [x[1] if x is not None else "-" for x in away_ps],
                            'away_ps_pay': [x[2] if x is not None else "-" for x in away_ps],
                            'home_ps_oid': [x[0] if x is not None else "-" for x in home_ps],
                            'home_ps_line': [x[1] if x is not None else "-" for x in home_ps],
                            'home_ps_pay': [x[2] if x is not None else "-" for x in home_ps],
                            'over_oid': [x[0] if x is not None else "-" for x in over],
                            'over_line': [x[1] if x is not None else "-" for x in over],
                            'over_pay': [x[2] if x is not None else "-" for x in over],
                            'under_oid': [x[0] if x is not None else "-" for x in under],
                            'under_line': [x[1] if x is not None else "-" for x in under],
                            'under_pay': [x[2] if x is not None else "-" for x in under],
                            })
    return df


def get_best_bet(game_id, bt_id, side):
    direc = 'ASC' if bt_id == 2 and side == 'O' else 'DESC'
    statement = """
    SELECT o_id, odds_line, odds_payout
    FROM make_odds as m
    WHERE m.g_id = '{g_id}'
    AND m.bt_id = {bt_id}
    AND m.odds_side = '{side}'
    ORDER BY odds_time, m.odds_line {dir}, m.odds_payout DESC;
    """.format(g_id=game_id, bt_id=bt_id, side=side, dir=direc)
    return conn.execute(statement).fetchone()


while 1:
    # updates the odds every 30 min
    try:
        with open('./Packages/db_inserter/db_inserter/.DBurl.txt') as file:
            DB_URL = file.readline()

        engine = create_engine(DB_URL)
        conn = engine.connect()
        update_make_odds()
        store_betting_data()
        conn.close()

    except Exception:
        pass

    dt_odds = datetime.now() + timedelta(minutes=30)
    dt_odds = dt_odds.replace(minute=5) if dt_odds.minute > 35 else dt_odds.replace(minute=5)

    while datetime.now() < dt_odds:
        time.sleep(120)


