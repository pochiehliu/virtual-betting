import pandas as pd
import datetime as dt
from sqlalchemy import *
from sqlalchemy import exc
from scraping.basketball_reference import *
from scraping.sbr_betting import *
from scraping.sbr_game_order import *
from db_inserter.table_transformer import *
from dateutil.relativedelta import *


with open('./Packages/db_inserter/db_inserter/.DBurl.txt') as file:
    DB_URL = file.readline()

engine = create_engine(DB_URL)


def db_select(statement):
    return pd.read_sql(statement, engine)


def get_betting_data():
    lower = (dt.datetime.now() - dt.timedelta(days=3)).strftime('%Y-%m-%d')
    upper = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    bounds = """game_time > '""" + lower + "' AND game_time < '" + upper
    games_statement = """SELECT game.g_id FROM game WHERE """ + bounds + "';"
    bet_statement = """
    SELECT *
    FROM make_odds
    WHERE make_odds.g_id in (
        SELECT game.g_id
        FROM game
        WHERE """ + bounds + "');"
    return db_select(games_statement), db_select(bet_statement)



"""
SELECT 
"""



