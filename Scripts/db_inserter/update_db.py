"""
This script will connect to the data base, find the most
up-to-date data, and update with new data.
"""
from sqlalchemy import *
from sqlalchemy import exc
from scraping.basketball_reference import *
from scraping.sbr_betting import *
from scraping.sbr_game_order import *
from sqlalchemy.pool import NullPool


DB_USER = "pdg2116"
DB_PASSWORD = "f5ih31DBMB"
DB_SERVER = "w4111.cisxo09blonu.us-east-1.rds.amazonaws.com"
DATABASEURI = "postgresql://" + DB_USER + ":" + DB_PASSWORD + "@" + DB_SERVER + "/w4111"

engine = create_engine(DATABASEURI)

teams = pd.read_csv(DATA_DIR + 'db_inserts/team.csv')
teams = dict(zip(teams.name, teams.t_id))

statement = """SELECT * FROM team;"""
teams = pd.read_sql(statement, engine)
teams = dict(zip(teams.name, teams.t_id))


"""
TABLES TO UPDATE:
1) game
    - gets updated every day with new day's games
    
2) game_stats

3) player
    - check if there are new players

4) player_game_stats
    - gets updated every day with new day's games

5) make_odds
    - gets update intermittently (a few times/day)
"""

"""
UPDATES GAME TABLE WITH NEW GAMES
"""

statement = """SELECT * FROM game ORDER BY g_id;"""
old_games = pd.read_sql(statement, engine).g_id.values
last_day = old_games[-1][:6]

current = dt.datetime.now()
cur_month = '0' + str(current.month) if current.month < 10 else str(current.month)
current = str(current.year) + cur_month

while last_day <= current:
    new_games = get_available_games(int(last_day[:4]), calendar.month_name[int(last_day[4:6])].lower())

    for game in new_games.iterrows:
        if game.g_id in old_games:
            continue
        else:
            values = str(tuple(game))
            insert = """INSERT INTO game VALUES """ + values
            engine.execute(insert)

            last_day = str(int(last_day) - 1) if int(last_day[-2:]) > 1 else str(int(last_day[:4]) - 1) + '12'



