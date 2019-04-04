"""
This script will connect to the data base, find the most
up-to-date data, and update with new data.
"""
import pandas as pd
from sqlalchemy import *
from sqlalchemy.pool import NullPool


DB_USER = "pdg2116"
DB_PASSWORD = "f5ih31DBMB"
DB_SERVER = "w4111.cisxo09blonu.us-east-1.rds.amazonaws.com"
DATABASEURI = "postgresql://" + DB_USER + ":" + DB_PASSWORD + "@" + DB_SERVER + "/w4111"

engine = create_engine(DATABASEURI)

"""
TABLES TO UPDATE:
1) player
    - check if there are new players

2) game
    - gets updated every day with new day's games

3) player_game_stats
    - gets updated every day with new day's games

4) make_odds
    - gets update intermittently (a few times/day)
    - 
    
 
"""











statement = """SELECT * FROM game ORDER BY game_time;"""
r = engine.execute(statement)

results = []
for i in r:
    results.append(i)
r.close()

statement = """SELECT * FROM player_game_stats;"""
df = pd.read_sql(statement, engine)

