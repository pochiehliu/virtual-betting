from sqlalchemy import *
import pandas as pd


DB_USER = "pdg2116"
DB_PASSWORD = "f5ih31DBMB"
DB_SERVER = "w4111.cisxo09blonu.us-east-1.rds.amazonaws.com"
DATABASEURL = "postgresql://" + DB_USER + ":" + DB_PASSWORD + "@" + DB_SERVER + "/w4111"
engine = create_engine(DATABASEURL)

df = pd.read_sql("""SELECT * FROM team;""", engine)
df.to_csv('test_df.csv')