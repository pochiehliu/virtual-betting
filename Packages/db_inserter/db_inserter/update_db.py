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

"""
GENERAL PURPOSE METHODS
"""


def select_all(table_name, conn):
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
UPDATES GAME TABLE

THIS SHOULD BE CALLED ONCE A DAY
"""


def _get_team_dict(conn):
    team_id_map = select_all('team', conn)
    return dict(zip(team_id_map.short, team_id_map.t_id))


def _get_new_games(last_month):
    scraper = BaskRefScraper()
    scraper.set_season_month(last_month[:4], last_month[4:], fix_season=False)
    scraper.set_month_page()
    return scraper.get_available_games()


def _update_game(game):
    return "UPDATE game SET game_time = '{gt}' WHERE g_id = '{g_id}';".format(gt=game.game_time,
                                                                              g_id=game.g_id)


def _insert_new_games(new_games, done_games, conn):
    for idx, game in new_games.iterrows():
        match = done_games.loc[done_games.g_id == game.g_id]
        if len(match) == 0:
            conn.execute(insert_to_db(table_name='game', row=game))
        elif pd.to_datetime(game.game_time) != match.iloc[0].game_time:
            conn.execute(_update_game(game))


def update_game_table(conn):
    team_id_map = _get_team_dict(conn)
    done_games = select_all('game ORDER BY g_id', conn)
    last_month = done_games.g_id.values[-1][:6]
    current_month = get_date(day=False)

    while last_month <= current_month:
        new_games = _get_new_games(last_month)
        new_games.loc[:, ['t_id_home', 't_id_away']] = new_games.iloc[:, -2:].applymap(lambda x: team_id_map[x])
        _insert_new_games(new_games, done_games, conn)
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


def _update_game_stats_table(game_df, conn):
    for idx, game in transform_game_stats(game_df).iterrows():
        conn.execute(insert_to_db(table_name='game_stats', row=game))


def _update_player_table(player_df, conn):
    done_players = select_all('player', conn)
    done_players = set(done_players.first_name + ' ' + done_players.last_name)

    for idx, player in transform_player(player_df).iterrows():
        name = player.first_name + ' ' + player.last_name
        if name not in done_players:
            conn.execute(insert_to_db(table_name='player', row=player[1:]))


def _update_player_game_stats(player_df, conn):
    players = select_all('player', conn)
    teams = select_all('team', conn)

    for idx, player in transform_player_game_stats(player_df, players, teams).iterrows():
        if idx % 50 == 0:
            conn.execute(insert_to_db(table_name='player_game_stats', row=player))


def update_stats_tables(conn):
    all_games = select_all('game', conn).g_id.values
    done_games = select_all('game_stats', conn).g_id.values
    missing_games = np.array(list(set(all_games).difference(set(done_games))))

    player_df, game_df = _get_pdf_gdf(missing_games)

    _update_game_stats_table(game_df, conn)
    _update_player_table(player_df, conn)
    _update_player_game_stats(player_df, conn)


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


def update_make_odds(conn):
    date = get_date(day=True)
    odds = _get_odds(date)
    game_order = _get_team_order(date)

    games = select_all('game', conn)
    teams = select_all('team', conn)

    make_odds = transform_make_odds(odds, game_order, games, teams)
    make_odds.loc[:, 'odds_time'] = str(dt.datetime.now())

    for idx, line in make_odds.iterrows():
        conn.execute(insert_to_db(table_name='make_odds', row=line[1:], columns=make_odds.columns[1:].tolist()))


"""
UPDATE BET DISPLAY CSV FILE
"""


def store_betting_data(conn):
    lower = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    upper = (dt.datetime.now() + dt.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    bounds = """game_time > '""" + lower + "' AND game_time < '" + upper
    games_statement = """SELECT * FROM game WHERE {b}';""".format(b=bounds)
    df = clean_display_data(pd.read_sql(games_statement, conn), conn)
    df.to_csv('betting_data.csv')


def clean_display_data(game_df, conn):
    teams = select_all('team', conn)
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


def get_best_bet(game_id, bt_id, side, conn):
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
