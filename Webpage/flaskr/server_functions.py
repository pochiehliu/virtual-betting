"""
This file contains a collection of python functions
that the main server calls to perform specific tasks.
"""
import pandas as pd
import re
from sqlalchemy import *
from sqlalchemy.pool import NullPool
import datetime as dt


def db_select(statement, conn):
    """
    Reads statement into pandas dataframe.
    """
    return pd.read_sql(statement, conn)


def register_user(form_list, conn):
    conn.execute(
        """
        INSERT INTO users (username, first_name, last_name, password, balance)
        VALUES (%s, %s, %s, %s, 1000);
        """, tuple(form_list))


def valid_amount(amount, session, conn):
    """
    Confirms if bet amount provided by user is valid by
    checking input format and whether there is enough
    funds to cover the bet.
    """
    if session.get('user_id') is None:
        flash('Please sign in to place bets.')
        return redirect('/')

    # Verify input format
    money = re.compile('|'.join([r'^\$?(\d*\.\d{1,2})$',
                                 r'^\$?(\d+)$',
                                 r'^\$(\d+\.?)$']))
    try:
        amount = [x for x in filter(None, money.match(amount).groups())][0]
    except AttributeError:
        error = "Improper bet amount input."
        return error

    # Verify Sufficient Funds
    statement = """SELECT balance
                   FROM users
                   WHERE u_id = {};
                """.format(session.get('user_id'))
    balance = conn.execute(statement).fetchone()[0]
    if float(amount) > balance:
        error = "Insufficient funds."
    elif float(amount) <= 0:
        error = "Please insert positive value."
    else:
        error = None
    return error


def get_betting_data():
    """
    Gets pre-fetched betting data.
    """
    df = pd.read_csv('Webpage/flaskr/betting_data.csv')
    numeric_cols = ['away_ml_pay', 'home_ml_pay',
                    'away_ps_pay', 'away_ps_line',
                    'home_ps_pay', 'home_ps_line',
                    'over_pay', 'over_line',
                    'under_pay', 'under_line']
    df.loc[:, numeric_cols] = df[numeric_cols].round(decimals=3)
    return df


def get_bet_history(user_id, conn):
    """
    Gets the user bet history. Used for balance calculating
    and "Profile" page display.
    """
    with open('Webpage/flaskr/major_queries/user_history.sql', 'r') as file:
        statement = file.read().replace('\n', ' ').replace('\t', ' ')
    df = pd.read_sql(statement.format(user=user_id), conn)

    # Format Data Frame
    df.loc[:, ['v_score', 'h_score']] = df[['v_score', 'h_score']].applymap(lambda x: 'TBD' if x == 0 else x)
    df['bet_time'] = df.bet_time.map(lambda x: dt.datetime.strftime(x, '%c')).values
    df['odds_side'] = df.odds_side.map(lambda x: 'Home' if x == 'H' else ('Visitor' if x == 'V'
                                                                    else ('Over' if x == 'O'
                                                                    else 'Under')))
    df['odds_line'] = df.odds_line.map(lambda x: 'Outright Win' if x == 0 else x)
    return df


def get_head_to_head(g_id, conn):
    """
    Gets head-to-head statistic comparison between two teams.
    """
    statement = """
                SELECT t_id_away, t_id_home
                FROM game
                WHERE g_id = '{}';
                """.format(g_id)
    away, home = conn.execute(statement).fetchone()
    with open('Webpage/flaskr/major_queries/head_to_head.sql', 'r') as file:
        statement = file.read().replace('\n', ' ').replace('\t', ' ')

    categories = ['Win %', 'Field Goal %', 'Three Point %',
                  'Rebounds per Game', 'Turnovers Per Game']
    away_stats = [conn.execute(i.format(tid=away)).fetchone()[0] for i in statement.split(';')[:-1]]
    home_stats = [conn.execute(i.format(tid=home)).fetchone()[0] for i in statement.split(';')[:-1]]

    df = pd.DataFrame(data={'category': categories,
                            'away_stat': away_stats,
                            'home_stat': home_stats})
    return df


def get_last_five(g_id, conn):
    """
    Gets results from last five matches between two teams.
    """
    teams = db_select("""SELECT * FROM team;""", conn)
    teams = dict(zip(teams.t_id, teams.name))
    statement = """
                SELECT t_id_away, t_id_home
                FROM game
                WHERE g_id = '{}';
                """.format(g_id)
    away, home = conn.execute(statement).fetchone()

    with open('Webpage/flaskr/major_queries/last_five.sql', 'r') as file:
        statement = file.read().replace('\n', ' ').replace('\t', ' ')

    df = pd.read_sql(statement.format(a=away, h=home), conn)
    df[['home_team', 'away_team']] = df[['t_id_home', 't_id_away']].applymap(lambda x: teams[x])

    away_cols = [x for x in df.columns if x.startswith('away_')]
    home_cols = [x for x in df.columns if x.startswith('home_')]
    df['date'] = df.game_time.map(lambda x: dt.datetime.strftime(x, '%c'))
    df['away_total'] = df[away_cols].sum(axis=1)
    df['home_total'] = df[home_cols].sum(axis=1)

    return df


def get_game_info(g_id, conn):
    """
    Gets basic game info to display on game page.
    """
    teams = db_select("""SELECT * FROM team;""", conn)
    teams = dict(zip(teams.t_id, teams.name))

    statement = """
                SELECT *
                FROM game
                WHERE g_id = '{}';
                """.format(g_id)
    game_df = db_select(statement, conn)

    # Maps team id to team name
    game_df[['t_id_home', 't_id_away']] = game_df.iloc[:, -2:].applymap(lambda x: teams[x])

    return game_df.iloc[0]


def update_balance(u_id, conn):
    """
    Updates balance of user.
    """
    def prof_loss(row):
        """
        Helper function to map game result to profit/loss
        """
        if row.win_lost == 'WON':
            return row.bet_size * (row.odds_payout - 1)
        elif row.win_lost == 'TIED':
            return 0
        else:
            return row.bet_size * -1

    with open('Webpage/flaskr/major_queries/user_history.sql', 'r') as file:
        statement = file.read().replace('\n', ' ').replace('\t', ' ')
    df = db_select(statement.format(user=u_id), conn)

    if len(df) > 0:
        balance = 1000 + df.apply(lambda x: prof_loss(x), axis=1).sum()
    else:
        balance = 1000

    statement = """
                UPDATE users SET balance = {b}
                WHERE u_id = {u};
                """.format(u=u_id, b=round(balance, 2))
    conn.execute(statement)


def get_yesterday(conn):
    """
    Gets yesterday's results in pandas data frame.
    """
    with open('Webpage/flaskr/major_queries/yesterday_results.sql', 'r') as file:
        statement = file.read().replace('\n', ' ').replace('\t', ' ')
    return pd.read_sql(statement, conn)
