"""
To run locally
    python main_server.py
Go to http://localhost:8111 in your browser
"""

import os
from sqlalchemy import *
from sqlalchemy.pool import NullPool
from flask import (
Flask, request, render_template, g,
redirect, Response, session, url_for,
flash
)
import traceback
import click
import pandas as pd
import datetime as dt
import time
import re


tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
print(tmpl_dir)
app = Flask(__name__, template_folder=tmpl_dir)
app.secret_key = os.urandom(7)

DATABASEURI = "postgresql://pdg2116:f5ih31DBMB@w4111.cisxo09blonu.us-east-1.rds.amazonaws.com/w4111"
engine = create_engine(DATABASEURI)


@app.before_request
def before_request():
  try:
    g.conn = engine.connect()
  except:
    print("Problem connecting to database")
    traceback.print_exc()
    g.conn = None


@app.teardown_request
def teardown_request(exception):
  try:
    g.conn.close()
  except Exception as e:
    pass


@app.route('/', methods=['GET', 'POST'])
def homepage():
    context = {}
    if session.get('user_id') is None:
        context['logged_in'] = False
    else:
        context['logged_in'] = True
        user_id = session['user_id']
        statement = "SELECT first_name, balance FROM users WHERE u_id = {id};".format(id=user_id)
        first, balance = engine.execute(statement).fetchone()
        context['first_name'] = first
        context['balance'] = '{:20,.2f}'.format(balance)

    # store_betting_data()
    betting_data = get_betting_data()
    context['betting_data'] = betting_data
    context['games_indicator'] = True if len(betting_data) != 0 else False

    if request.method == 'POST':
        amount = request.form['amount']
        match = request.form['game']
        bet = request.form['bet']

        if bet == 'undefined' or match == 'undefined':
            bet_result = "Please select both a game and bet type."
        else:
            o_id = int(context['betting_data'].iloc[int(match)][bet + '_oid'])
            print(o_id)
            error =  valid_amount(amount=amount)
            if error is None:
                u_id = int(session['user_id'])
                statement = "INSERT INTO place_bet (o_id, u_id, bet_size) VALUES {};".format(str((o_id, u_id, amount)))
                g.conn.execute(statement)
                bet_result = 'Successfully placed bet!'

                # TODO: implement balance update for user
                # update_balance(u_id)
            else:
                bet_result = error
        flash(bet_result)
    return render_template("homepage.html", **context)


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = engine.execute('SELECT * FROM users WHERE username = %s;',(username,)).fetchone()
        if user is None:
            error = 'Incorrect username.'
        elif password != user['password']:
            error = 'Incorrect password.'
        else:
            session.clear()
            session['user_id'] = user['u_id']
            session['logged_in'] = True
            flash('Successfully logged in!')
            return redirect('/')
        flash(error)
    return render_template('login.html')


@app.route('/register', methods=['GET', 'POST'])
def register():
    if session.get('user_id') is not None:
        flash('Already signed in')
        return redirect('/')

    if request.method == 'POST':
        username = request.form['username']
        first_name = request.form['first']
        last_name = request.form['last']
        password = request.form['password']
        form_list = [username, first_name, last_name, password]

        user = engine.execute('SELECT id FROM user WHERE username = "%s"', (username,)).fetchone()
        if users is not None:
            flash('User "{}" is already registered.'.format(username))
        else:
            register_user(form_list)
            session.clear()
            session['user_id'] = user['id']
            return render_template('homepage.html')
    return render_template('register.html')


@app.route('/profile', methods=['GET'])
def profile():
    if session.get('user_id') is None:
        flash("Must log in to access profile.")
        return redirect('/login')
    return render_template('profile.html')


@app.route('/logout', methods=['GET'])
def logout():
    session.clear()
    flash("Successfully logged out")
    return redirect('/login')


"""
General Functions
"""

def register_user(form_list):
    g.conn.execute(
        """INSERT INTO users (username, first_name, last_name, password)
        VALUES (?, ?, ?, ?);""", tuple(form_list)
    )

def valid_amount(amount):
    # verify input
    money = re.compile('|'.join([r'^\$?(\d*\.\d{1,2})$', r'^\$?(\d+)$', r'^\$(\d+\.?)$']))
    try:
        amount = [x for x in filter(None, money.match(amount).groups())][0]
    except AttributeError:
        return "Improper bet amount input."

    # verify sufficient funds
    if float(amount) > session.get('balance'):
        return "Insufficient funds."


def db_select(statement):
    return pd.read_sql(statement, g.conn)


def get_betting_data():
    df = pd.read_csv('betting_data.csv')
    numeric_cols = ['away_ml_pay', 'home_ml_pay',
                    'away_ps_pay', 'away_ps_line',
                    'home_ps_pay', 'home_ps_line',
                    'over_pay', 'over_line',
                    'under_pay', 'under_line']
    df.loc[:, numeric_cols] = df[numeric_cols].round(decimals=3)
    return df

def store_betting_data():
    lower = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    upper = (dt.datetime.now() + dt.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    bounds = """game_time > '""" + lower + "' AND game_time < '" + upper
    games_statement = """SELECT * FROM game WHERE {b}';""".format(b=bounds)
    bet_statement = """
    SELECT *
    FROM make_odds AS mo
    WHERE mo.g_id in (
        SELECT game.g_id
        FROM game
        WHERE {b}')
    AND mo.odds_time = (SELECT MAX(odds_time)
                        FROM make_odds);""".format(b=bounds)
    df = clean_display_data(db_select(games_statement), db_select(bet_statement))
    df.to_csv('betting_data.csv')

def clean_display_data(game_df, bet_df):
    teams = db_select("""SELECT * FROM team;""")
    teams = dict(zip(teams.t_id, teams.name))
    game_df[['t_id_home', 't_id_away']] = game_df.iloc[:, -2:].applymap(lambda x: teams[x])

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
                            'away_team': away_team,
                            'home_team': home_team,
                            'away_ml_oid': [x[0] for x in away_ml],
                            'away_ml_pay': [x[2] for x in away_ml],
                            'home_ml_oid': [x[0] for x in home_ml],
                            'home_ml_pay': [x[2] for x in home_ml],
                            'away_ps_oid': [x[0] for x in away_ps],
                            'away_ps_line': [x[1] for x in away_ps],
                            'away_ps_pay': [x[2] for x in away_ps],
                            'home_ps_oid': [x[0] for x in home_ps],
                            'home_ps_line': [x[1] for x in home_ps],
                            'home_ps_pay': [x[2] for x in home_ps],
                            'over_oid': [x[0] for x in over],
                            'over_line': [x[1] for x in over],
                            'over_pay': [x[2] for x in over],
                            'under_oid': [x[0] for x in under],
                            'under_line': [x[1] for x in under],
                            'under_pay': [x[2] for x in under],
                            })
    return df


def get_best_bet(game_id, bt_id, side):
    dir = 'ASC' if bt_id == 2 and side == 'O' else 'DESC'
    statement = """
    SELECT o_id, odds_line, odds_payout
    FROM make_odds as m
    WHERE m.g_id = '{g_id}'
    AND m.bt_id = {bt_id}
    AND m.odds_side = '{side}'
    ORDER BY m.odds_line {dir}, m.odds_payout DESC;
    """.format(g_id=game_id, bt_id=bt_id, side=side, dir=dir)
    return engine.execute(statement).fetchone()


if __name__ == "__main__":
  @click.command()
  @click.option('--debug', is_flag=True)
  @click.option('--threaded', is_flag=True)
  @click.argument('HOST', default='0.0.0.0')
  @click.argument('PORT', default=8111, type=int)
  def run(debug, threaded, host, port):
    HOST, PORT = host, port
    print("running on %s:%d" % (HOST, PORT))
    app.run(host=HOST, port=PORT, debug=debug, threaded=threaded)
  run()
