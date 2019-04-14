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


tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
print(tmpl_dir)
app = Flask(__name__, template_folder=tmpl_dir)
app.secret_key = os.urandom(2)

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


@app.route('/')
def homepage(bet_msg=None):
    """
    request.method:   "GET" or "POST"
    request.form:     if the browser submitted a form, this contains the data in the form
    request.args:     dictionary of URL arguments e.g., {a:1, b:2} for http://localhost?a=1&b=2
    """
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

    context['betting_data'] = get_betting_data()

    return render_template("homepage.html", **context)


@app.route('/user_place_bet', methods=['POST'])
def user_place_bet():
    """
    When user places a bet from the homepage
    """
    # TODO: need to know which site the user came from when placing bet
    amount = request.form['amount']
    o_id = request.form['odds_id']
    error =  valid_amount(amount=amount, bet=o_id)
    if error is not None:
        statement = "INSERT INTO place_bet (o_id, u_ud, bet_size) VALUES {};".format(str((o_id, u_id, amount)))
        g.conn.execute(statement)
        bet_result = 'Successfully placed bet!'
    else:
        bet_result = error
    flash(bet_result)
    return redirect('/')


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
            session['balance'] = user['balance']
            session['logged_in'] = True
            return redirect('/')
        flash(error)
    return render_template('login.html')


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        first_name = request.form['first']
        last_name = request.form['last']
        password = request.form['password']
        form_list = [username, first_name, last_name, password]

        if None in form_list:
            error = 'Please fill in all fields.'
        else:
            user = engine.execute('SELECT id FROM user WHERE username = "%s"', (username,)).fetchone()
            if users is not None:
                error = 'User {} is already registered.'.format(username)
            else:
                register_user(form_list)
                session.clear()
                session['user_id'] = user['id']
                return render_template('homepage.html')
        flash(error)
    return render_template('register.html')


@app.route('/logout', methods=['GET'])
def logout():
    session.clear()
    flash("Successfully logged out")
    return render_template('login.html')


"""
General Functions
"""

def register_user(form_list):
    g.conn.execute(
        """INSERT INTO users (username, first_name, last_name, password)
        VALUES (?, ?, ?, ?)""", tuple(form_list)
    )

def valid_amount(amount):
    # verify input
    money = re.compile('|'.join([r'^\$?(\d*\.\d{1,2})$', r'^\$?(\d+)$', r'^\$(\d+\.?)$']))
    try:
        amount = [x for x in filter(None, money.match(amount).groups())][0]
    except AttributeError:
        return "Incorrect Input"

    # verify sufficient funds
    if amount > session.get('balance'):
        return "Insufficient Funds"


def db_select(statement):
    return pd.read_sql(statement, g.conn)


def get_betting_data():
    lower = (dt.datetime.now() - dt.timedelta(days=1)).strftime('%Y-%m-%d')
    upper = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    bounds = """game_time > '""" + lower + "' AND game_time < '" + upper
    games_statement = """SELECT * FROM game WHERE {b}';""".format(b=bounds)
    bet_statement = """
    SELECT *
    FROM make_odds
    WHERE make_odds.g_id in (
        SELECT game.g_id
        FROM game
        WHERE {b}');""".format(b=bounds)
    return clean_display_data(db_select(games_statement), db_select(bet_statement))


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
    return zip(game_times, away_team, home_team, away_ml, home_ml, away_ps, home_ps, over, under)
    # Link to deeper page


def get_best_bet(game_id, bt_id, side):
    dir = 'ASC' if bt_id == 2 and side == 'O' else 'DESC'
    statement = """
    SELECT odds_line, odds_payout
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
