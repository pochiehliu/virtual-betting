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
redirect, Response, session, url_for
)
import traceback
import click
import pandas as pd
import datetime as dt
import time


tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
print(tmpl_dir)
app = Flask(__name__, template_folder=tmpl_dir)

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
def homepage():
    """
    request.method:   "GET" or "POST"
    request.form:     if the browser submitted a form, this contains the data in the form
    request.args:     dictionary of URL arguments e.g., {a:1, b:2} for http://localhost?a=1&b=2
    """
    user_id = session.get('user_id')
    if user_id is None:
        statement = None
        balance = None
    else:
        statement = "SELECT balance FROM users WHERE u_id = " + user_id + ';'
        balance = db_select(statement).iloc[0, 0]

    betting_data = get_betting_data()

    return render_template("homepage.html", **betting_data)


@app.route('/user_place_bet', methods=['POST'])
def user_place_bet():
    """
    When user places a bet from the homepage
    """
    # TODO: need to know which site the user came from when placing bet
    amount = request.form['amount']
    o_id = request.form['odds_id']
    # TODO: implement checking function doing the following:
    # TODO: check that this is value Amount
    # TODO: check that user has at least this amount in balance
    error =  valid_amount(amount=amount, bet=o_id)
    if error is not None:
        statement = "INSERT INTO place_bet (o_id, u_ud, bet_size) VALUES "
        statement = statement + str((o_id, u_id, amount)) + ';'
        g.conn.execute(statement)
        return redirect('/', result='Successfully placed bet!')
    else:
        return redirect('/', result=error)


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = engine.execute(
            'SELECT * FROM user WHERE username = ?',
            (username,)).fetchone()
        if user is None:
            error = 'Incorrect username.'
        elif password != user['password']:
            error = 'Incorrect password.'
        else:
            session.clear()
            session['user_id'] = user['id']
            return render_template('homepage.html')
    return render_template('login.html', error=error)


@app.route('/register', methods=('GET', 'POST'))
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
            users = engine.execute('SELECT id FROM user WHERE username = ?',
                                   (username,)).fetchone()

            if users is not None:
                error = 'User {} is already registered.'.format(username)
            else:
                register_user(form_list)
                user = engine.execute('SELECT id FROM user WHERE username = ?',
                                      (username,)).fetchone()
                session.clear()
                session['user_id'] = user['id']
                return render_template('homepage.html')
        return render_template('register.html', error=error)


"""
General Functions
"""

def register_user(form_list):
    g.conn.execute(
        """INSERT INTO users (username, first_name, last_name, password)
        VALUES (?, ?, ?, ?)""", tuple(form_list)
    )

def valid_amount(amount, bet):
    # TODO: fill this function
    pass


def db_select(statement):
    return pd.read_sql(statement, g.conn)


def get_betting_data():
    lower = (dt.datetime.now() - dt.timedelta(days=1)).strftime('%Y-%m-%d')
    upper = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    bounds = """game_time > '""" + lower + "' AND game_time < '" + upper + "');"
    statement = """
    SELECT *
    FROM make_odds
    WHERE make_odds.g_id = (
        SELECT game.g_id
        FROM game
        WHERE """ + bounds
    return db_select(statement)


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
