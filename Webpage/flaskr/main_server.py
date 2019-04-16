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


@app.context_processor
def override_url_for():
    return dict(url_for=dated_url_for)

def dated_url_for(endpoint, **values):
    if endpoint == 'static':
        filename = values.get('filename', None)
        if filename:
            file_path = os.path.join(app.root_path,
                                     endpoint, filename)
            values['q'] = int(os.stat(file_path).st_mtime)
    return url_for(endpoint, **values)


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
        first, balance = g.conn.execute(statement).fetchone()
        context['first_name'] = first
        context['balance'] = '{:20,.2f}'.format(balance)

    betting_data = get_betting_data()
    context['betting_data'] = betting_data
    context['games_indicator'] = True if len(betting_data) != 0 else False
    yesterday_games = get_yesterday()
    context['yes_games'] = yesterday_games
    context['yes_games_indicator'] = True if len(yesterday_games) != 0 else False


    if request.method == 'POST':
        if session.get('user_id') is None:
            flash("Must log in to place bets.")
            return redirect('/login')
        amount = request.form['amount']
        match = request.form['game']
        bet = request.form['bet']

        if bet == 'undefined' or match == 'undefined':
            bet_result = "Please select both a game and bet type."
        else:
            try:
                o_id = int(context['betting_data'].iloc[int(match)][bet + '_oid'])
            except ValueError:
                o_id = '-'

            if o_id == '-':
                bet_result = "Sorry, bet is not available."
            else:
                error =  valid_amount(amount=amount)
                if error is None:
                    u_id = int(session['user_id'])
                    statement = "INSERT INTO place_bet (o_id, u_id, bet_size) VALUES {};".format(str((o_id, u_id, amount)))
                    g.conn.execute(statement)
                    update_balance(u_id)
                    bet_result = 'Successfully placed bet!'
                    context['balance'] = g.conn.execute("SELECT balance FROM users WHERE u_id = {};".format(session.get('user_id'))).fetchone()[0]
                else:
                    bet_result = error
        flash(bet_result)
    return render_template("homepage.html", **context)


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = g.conn.execute('SELECT * FROM users WHERE username = %s;',(username,)).fetchone()
        if user is None:
            flash('Incorrect username.')
        elif password != user['password']:
            flash('Incorrect password.')
        else:
            session.clear()
            session['user_id'] = user['u_id']
            session['logged_in'] = True
            update_balance(user['u_id'])
            flash('Successfully logged in!')
            return redirect('/')
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

        user = g.conn.execute('SELECT u_id FROM users WHERE username = %s', (username,)).fetchone()
        if user is not None:
            flash('User "{}" is already registered.'.format(username))
        else:
            register_user(form_list)
            session.clear()
            session['user_id'] = g.conn.execute('SELECT u_id FROM users WHERE username = %s', (username,)).fetchone()[0]
            flash('Thank you for joining, first_name! We have loaded your account with $1000 to bet with! Enjoy!'.format(username))
            return redirect('/')
    return render_template('register.html')


@app.route('/profile', methods=['GET'])
def profile():
    if session.get('user_id') is None:
        flash("Must log in to access profile.")
        return redirect('/login')
    else:
        context = {}
        user_id = session['user_id']
        statement = "SELECT username, first_name, last_name, balance FROM users WHERE u_id = {id};".format(id=user_id)
        username, first, last, balance = g.conn.execute(statement).fetchone()
        context['username'] = username
        context['first_name'] = first
        context['last_name'] = last
        context['balance'] = '{:20,.2f}'.format(balance)

        bet_history = get_bet_history(user_id)
        context['win_count'] = (bet_history.win_lost == "WON").sum()
        context['loss_count'] = (bet_history.win_lost == "LOST").sum()
        context['tie_count'] = (bet_history.win_lost == "TIED").sum()
        context['bet_indicator'] = True if len(bet_history) > 0 else False
        context['bet_history'] = bet_history.iloc[:20]

    return render_template('profile.html', **context)


@app.route('/logout', methods=['GET'])
def logout():
    session.clear()
    flash("Successfully logged out")
    return redirect('/login')


@app.route('/gamepage', methods=['GET'])
def gamepage():
    g_id = request.args['gid']
    context = {}

    game_info = get_game_info(g_id)
    context['away_team'] = game_info.t_id_away
    context['home_team'] = game_info.t_id_home
    context['game_time'] = game_info.game_time
    context['head_to_head'] = get_head_to_head(g_id)
    context['last_five'] = get_last_five(g_id)

    return render_template('gamepage.html', **context)


"""
General Functions
"""

def register_user(form_list):
    g.conn.execute(
        """INSERT INTO users (username, first_name, last_name, password, balance)
        VALUES (%s, %s, %s, %s, 1000);""", tuple(form_list)
    )


def valid_amount(amount):
    if session.get('user_id') is None:
        flash('Please sign in to place bets.')
        return redirect('/')

    # verify input
    money = re.compile('|'.join([r'^\$?(\d*\.\d{1,2})$', r'^\$?(\d+)$', r'^\$(\d+\.?)$']))
    try:
        amount = [x for x in filter(None, money.match(amount).groups())][0]
    except AttributeError:
        return "Improper bet amount input."

    # verify sufficient funds
    balance = g.conn.execute("SELECT balance FROM users WHERE u_id = {};".format(session.get('user_id'))).fetchone()[0]
    if float(amount) > balance:
        return "Insufficient funds."
    elif float(amount) <= 0:
        return "Please insert postive value."


def db_select(statement):
    return pd.read_sql(statement, g.conn)


def get_betting_data():
    df = pd.read_csv('Webpage/flaskr/betting_data.csv')
    numeric_cols = ['away_ml_pay', 'home_ml_pay',
                    'away_ps_pay', 'away_ps_line',
                    'home_ps_pay', 'home_ps_line',
                    'over_pay', 'over_line',
                    'under_pay', 'under_line']
    df.loc[:, numeric_cols] = df[numeric_cols].round(decimals=3)
    return df


def get_bet_history(user_id):
    with open('Webpage/flaskr/major_queries/user_history.sql', 'r') as file:
        statement = file.read().replace('\n', ' ').replace('\t', ' ')
    df = pd.read_sql(statement.format(user=user_id), g.conn)

    # format df
    df.loc[:, ['v_score', 'h_score']] = df[['v_score', 'h_score']].applymap(lambda x: 'N/A' if x == 0 else x)
    df['bet_time'] = df.bet_time.map(lambda x: dt.datetime.strftime(x, '%c')).values
    df['odds_side'] = df.odds_side.map(lambda x: 'HOME' if x == 'H' else ('VISITOR' if x == 'V'
                                                                    else ('OVER' if x == 'O'
                                                                    else 'UNDER')))
    df['odds_line'] = df.odds_line.map(lambda x: 'Outright win' if x == 0 else x)
    return df


def get_head_to_head(g_id):
    away, home = g.conn.execute("SELECT t_id_away, t_id_home FROM game WHERE g_id = '{}';".format(g_id)).fetchone()
    with open('Webpage/flaskr/major_queries/head_to_head.sql', 'r') as file:
        statement = file.read().replace('\n', ' ').replace('\t', ' ')

    categories = ['Win %', 'Field Goal %', 'Three Point %', 'Rebound per Game', 'Turnover Per Game']
    away_stats = [g.conn.execute(i.format(tid=away)).fetchone()[0] for i in statement.split(';')[:-1]]
    home_stats = [g.conn.execute(i.format(tid=home)).fetchone()[0] for i in statement.split(';')[:-1]]

    df = pd.DataFrame(data={'category': categories, 'away_stat': away_stats,'home_stat': home_stats})

    return df


def get_last_five(g_id):
    teams = db_select("""SELECT * FROM team;""")
    teams = dict(zip(teams.t_id, teams.name))
    away, home = g.conn.execute("SELECT t_id_away, t_id_home FROM game WHERE g_id = '{}';".format(g_id)).fetchone()
    with open('Webpage/flaskr/major_queries/last_five.sql', 'r') as file:
        statement = file.read().replace('\n', ' ').replace('\t', ' ')

    df = pd.read_sql(statement.format(a=away, h=home), g.conn)
    df[['home_team', 'away_team']] = df[['t_id_home', 't_id_away']].applymap(lambda x: teams[x])

    away_cols = [x for x in df.columns if x.startswith('away_')]
    home_cols = [x for x in df.columns if x.startswith('home_')]
    df['date'] = df.game_time.map(lambda x: dt.datetime.strftime(x, '%c'))
    df['away_total'] = df[away_cols].sum(axis=1)
    df['home_total'] = df[home_cols].sum(axis=1)

    return df


def get_game_info(g_id):
    teams = db_select("""SELECT * FROM team;""")
    teams = dict(zip(teams.t_id, teams.name))
    game_df = pd.read_sql("SELECT * FROM game WHERE g_id = '{}';".format(g_id), g.conn)
    game_df[['t_id_home', 't_id_away']] = game_df.iloc[:, -2:].applymap(lambda x: teams[x])
    return game_df.iloc[0]


def update_balance(u_id):
    def prof_loss_calc(row):
            return row.bet_size * row.odds_payout if row.win_lost== 'WON' else -1 * row.bet_size
    with open('Webpage/flaskr/major_queries/user_history.sql', 'r') as file:
        statement = file.read().replace('\n', ' ').replace('\t', ' ')
    df = pd.read_sql(statement.format(user=u_id), g.conn)
    balance = 1000 + df.apply(lambda row: prof_loss_calc(row), axis=1).sum() if len(df) > 0 else 1000
    g.conn.execute("""UPDATE users SET balance = {b} WHERE u_id = {u}""".format(u=u_id, b=round(balance, 2)))


def get_yesterday():
    with open('Webpage/flaskr/major_queries/yesterday_results.sql', 'r') as file:
        statement = file.read().replace('\n', ' ').replace('\t', ' ')
    return pd.read_sql(statement, g.conn)


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
