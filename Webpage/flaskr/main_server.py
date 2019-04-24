"""
This is the main server.

Contains page functions for entire site.

Calls functions in server_functions.py.
"""

import os
from flask import (
Flask, request, render_template, g,
redirect, Response, session, url_for,
flash)
import traceback
import click
import time
from server_functions import *


tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=tmpl_dir)
app.secret_key = os.urandom(7)

with open('Webpage/flaskr/.DBurl.txt') as file:
    DB_URL = file.readline()
engine = create_engine(DB_URL)


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
        print("Problem g.connecting to database")
        traceback.print_exc()
        g.conn = None


@app.teardown_request
def teardown_request(exception):
  try:
    g.conn.close()
  except Exception:
    pass


@app.route('/', methods=['GET', 'POST'])
def homepage():
    context = {}
    if session.get('user_id') is None:
        context['logged_in'] = False
    else:
        context['logged_in'] = True
        user_id = session['user_id']
        statement = """
                    SELECT first_name, balance
                    FROM users
                    WHERE u_id = {id};
                    """.format(id=user_id)
        first, balance = g.conn.execute(statement).fetchone()
        context['first_name'] = first
        context['balance'] = '{:20,.2f}'.format(balance)

    betting_data = get_betting_data()
    context['betting_data'] = betting_data
    context['games_indicator'] = True if len(betting_data) != 0 else False
    yester_games = get_yesterday(g.conn)
    context['yes_games'] = yester_games
    context['yes_games_indicator'] = True if len(yester_games) != 0 else False

    # When user submits a bet
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
                o_id = int(context['betting_data'].iloc[int(match)][bet +
                                                                    '_oid'])
            except ValueError:
                o_id = '-'

            if o_id == '-':
                bet_result = "Match is in progress; bet is no longer available."
            else:
                error =  valid_amount(amount=amount, session=session,
                                      conn=g.conn)
                if error is None:
                    u_id = int(session['user_id'])
                    statement = """
                                INSERT INTO place_bet (o_id, u_id, bet_size)
                                VALUES {};
                                """.format(str((o_id, u_id, amount)))
                    g.conn.execute(statement)
                    update_balance(u_id, g.conn)
                    statement = """
                                SELECT balance
                                FROM users
                                WHERE u_id = {};
                                """.format(session.get('user_id'))
                    context['balance'] = g.conn.execute(statement).fetchone()[0]
                    bet_result = 'Successfully placed bet!'
                else:
                    bet_result = error

        flash(bet_result)
    return render_template("homepage.html", **context)


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = g.conn.execute("""
                              SELECT *
                              FROM users
                              WHERE username = %s;
                              """,(username,)).fetchone()
        if user is None:
            flash('Incorrect username.')
        elif password != user['password']:
            flash('Incorrect password.')
        else:
            session.clear()
            session['user_id'] = user['u_id']
            session['logged_in'] = True
            update_balance(user['u_id'], g.conn)
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

        user = g.conn.execute("""
                              SELECT u_id
                              FROM users
                              WHERE username = %s;
                              """, (username,)).fetchone()
        if user is not None:
            flash('User "{}" is already registered.'.format(username))
        else:
            register_user(form_list, g.conn)
            session.clear()
            session['user_id'] = g.conn.execute("""
                                                SELECT u_id
                                                FROM users
                                                WHERE username = %s;
                                                """, (username,)).fetchone()[0]
            session['logged_in'] = True
            flash("""
                  Thank you for joining, {}!
                  We have loaded your account with $1000 to bet with!
                  Enjoy!
                  """.format(first_name))
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
        statement = """
                    SELECT username, first_name, last_name, balance
                    FROM users
                    WHERE u_id = {id};
                    """.format(id=user_id)
        username, first, last, balance = g.conn.execute(statement).fetchone()
        context['username'] = username
        context['first_name'] = first
        context['last_name'] = last
        context['balance'] = '{:20,.2f}'.format(balance)

        bet_history = get_bet_history(user_id, g.conn)
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

    game_info = get_game_info(g_id, g.conn)
    context['away_team'] = game_info.t_id_away
    context['home_team'] = game_info.t_id_home
    context['game_time'] = game_info.game_time
    context['head_to_head'] = get_head_to_head(g_id, g.conn)
    context['last_five'] = get_last_five(g_id, g.conn)

    return render_template('gamepage.html', **context)


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
