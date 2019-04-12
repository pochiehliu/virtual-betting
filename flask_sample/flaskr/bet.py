from flask import (
    Blueprint, flash, g, redirect, render_template, request, url_for
)

from werkzeug.exceptions import abort

from flaskr.auth import login_required
from flaskr.db import get_db

bp = Blueprint('Virtualbet', __name__)

######################## helper funcion ####################################

def get_game():
    games = get_db().execute(
        'SELECT g_id FROM game WHERE game_time > NOW()'
        ).fetchall()
    return games

def get_odds():
    odds = get_db().execute(
        'SELECT odds_table.odds FROM game, odds_table(?) WHERE game.g_id = odds_table.gid'
        ).fetchall()
    return odds

def get_hist_bets():
    hist_bets = get_db().execute

############################### index route #######################
@bp.route('/')
def index():
    # show game and odds
    games = get_game()
    odds = get_odds()
    
    return render_template('bet/index.html', posts=posts)
###################################################################

################################ bet page #########################
@bp.route('/bet', methods = ('GET', 'POST'))
@login_required
def bet():
    games = get_game()
    odds = get_odds()
    hist_bets = get_hist_bets()

    if request.method == 'POST':
        odds_side = request.form['Team']
        bet_size = request.form['Amount']
        error = None

        if odds_side is None or odds_side not in games.gid:
            error = 'Must pick a team.'

        elif bet_size is None or bet_size <= 0:
            error = 'Invalid amount.'

        if error is not None:
            flash(error)

        else:
            db = get_db()
            db.execute(
                'INSERT INTO place_bet (o_id, bet_time, u_id, bet_size)'
                ' VALUES (?, ?, ?, ?)'
            )
            db.commit()
            return redirect(url_for('Virtualbet/bet.html'))
    return render_template('Virtualbet/bet.html')
#####################################################################
        
