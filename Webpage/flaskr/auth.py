
import functools

from flask import (
    Blueprint,
    flash,
    g,
    redirect,
    render_template,
    request,  # there are four methods including GET, POST, UPDATE and ...
    session,
    url_for
)


from werkzeug.security import check_password_hash, generate_password_hash

from flaskr.db import get_db

############ bp basic set up ###################################

bp = Blueprint('auth', __name__, url_prefix='/auth')

################################################################

######## sub page? #############################################

## sub1, register
@bp.route('/register', methods=('GET', 'POST'))
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        db = get_db()
        error = None

        if not username:
            error = 'Username is required.'
        elif not password:
            error = 'Password is required.'
        elif db.execute(
            'SELECT id FROM user WHERE username = ?', (username,)
        ).fetchone() is not None:
            error = 'User {} is already registered.'.format(username)
		### need to modify it to prevent sql injection ###########

        if error is None:
            db.execute(
                'INSERT INTO user (username, password) VALUES (?, ?)',
                (username, generate_password_hash(password))
            )
            ### need to modify it to prevent sql injection ###########
            db.commit()
            return redirect(url_for('auth.login'))

        flash(error)

    return render_template('auth/register.html')

 ## sub2, login
@bp.route('/login', methods=('GET', 'POST'))
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        db = get_db()
        error = None
        user = db.execute(
            'SELECT * FROM user WHERE username = ?', (username,)
        ).fetchone()

        if user is None:
            error = 'Incorrect username.'
        elif not check_password_hash(user['password'], password):
            error = 'Incorrect password.'

        if error is None:
            session.clear()
            session['user_id'] = user['id']
            # session will remember the user login information
            # session is a dicitionary
            return redirect(url_for('index'))

        flash(error)

    return render_template('auth/login.html')

## sub3, helper function? use to check the user is login or not
@bp.before_app_request
def load_logged_in_user():
    user_id = session.get('user_id')

    if user_id is None:
        g.user = None
    else:
        g.user = get_db().execute(
            'SELECT * FROM user WHERE id = ?', (user_id,)
            ### need to modify it to prevent sql injection ###########
        ).fetchone()

## sub4, logout
@bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

## sub5, helper function, use to check when user post/delete/operate during log in
def login_required(view):
    @functools.wraps(view)
    def wrapped_view(**kwargs):
        if g.user is None:
            return redirect(url_for('auth.login'))

        return view(**kwargs)

    return wrapped_view
