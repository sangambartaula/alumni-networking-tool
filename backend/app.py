# app.py
# app.py  (TOP OF FILE)

import os
from dotenv import load_dotenv  # <- import this BEFORE calling it
load_dotenv(os.getenv("DOTENV_PATH", ".env"))  # <- load .env early

from flask import Flask, redirect, request, url_for, session, send_from_directory
import requests  # for OAuth token exchange
import mysql.connector  # for MySQL connection
import secrets




app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'default_secret_key')

# MySQL credentials
mysql_host = os.getenv('MYSQLHOST')
mysql_user = os.getenv('MYSQLUSER')
mysql_pass = os.getenv('MYSQLPASSWORD')
mysql_db = os.getenv('MYSQL_DATABASE')
mysql_port = int(os.getenv('MYSQLPORT', 3306))

# LinkedIn OAuth
CLIENT_ID = os.getenv("LINKEDIN_CLIENT_ID")
CLIENT_SECRET = os.getenv("LINKEDIN_CLIENT_SECRET")
REDIRECT_URI = os.getenv("LINKEDIN_REDIRECT_URI")

# ---------------------- Existing routes ----------------------
@app.route('/')
def home():
    return send_from_directory('../frontend/public', 'index.html')

@app.route('/about')
def about():
    return 'About page coming soon', 200


@app.route('/alumni_style.css')
def alumni_css():
    return send_from_directory('../frontend/public', 'alumni_style.css')

@app.route('/app.js')
def serve_js():
    return send_from_directory('../frontend/public', 'app.js')

@app.route('/assets/<path:filename>')
def assets(filename):
    return send_from_directory('../frontend/public/assets', filename)

@app.route('/logout')
def logout():
    session.clear()
    return redirect('/')


# ---------------------- LinkedIn OAuth routes ----------------------

@app.route('/login/linkedin')
def login_linkedin():
    """Redirect user to LinkedIn's OAuth authorization page"""
    state = secrets.token_urlsafe(16)  # random string for CSRF protection
    session['oauth_state'] = state

    # Use OpenID Connect scopes to get profile and email
    scope = 'openid profile email'

    auth_url = (
        f"https://www.linkedin.com/oauth/v2/authorization?"
        f"response_type=code&client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
        f"&scope={scope}&state={state}&prompt=login" # force login each time for demonstration!
    )
    return redirect(auth_url)


@app.route('/auth/linkedin/callback')
def linkedin_callback():
    """Handle LinkedIn OAuth callback"""
    code = request.args.get('code')
    state = request.args.get('state')

    # Verify state to prevent CSRF (Cross-Site Request Forgery)
    if state != session.get('oauth_state'):
        return "Error: State mismatch. Potential CSRF attack.", 400

    if not code:
        return "Error: No code returned from LinkedIn", 400

    # Exchange authorization code for access token
    token_url = 'https://www.linkedin.com/oauth/v2/accessToken'
    data = {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': REDIRECT_URI,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    }
    resp = requests.post(token_url, data=data, headers={'Content-Type': 'application/x-www-form-urlencoded'})
    if resp.status_code != 200:
        return f"Error fetching access token: {resp.text}", 400

    access_token = resp.json().get('access_token')
    session['linkedin_token'] = access_token

    # Fetch user profile and email
    headers = {'Authorization': f'Bearer {access_token}'}
    userinfo_resp = requests.get('https://api.linkedin.com/v2/userinfo', headers=headers)

    if userinfo_resp.status_code != 200:
        return f"Error fetching LinkedIn user info: {userinfo_resp.text}", 400

    session['linkedin_profile'] = userinfo_resp.json()

    # After successful login, redirect to alumni dashboard
    return redirect('/alumni')

# ---------------------- Check for login ----------------------
def login_required(f):
    from functools import wraps
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'linkedin_token' not in session:
            return redirect(url_for('login_linkedin'))
        return f(*args, **kwargs)
    return decorated_function

# Apply it to alumni route
@app.route('/alumni')
@login_required
def alumni_page():
    return send_from_directory('../frontend/public', 'alumni.html')


# ---------------------- Error handler ----------------------
@app.errorhandler(404)
def not_found(e):
    return 'Page not found', 404

if __name__ == "__main__":
    app.run()
