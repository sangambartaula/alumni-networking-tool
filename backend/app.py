# app.py
from flask import Flask, redirect, request, url_for, session, send_from_directory, jsonify
from dotenv import load_dotenv
from functools import wraps
import os
import requests  # for OAuth token exchange
import mysql.connector # for MySQL connection
import secrets
from database import get_connection

load_dotenv()

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

# ---------------------- Helper functions ----------------------

def get_current_user_id():
    """Get the current logged-in user's ID from LinkedIn profile"""
    if 'linkedin_profile' not in session:
        return None
    
    linkedin_profile = session['linkedin_profile']
    linkedin_id = linkedin_profile.get('sub')  # LinkedIn's unique ID
    
    # Get user from database
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users WHERE linkedin_id = %s", (linkedin_id,))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        print(f"Error getting user ID: {e}")
        return None
    finally:
        conn.close()

def login_required(f):
    """Decorator to require login"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'linkedin_token' not in session:
            return redirect(url_for('login_linkedin'))
        return f(*args, **kwargs)
    return decorated_function

def api_login_required(f):
    """Decorator to require login for API endpoints"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'linkedin_token' not in session:
            return jsonify({"error": "Not authenticated"}), 401
        return f(*args, **kwargs)
    return decorated_function

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

    linkedin_profile = userinfo_resp.json()
    session['linkedin_profile'] = linkedin_profile

    # NEW: Save/update user in database
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (linkedin_id, email, first_name, last_name, headline)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    email = VALUES(email),
                    first_name = VALUES(first_name),
                    last_name = VALUES(last_name),
                    headline = VALUES(headline),
                    updated_at = CURRENT_TIMESTAMP
            """, (
                linkedin_profile.get('sub'),
                linkedin_profile.get('email'),
                linkedin_profile.get('given_name'),
                linkedin_profile.get('family_name'),
                linkedin_profile.get('headline', '')
            ))
            conn.commit()
    except Exception as e:
        print(f"Error saving user to database: {e}")
    finally:
        conn.close()

    # After successful login, redirect to alumni dashboard
    return redirect('/alumni')

# ---------------------- Alumni page ----------------------
@app.route('/alumni')
@login_required
def alumni_page():
    return send_from_directory('../frontend/public', 'alumni.html')


# ---------------------- API endpoints for user interactions ----------------------

@app.route('/api/interaction', methods=['POST'])
@api_login_required
def add_interaction():
    """
    Add or update a user interaction (bookmarked, connected)
    
    Expected JSON body:
    {
        "alumni_id": 123,
        "interaction_type": "bookmarked" or "connected",
        "notes": "optional notes"
    }
    """
    try:
        print("DEBUG: add_interaction called")
        print(f"DEBUG: Session keys: {session.keys()}")
        print(f"DEBUG: Session linkedin_profile: {session.get('linkedin_profile')}")
        
        data = request.get_json()
        alumni_id = data.get('alumni_id')
        interaction_type = data.get('interaction_type')
        notes = data.get('notes', '')
        
        print(f"DEBUG: Request data - alumni_id: {alumni_id}, type: {interaction_type}")
        
        # Validate input
        if not alumni_id or not interaction_type:
            return jsonify({"error": "Missing alumni_id or interaction_type"}), 400
        
        if interaction_type not in ['bookmarked', 'connected']:
            return jsonify({"error": "Invalid interaction_type. Must be 'bookmarked' or 'connected'"}), 400
        
        # Get current user
        user_id = get_current_user_id()
        print(f"DEBUG: user_id returned: {user_id}")
        
        if not user_id:
            print("DEBUG: User not found! Checking database...")
            # Debug: check if user exists in database
            try:
                conn = get_connection()
                with conn.cursor() as cur:
                    cur.execute("SELECT id, linkedin_id, email FROM users LIMIT 5")
                    users = cur.fetchall()
                    print(f"DEBUG: Users in database: {users}")
                conn.close()
            except Exception as debug_err:
                print(f"DEBUG: Error checking database: {debug_err}")
            
            return jsonify({"error": "User not found"}), 401
        
        # Insert or update interaction
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO user_interactions (user_id, alumni_id, interaction_type, notes)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        notes = VALUES(notes),
                        updated_at = CURRENT_TIMESTAMP
                """, (user_id, alumni_id, interaction_type, notes))
                conn.commit()
            
            print(f"DEBUG: Interaction saved successfully")
            return jsonify({
                "success": True,
                "message": f"{interaction_type} added successfully"
            }), 200
        except mysql.connector.Error as err:
            print(f"DEBUG: MySQL error: {err}")
            conn.rollback()
            return jsonify({"error": f"Database error: {str(err)}"}), 500
        finally:
            conn.close()
    
    except Exception as e:
        print(f"DEBUG: Exception: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Server error: {str(e)}"}), 500


@app.route('/api/interaction', methods=['DELETE'])
@api_login_required
def remove_interaction():
    """
    Remove a user interaction
    
    Expected JSON body:
    {
        "alumni_id": 123,
        "interaction_type": "bookmarked" or "connected"
    }
    """
    try:
        data = request.get_json()
        alumni_id = data.get('alumni_id')
        interaction_type = data.get('interaction_type')
        
        if not alumni_id or not interaction_type:
            return jsonify({"error": "Missing alumni_id or interaction_type"}), 400
        
        user_id = get_current_user_id()
        if not user_id:
            return jsonify({"error": "User not found"}), 401
        
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM user_interactions
                    WHERE user_id = %s AND alumni_id = %s AND interaction_type = %s
                """, (user_id, alumni_id, interaction_type))
                conn.commit()
            
            return jsonify({"success": True, "message": "Interaction removed"}), 200
        except mysql.connector.Error as err:
            conn.rollback()
            return jsonify({"error": f"Database error: {str(err)}"}), 500
        finally:
            conn.close()
    
    except Exception as e:
        return jsonify({"error": f"Server error: {str(e)}"}), 500


@app.route('/api/user-interactions', methods=['GET'])
@api_login_required
def get_user_interactions():
    """
    Get all interactions for current user
    
    Returns array of interactions with alumni_id, interaction_type, notes, etc.
    """
    try:
        user_id = get_current_user_id()
        if not user_id:
            return jsonify({"error": "User not found"}), 401
        
        conn = get_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute("""
                    SELECT id, alumni_id, interaction_type, notes, created_at, updated_at
                    FROM user_interactions
                    WHERE user_id = %s
                    ORDER BY updated_at DESC
                """, (user_id,))
                interactions = cur.fetchall()
            
            # Convert datetime objects to strings for JSON serialization
            for interaction in interactions:
                interaction['created_at'] = interaction['created_at'].isoformat() if interaction['created_at'] else None
                interaction['updated_at'] = interaction['updated_at'].isoformat() if interaction['updated_at'] else None
            
            return jsonify({
                "success": True,
                "interactions": interactions
            }), 200
        finally:
            conn.close()
    
    except Exception as e:
        return jsonify({"error": f"Server error: {str(e)}"}), 500


# ---------------------- Error handler ----------------------
@app.errorhandler(404)
def not_found(e):
    return 'Page not found', 404

if __name__ == "__main__":
    app.run()