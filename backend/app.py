from flask import Flask, redirect, request, url_for, session, send_from_directory, jsonify
from dotenv import load_dotenv
from functools import wraps
import os
import requests  # for OAuth token exchange
import mysql.connector  # for MySQL connection
import secrets
from database import get_connection
from geocoding import geocode_location

load_dotenv()

app = Flask(
    __name__,
    static_folder="../frontend/public",
    static_url_path=""
)

app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'default_secret_key')

# Development toggle: set DISABLE_DB=1 in .env to skip all DB work (useful when RDS is down)
# This allows the backend to serve the frontend and static assets even without a database.
DISABLE_DB = os.getenv("DISABLE_DB", "0") == "1"
app.logger.info(f"DISABLE_DB = {DISABLE_DB}")

# SQLite fallback toggle: set USE_SQLITE_FALLBACK=1 to enable local SQLite backup
# This enables a "hybrid" mode where the app uses a local SQLite database if the
# remote MySQL database is unreachable.
USE_SQLITE_FALLBACK = os.getenv('USE_SQLITE_FALLBACK', '1') == '1'
app.logger.info(f"USE_SQLITE_FALLBACK = {USE_SQLITE_FALLBACK}")

# MySQL configuration (connections use get_connection())
mysql_host = os.getenv('MYSQLHOST')
mysql_user = os.getenv('MYSQLUSER')
mysql_pass = os.getenv('MYSQLPASSWORD')
mysql_db = os.getenv('MYSQL_DATABASE')
mysql_port = int(os.getenv('MYSQLPORT', 3306))

# LinkedIn OAuth
CLIENT_ID = os.getenv("LINKEDIN_CLIENT_ID")
CLIENT_SECRET = os.getenv("LINKEDIN_CLIENT_SECRET")
REDIRECT_URI = os.getenv("LINKEDIN_REDIRECT_URI")

# ---------------------- Access Control Configuration ----------------------

# Authorized email domains (faculty only)
# We restrict access to official faculty/staff emails to ensure ONLY authorized
# university personnel can view the networking tool's data.
AUTHORIZED_DOMAINS = ['@unt.edu']

def is_authorized_user(email):
    """
    Check if user email is authorized to access the system.
    Returns True if:
    1. Email is in the database whitelist, OR
    2. Email ends with an authorized domain (@unt.edu) AND is NOT a student email (@my.unt.edu)
    """
    if not email:
        return False
    email_lower = email.lower().strip()
    
    # Check database whitelist first (allows specific exceptions)
    try:
        from database import get_authorized_emails
        authorized_emails = get_authorized_emails()
        authorized_email_list = [e['email'].lower() for e in authorized_emails]
        
        if email_lower in authorized_email_list:
            app.logger.info(f"User {email_lower} authorized via database whitelist")
            return True
    except Exception as e:
        app.logger.error(f"Error checking authorized emails from database: {e}")
    
    # Explicitly block student emails (@my.unt.edu)
    if email_lower.endswith('@my.unt.edu'):
        app.logger.warning(f"Student email blocked: {email_lower}")
        return False
    
    # Check if email ends with authorized domain
    for domain in AUTHORIZED_DOMAINS:
        if email_lower.endswith(domain.lower()):
            app.logger.info(f"User {email_lower} authorized via domain {domain}")
            return True
    
    app.logger.warning(f"Unauthorized email attempted access: {email_lower}")
    return False

# ---------------------- Helper functions ----------------------


# =============================================================================
# DISCIPLINE CLASSIFICATION (computed on-the-fly, NOT stored in DB)
# Ordered list: first match wins.  Priority per alumni: job_title > degree > headline
# =============================================================================
# Approved engineering disciplines for the frontend filter
APPROVED_ENGINEERING_DISCIPLINES = [
    'Software, Data & AI Engineering',
    'Embedded, Electrical & Hardware Engineering',
    'Mechanical & Energy Engineering',
    'Biomedical Engineering',
    'Materials Science & Manufacturing',
    'Construction & Engineering Management',
]


def classify_degree(degree_field, headline=''):
    """
    Classify degree into Undergraduate / Graduate / PhD.
    Checks degree field first, then falls back to headline keywords.
    This is necessary because LinkedIn data is often unstructured; some users
    put their degree in their headline rather than the education section.
    Returns the level string or None if unclassifiable.
    """
    degree_lower = (degree_field or '').lower()
    if degree_lower:
        if any(t in degree_lower for t in ['bachelor', 'b.s.', 'b.a.', 'b.sc', 'undergraduate']):
            return 'Undergraduate'
        if any(t in degree_lower for t in ['master', 'm.s.', 'm.a.', 'mba', 'm.sc', 'graduate']):
            return 'Graduate'
        if any(t in degree_lower for t in ['doctor', 'ph.d', 'phd', 'doctorate']):
            return 'PhD'

    headline_lower = (headline or '').lower()
    if headline_lower:
        if any(t in headline_lower for t in ['bachelor', 'b.s.', 'b.a.', 'b.sc']):
            return 'Undergraduate'
        if any(t in headline_lower for t in ['master', 'm.s.', 'm.a.', 'mba', 'm.sc']):
            return 'Graduate'
        if any(t in headline_lower for t in ['doctor', 'ph.d', 'phd', 'doctorate']):
            return 'PhD'

    return None


# =============================================================================
# HEATMAP CACHE
# =============================================================================
import time as _time
_heatmap_cache = {}  # key: continent_filter -> {"data": ..., "ts": ...}
_HEATMAP_CACHE_TTL = 60  # seconds


def get_current_user_id():
    """
    Get the current logged-in user's database ID.
    In development mode (DISABLE_DB=1), returns a stable placeholder ID.
    """
    if 'linkedin_profile' not in session:
        return None

    linkedin_profile = session['linkedin_profile']
    linkedin_id = linkedin_profile.get('sub')  # LinkedIn's unique ID

    if DISABLE_DB:
        # If SQLite fallback is enabled, try to use it even in DISABLE_DB mode
        if not USE_SQLITE_FALLBACK:
            # Give APIs a consistent user id during demos with no DB
            session.setdefault('_dev_user_id', 1)
            return session['_dev_user_id']

    conn = None
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
        if conn:
            try:
                conn.close()
            except Exception:
                pass

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

# ---------------------- Static/Basic routes ----------------------
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

@app.route('/access-denied')
def access_denied():
    """Show access denied page for unauthorized users."""
    return send_from_directory('../frontend/public', 'access_denied.html'), 403


# ---------------------- LinkedIn OAuth routes ----------------------
@app.route('/login/linkedin')
def login_linkedin():
    """Redirect user to LinkedIn's OAuth authorization page"""
    state = secrets.token_urlsafe(16)  # random string for CSRF protection
    session['oauth_state'] = state

    # Use OpenID Connect scopes to get profile and email.
    # 'openid' is required for the OIDC flow, and 'profile'/'email' grant
    # access to the user's basic identification and contact info.
    scope = 'openid profile email'

    auth_url = (
        f"https://www.linkedin.com/oauth/v2/authorization?"
        f"response_type=code&client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
        f"&scope={scope}&state={state}&prompt=login"
    )
    return redirect(auth_url)

@app.route('/auth/linkedin/callback')
def linkedin_callback():
    """Handle LinkedIn OAuth callback"""
    code = request.args.get('code')
    state = request.args.get('state')

    # Verify state to prevent CSRF (Cross-Site Request Forgery).
    # We compare the 'state' returned by LinkedIn with the one we stored
    # in the session before the redirect.
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

    # Fetch user profile (OpenID Connect userinfo)
    headers = {'Authorization': f'Bearer {access_token}'}
    userinfo_resp = requests.get('https://api.linkedin.com/v2/userinfo', headers=headers)
    if userinfo_resp.status_code != 200:
        return f"Error fetching LinkedIn user info: {userinfo_resp.text}", 400

    linkedin_profile = userinfo_resp.json()
    session['linkedin_profile'] = linkedin_profile

    # ---- ACCESS CONTROL: Check if user is authorized ----
    # After obtaining the email from LinkedIn, we verify it against our
    # domain and whitelist rules before allowing a session to be established.
    user_email = linkedin_profile.get('email')
    if not is_authorized_user(user_email):
        app.logger.warning(f"⚠️ Unauthorized access attempt by: {user_email}")
        session.clear()  # Clear session to prevent any access
        return redirect('/access-denied')
    # ------------------------------------------------------

    if DISABLE_DB:
        return redirect('/alumni')

    # ----------------------------------------------------

    # Save/update user in database (safe connection handling)
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (linkedin_id, email, first_name, last_name)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    email = VALUES(email),
                    first_name = VALUES(first_name),
                    last_name = VALUES(last_name),
                    updated_at = CURRENT_TIMESTAMP
            """, (
                linkedin_profile.get('sub'),
                linkedin_profile.get('email'),
                linkedin_profile.get('given_name'),
                linkedin_profile.get('family_name'),
            ))
            conn.commit()
    except Exception as e:
        app.logger.error(f"❌ Error saving user to database: {e}")
        # Let user proceed even if DB write failed
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    # After successful login, redirect to alumni dashboard
    return redirect('/alumni')

# ---------------------- Alumni page ----------------------
@app.route('/alumni')
@login_required
def alumni_page():
    return send_from_directory('../frontend/public', 'alumni.html')

@app.route('/heatmap')
@login_required
def heatmap_page():
    return send_from_directory('../frontend/public', 'heatmap.html')

@app.route('/analytics')
@login_required
def analytics_page():
    return send_from_directory('../frontend/public', 'analytics.html')

@app.route('/heatmap.js')
def serve_heatmap_js():
    return send_from_directory('../frontend/public', 'heatmap.js')

@app.route('/heatmap_style.css')
def serve_heatmap_css():
    return send_from_directory('../frontend/public', 'heatmap_style.css')

# ---------------------- API endpoints for user interactions ----------------------
@app.route('/api/interaction', methods=['POST'])
@api_login_required
def add_interaction():
    """
    Add or update a user interaction (bookmarked, connected).
    This tracks which alumni a user has engaged with, allowing for
    personalized relationship management within the tool.
    Body:
    { "alumni_id": 123, "interaction_type": "bookmarked"|"connected", "notes": "..." }
    """
    # Short-circuit in dev when DB is disabled
    if DISABLE_DB:
        if not USE_SQLITE_FALLBACK:
            return jsonify({"success": True, "message": "DB disabled (dev). No-op."}), 200

    try:
        data = request.get_json()
        alumni_id = data.get('alumni_id')
        interaction_type = data.get('interaction_type')
        notes = data.get('notes', '')

        if not alumni_id or not interaction_type:
            return jsonify({"error": "Missing alumni_id or interaction_type"}), 400
        if interaction_type not in ['bookmarked', 'connected']:
            return jsonify({"error": "Invalid interaction_type. Must be 'bookmarked' or 'connected'"}), 400

        user_id = get_current_user_id()
        if not user_id:
            return jsonify({"error": "User not found"}), 401

        conn = get_connection()
        use_sqlite = DISABLE_DB and USE_SQLITE_FALLBACK
        
        try:
            if use_sqlite:
                # SQLite mode - use INSERT OR REPLACE pattern
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO user_interactions (user_id, alumni_id, interaction_type, notes, created_at, updated_at)
                    VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
                    ON CONFLICT(user_id, alumni_id, interaction_type) DO UPDATE SET
                        notes = excluded.notes,
                        updated_at = datetime('now')
                """, (user_id, alumni_id, interaction_type, notes))
                conn.commit()
            else:
                # MySQL mode - use ON DUPLICATE KEY UPDATE
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO user_interactions (user_id, alumni_id, interaction_type, notes)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            notes = VALUES(notes),
                            updated_at = CURRENT_TIMESTAMP
                    """, (user_id, alumni_id, interaction_type, notes))
                    conn.commit()
            return jsonify({"success": True, "message": f"{interaction_type} added successfully"}), 200
        except Exception as err:
            try:
                conn.rollback()
            except Exception:
                pass
            app.logger.error(f"❌ Database error adding interaction: {err}")
            return jsonify({"error": f"Database error: {str(err)}"}), 500
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    except Exception as e:
        app.logger.error(f"❌ Server error adding interaction: {e}")
        return jsonify({"error": f"Server error: {str(e)}"}), 500


@app.route('/api/interaction', methods=['DELETE'])
@api_login_required
def remove_interaction():
    """
    Remove a user interaction
    Body:
    { "alumni_id": 123, "interaction_type": "bookmarked"|"connected" }
    """
    # Short-circuit in dev when DB is disabled
    if DISABLE_DB:
        if not USE_SQLITE_FALLBACK:
            return jsonify({"success": True, "message": "DB disabled (dev). No-op."}), 200

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
        use_sqlite = DISABLE_DB and USE_SQLITE_FALLBACK
        
        try:
            if use_sqlite:
                # SQLite mode - use ? placeholders
                cursor = conn.cursor()
                cursor.execute("""
                    DELETE FROM user_interactions
                    WHERE user_id = ? AND alumni_id = ? AND interaction_type = ?
                """, (user_id, alumni_id, interaction_type))
                conn.commit()
            else:
                # MySQL mode - use %s placeholders
                with conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM user_interactions
                        WHERE user_id = %s AND alumni_id = %s AND interaction_type = %s
                    """, (user_id, alumni_id, interaction_type))
                    conn.commit()
            return jsonify({"success": True, "message": "Interaction removed"}), 200
        except Exception as err:
            try:
                conn.rollback()
            except Exception:
                pass
            app.logger.error(f"❌ Database error removing interaction: {err}")
            return jsonify({"error": f"Database error: {str(err)}"}), 500
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    except Exception as e:
        app.logger.error(f"❌ Server error removing interaction: {e}")
        return jsonify({"error": f"Server error: {str(e)}"}), 500


@app.route('/api/user-interactions', methods=['GET'])
@api_login_required
def get_user_interactions():
    """
    Get all interactions for current user
    """
    if DISABLE_DB:
        # Short-circuit in dev when DB is disabled unless fallback is enabled
        if not USE_SQLITE_FALLBACK:
            return jsonify({"success": True, "interactions": []}), 200

    try:
        user_id = get_current_user_id()
        if not user_id:
            return jsonify({"error": "User not found"}), 401

        conn = get_connection()
        use_sqlite = DISABLE_DB and USE_SQLITE_FALLBACK
        
        try:
            if use_sqlite:
                # SQLite mode - use dictionary cursor for dict results
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute("""
                        SELECT id, alumni_id, interaction_type, notes, created_at, updated_at
                        FROM user_interactions
                        WHERE user_id = ?
                        ORDER BY updated_at DESC
                    """, (user_id,))
                    interactions = cursor.fetchall()
            else:
                # MySQL mode - use dictionary cursor
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
                created_at = interaction.get('created_at')
                updated_at = interaction.get('updated_at')
                if hasattr(created_at, 'isoformat'):
                    interaction['created_at'] = created_at.isoformat()
                if hasattr(updated_at, 'isoformat'):
                    interaction['updated_at'] = updated_at.isoformat()

            return jsonify({"success": True, "interactions": interactions}), 200
        except Exception as err:
            app.logger.error(f"❌ Database error getting user interactions: {err}")
            return jsonify({"error": f"Database error: {str(err)}"}), 500
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    except Exception as e:
        app.logger.error(f"❌ Server error getting user interactions: {e}")
        return jsonify({"error": f"Server error: {str(e)}"}), 500



@app.route('/api/alumni', methods=['GET'])
def api_get_alumni():
    """
    Fetch all alumni from the database with pagination support.
    """
    # Short-circuit in dev when DB is disabled
    if DISABLE_DB:
        if not USE_SQLITE_FALLBACK:
            return jsonify({"success": True, "alumni": []}), 200

    try:
        try:
            limit = int(request.args.get('limit', 10000))  # Default to large number to get all records
        except Exception:
            limit = 10000
        try:
            offset = int(request.args.get('offset', 0))
        except Exception:
            offset = 0

        conn = get_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute("""
                    SELECT a.id, a.first_name, a.last_name, a.grad_year, a.degree, a.major,
                           a.linkedin_url, a.current_job_title, a.company, a.location, a.headline,
                           a.updated_at, njt.normalized_title, nc.normalized_company,
                           a.working_while_studying
                    FROM alumni a
                    LEFT JOIN normalized_job_titles njt ON a.normalized_job_title_id = njt.id
                    LEFT JOIN normalized_companies nc ON a.normalized_company_id = nc.id
                    ORDER BY a.last_name ASC, a.first_name ASC
                    LIMIT %s OFFSET %s
                """, (limit, offset))
                rows = cur.fetchall()

            alumni = []
            for r in rows:
                full_degree = r.get('degree') or ''
                degree_level = classify_degree(full_degree, r.get('headline', ''))

                # Read discipline from DB — set at scrape time by Groq, not computed here
                discipline = r.get('major') or 'Other'
                
                alumni.append({
                    "id": r.get('id'),
                    "name": f"{r.get('first_name','').strip()} {r.get('last_name','').strip()}".strip(),

                    # WHAT ANALYTICS.JS EXPECTS:
                    "current_job_title": r.get('current_job_title'),
                    "company": r.get('company'),
                    "grad_year": r.get('grad_year'),
                    "major": discipline,

                    # OPTIONAL: keep backwards-compatible for alumni UI (if needed)
                    "role": r.get('current_job_title'),

                    # Existing fields
                    "headline": r.get('headline'),
                    "class": r.get('grad_year'),
                    "location": r.get('location'),
                    "linkedin": r.get('linkedin_url'),
                    "degree": degree_level,
                    "full_degree": full_degree,
                    "updated_at": r.get('updated_at'),
                    "normalized_title": r.get('normalized_title'),
                    "normalized_company": r.get('normalized_company'),
                    # Prefer the fine-grained status string ("yes"/"no"/"currently");
                    # fall back to deriving from the boolean if status not yet stored.
                    "working_while_studying": (
                        r.get('working_while_studying_status')
                        or (True if r.get('working_while_studying') else
                            (False if r.get('working_while_studying') is not None else None))
                    )
                })

            return jsonify({"success": True, "alumni": alumni}), 200
        except Exception as err:
            app.logger.error(f"❌ Database error fetching alumni: {err}")
            return jsonify({"error": f"Database error: {str(err)}"}), 500
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    except mysql.connector.Error as err:
        app.logger.error(f"❌ MySQL error fetching alumni: {err}")
        return jsonify({"error": f"Database error: {str(err)}"}), 500
    except Exception as e:
        app.logger.error(f"❌ Error fetching alumni: {e}")
        return jsonify({"error": f"Server error: {str(e)}"}), 500


@app.route('/api/alumni/<int:alumni_id>', methods=['GET'])
def api_get_alumni_detail(alumni_id):
    """
    Fetch full detail for a single alumni by ID.
    Returns all raw education/experience fields (up to 3 each),
    headline, and updated_at for the detailed profile view.
    """
    if DISABLE_DB:
        if not USE_SQLITE_FALLBACK:
            return jsonify({"error": "Alumni not found"}), 404

    try:
        conn = get_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute("""
                    SELECT a.id, a.first_name, a.last_name, a.grad_year, a.degree, a.major,
                           a.linkedin_url, a.current_job_title, a.company, a.location, a.headline,
                           a.updated_at,
                           a.school, a.school2, a.school3,
                           a.degree AS degree_raw, a.degree2, a.degree3,
                           a.major AS major_raw, a.major2, a.major3,
                           a.school_start_date, a.job_start_date, a.job_end_date,
                           a.exp2_title, a.exp2_company, a.exp2_dates,
                           a.exp3_title, a.exp3_company, a.exp3_dates
                    FROM alumni a
                    WHERE a.id = %s
                    LIMIT 1
                """, (alumni_id,))
                r = cur.fetchone()

            if not r:
                return jsonify({"error": "Alumni not found"}), 404

            # Format updated_at for JSON
            updated_at = r.get('updated_at')
            if hasattr(updated_at, 'isoformat'):
                updated_at = updated_at.isoformat()

            detail = {
                "id": r.get('id'),
                "name": f"{(r.get('first_name') or '').strip()} {(r.get('last_name') or '').strip()}".strip(),
                "headline": r.get('headline'),
                "location": r.get('location'),
                "linkedin": r.get('linkedin_url'),
                "updated_at": updated_at,

                # Education 1 (UNT primary)
                "school": r.get('school') or 'University of North Texas',
                "degree": r.get('degree_raw'),
                "major": r.get('major_raw'),
                "grad_year": r.get('grad_year'),
                "school_start_date": r.get('school_start_date'),

                # Education 2
                "school2": r.get('school2'),
                "degree2": r.get('degree2'),
                "major2": r.get('major2'),

                # Education 3
                "school3": r.get('school3'),
                "degree3": r.get('degree3'),
                "major3": r.get('major3'),

                # Experience 1
                "current_job_title": r.get('current_job_title'),
                "company": r.get('company'),
                "job_start_date": r.get('job_start_date'),
                "job_end_date": r.get('job_end_date'),

                # Experience 2
                "exp2_title": r.get('exp2_title'),
                "exp2_company": r.get('exp2_company'),
                "exp2_dates": r.get('exp2_dates'),

                # Experience 3
                "exp3_title": r.get('exp3_title'),
                "exp3_company": r.get('exp3_company'),
                "exp3_dates": r.get('exp3_dates'),
            }

            return jsonify({"success": True, "alumni": detail}), 200
        except Exception as err:
            app.logger.error(f"❌ Database error fetching alumni detail {alumni_id}: {err}")
            return jsonify({"error": f"Database error: {str(err)}"}), 500
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    except Exception as e:
        app.logger.error(f"❌ Error fetching alumni detail: {e}")
        return jsonify({"error": f"Server error: {str(e)}"}), 500


@app.route('/profile_modal.js')
def serve_profile_modal_js():
    return send_from_directory('../frontend/public', 'profile_modal.js')

@app.route('/profile_modal.css')
def serve_profile_modal_css():
    return send_from_directory('../frontend/public', 'profile_modal.css')

@app.route('/profile_modal_test.js')
def serve_profile_modal_test_js():
    return send_from_directory('../frontend/public', 'profile_modal_test.js')


# ===== NOTES API ENDPOINTS =====

@app.route('/api/notes/<int:alumni_id>', methods=['GET'])
@api_login_required
def get_notes(alumni_id):
    """Get notes for a specific alumni (for current logged-in user)"""
    try:
        user_id = get_current_user_id()
        
        if DISABLE_DB:
            if not USE_SQLITE_FALLBACK:
                return jsonify({"success": True, "note": None}), 200
        
        conn = get_connection()
        use_sqlite = DISABLE_DB and USE_SQLITE_FALLBACK
        
        try:
            if use_sqlite:
                # SQLite mode - use dictionary cursor for dict results
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute("""
                        SELECT id, note_content, created_at, updated_at
                        FROM notes
                        WHERE user_id = ? AND alumni_id = ?
                        LIMIT 1
                    """, (user_id, alumni_id))
                    note = cursor.fetchone()
            else:
                # MySQL mode - use %s placeholders
                with conn.cursor(dictionary=True) as cur:
                    cur.execute("""
                        SELECT id, note_content, created_at, updated_at
                        FROM notes
                        WHERE user_id = %s AND alumni_id = %s
                        LIMIT 1
                    """, (user_id, alumni_id))
                    note = cur.fetchone()
            
            if note:
                # Handle timestamp formatting
                created_at = note.get('created_at')
                updated_at = note.get('updated_at')
                if hasattr(created_at, 'isoformat'):
                    created_at = created_at.isoformat()
                if hasattr(updated_at, 'isoformat'):
                    updated_at = updated_at.isoformat()
                
                return jsonify({
                    "success": True,
                    "note": {
                        "id": note['id'],
                        "note_content": note['note_content'],
                        "created_at": created_at,
                        "updated_at": updated_at
                    }
                }), 200
            else:
                return jsonify({"success": True, "note": None}), 200
        except Exception as err:
            app.logger.error(f"❌ Database error getting notes for alumni {alumni_id}: {err}")
            return jsonify({"success": False, "error": f"Database error: {str(err)}"}), 500
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        
    except Exception as e:
        app.logger.error(f"❌ Error getting notes: {e}")
        return jsonify({"success": False, "error": str(e)}), 500



@app.route('/api/notes', methods=['GET'])
@api_login_required
def get_all_notes():
    """Get all notes for the current logged-in user, grouped by alumni_id"""
    try:
        user_id = get_current_user_id()
        
        if DISABLE_DB:
            if not USE_SQLITE_FALLBACK:
                return jsonify({"success": True, "notes": {}}), 200
        
        conn = get_connection()
        use_sqlite = DISABLE_DB and USE_SQLITE_FALLBACK
        
        try:
            if use_sqlite:
                # SQLite mode - use dictionary cursor for dict results
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute("""
                        SELECT id, alumni_id, note_content, created_at, updated_at
                        FROM notes
                        WHERE user_id = ?
                        ORDER BY updated_at DESC
                    """, (user_id,))
                    rows = cursor.fetchall()
            else:
                # MySQL mode - use dictionary cursor
                with conn.cursor(dictionary=True) as cur:
                    cur.execute("""
                        SELECT id, alumni_id, note_content, created_at, updated_at
                        FROM notes
                        WHERE user_id = %s
                        ORDER BY updated_at DESC
                    """, (user_id,))
                    rows = cur.fetchall()
            
            # Group notes by alumni_id for easy frontend lookup
            notes_by_alumni = {}
            for note in rows:
                alumni_id = note['alumni_id']
                # Handle timestamp formatting
                created_at = note.get('created_at')
                updated_at = note.get('updated_at')
                if hasattr(created_at, 'isoformat'):
                    created_at = created_at.isoformat()
                if hasattr(updated_at, 'isoformat'):
                    updated_at = updated_at.isoformat()
                
                notes_by_alumni[alumni_id] = {
                    "id": note['id'],
                    "alumni_id": alumni_id,
                    "note_content": note['note_content'],
                    "created_at": created_at,
                    "updated_at": updated_at
                }
            
            return jsonify({
                "success": True,
                "notes": notes_by_alumni,
                "count": len(notes_by_alumni)
            }), 200
        except Exception as err:
            app.logger.error(f"❌ Database error getting all notes: {err}")
            return jsonify({"success": False, "error": f"Database error: {str(err)}"}), 500
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        
    except Exception as e:
        app.logger.error(f"❌ Error getting all notes: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/notes/<int:alumni_id>', methods=['POST'])
@api_login_required
def save_notes(alumni_id):
    """Save or update notes for a specific alumni"""
    try:
        user_id = get_current_user_id()
        data = request.get_json()
        note_content = data.get('note_content', '')
        
        if DISABLE_DB:
            if not USE_SQLITE_FALLBACK:
                return jsonify({"success": True, "message": "Notes saved (DB disabled)"}), 200
        
        conn = get_connection()
        use_sqlite = DISABLE_DB and USE_SQLITE_FALLBACK
        
        try:
            if use_sqlite:
                # SQLite mode - use ? placeholders and datetime('now')
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT id FROM notes
                    WHERE user_id = ? AND alumni_id = ?
                """, (user_id, alumni_id))
                existing_note = cursor.fetchone()
                
                if existing_note:
                    cursor.execute("""
                        UPDATE notes
                        SET note_content = ?, updated_at = datetime('now')
                        WHERE user_id = ? AND alumni_id = ?
                    """, (note_content, user_id, alumni_id))
                else:
                    cursor.execute("""
                        INSERT INTO notes (user_id, alumni_id, note_content, created_at, updated_at)
                        VALUES (?, ?, ?, datetime('now'), datetime('now'))
                    """, (user_id, alumni_id, note_content))
                
                conn.commit()
            else:
                # MySQL mode - use %s placeholders and NOW()
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT id FROM notes
                        WHERE user_id = %s AND alumni_id = %s
                    """, (user_id, alumni_id))
                    existing_note = cur.fetchone()
                    
                    if existing_note:
                        cur.execute("""
                            UPDATE notes
                            SET note_content = %s, updated_at = NOW()
                            WHERE user_id = %s AND alumni_id = %s
                        """, (note_content, user_id, alumni_id))
                    else:
                        cur.execute("""
                            INSERT INTO notes (user_id, alumni_id, note_content, created_at, updated_at)
                            VALUES (%s, %s, %s, NOW(), NOW())
                        """, (user_id, alumni_id, note_content))
                    
                    conn.commit()
            
            return jsonify({"success": True, "message": "Note saved successfully"}), 200
        except Exception as err:
            try:
                conn.rollback()
            except Exception:
                pass
            app.logger.error(f"❌ Database error saving notes for alumni {alumni_id}: {err}")
            return jsonify({"success": False, "error": f"Database error: {str(err)}"}), 500
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        
    except Exception as e:
        app.logger.error(f"❌ Error saving notes: {e}")
        return jsonify({"success": False, "error": str(e)}), 500



@app.route('/api/notes/<int:alumni_id>', methods=['DELETE'])
@api_login_required
def delete_notes(alumni_id):
    """Delete notes for a specific alumni"""
    try:
        user_id = get_current_user_id()
        
        if DISABLE_DB:
            if not USE_SQLITE_FALLBACK:
                return jsonify({"success": True, "message": "Notes deleted (DB disabled)"}), 200
        
        conn = get_connection()
        use_sqlite = DISABLE_DB and USE_SQLITE_FALLBACK
        
        try:
            if use_sqlite:
                # SQLite mode - use ? placeholders
                cursor = conn.cursor()
                cursor.execute("""
                    DELETE FROM notes
                    WHERE user_id = ? AND alumni_id = ?
                """, (user_id, alumni_id))
                conn.commit()
            else:
                # MySQL mode - use %s placeholders
                with conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM notes
                        WHERE user_id = %s AND alumni_id = %s
                    """, (user_id, alumni_id))
                    conn.commit()
            
            return jsonify({"success": True, "message": "Note deleted successfully"}), 200
        except Exception as err:
            try:
                conn.rollback()
            except Exception:
                pass
            return jsonify({"success": False, "error": f"Database error: {str(err)}"}), 500
        finally:
            try:
                conn.close()
            except Exception:
                pass
        
    except Exception as e:
        app.logger.error(f"Error deleting notes: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ===== AUTHORIZED EMAILS API ENDPOINTS =====

@app.route('/api/authorized-emails', methods=['GET'])
@api_login_required
def get_authorized_emails_api():
    """Get all authorized emails from the database"""
    try:
        from database import get_authorized_emails
        emails = get_authorized_emails()
        return jsonify({"success": True, "emails": emails}), 200
    except Exception as e:
        app.logger.error(f"Error fetching authorized emails: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/authorized-emails', methods=['POST'])
@api_login_required
def add_authorized_email_api():
    """Add an email to the authorized emails list"""
    try:
        data = request.get_json()
        email = data.get('email', '').strip().lower()
        notes = data.get('notes', '').strip()
        
        if not email:
            return jsonify({"success": False, "error": "Email is required"}), 400
        
        # Basic email validation
        if '@' not in email or '.' not in email.split('@')[-1]:
            return jsonify({"success": False, "error": "Invalid email format"}), 400
        
        # Get current user ID to track who added the email
        user_id = get_current_user_id()
        
        from database import add_authorized_email
        success = add_authorized_email(email, added_by_user_id=user_id, notes=notes)
        
        if success:
            return jsonify({"success": True, "message": f"Email {email} added to whitelist"}), 200
        else:
            return jsonify({"success": False, "error": "Failed to add email"}), 500
    except Exception as e:
        app.logger.error(f"Error adding authorized email: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/authorized-emails', methods=['DELETE'])
@api_login_required
def remove_authorized_email_api():
    """Remove an email from the authorized emails list"""
    try:
        data = request.get_json()
        email = data.get('email', '').strip().lower()
        
        if not email:
            return jsonify({"success": False, "error": "Email is required"}), 400
        
        from database import remove_authorized_email
        success = remove_authorized_email(email)
        
        if success:
            return jsonify({"success": True, "message": f"Email {email} removed from whitelist"}), 200
        else:
            return jsonify({"success": False, "error": "Failed to remove email"}), 500
    except Exception as e:
        app.logger.error(f"Error removing authorized email: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ===== HEATMAP API ENDPOINT =====

def get_continent(lat, lon):
    """Rough bounding-box mapping from lat/lon → continent."""
    if lat >= 5 and lat <= 83 and lon >= -170 and lon <= -52:
        return "North America"
    if lat >= -56 and lat <= 13 and lon >= -81 and lon <= -34:
        return "South America"
    if lat >= 35 and lat <= 71 and lon >= -25 and lon <= 45:
        return "Europe"
    if lat >= 1 and lat <= 77 and lon >= 26 and lon <= 180:
        return "Asia"
    if lat >= -35 and lat <= 38 and lon >= -20 and lon <= 52:
        return "Africa"
    if lat >= -50 and lat <= 0 and lon >= 110 and lon <= 180:
        return "Oceania"
    if lat <= -60:
        return "Antarctica"
    return "Unknown"


@app.route('/api/heatmap', methods=['GET'])
def get_heatmap_data():
    """
    Return aggregated alumni location data for the heatmap.

    - Groups alumni by rounded lat/lon (city-level clusters)
    - Includes a limited sample of alumni per cluster for popups
    - Uses a 60-second in-memory cache to avoid redundant DB queries
    """
    continent_filter = request.args.get("continent") or None

    if DISABLE_DB:
        if not USE_SQLITE_FALLBACK:
            return jsonify({"success": True, "locations": [], "total_alumni": 0, "max_count": 0}), 200

    # --- Check cache ---
    cache_key = continent_filter or "__all__"
    cached = _heatmap_cache.get(cache_key)
    if cached and (_time.time() - cached["ts"]) < _HEATMAP_CACHE_TTL:
        return jsonify(cached["data"]), 200

    try:
        conn = get_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute("""
                    SELECT id,
                           first_name,
                           last_name,
                           location,
                           latitude,
                           longitude,
                           current_job_title,
                           headline,
                           company,
                           linkedin_url,
                           created_at
                    FROM alumni
                    WHERE latitude IS NOT NULL
                      AND longitude IS NOT NULL
                    ORDER BY location ASC
                """)
                rows = cur.fetchall()

            # ----- city-level clustering -----
            location_clusters = {}
            location_details = {}
            total_alumni = len(rows)

            for row in rows:
                lat = row["latitude"]
                lon = row["longitude"]

                continent = get_continent(lat, lon)
                if continent_filter and continent != continent_filter:
                    continue

                cluster_key = (round(lat, 3), round(lon, 3))

                if cluster_key not in location_clusters:
                    location_clusters[cluster_key] = 0
                    location_details[cluster_key] = {
                        "location": row["location"],
                        "latitude": lat,
                        "longitude": lon,
                        "continent": continent,
                        "sample_alumni": []
                    }

                location_clusters[cluster_key] += 1

                location_details[cluster_key]["sample_alumni"].append({
                    "id": row["id"],
                    "name": f"{row['first_name']} {row['last_name']}".strip(),
                    "role": row["current_job_title"] or row["headline"] or "Alumni",
                    "company": row["company"],
                    "linkedin": row["linkedin_url"],
                    "created_at": row["created_at"].isoformat() if hasattr(row.get("created_at"), 'isoformat') else row.get("created_at")
                })

                if "location_counts" not in location_details[cluster_key]:
                    location_details[cluster_key]["location_counts"] = {}

                loc_str = row["location"]
                location_details[cluster_key]["location_counts"][loc_str] = location_details[cluster_key]["location_counts"].get(loc_str, 0) + 1

            locations = []
            max_count = 0

            for cluster_key, count in location_clusters.items():
                details = location_details[cluster_key]
                max_count = max(max_count, count)

                location_counts = details.get("location_counts", {})
                if location_counts:
                    sorted_locs = sorted(location_counts.items(), key=lambda x: (-x[1], x[0]))
                    majority_location_name = sorted_locs[0][0]
                else:
                    majority_location_name = details["location"]

                locations.append({
                    "latitude": details["latitude"],
                    "longitude": details["longitude"],
                    "location": majority_location_name,
                    "continent": details["continent"],
                    "count": count,
                    "sample_alumni": details["sample_alumni"]
                })

            response_data = {
                "success": True,
                "locations": locations,
                "total_alumni": total_alumni,
                "max_count": max_count
            }

            # Store in cache
            _heatmap_cache[cache_key] = {"data": response_data, "ts": _time.time()}

            return jsonify(response_data), 200

        finally:
            try:
                conn.close()
            except Exception:
                pass

    except mysql.connector.Error as err:
        app.logger.error(f"MySQL error fetching heatmap data: {err}")
        return jsonify({"error": f"Database error: {str(err)}"}), 500
    except Exception as e:
        app.logger.error(f"Error fetching heatmap data: {e}")
        return jsonify({"error": f"Server error: {str(e)}"}), 500


@app.route("/api/geocode")
@api_login_required
def api_geocode():
    query = request.args.get("q", "").strip()

    if not query:
        return jsonify({"success": False, "results": []}), 400

    results = geocode_location(query)

    return jsonify({
        "success": True,
        "count": len(results),
        "results": results
    })


# ---------------------- Fallback Status API ----------------------
@app.route('/api/fallback-status', methods=['GET'])
def get_fallback_status_api():
    """Get the current status of the SQLite fallback system."""
    if not USE_SQLITE_FALLBACK:
        return jsonify({
            "success": True,
            "enabled": False,
            "message": "SQLite fallback is disabled"
        }), 200
    
    try:
        from sqlite_fallback import get_fallback_status
        status = get_fallback_status()
        return jsonify({
            "success": True,
            "enabled": True,
            **status
        }), 200
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# ---------------------- Majors / Filter API ----------------------

@app.route('/api/alumni/majors', methods=['GET'])
@api_login_required
def api_get_majors():
    """
    Return the static list of approved engineering disciplines.
    Disciplines are computed on-the-fly from alumni data, not stored in DB.
    """
    resp = jsonify({"success": True, "majors": sorted(APPROVED_ENGINEERING_DISCIPLINES)})
    resp.headers['Cache-Control'] = 'public, max-age=3600'
    return resp, 200


@app.route('/api/alumni/filter', methods=['GET'])
def api_filter_alumni():
    """
    Filter alumni by major (discipline) and other criteria.
    NOTE: discipline is computed on-the-fly via infer_discipline(), NOT from the DB.
    NOTE: Public endpoint - no authentication required
    Query params:
      - major: filter by engineering discipline
      - location, company, job_title, grad_year, degree: other filters
      - limit (default 10000), offset (default 0)
    """
    if DISABLE_DB:
        if not USE_SQLITE_FALLBACK:
            return jsonify({"success": True, "alumni": []}), 200

    try:
        # Get filter parameters
        major = request.args.get('major', '').strip()
        location = request.args.get('location', '').strip()
        company = request.args.get('company', '').strip()
        job_title = request.args.get('job_title', '').strip()
        grad_year = request.args.get('grad_year', '').strip()
        degree_filter = request.args.get('degree', '').strip()

        # Validate major parameter
        if major and major not in APPROVED_ENGINEERING_DISCIPLINES:
            return jsonify({"error": f"Invalid engineering discipline: {major}"}), 400

        try:
            limit = int(request.args.get('limit', 10000))
        except Exception:
            limit = 10000
        try:
            offset = int(request.args.get('offset', 0))
        except Exception:
            offset = 0

        conn = get_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                # Build dynamic WHERE clause -- discipline (major) is NOT filtered in SQL
                where_clauses = []
                params = []

                if location:
                    where_clauses.append("a.location LIKE %s")
                    params.append(f"%{location}%")
                if company:
                    where_clauses.append("(a.company LIKE %s OR nc.normalized_company LIKE %s)")
                    params.append(f"%{company}%")
                    params.append(f"%{company}%")
                if job_title:
                    where_clauses.append("(a.current_job_title LIKE %s OR njt.normalized_title LIKE %s)")
                    params.append(f"%{job_title}%")
                    params.append(f"%{job_title}%")
                if grad_year:
                    where_clauses.append("a.grad_year = %s")
                    params.append(int(grad_year))
                if degree_filter:
                    if degree_filter.lower() == 'undergraduate':
                        where_clauses.append("(a.degree LIKE '%Bachelor%' OR a.degree LIKE '%B.S.%' OR a.degree LIKE '%B.A.%')")
                    elif degree_filter.lower() == 'graduate':
                        where_clauses.append("(a.degree LIKE '%Master%' OR a.degree LIKE '%M.S.%' OR a.degree LIKE '%MBA%')")
                    elif degree_filter.lower() == 'phd':
                        where_clauses.append("(a.degree LIKE '%Ph.D%' OR a.degree LIKE '%PhD%' OR a.degree LIKE '%Doctor%')")

                where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

                query = f"""
                    SELECT a.id, a.first_name, a.last_name, a.grad_year, a.degree, a.linkedin_url,
                           a.current_job_title, a.company, a.location, a.headline,
                           njt.normalized_title, nc.normalized_company
                    FROM alumni a
                    LEFT JOIN normalized_job_titles njt ON a.normalized_job_title_id = njt.id
                    LEFT JOIN normalized_companies nc ON a.normalized_company_id = nc.id
                    WHERE {where_clause}
                    ORDER BY a.last_name ASC, a.first_name ASC
                """
                cur.execute(query, params)
                rows = cur.fetchall()

                alumni = []
                for r in rows:
                    full_degree = r.get('degree') or ''
                    degree_level = classify_degree(full_degree, r.get('headline', ''))

                    # Read discipline from DB — set at scrape time by Groq
                    discipline = r.get('major') or 'Other'

                    # Apply discipline filter in Python (not SQL)
                    if major and discipline != major:
                        continue

                    alumni.append({
                        "id": r.get('id'),
                        "name": f"{r.get('first_name','').strip()} {r.get('last_name','').strip()}".strip(),
                        "current_job_title": r.get('current_job_title'),
                        "company": r.get('company'),
                        "grad_year": r.get('grad_year'),
                        "major": discipline,
                        "role": r.get('current_job_title'),
                        "headline": r.get('headline'),
                        "class": r.get('grad_year'),
                        "location": r.get('location'),
                        "linkedin": r.get('linkedin_url'),
                        "degree": degree_level,
                        "full_degree": full_degree,
                        "normalized_title": r.get('normalized_title'),
                        "normalized_company": r.get('normalized_company')
                    })

                # Apply offset/limit after Python-side filtering
                paginated = alumni[offset:offset + limit]
                return jsonify({"success": True, "alumni": paginated, "count": len(alumni)}), 200
        finally:
            try:
                conn.close()
            except Exception:
                pass

    except Exception as e:
        app.logger.error(f"Error filtering alumni: {e}")
        return jsonify({"error": f"Server error: {str(e)}"}), 500


# ---------------------- Error handler ----------------------
@app.errorhandler(404)
def not_found(e):
    return 'Page not found', 404


# ---------------------- Main entry point ----------------------
if __name__ == "__main__":
    # Initialize SQLite fallback system first (syncs from cloud if available)
    if USE_SQLITE_FALLBACK and not DISABLE_DB:
        try:
            from sqlite_fallback import init_fallback_system
            init_fallback_system()
        except Exception as e:
            app.logger.warning(f"SQLite fallback initialization failed: {e}")
            app.logger.info("Continuing with direct database connection...")

    # Initialize database (but skip re-seeding if data exists)
    from database import init_db, seed_alumni_data, ensure_normalized_job_title_column, ensure_normalized_degree_column, ensure_normalized_company_column
    if not DISABLE_DB:
        try:
            init_db()
            # Ensure migration columns exist (safe to call repeatedly)
            ensure_normalized_job_title_column()
            ensure_normalized_degree_column()
            ensure_normalized_company_column()
            seed_alumni_data()
        except Exception as e:
            app.logger.error(f"Failed to initialize database: {e}")
            if not USE_SQLITE_FALLBACK:
                exit(1)
            else:
                app.logger.info("Continuing with SQLite fallback...")

    app.run()
