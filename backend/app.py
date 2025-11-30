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

#app = Flask(__name__)
app = Flask(
    __name__,
    static_folder="../frontend/public",
    static_url_path=""
)

app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'default_secret_key')

# Development toggle: set DISABLE_DB=1 in .env to skip all DB work (useful when RDS is down)
DISABLE_DB = os.getenv("DISABLE_DB", "0") == "1"
app.logger.info(f"DISABLE_DB = {DISABLE_DB}")

# MySQL credentials (kept for reference; connections use get_connection())
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
    """Get the current logged-in user's DB id from LinkedIn profile.
       In dev (DISABLE_DB=1), return a stable placeholder id from session.
    """
    if 'linkedin_profile' not in session:
        return None

    linkedin_profile = session['linkedin_profile']
    linkedin_id = linkedin_profile.get('sub')  # LinkedIn's unique ID

    if DISABLE_DB:
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
        f"&scope={scope}&state={state}&prompt=login"  # force login each time for demonstration
    )
    return redirect(auth_url)

@app.route('/auth/linkedin/callback')
def linkedin_callback():
    """Handle LinkedIn OAuth callback"""
    code = request.args.get('code')
    state = request.args.get('state')

    # Verify state to prevent CSRF
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

    # ---- DEV BYPASS: skip DB completely if disabled ----
    if DISABLE_DB:
        app.logger.info("DB BYPASS active; redirecting to /alumni")
        return redirect('/alumni')
    # ----------------------------------------------------

    # Save/update user in database (safe connection handling)
    conn = None
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
        app.logger.error(f"Error saving user to database: {e}")
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
    Add or update a user interaction (bookmarked, connected)
    Body:
    { "alumni_id": 123, "interaction_type": "bookmarked"|"connected", "notes": "..." }
    """
    # Short-circuit in dev when DB is disabled
    if DISABLE_DB:
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
            return jsonify({"success": True, "message": f"{interaction_type} added successfully"}), 200
        except mysql.connector.Error as err:
            conn.rollback()
            return jsonify({"error": f"Database error: {str(err)}"}), 500
        finally:
            try:
                conn.close()
            except Exception:
                pass

    except Exception as e:
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
            try:
                conn.close()
            except Exception:
                pass

    except Exception as e:
        return jsonify({"error": f"Server error: {str(e)}"}), 500

@app.route('/api/user-interactions', methods=['GET'])
@api_login_required
def get_user_interactions():
    """
    Get all interactions for current user
    """
    # Short-circuit in dev when DB is disabled
    if DISABLE_DB:
        return jsonify({"success": True, "interactions": []}), 200

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

            return jsonify({"success": True, "interactions": interactions}), 200
        finally:
            try:
                conn.close()
            except Exception:
                pass

    except Exception as e:
        return jsonify({"error": f"Server error: {str(e)}"}), 500


@app.route('/api/alumni', methods=['GET'])
def api_get_alumni():
    """
    Return a list of alumni from the database.
    Query params: limit (default: all records), offset (default 0)
    """
    # Short-circuit in dev when DB is disabled
    if DISABLE_DB:
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
                    SELECT id, first_name, last_name, grad_year, degree, linkedin_url, current_job_title, company, location, headline
                    FROM alumni
                    ORDER BY last_name ASC, first_name ASC
                    LIMIT %s OFFSET %s
                """, (limit, offset))
                rows = cur.fetchall()

            alumni = []
            for r in rows:
                # Extract degree level from full degree string
                full_degree = r.get('degree', '') or ''
                degree_level = 'Other'
                
                # Normalize to lowercase for matching
                degree_lower = full_degree.lower()
                
                if degree_lower:
                    if any(term in degree_lower for term in ['bachelor', 'b.s.', 'b.a.', 'b.sc', 'undergraduate']):
                        degree_level = 'Undergraduate'
                    elif any(term in degree_lower for term in ['master', 'm.s.', 'm.a.', 'mba', 'm.sc', 'graduate']):
                        degree_level = 'Graduate'
                    elif any(term in degree_lower for term in ['doctor', 'ph.d', 'phd', 'doctorate']):
                        degree_level = 'PhD'
                
                alumni.append({
                    "id": r.get('id'),
                    "name": f"{r.get('first_name','').strip()} {r.get('last_name','').strip()}".strip(),

                    # WHAT ANALYTICS.JS EXPECTS:
                    "current_job_title": r.get('current_job_title'),
                    "company": r.get('company'),
                    "grad_year": r.get('grad_year'),

                    # OPTIONAL: keep backwards-compatible for alumni UI (if needed)
                    "role": r.get('current_job_title'),

                    # Existing fields
                    "headline": r.get('headline'),
                    "class": r.get('grad_year'),
                    "location": r.get('location'),
                    "linkedin": r.get('linkedin_url'),
                    "degree": degree_level,
                    "full_degree": full_degree
                })

            return jsonify({"success": True, "alumni": alumni}), 200
        finally:
            try:
                conn.close()
            except Exception:
                pass

    except mysql.connector.Error as err:
        app.logger.error(f"MySQL error fetching alumni: {err}")
        return jsonify({"error": f"Database error: {str(err)}"}), 500
    except Exception as e:
        app.logger.error(f"Error fetching alumni: {e}")
        return jsonify({"error": f"Server error: {str(e)}"}), 500

# ===== NOTES API ENDPOINTS =====

@app.route('/api/notes/<int:alumni_id>', methods=['GET'])
@api_login_required
def get_notes(alumni_id):
    """Get notes for a specific alumni (for current logged-in user)"""
    try:
        user_id = get_current_user_id()
        
        if DISABLE_DB:
            return jsonify({"success": True, "note": None}), 200
        
        conn = get_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute("""
                    SELECT id, note_content, created_at, updated_at
                    FROM notes
                    WHERE user_id = %s AND alumni_id = %s
                    LIMIT 1
                """, (user_id, alumni_id))
                
                note = cur.fetchone()
                
                if note:
                    return jsonify({
                        "success": True,
                        "note": {
                            "id": note['id'],
                            "note_content": note['note_content'],
                            "created_at": note['created_at'].isoformat() if note['created_at'] else None,
                            "updated_at": note['updated_at'].isoformat() if note['updated_at'] else None
                        }
                    }), 200
                else:
                    return jsonify({"success": True, "note": None}), 200
        finally:
            try:
                conn.close()
            except Exception:
                pass
        
    except Exception as e:
        app.logger.error(f"Error getting notes: {e}")
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
            return jsonify({"success": True, "message": "Notes saved (DB disabled)"}), 200
        
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                # Check if note exists
                cur.execute("""
                    SELECT id FROM notes
                    WHERE user_id = %s AND alumni_id = %s
                """, (user_id, alumni_id))
                
                existing_note = cur.fetchone()
                
                if existing_note:
                    # Update existing note
                    cur.execute("""
                        UPDATE notes
                        SET note_content = %s, updated_at = NOW()
                        WHERE user_id = %s AND alumni_id = %s
                    """, (note_content, user_id, alumni_id))
                else:
                    # Create new note
                    cur.execute("""
                        INSERT INTO notes (user_id, alumni_id, note_content, created_at, updated_at)
                        VALUES (%s, %s, %s, NOW(), NOW())
                    """, (user_id, alumni_id, note_content))
                
                conn.commit()
                
                return jsonify({"success": True, "message": "Note saved successfully"}), 200
        except mysql.connector.Error as err:
            conn.rollback()
            return jsonify({"success": False, "error": f"Database error: {str(err)}"}), 500
        finally:
            try:
                conn.close()
            except Exception:
                pass
        
    except Exception as e:
        app.logger.error(f"Error saving notes: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/notes/<int:alumni_id>', methods=['DELETE'])
@api_login_required
def delete_notes(alumni_id):
    """Delete notes for a specific alumni"""
    try:
        user_id = get_current_user_id()
        
        if DISABLE_DB:
            return jsonify({"success": True, "message": "Notes deleted (DB disabled)"}), 200
        
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM notes
                    WHERE user_id = %s AND alumni_id = %s
                """, (user_id, alumni_id))
                
                conn.commit()
                
                return jsonify({"success": True, "message": "Note deleted successfully"}), 200
        except mysql.connector.Error as err:
            conn.rollback()
            return jsonify({"success": False, "error": f"Database error: {str(err)}"}), 500
        finally:
            try:
                conn.close()
            except Exception:
                pass
        
    except Exception as e:
        app.logger.error(f"Error deleting notes: {e}")
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
    Get alumni location data (latitude, longitude, count) for heatmap visualization.
    Returns aggregated data grouped by location to reduce payload size.
    """
    continent_filter = request.args.get("continent")

    # Short-circuit in dev when DB is disabled
    if DISABLE_DB:
        return jsonify({"success": True, "locations": []}), 200

    try:
        conn = get_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                # Get all alumni with valid coordinates
                cur.execute("""
                    SELECT id, first_name, last_name, location, latitude, longitude, 
                           current_job_title, headline, company, linkedin_url
                    FROM alumni
                    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                    ORDER BY location ASC
                """)
                rows = cur.fetchall()

            # Aggregate locations (group by lat/lon rounded to 2 decimals for clustering)
            location_clusters = {}
            location_details = {}  # Store first alumni at each unique location
            
            for row in rows:
                lat = row['latitude']
                lon = row['longitude']

                continent = get_continent(lat, lon)
                if continent_filter and continent != continent_filter:
                    continue
                # Use rounded coordinates as cluster key (reduces fragmentation)
                cluster_key = (round(lat, 2), round(lon, 2))
                
                if cluster_key not in location_clusters:
                    location_clusters[cluster_key] = 0
                    location_details[cluster_key] = {
                        "location": row['location'],
                        "latitude": lat,
                        "longitude": lon,
                        "continent": continent,
                        "sample_alumni": []
                    }
                
                location_clusters[cluster_key] += 1
                
                # Store first 3 alumni examples at each location
                if len(location_details[cluster_key]["sample_alumni"]) < 50:
                    location_details[cluster_key]["sample_alumni"].append({
                        "id": row['id'],
                        "name": f"{row['first_name']} {row['last_name']}".strip(),
                        "role": row['current_job_title'] or row['headline'] or 'Alumni',
                        "company": row['company'],
                        "linkedin": row['linkedin_url']
                    })
            
            # Build final response
            locations = []
            for cluster_key, count in location_clusters.items():
                details = location_details[cluster_key]
                locations.append({
                    "latitude": details["latitude"],
                    "longitude": details["longitude"],
                    "location": details["location"],
                    "continent": details["continent"],
                    "count": count,
                    "sample_alumni": details["sample_alumni"]
                })
            
            return jsonify({
                "success": True,
                "locations": locations,
                "total_alumni": len(rows)
            }), 200
            
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

# ---------------------- Error handler ----------------------
@app.errorhandler(404)
def not_found(e):
    return 'Page not found', 404

if __name__ == "__main__":
    # Initialize database first (but skip re-seeding if data exists)
    from database import init_db, seed_alumni_data
    if not DISABLE_DB:
        try:
            init_db()
            # Seed alumni data from CSV file
            seed_alumni_data()
        except Exception as e:
            app.logger.error(f"Failed to initialize database: {e}")
            exit(1)
    app.run()




