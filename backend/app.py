from flask import Flask, redirect, request, url_for, session, send_from_directory, jsonify
from dotenv import load_dotenv
from functools import wraps
from collections import Counter
import os
import logging
import re
from time import perf_counter
import requests  # for OAuth token exchange
import mysql.connector  # for MySQL connection
import secrets
from database import get_connection
from geocoding import geocode_location, search_location_candidates
from unt_alumni_status import (
    UNT_ALUMNI_STATUS_VALUES,
    compute_unt_alumni_status_from_row,
)
import sys

_SCRAPER_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scraper"))
if _SCRAPER_DIR not in sys.path:
    sys.path.insert(0, _SCRAPER_DIR)

try:
    from job_title_normalization import (
        normalize_title_deterministic,
        normalize_title_with_groq,
        get_all_normalized_titles,
    )
    from company_normalization import (
        normalize_company_deterministic,
        normalize_company_with_groq,
        get_all_normalized_companies,
    )
except Exception as _norm_import_err:
    app_logger = logging.getLogger(__name__)
    app_logger.warning(f"Normalization imports unavailable in app.py: {_norm_import_err}")
    normalize_title_deterministic = None
    normalize_title_with_groq = None
    get_all_normalized_titles = None
    normalize_company_deterministic = None
    normalize_company_with_groq = None
    get_all_normalized_companies = None

load_dotenv()

class _SuppressWerkzeugAccessLogFilter(logging.Filter):
    """Hide only oversized request URL logs while preserving normal access logs."""
    _request_line_pattern = re.compile(
        r'"(?P<method>GET|POST|PUT|PATCH|DELETE|OPTIONS|HEAD)\s+(?P<path>[^"]+?)\s+HTTP/\d(?:\.\d)?"\s+\d{3}\s+-'
    )
    _max_logged_path_length = 220

    def filter(self, record):
        message = record.getMessage()
        match = self._request_line_pattern.search(message)
        if not match:
            return True

        path = match.group("path") or ""
        if len(path) <= self._max_logged_path_length:
            return True

        # Suppress extremely long query-string request lines that flood console output.
        return False


# Keep startup lines visible while suppressing per-request access log noise.
# Set QUIET_HTTP_LOGS=0 to restore default request logging.
if os.getenv("QUIET_HTTP_LOGS", "1") == "1":
    werkzeug_logger = logging.getLogger("werkzeug")
    werkzeug_logger.setLevel(logging.INFO)
    if not any(getattr(f, "_quiet_http_filter", False) for f in werkzeug_logger.filters):
        access_filter = _SuppressWerkzeugAccessLogFilter()
        access_filter._quiet_http_filter = True
        werkzeug_logger.addFilter(access_filter)

app = Flask(
    __name__,
    static_folder="../frontend/public",
    static_url_path=""
)

def _is_production_mode():
    """
    Detect production-like runtime where we must not allow insecure defaults.
    """
    env_value = (
        os.getenv('APP_ENV')
        or os.getenv('FLASK_ENV')
        or os.getenv('ENV')
        or ''
    ).strip().lower()
    return env_value in {'prod', 'production'}


def _configure_secret_key(flask_app):
    secret_key = (os.getenv('SECRET_KEY') or '').strip()
    is_production = _is_production_mode()

    if is_production:
        if not secret_key:
            raise RuntimeError("SECRET_KEY environment variable is required in production.")
        if len(secret_key) < 32:
            raise RuntimeError("SECRET_KEY must be at least 32 characters in production.")
        flask_app.config['SECRET_KEY'] = secret_key
        return

    # Local/dev fallback to keep setup friction low without shipping a static key.
    flask_app.config['SECRET_KEY'] = secret_key or secrets.token_urlsafe(32)


_configure_secret_key(app)

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

_SCRAPER_ACTIVITY_NAME_HINTS = {
    "sangam": "Sangam Bartaula",
    "sachin": "Sachin Banjade",
    "abishek": "Abishek Lamichhane",
    "abhishek": "Abishek Lamichhane",
    "lamichhane": "Abishek Lamichhane",
    "niranjan": "Niranjan Paudel",
    "paudel": "Niranjan Paudel",
    "shrish": "Shrish Acharya",
    "acharya": "Shrish Acharya",
}

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
        from database import is_email_authorized
        if is_email_authorized(email_lower):
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


def _resolve_scraper_display_name(email, users_by_email):
    email_lower = (email or "").strip().lower()
    if not email_lower:
        return "Unknown Scraper"

    user_row = users_by_email.get(email_lower, {})
    first = (user_row.get("first_name") or "").strip()
    last = (user_row.get("last_name") or "").strip()
    full_name = f"{first} {last}".strip()
    if full_name:
        return full_name

    local_part = email_lower.split("@", 1)[0]
    for hint, display_name in _SCRAPER_ACTIVITY_NAME_HINTS.items():
        if hint in local_part:
            return display_name

    return email_lower


def _get_or_create_normalized_entity_id(cur, table, column, value):
    value = (value or "").strip()
    if not value:
        return None
    try:
        cur.execute(
            f"INSERT INTO {table} ({column}) VALUES (%s) ON DUPLICATE KEY UPDATE {column}=VALUES({column})",
            (value,),
        )
    except Exception:
        cur.execute(f"INSERT OR IGNORE INTO {table} ({column}) VALUES (?)", (value,))

    try:
        cur.execute(f"SELECT id FROM {table} WHERE {column} = %s", (value,))
    except Exception:
        cur.execute(f"SELECT id FROM {table} WHERE {column} = ?", (value,))
    row = cur.fetchone()
    if not row:
        return None
    return row["id"] if isinstance(row, dict) else row[0]


def _looks_like_location_text(text):
    t = (text or "").strip().lower()
    if not t:
        return False
    if re.fullmatch(r"[a-z\s]+,\s*[a-z\s]+", t):
        return any(token in t for token in ("texas", "united states", "county", "metroplex", "area"))
    return t in {"denton", "denton, texas", "denton county, texas", "texas", "united states"}

# ---------------------- Helper functions ----------------------


# =============================================================================
# DISCIPLINE CLASSIFICATION (computed on-the-fly, NOT stored in DB)
# Ordered list: first match wins.  Priority per alumni: job_title > degree > headline
# =============================================================================
# Approved engineering disciplines for the frontend filter
APPROVED_ENGINEERING_DISCIPLINES = [
    'Software, Data, AI & Cybersecurity',
    'Embedded, Electrical & Hardware Engineering',
    'Mechanical Engineering & Manufacturing',
    'Biomedical Engineering',
    'Construction & Engineering Management',
]
_APPROVED_DISCIPLINES_SET = set(APPROVED_ENGINEERING_DISCIPLINES)

# Keep legacy labels readable while converging to canonical discipline names.
_DISCIPLINE_CANONICAL_MAP = {
    'Software, Data, AI & Cybersecurity': 'Software, Data, AI & Cybersecurity',
    'Software, Data & AI Engineering': 'Software, Data, AI & Cybersecurity',
    'Software, Data, AI & Cybersecurity Engineering': 'Software, Data, AI & Cybersecurity',
    'Embedded, Electrical & Hardware Engineering': 'Embedded, Electrical & Hardware Engineering',
    'Mechanical Engineering & Manufacturing': 'Mechanical Engineering & Manufacturing',
    'Biomedical Engineering': 'Biomedical Engineering',
    'Construction & Engineering Management': 'Construction & Engineering Management',
    # Legacy values
    'Mechanical & Energy Engineering': 'Mechanical Engineering & Manufacturing',
    'Materials Science & Manufacturing': 'Mechanical Engineering & Manufacturing',
}

_CANONICAL_TO_EQUIVALENT_DISCIPLINES = {
    'Software, Data, AI & Cybersecurity': [
        'Software, Data, AI & Cybersecurity',
        'Software, Data & AI Engineering',
        'Software, Data, AI & Cybersecurity Engineering',
    ],
    'Embedded, Electrical & Hardware Engineering': ['Embedded, Electrical & Hardware Engineering'],
    'Mechanical Engineering & Manufacturing': [
        'Mechanical Engineering & Manufacturing',
        'Mechanical & Energy Engineering',
        'Materials Science & Manufacturing',
    ],
    'Biomedical Engineering': ['Biomedical Engineering'],
    'Construction & Engineering Management': ['Construction & Engineering Management'],
}

# Canonical UNT major labels (must stay aligned with scraper/major_normalization.py).
APPROVED_UNT_MAJORS = [
    "Artificial Intelligence",
    "Biomedical Engineering",
    "Computer Engineering",
    "Computer Science",
    "Construction Engineering Technology",
    "Construction Management",
    "Cybersecurity",
    "Data Engineering",
    "Electrical Engineering",
    "Engineering Management",
    "Geographic Information Systems + Computer Science",
    "Information Technology",
    "Materials Science and Engineering",
    "Mechanical and Energy Engineering",
    "Mechanical Engineering Technology",
    "Semiconductor Manufacturing Engineering",
    "Other",
]
_APPROVED_UNT_MAJORS_SET = set(APPROVED_UNT_MAJORS)


def _resolve_discipline(row):
    """
    Resolve engineering discipline from dedicated column first.
    Fallback to legacy storage where discipline was incorrectly kept in major.
    """
    discipline = (row.get('discipline') or '').strip()
    if discipline:
        canonical = _DISCIPLINE_CANONICAL_MAP.get(discipline)
        if canonical in _APPROVED_DISCIPLINES_SET:
            return canonical

    legacy_major = (row.get('major') or '').strip()
    if legacy_major:
        canonical = _DISCIPLINE_CANONICAL_MAP.get(legacy_major)
        if canonical in _APPROVED_DISCIPLINES_SET:
            return canonical

    return 'Other'


def _normalize_requested_discipline(value):
    canonical = _DISCIPLINE_CANONICAL_MAP.get((value or '').strip())
    return canonical or ''


def _expand_discipline_filter_values(canonical_disciplines):
    expanded = []
    for canonical in canonical_disciplines:
        for alias in _CANONICAL_TO_EQUIVALENT_DISCIPLINES.get(canonical, [canonical]):
            if alias not in expanded:
                expanded.append(alias)
    return expanded


def _resolve_major(row):
    """
    Return canonical major only.
    Never return discipline labels in this field.
    """
    standardized_major = (row.get('standardized_major') or '').strip()
    if standardized_major in _APPROVED_UNT_MAJORS_SET:
        return standardized_major

    raw_major = (row.get('major') or '').strip()
    if raw_major in _DISCIPLINE_CANONICAL_MAP:
        return ''
    if raw_major in _APPROVED_UNT_MAJORS_SET:
        return raw_major
    return ''


def _resolve_majors_list(row):
    """
    Return all canonical majors for this alumni as a list.
    Includes standardized_major_alt for multi-entry cases (CS&E -> CS + CE).
    """
    majors = []
    primary = _resolve_major(row)
    if primary:
        majors.append(primary)
    alt = (row.get('standardized_major_alt') or '').strip()
    if alt and alt in _APPROVED_UNT_MAJORS_SET and alt not in majors:
        majors.append(alt)
    return majors


def _resolve_full_major(row):
    """
    Return display major.
    Prefer canonical major; fallback to non-discipline raw major text.
    """
    canonical_major = _resolve_major(row)
    if canonical_major:
        return canonical_major

    raw_major = (row.get('major') or '').strip()
    if raw_major and raw_major not in _DISCIPLINE_CANONICAL_MAP:
        return raw_major
    return ''


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
            return 'Bachelors'
        if any(t in degree_lower for t in ['master', 'm.s.', 'm.a.', 'mba', 'm.sc', 'graduate']):
            return 'Masters'
        if any(t in degree_lower for t in ['doctor', 'ph.d', 'phd', 'doctorate']):
            return 'PhD'

    headline_lower = (headline or '').lower()
    if headline_lower:
        if any(t in headline_lower for t in ['bachelor', 'b.s.', 'b.a.', 'b.sc']):
            return 'Bachelors'
        if any(t in headline_lower for t in ['master', 'm.s.', 'm.a.', 'mba', 'm.sc']):
            return 'Masters'
        if any(t in headline_lower for t in ['doctor', 'ph.d', 'phd', 'doctorate']):
            return 'PhD'

    return None


_SENIORITY_FILTER_LABELS = {
    "intern": "Intern",
    "mid": "Mid",
    "senior": "Senior",
    "manager": "Manager",
    "executive": "Executive",
}

_SENIORITY_EXECUTIVE_PATTERN = re.compile(
    r"\b(?:director|head|vice president|president|chief|cxo|c[a-z]o|partner|founder|co founder|owner"
    r")\b",
    re.IGNORECASE,
)
_SENIORITY_MANAGER_PATTERN = re.compile(
    r"\b(?:manager|supervisor|project manager|program manager|scrum master)\b",
    re.IGNORECASE,
)
_SENIORITY_SENIOR_PATTERN = re.compile(
    r"\b(?:senior|lead|principal|staff|specialist)\b",
    re.IGNORECASE,
)
_SENIORITY_MID_PATTERN = re.compile(
    r"\b(?:junior|entry level|associate)\b",
    re.IGNORECASE,
)
_SENIORITY_INTERN_PATTERN = re.compile(
    r"\b(?:intern|trainee|apprentice)\b",
    re.IGNORECASE,
)


def _normalize_title_for_seniority(job_title):
    if job_title is None:
        return ""

    normalized = str(job_title).strip().lower()
    if not normalized:
        return ""

    normalized = normalized.replace("&", " and ")
    normalized = re.sub(r"[^a-z0-9\s]", " ", normalized)
    normalized = re.sub(r"\bjr\b", "junior", normalized)
    normalized = re.sub(r"\bsr\b", "senior", normalized)
    normalized = re.sub(r"\bsvp\b", "senior vice president", normalized)
    normalized = re.sub(r"\bevp\b", "executive vice president", normalized)
    normalized = re.sub(r"\bvp\b", "vice president", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _coerce_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def classify_seniority_bucket(job_title, relevant_experience_months=None):
    """
    Classify alumni into UI buckets from the current title only.

    Buckets: Intern / Mid / Senior / Manager / Executive.
    - Title keywords are the sole driver.
    - Relevant experience months do not change the bucket; they are only used
      for warnings/flags elsewhere (see seniority_detector).
    - Missing or empty title falls back to internal "Others".
    """
    normalized_title = _normalize_title_for_seniority(job_title)
    if not normalized_title:
        return "Others"

    if _SENIORITY_EXECUTIVE_PATTERN.search(normalized_title):
        return "Executive"
    if _SENIORITY_MANAGER_PATTERN.search(normalized_title):
        return "Manager"
    if _SENIORITY_SENIOR_PATTERN.search(normalized_title):
        return "Senior"
    if _SENIORITY_MID_PATTERN.search(normalized_title):
        return "Mid"
    if _SENIORITY_INTERN_PATTERN.search(normalized_title):
        return "Intern"

    # No seniority keywords in the title: default to Mid.
    return "Mid"


def _parse_seniority_filters(raw_values):
    values = []
    seen = set()
    for raw in raw_values:
        key = (raw or "").strip().lower()
        if not key:
            continue
        if key not in _SENIORITY_FILTER_LABELS:
            raise ValueError("Invalid seniority. Use Intern, Mid, Senior, Manager, or Executive.")
        if key in seen:
            continue
        seen.add(key)
        values.append(_SENIORITY_FILTER_LABELS[key])
    return values


def _rank_filter_option_counts(counts, query='', limit=15):
    """
    Rank filter options similar to analytics autocomplete behavior.
    - Empty query: popular entries first (count desc), then alphabetical.
    - With query: exact match, then starts-with, then contains; ties by popularity.
    """
    q = (query or '').strip().lower()
    entries = [(value, count) for value, count in counts.items() if value]

    if q:
        entries = [item for item in entries if q in item[0].lower()]

        def sort_key(item):
            value, count = item
            value_lower = value.lower()
            if value_lower == q:
                rank = 0
            elif value_lower.startswith(q):
                rank = 1
            else:
                rank = 2
            return (rank, -count, value_lower)

        entries.sort(key=sort_key)
    else:
        entries.sort(key=lambda item: (-item[1], item[0].lower()))

    return [{"value": value, "count": count} for value, count in entries[:limit]]


def _parse_unt_alumni_status_filter(raw_value):
    value = (raw_value or '').strip().lower()
    if not value:
        return ''
    if value not in UNT_ALUMNI_STATUS_VALUES:
        raise ValueError("Invalid unt_alumni_status. Use yes, no, or unknown.")
    return value


def _parse_multi_value_param(param_name):
    """
    Parse repeated or comma-separated query params into a cleaned list.
    Example:
      ?location=Dallas&location=Austin
      ?location=Dallas,Austin

    Notes:
      - Some values legitimately contain commas (e.g. "Austin, TX",
        "Software, Data, AI & Cybersecurity"). For these params we preserve
        each raw query value as-is and rely on repeated params for multi-select.
    """
    raw_values = [str(raw or "").strip() for raw in request.args.getlist(param_name)]
    raw_values = [v for v in raw_values if v]
    if not raw_values:
        return []

    # These fields often contain commas in a single value, so do not split.
    preserve_commas_params = {"location", "role", "company", "major"}
    if param_name in preserve_commas_params:
        return raw_values

    values = []
    for raw in raw_values:
        for part in raw.split(","):
            cleaned = part.strip()
            if cleaned:
                values.append(cleaned)
    return values


def _validation_error(message, field=None, code="validation_error", details=None, status=400):
    payload = {
        "error": {
            "code": code,
            "message": message,
        }
    }
    if field:
        payload["error"]["field"] = field
    if details:
        payload["error"]["details"] = details
    return jsonify(payload), status


def _parse_int_list_param(param_name, strict=False):
    """
    Parse repeated or comma-separated integer query params.
    Invalid values are ignored unless strict=True.
    """
    values = []
    for raw in request.args.getlist(param_name):
        for part in (raw or "").split(","):
            cleaned = part.strip()
            if not cleaned:
                continue
            try:
                values.append(int(cleaned))
            except Exception:
                if strict:
                    raise ValueError(f"Invalid integer value for {param_name}: {cleaned}")
                continue
    return values


def _parse_optional_non_negative_int(param_name):
    raw_value = (request.args.get(param_name, "") or "").strip()
    if not raw_value:
        return None
    try:
        value = int(raw_value)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid integer value for {param_name}: {raw_value}")
    if value < 0:
        raise ValueError(f"{param_name} must be non-negative.")
    return value


def _validate_min_max(min_value, max_value, min_name, max_name):
    if min_value is None or max_value is None:
        return
    if min_value > max_value:
        raise ValueError(f"{min_name} cannot be greater than {max_name}.")


# =============================================================================
# HEATMAP CACHE
# =============================================================================
import time as _time
_heatmap_cache = {}  # key: continent_filter -> {"data": ..., "ts": ...}
_HEATMAP_CACHE_TTL = 60  # seconds


def _is_logged_in():
    """Return True if a valid session exists (LinkedIn or email/password)."""
    return 'linkedin_token' in session or 'user_email' in session


def _get_session_email():
    """Return the logged-in user's email regardless of auth method."""
    if 'user_email' in session:
        return session['user_email']
    profile = session.get('linkedin_profile')
    if profile:
        return profile.get('email')
    return None


def get_current_user_id():
    """
    Get the current logged-in user's database ID.
    Supports both LinkedIn OAuth and email/password sessions.
    In development mode (DISABLE_DB=1), returns a stable placeholder ID.
    """
    if not _is_logged_in():
        return None

    # Email/password session path
    if 'user_email' in session:
        email = session['user_email']
        conn = None
        try:
            conn = get_connection()
            with conn.cursor() as cur:
                try:
                    cur.execute("SELECT id FROM users WHERE LOWER(email) = LOWER(?)", (email,))
                except Exception:
                    cur.execute("SELECT id FROM users WHERE LOWER(email) = LOWER(%s)", (email,))
                result = cur.fetchone()
                return (result['id'] if isinstance(result, dict) else result[0]) if result else None
        except Exception as e:
            app.logger.error(f"Error getting user ID by email: {e}")
            return None
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # LinkedIn session path
    linkedin_profile = session.get('linkedin_profile')
    if not linkedin_profile:
        return None
    linkedin_id = linkedin_profile.get('sub')  # LinkedIn's unique ID

    if DISABLE_DB:
        if not USE_SQLITE_FALLBACK:
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
    """Decorator to require login (supports both LinkedIn and email/password sessions)."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not _is_logged_in():
            return redirect('/login')
        # Enforce must_change_password for page routes
        if session.get('must_change_password') and request.path != '/change-password':
            return redirect('/change-password')
        return f(*args, **kwargs)
    return decorated_function

def api_login_required(f):
    """Decorator to require login for API endpoints (supports both auth methods)."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not _is_logged_in():
            return jsonify({"error": "Not authenticated"}), 401
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    """Decorator to require admin role.  Must be used after api_login_required."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        email = _get_session_email()
        if not email:
            return jsonify({"error": "Not authenticated"}), 401
        from database import get_user_by_email
        user = get_user_by_email(email)
        if not user or user.get('role') != 'admin':
            return jsonify({"error": "Admin access required"}), 403
        return f(*args, **kwargs)
    return decorated_function

# ---------------------- Static/Basic routes ----------------------
@app.route('/')
def home():
    if _is_logged_in():
        return redirect('/alumni')
    return send_from_directory('../frontend/public', 'index.html')

@app.route('/login')
def login_page():
    if _is_logged_in():
        return redirect('/alumni')
    return send_from_directory('../frontend/public', 'index.html')

@app.route('/register')
def register_page():
    if _is_logged_in():
        return redirect('/alumni')
    return send_from_directory('../frontend/public', 'register.html')

@app.route('/change-password')
def change_password_page():
    if not _is_logged_in():
        return redirect('/login')
    return send_from_directory('../frontend/public', 'change_password.html')

@app.route('/about')
def about():
    return send_from_directory('../frontend/public', 'about.html')

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


# ---------------------- Auth API routes ----------------------

@app.route('/api/auth/login', methods=['POST'])
def api_auth_login():
    """Email/password login endpoint."""
    from auth import verify_password
    from database import get_user_by_email, record_failed_login, reset_failed_login
    import datetime

    data = request.get_json(silent=True) or {}
    email = (data.get('email') or '').strip().lower()
    password = data.get('password') or ''

    if not email or not password:
        return jsonify({"error": "Email and password are required."}), 400

    user = get_user_by_email(email)
    generic_error = "Invalid credentials"

    # Block if locked out natively
    if user and user.get("lock_until"):
        lock_until_val = user["lock_until"]
        if isinstance(lock_until_val, str):
            try:
                lock_dt = datetime.datetime.strptime(lock_until_val, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                lock_dt = datetime.datetime.utcnow() # fallback
        else:
            lock_dt = lock_until_val
            
        if lock_dt > datetime.datetime.utcnow():
            delta = lock_dt - datetime.datetime.utcnow()
            mins = int(delta.total_seconds() / 60) + 1
            return jsonify({"error": f"Too many failed attempts. Try again after {mins} minutes."}), 401

    if not user:
        return jsonify({"error": generic_error}), 401

    if not is_authorized_user(email):
        # Still generic to avoid side-channel leaking
        return jsonify({"error": generic_error}), 401

    if not user.get('password_hash'):
        # Track failure against email since no password allowed anyways
        attempts, lock = record_failed_login(email)
        if lock:
            return jsonify({"error": "Too many failed attempts. Try again after 30 minutes."}), 401
        return jsonify({"error": generic_error}), 401

    if not verify_password(password, user['password_hash']):
        attempts, lock = record_failed_login(email)
        if lock:
            return jsonify({"error": "Too many failed attempts. Try again after 30 minutes."}), 401
        return jsonify({"error": generic_error}), 401

    # Success — set session
    reset_failed_login(email)
    session['user_email'] = email
    session['user_role'] = user.get('role', 'user')
    must_change = bool(user.get('must_change_password'))
    session['must_change_password'] = must_change

    return jsonify({
        "success": True,
        "must_change_password": must_change,
        "redirect": "/change-password" if must_change else "/alumni",
    }), 200


@app.route('/api/auth/register', methods=['POST'])
def api_auth_register():
    """Public registration (whitelist-gated)."""
    from auth import validate_password_policy, hash_password
    from database import get_user_by_email, create_user_with_password, is_email_authorized

    data = request.get_json(silent=True) or {}
    email = (data.get('email') or '').strip().lower()
    password = data.get('password') or ''

    if not email or not password:
        return jsonify({"error": "Email and password are required."}), 400

    # Whitelist check
    if not is_authorized_user(email):
        return jsonify({"error": "Your email is not authorized to register. Please contact an admin."}), 403

    # Check for existing user
    existing = get_user_by_email(email)
    if existing:
        if existing.get('password_hash'):
            return jsonify({"error": "An account with this email already exists. Please log in."}), 409
        # Existing LinkedIn user creating password handled via /api/auth/create-password
        return jsonify({"error": "An account with this email already exists. Log in with LinkedIn and create a password in Settings."}), 409

    # Password policy
    valid, failures = validate_password_policy(password)
    if not valid:
        return jsonify({"error": "Password does not meet requirements.", "details": failures}), 400

    pw_hash = hash_password(password)
    success = create_user_with_password(email, pw_hash, role="user")
    if not success:
        return jsonify({"error": "Failed to create account. Please try again."}), 500

    return jsonify({"success": True, "message": "Account created. Please log in."}), 201


@app.route('/api/auth/me', methods=['GET'])
@api_login_required
def api_auth_me():
    """Return current user info for the frontend (role, auth_type, email)."""
    email = _get_session_email()
    if not email:
        return jsonify({"error": "Not authenticated"}), 401

    from database import get_user_by_email
    user = get_user_by_email(email)
    if not user:
        return jsonify({"error": "User not found"}), 404

    return jsonify({
        "email": user.get('email', ''),
        "first_name": user.get('first_name', ''),
        "last_name": user.get('last_name', ''),
        "role": user.get('role', 'user'),
        "auth_type": user.get('auth_type', ''),
        "must_change_password": bool(user.get('must_change_password')),
    }), 200


@app.route('/api/auth/change-password', methods=['POST'])
@api_login_required
def api_auth_change_password():
    """Change own password (requires current password)."""
    from auth import verify_password, validate_password_policy, hash_password
    from database import get_user_by_email, update_user_password

    email = _get_session_email()
    data = request.get_json(silent=True) or {}
    current_pw = data.get('current_password') or ''
    new_pw = data.get('new_password') or ''

    if not current_pw or not new_pw:
        return jsonify({"error": "Current password and new password are required."}), 400

    user = get_user_by_email(email)
    if not user or not user.get('password_hash'):
        return jsonify({"error": "No password set on this account."}), 400

    if not verify_password(current_pw, user['password_hash']):
        return jsonify({"error": "Current password is incorrect."}), 401

    valid, failures = validate_password_policy(new_pw)
    if not valid:
        return jsonify({"error": "New password does not meet requirements.", "details": failures}), 400

    update_user_password(email, hash_password(new_pw))
    session.pop('must_change_password', None)
    return jsonify({"success": True, "message": "Password changed."}), 200


@app.route('/api/auth/create-password', methods=['POST'])
@api_login_required
def api_auth_create_password():
    """LinkedIn-only users create a password to enable email/password login."""
    from auth import validate_password_policy, hash_password
    from database import get_user_by_email, update_user_password

    email = _get_session_email()
    data = request.get_json(silent=True) or {}
    new_pw = data.get('new_password') or ''

    if not new_pw:
        return jsonify({"error": "Password is required."}), 400

    user = get_user_by_email(email)
    if not user:
        return jsonify({"error": "User not found."}), 404

    valid, failures = validate_password_policy(new_pw)
    if not valid:
        return jsonify({"error": "Password does not meet requirements.", "details": failures}), 400

    # Determine new auth_type
    current_type = user.get('auth_type', 'linkedin_only')
    new_type = 'both' if current_type == 'linkedin_only' else current_type

    update_user_password(email, hash_password(new_pw), auth_type=new_type)
    return jsonify({"success": True, "message": "Password created. You can now log in with email and password."}), 200


@app.route('/api/auth/force-change-password', methods=['POST'])
@api_login_required
def api_auth_force_change_password():
    """Handle must_change_password flow (no current password required)."""
    from auth import validate_password_policy, hash_password
    from database import get_user_by_email, update_user_password

    email = _get_session_email()
    data = request.get_json(silent=True) or {}
    new_pw = data.get('new_password') or ''

    if not new_pw:
        return jsonify({"error": "Password is required."}), 400

    user = get_user_by_email(email)
    if not user:
        return jsonify({"error": "User not found."}), 404

    if not user.get('must_change_password'):
        return jsonify({"error": "Password change not required."}), 400

    valid, failures = validate_password_policy(new_pw)
    if not valid:
        return jsonify({"error": "Password does not meet requirements.", "details": failures}), 400

    update_user_password(email, hash_password(new_pw))
    session.pop('must_change_password', None)
    return jsonify({"success": True, "message": "Password set successfully.", "redirect": "/alumni"}), 200


@app.route('/api/auth/linkedin-available', methods=['GET'])
def api_auth_linkedin_available():
    """Return whether LinkedIn OAuth is configured (frontend uses this to show/hide button)."""
    available = bool(CLIENT_ID and CLIENT_SECRET and REDIRECT_URI)
    return jsonify({"available": available}), 200


# ---------------------- Admin User Management API ----------------------

@app.route('/api/admin/users', methods=['GET'])
@api_login_required
@admin_required
def api_admin_get_users():
    """List all users (admin only)."""
    from database import get_all_users
    users = get_all_users()
    return jsonify({"success": True, "users": users}), 200


@app.route('/api/admin/users', methods=['POST'])
@api_login_required
@admin_required
def api_admin_add_user():
    """Admin adds a new user. Also adds to whitelist."""
    from database import get_user_by_email, create_user_with_password, add_authorized_email

    data = request.get_json(silent=True) or {}
    email = (data.get('email') or '').strip().lower()
    role = data.get('role', 'user')

    if not email:
        return jsonify({"error": "Email is required."}), 400
    if role not in ('admin', 'user'):
        return jsonify({"error": "Role must be 'admin' or 'user'."}), 400

    existing = get_user_by_email(email)
    if existing:
        return jsonify({"error": f"User {email} already exists."}), 409

    # Add to whitelist and create user (no password — they must register or be reset)
    add_authorized_email(email, added_by_user_id=get_current_user_id(), notes=f"Added by admin")
    success = create_user_with_password(email, None, role=role)
    if not success:
        return jsonify({"error": "Failed to create user."}), 500

    # Flag for password creation on first login
    from database import set_must_change_password
    set_must_change_password(email, True)

    return jsonify({"success": True, "message": f"User {email} created with role {role}."}), 201


@app.route('/api/admin/users', methods=['DELETE'])
@api_login_required
@admin_required
def api_admin_delete_user():
    """Admin removes a user."""
    from database import delete_user

    data = request.get_json(silent=True) or {}
    email = (data.get('email') or '').strip().lower()
    if not email:
        return jsonify({"error": "Email is required."}), 400

    # Prevent self-deletion
    if email == _get_session_email():
        return jsonify({"error": "Cannot delete your own account."}), 400

    success = delete_user(email)
    if not success:
        return jsonify({"error": "Failed to delete user."}), 500

    return jsonify({"success": True, "message": f"User {email} deleted."}), 200


@app.route('/api/admin/users/role', methods=['PUT'])
@api_login_required
@admin_required
def api_admin_update_role():
    """Admin changes a user's role."""
    from database import update_user_role

    data = request.get_json(silent=True) or {}
    email = (data.get('email') or '').strip().lower()
    role = data.get('role', '')

    if not email:
        return jsonify({"error": "Email is required."}), 400
    if role not in ('admin', 'user'):
        return jsonify({"error": "Role must be 'admin' or 'user'."}), 400

    success = update_user_role(email, role)
    if not success:
        return jsonify({"error": "Failed to update role."}), 500

    return jsonify({"success": True, "message": f"{email} is now {role}."}), 200


@app.route('/api/admin/users/reset-password', methods=['POST'])
@api_login_required
@admin_required
def api_admin_reset_password():
    """Admin resets a user's password — user must set a new one on next login."""
    from database import admin_reset_password

    data = request.get_json(silent=True) or {}
    email = (data.get('email') or '').strip().lower()
    if not email:
        return jsonify({"error": "Email is required."}), 400

    success = admin_reset_password(email)
    if not success:
        return jsonify({"error": "Failed to reset password."}), 500

    return jsonify({"success": True, "message": f"Password reset for {email}. They must set a new password on next login."}), 200


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

    # Also set user_email in session to unify with email/password sessions
    if user_email:
        session['user_email'] = user_email.lower().strip()

    if DISABLE_DB:
        return redirect('/alumni')

    # ----------------------------------------------------

    # Save/update user in database (safe connection handling)
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # Check if user already exists (to determine auth_type)
            try:
                cur.execute("SELECT password_hash, auth_type FROM users WHERE LOWER(email) = LOWER(%s)",
                            (user_email,))
            except Exception:
                cur.execute("SELECT password_hash, auth_type FROM users WHERE LOWER(email) = LOWER(?)",
                            (user_email,))
            existing_row = cur.fetchone()

            if existing_row:
                has_password = bool(existing_row[0] if not isinstance(existing_row, dict)
                                     else existing_row.get('password_hash'))
                new_auth_type = 'both' if has_password else 'linkedin_only'
            else:
                new_auth_type = 'linkedin_only'

            cur.execute("""
                INSERT INTO users (linkedin_id, email, first_name, last_name, auth_type)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    email = VALUES(email),
                    first_name = VALUES(first_name),
                    last_name = VALUES(last_name),
                    auth_type = VALUES(auth_type),
                    updated_at = CURRENT_TIMESTAMP
            """, (
                linkedin_profile.get('sub'),
                linkedin_profile.get('email'),
                linkedin_profile.get('given_name'),
                linkedin_profile.get('family_name'),
                new_auth_type,
            ))
            conn.commit()

            # Set role in session
            session['user_role'] = 'user'
            try:
                from database import get_user_by_email
                db_user = get_user_by_email(user_email)
                if db_user:
                    session['user_role'] = db_user.get('role', 'user')
                    if db_user.get('must_change_password'):
                        session['must_change_password'] = True
            except Exception:
                pass

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
    if session.get('must_change_password'):
        return redirect('/change-password')
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
    Get interactions for current user.
    Optional query param:
      - alumni_ids: comma-separated list of IDs to limit payload to currently loaded rows
    """
    if DISABLE_DB:
        # Short-circuit in dev when DB is disabled unless fallback is enabled
        if not USE_SQLITE_FALLBACK:
            return jsonify({"success": True, "interactions": [], "count": 0, "bookmarked_total": 0}), 200

    try:
        user_id = get_current_user_id()
        if not user_id:
            return jsonify({"error": "User not found"}), 401

        alumni_ids = _parse_int_list_param('alumni_ids')
        # Guardrail: do not allow extremely large IN lists from the client.
        alumni_ids = alumni_ids[:1000]

        conn = get_connection()
        use_sqlite = DISABLE_DB and USE_SQLITE_FALLBACK

        try:
            query_start = perf_counter()

            interactions = []
            bookmarked_total = 0

            if use_sqlite:
                # SQLite mode
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute(
                        """
                        SELECT COUNT(*) AS bookmarked_total
                        FROM user_interactions
                        WHERE user_id = ? AND interaction_type = 'bookmarked'
                        """,
                        (user_id,),
                    )
                    if hasattr(cursor, "fetchone"):
                        count_row = cursor.fetchone() or {}
                    else:
                        fallback_rows = cursor.fetchall() or []
                        count_row = fallback_rows[0] if fallback_rows else {}
                    bookmarked_total = count_row.get('bookmarked_total', 0) or 0

                    sql = """
                        SELECT id, alumni_id, interaction_type, notes, created_at, updated_at
                        FROM user_interactions
                        WHERE user_id = ?
                    """
                    params = [user_id]
                    if alumni_ids:
                        placeholders = ",".join(["?"] * len(alumni_ids))
                        sql += f" AND alumni_id IN ({placeholders})"
                        params.extend(alumni_ids)
                    sql += " ORDER BY updated_at DESC"
                    cursor.execute(sql, tuple(params))
                    interactions = cursor.fetchall() or []
            else:
                # MySQL mode
                with conn.cursor(dictionary=True) as cur:
                    cur.execute(
                        """
                        SELECT COUNT(*) AS bookmarked_total
                        FROM user_interactions
                        WHERE user_id = %s AND interaction_type = 'bookmarked'
                        """,
                        (user_id,),
                    )
                    if hasattr(cur, "fetchone"):
                        count_row = cur.fetchone() or {}
                    else:
                        fallback_rows = cur.fetchall() or []
                        count_row = fallback_rows[0] if fallback_rows else {}
                    bookmarked_total = count_row.get('bookmarked_total', 0) or 0

                    sql = """
                        SELECT id, alumni_id, interaction_type, notes, created_at, updated_at
                        FROM user_interactions
                        WHERE user_id = %s
                    """
                    params = [user_id]
                    if alumni_ids:
                        placeholders = ",".join(["%s"] * len(alumni_ids))
                        sql += f" AND alumni_id IN ({placeholders})"
                        params.extend(alumni_ids)
                    sql += " ORDER BY updated_at DESC"
                    cur.execute(sql, tuple(params))
                    interactions = cur.fetchall() or []

            query_ms = (perf_counter() - query_start) * 1000.0
            app.logger.debug(
                "api.user_interactions query_ms=%.2f returned=%d filtered_ids=%d",
                query_ms,
                len(interactions),
                len(alumni_ids),
            )

            # Convert datetime objects to strings for JSON serialization
            for interaction in interactions:
                created_at = interaction.get('created_at')
                updated_at = interaction.get('updated_at')
                if hasattr(created_at, 'isoformat'):
                    interaction['created_at'] = created_at.isoformat()
                if hasattr(updated_at, 'isoformat'):
                    interaction['updated_at'] = updated_at.isoformat()

            return jsonify({
                "success": True,
                "interactions": interactions,
                "count": len(interactions),
                "bookmarked_total": bookmarked_total,
            }), 200
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
    Fetch alumni with server-side pagination.
    Supports limit/offset and optional server-side filters for faster initial load.
    """
    # Short-circuit in dev when DB is disabled
    if DISABLE_DB:
        if not USE_SQLITE_FALLBACK:
            return jsonify({
                "success": True,
                "items": [],
                "alumni": [],
                "total": 0,
                "has_more": False,
                "limit": 0,
                "offset": 0,
            }), 200

    try:
        try:
            # Keep initial payload small to improve first paint time.
            limit = int(request.args.get('limit', 250))
        except Exception:
            limit = 250
        limit = max(1, min(limit, 500))

        try:
            offset = int(request.args.get('offset', 0))
        except Exception:
            offset = 0
        offset = max(0, offset)

        search_term = (request.args.get('q', '') or '').strip().lower()
        location_filters = _parse_multi_value_param('location')
        role_filters = _parse_multi_value_param('role')
        company_filters = _parse_multi_value_param('company')
        discipline_filter_logic = (request.args.get('major_logic', 'and') or 'and').strip().lower()
        if discipline_filter_logic not in {'and', 'or'}:
            return _validation_error("Invalid major_logic. Use 'and' or 'or'.", field='major_logic')

        raw_discipline_filters = _parse_multi_value_param('major')
        discipline_filters = []
        invalid_disciplines = []
        for value in raw_discipline_filters:
            canonical = _normalize_requested_discipline(value)
            if not canonical:
                invalid_disciplines.append(value)
                continue
            if canonical not in discipline_filters:
                discipline_filters.append(canonical)

        major_filters = [m for m in _parse_multi_value_param('standardized_major') if m in _APPROVED_UNT_MAJORS_SET]
        degree_filters = [d.lower() for d in _parse_multi_value_param('degree')]
        try:
            grad_year_filters = _parse_int_list_param('grad_year', strict=True)
        except ValueError as e:
            return _validation_error(str(e), field='grad_year')
        try:
            grad_year_from = _parse_optional_non_negative_int('grad_year_from')
            grad_year_to = _parse_optional_non_negative_int('grad_year_to')
            _validate_min_max(grad_year_from, grad_year_to, 'grad_year_from', 'grad_year_to')
        except ValueError as e:
            field = 'grad_year_from' if 'grad_year_from' in str(e) else 'grad_year_to'
            return _validation_error(str(e), field=field)
        working_while_studying_filter = (request.args.get('working_while_studying', '') or '').strip().lower()

        # Experience range filter (in months)
        include_unknown_experience_raw = (request.args.get('include_unknown_experience', '0') or '0').strip().lower()
        include_unknown_experience = include_unknown_experience_raw in {'1', 'true', 'yes'}
        try:
            exp_min = _parse_optional_non_negative_int('exp_min')
            exp_max = _parse_optional_non_negative_int('exp_max')
            _validate_min_max(exp_min, exp_max, 'exp_min', 'exp_max')
        except ValueError as e:
            field = 'exp_min' if 'exp_min' in str(e) else 'exp_max'
            return _validation_error(str(e), field=field)

        sort_key = (request.args.get('sort', 'name') or 'name').strip().lower()
        sort_direction = (request.args.get('direction', 'asc') or 'asc').strip().lower()
        sort_direction = 'DESC' if sort_direction == 'desc' else 'ASC'
        bookmarked_only = (request.args.get('bookmarked_only', '0') or '0').strip().lower() in {'1', 'true', 'yes'}

        try:
            unt_alumni_status_filter = _parse_unt_alumni_status_filter(request.args.get('unt_alumni_status', ''))
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        try:
            seniority_filters = _parse_seniority_filters(_parse_multi_value_param('seniority'))
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        seniority_filter_set = set(seniority_filters)

        if invalid_disciplines:
            return jsonify({"error": f"Invalid engineering discipline: {invalid_disciplines[0]}"}), 400

        user_id_for_bookmark_filter = None
        if bookmarked_only:
            user_id_for_bookmark_filter = get_current_user_id()
            if not user_id_for_bookmark_filter:
                return jsonify({"error": "User not found"}), 401

        conn = get_connection()
        try:
            query_start = perf_counter()
            with conn.cursor(dictionary=True) as cur:
                where_clauses = []
                params = []

                if search_term:
                    where_clauses.append(
                        """
                        LOWER(CONCAT_WS(' ',
                            COALESCE(a.first_name, ''),
                            COALESCE(a.last_name, ''),
                            COALESCE(a.current_job_title, ''),
                            COALESCE(a.company, ''),
                            COALESCE(a.headline, '')
                        )) LIKE %s
                        """
                    )
                    params.append(f"%{search_term}%")

                if location_filters:
                    placeholders = ",".join(["%s"] * len(location_filters))
                    where_clauses.append(f"a.location IN ({placeholders})")
                    params.extend(location_filters)

                if role_filters:
                    role_conditions = []
                    for role_value in role_filters:
                        role_conditions.append(
                            "(LOWER(COALESCE(a.current_job_title,'')) LIKE %s OR LOWER(COALESCE(njt.normalized_title,'')) LIKE %s)"
                        )
                        role_like = f"%{role_value.lower()}%"
                        params.extend([role_like, role_like])
                    where_clauses.append("(" + " OR ".join(role_conditions) + ")")

                if company_filters:
                    company_conditions = []
                    for company_value in company_filters:
                        company_conditions.append(
                            "(LOWER(COALESCE(a.company,'')) LIKE %s OR LOWER(COALESCE(nc.normalized_company,'')) LIKE %s)"
                        )
                        company_like = f"%{company_value.lower()}%"
                        params.extend([company_like, company_like])
                    where_clauses.append("(" + " OR ".join(company_conditions) + ")")

                discipline_clause = None
                discipline_params = []
                if discipline_filters:
                    expanded_disciplines = _expand_discipline_filter_values(discipline_filters)
                    placeholders = ",".join(["%s"] * len(expanded_disciplines))
                    discipline_clause = f"a.discipline IN ({placeholders})"
                    discipline_params.extend(expanded_disciplines)

                major_clause = None
                major_params = []
                if major_filters:
                    mf_conditions = []
                    for mf in major_filters:
                        mf_conditions.append("(a.standardized_major = %s OR a.standardized_major_alt = %s)")
                        major_params.extend([mf, mf])
                    major_clause = "(" + " OR ".join(mf_conditions) + ")"

                if discipline_clause and major_clause:
                    if discipline_filter_logic == 'or':
                        where_clauses.append(f"({discipline_clause} OR {major_clause})")
                        params.extend(discipline_params)
                        params.extend(major_params)
                    else:
                        where_clauses.append(discipline_clause)
                        params.extend(discipline_params)
                        where_clauses.append(major_clause)
                        params.extend(major_params)
                elif discipline_clause:
                    where_clauses.append(discipline_clause)
                    params.extend(discipline_params)
                elif major_clause:
                    where_clauses.append(major_clause)
                    params.extend(major_params)

                if grad_year_filters:
                    placeholders = ",".join(["%s"] * len(grad_year_filters))
                    where_clauses.append(f"a.grad_year IN ({placeholders})")
                    params.extend(grad_year_filters)

                if grad_year_from is not None:
                    where_clauses.append("a.grad_year >= %s")
                    params.append(grad_year_from)

                if grad_year_to is not None:
                    where_clauses.append("a.grad_year <= %s")
                    params.append(grad_year_to)

                if degree_filters:
                    degree_sql = []
                    for degree_filter in degree_filters:
                        if degree_filter in ('bachelors', 'undergraduate'):
                            degree_sql.append(
                                "(LOWER(COALESCE(a.degree,'')) LIKE %s OR LOWER(COALESCE(a.headline,'')) LIKE %s)"
                            )
                            params.extend(['%bachelor%', '%bachelor%'])
                        elif degree_filter in ('masters', 'graduate'):
                            degree_sql.append(
                                "(LOWER(COALESCE(a.degree,'')) LIKE %s OR LOWER(COALESCE(a.headline,'')) LIKE %s OR LOWER(COALESCE(a.degree,'')) LIKE %s)"
                            )
                            params.extend(['%master%', '%master%', '%mba%'])
                        elif degree_filter == 'phd':
                            degree_sql.append(
                                "(LOWER(COALESCE(a.degree,'')) LIKE %s OR LOWER(COALESCE(a.headline,'')) LIKE %s)"
                            )
                            params.extend(['%phd%', '%phd%'])
                    if degree_sql:
                        where_clauses.append("(" + " OR ".join(degree_sql) + ")")

                if working_while_studying_filter == 'yes':
                    where_clauses.append(
                        "(a.working_while_studying = 1 OR LOWER(COALESCE(a.working_while_studying_status,'')) IN ('yes','currently'))"
                    )
                elif working_while_studying_filter == 'no':
                    where_clauses.append(
                        "(a.working_while_studying = 0 OR LOWER(COALESCE(a.working_while_studying_status,'')) = 'no')"
                    )

                # Experience range filter (values in months)
                experience_expr = "CAST(NULLIF(a.relevant_experience_months, '') AS INTEGER)"
                experience_unknown_expr = "NULLIF(a.relevant_experience_months, '') IS NULL"

                if exp_min is not None:
                    if include_unknown_experience:
                        where_clauses.append(f"(({experience_expr} >= %s) OR ({experience_unknown_expr}))")
                    else:
                        where_clauses.append(f"{experience_expr} >= %s")
                    params.append(exp_min)
                if exp_max is not None:
                    if include_unknown_experience:
                        where_clauses.append(f"(({experience_expr} <= %s) OR ({experience_unknown_expr}))")
                    else:
                        where_clauses.append(f"{experience_expr} <= %s")
                    params.append(exp_max)

                if bookmarked_only:
                    where_clauses.append(
                        "EXISTS (SELECT 1 FROM user_interactions ui WHERE ui.user_id = %s AND ui.alumni_id = a.id AND ui.interaction_type = 'bookmarked')"
                    )
                    params.append(user_id_for_bookmark_filter)

                where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

                if sort_key == 'year':
                    order_clause = (
                        "CASE WHEN a.grad_year IS NULL THEN 1 ELSE 0 END ASC, "
                        f"a.grad_year {sort_direction}, "
                        "a.first_name ASC, a.last_name ASC"
                    )
                elif sort_key == 'updated':
                    order_clause = f"a.updated_at {sort_direction}, a.last_name ASC, a.first_name ASC"
                else:
                    order_clause = f"LOWER(a.first_name) {sort_direction}, LOWER(a.last_name) {sort_direction}"

                select_sql = f"""
                    SELECT a.id, a.first_name, a.last_name, a.grad_year, a.degree, a.major, a.discipline,
                           a.standardized_major, a.standardized_major_alt,
                           a.linkedin_url, a.current_job_title, a.company, a.location, a.headline,
                           a.updated_at, njt.normalized_title, nc.normalized_company,
                           a.working_while_studying, a.working_while_studying_status,
                           a.school, a.school2, a.school3,
                           a.degree2, a.degree3, a.major2, a.major3,
                           a.school_start_date, a.job_start_date, a.job_end_date,
                           a.job_employment_type, a.exp2_employment_type, a.exp3_employment_type,
                           a.exp2_title, a.exp2_company, a.exp2_dates,
                           a.exp3_title, a.exp3_company, a.exp3_dates,
                           a.seniority_level,
                           a.relevant_experience_months
                    FROM alumni a
                    LEFT JOIN normalized_job_titles njt ON a.normalized_job_title_id = njt.id
                    LEFT JOIN normalized_companies nc ON a.normalized_company_id = nc.id
                    WHERE {where_clause}
                    ORDER BY {order_clause}
                """

                requires_python_filtering = bool(unt_alumni_status_filter or seniority_filter_set)
                if requires_python_filtering:
                    # unt_alumni_status and seniority are derived fields, so we filter in Python.
                    cur.execute(select_sql, tuple(params))
                    prefiltered_rows = cur.fetchall() or []
                    filtered_rows = []
                    for row in prefiltered_rows:
                        status = compute_unt_alumni_status_from_row(row)
                        seniority_bucket = classify_seniority_bucket(
                            row.get('current_job_title'),
                            row.get('relevant_experience_months'),
                        )
                        if unt_alumni_status_filter and status != unt_alumni_status_filter:
                            continue

                        if seniority_filter_set and seniority_bucket not in seniority_filter_set:
                            continue

                        row['_computed_seniority_bucket'] = seniority_bucket
                        filtered_rows.append(row)
                    total = len(filtered_rows)
                    rows = filtered_rows[offset:offset + limit]
                else:
                    cur.execute(f"""
                        SELECT COUNT(*) AS total
                        FROM alumni a
                        LEFT JOIN normalized_job_titles njt ON a.normalized_job_title_id = njt.id
                        LEFT JOIN normalized_companies nc ON a.normalized_company_id = nc.id
                        WHERE {where_clause}
                    """, tuple(params))
                    if hasattr(cur, "fetchone"):
                        total_row = cur.fetchone() or {}
                    else:
                        fallback_rows = cur.fetchall() or []
                        total_row = fallback_rows[0] if fallback_rows else {}
                    total = int(total_row.get('total', 0) or 0)

                    cur.execute(
                        select_sql + " LIMIT %s OFFSET %s",
                        tuple(params + [limit, offset]),
                    )
                    rows = cur.fetchall() or []

            query_ms = (perf_counter() - query_start) * 1000.0

            serialization_start = perf_counter()
            alumni = []
            for r in rows:
                unt_alumni_status = compute_unt_alumni_status_from_row(r)

                full_degree = r.get('degree') or ''
                degree_level = classify_degree(full_degree, r.get('headline', ''))
                major = _resolve_major(r)
                full_major = _resolve_full_major(r)
                standardized_majors = _resolve_majors_list(r)
                discipline = _resolve_discipline(r)
                seniority_bucket = (
                    r.get('_computed_seniority_bucket')
                    or classify_seniority_bucket(
                        r.get('current_job_title'),
                        r.get('relevant_experience_months'),
                    )
                )
                updated_at = r.get('updated_at')
                if hasattr(updated_at, 'isoformat'):
                    updated_at = updated_at.isoformat()

                alumni.append({
                    "id": r.get('id'),
                    "first": (r.get('first_name') or '').strip(),
                    "last": (r.get('last_name') or '').strip(),
                    "name": f"{r.get('first_name','').strip()} {r.get('last_name','').strip()}".strip(),

                    # WHAT ANALYTICS.JS EXPECTS:
                    "current_job_title": r.get('current_job_title'),
                    "title": r.get('current_job_title'),
                    "company": r.get('company'),
                    "grad_year": r.get('grad_year'),
                    "major": major,
                    "major_raw": r.get('major'),
                    "standardized_majors": standardized_majors,
                    "discipline": discipline,

                    # Keep raw role text for card display.
                    "role": r.get('current_job_title'),

                    # Existing fields
                    "headline": r.get('headline'),
                    "class": r.get('grad_year'),
                    "location": r.get('location'),
                    "linkedin": r.get('linkedin_url'),
                    "linkedin_url": r.get('linkedin_url'),
                    "degree": degree_level,
                    "degree_raw": full_degree,
                    "full_degree": full_degree,
                    "full_major": full_major,
                    "school": r.get('school') or 'University of North Texas',
                    "school_start": r.get('school_start_date'),
                    "school_start_date": r.get('school_start_date'),
                    "school2": r.get('school2'),
                    "degree2": r.get('degree2'),
                    "major2": r.get('major2'),
                    "school3": r.get('school3'),
                    "degree3": r.get('degree3'),
                    "major3": r.get('major3'),
                    "job_start": r.get('job_start_date'),
                    "job_start_date": r.get('job_start_date'),
                    "job_end": r.get('job_end_date'),
                    "job_end_date": r.get('job_end_date'),
                    "job_employment_type": r.get('job_employment_type'),
                    "exp_2_title": r.get('exp2_title'),
                    "exp2_title": r.get('exp2_title'),
                    "exp_2_company": r.get('exp2_company'),
                    "exp2_company": r.get('exp2_company'),
                    "exp_2_dates": r.get('exp2_dates'),
                    "exp2_dates": r.get('exp2_dates'),
                    "exp_2_employment_type": r.get('exp2_employment_type'),
                    "exp2_employment_type": r.get('exp2_employment_type'),
                    "exp_3_title": r.get('exp3_title'),
                    "exp3_title": r.get('exp3_title'),
                    "exp_3_company": r.get('exp3_company'),
                    "exp3_company": r.get('exp3_company'),
                    "exp_3_dates": r.get('exp3_dates'),
                    "exp3_dates": r.get('exp3_dates'),
                    "exp_3_employment_type": r.get('exp3_employment_type'),
                    "exp3_employment_type": r.get('exp3_employment_type'),
                    "updated_at": updated_at,
                    "normalized_title": r.get('normalized_title'),
                    "normalized_company": r.get('normalized_company'),
                    "unt_alumni_status": unt_alumni_status,
                    # Prefer the fine-grained status string ("yes"/"no"/"currently");
                    # fall back to deriving from the boolean if status not yet stored.
                    "working_while_studying": (
                        r.get('working_while_studying_status')
                        or (True if r.get('working_while_studying') else
                            (False if r.get('working_while_studying') is not None else None))
                    ),
                    "seniority_level": seniority_bucket,
                    "seniority_bucket": seniority_bucket,
                    "relevant_experience_months": r.get('relevant_experience_months'),
                })

            serialization_ms = (perf_counter() - serialization_start) * 1000.0
            has_more = (offset + len(alumni)) < total

            app.logger.debug(
                "api.alumni query_ms=%.2f serialization_ms=%.2f rows=%d total=%d limit=%d offset=%d",
                query_ms,
                serialization_ms,
                len(alumni),
                total,
                limit,
                offset,
            )

            return jsonify({
                "success": True,
                "items": alumni,
                # Backward compatibility for existing clients.
                "alumni": alumni,
                "total": total,
                "has_more": has_more,
                "limit": limit,
                "offset": offset,
            }), 200
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
                           a.standardized_major, a.standardized_major_alt,
                           a.linkedin_url, a.current_job_title, a.company, a.location, a.headline,
                           a.updated_at,
                           a.school, a.school2, a.school3,
                           a.degree AS degree_raw, a.degree2, a.degree3,
                           a.major AS major_raw, a.major2, a.major3,
                           a.school_start_date, a.job_start_date, a.job_end_date,
                           a.job_employment_type, a.exp2_employment_type, a.exp3_employment_type,
                           a.exp2_title, a.exp2_company, a.exp2_dates,
                           a.exp3_title, a.exp3_company, a.exp3_dates,
                           a.seniority_level
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
                "major": _resolve_major(r),
                "full_major": _resolve_full_major(r),
                "standardized_majors": _resolve_majors_list(r),
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
                "job_employment_type": r.get('job_employment_type'),

                # Experience 2
                "exp2_title": r.get('exp2_title'),
                "exp2_company": r.get('exp2_company'),
                "exp2_dates": r.get('exp2_dates'),
                "exp2_employment_type": r.get('exp2_employment_type'),

                # Experience 3
                "exp3_title": r.get('exp3_title'),
                "exp3_company": r.get('exp3_company'),
                "exp3_dates": r.get('exp3_dates'),
                "exp3_employment_type": r.get('exp3_employment_type'),

                # Seniority classification
                "seniority_level": r.get('seniority_level'),
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


@app.route('/api/alumni/<int:alumni_id>', methods=['PUT'])
@api_login_required
def update_alumni(alumni_id):
    """Update alumni record with validation"""
    try:
        user_id = get_current_user_id()
        if not user_id:
            return jsonify({"error": "User not found"}), 401

        # Parse JSON body
        data = request.get_json() or {}

        standardize_with_groq = data.get('standardize_with_groq', True)
        if isinstance(standardize_with_groq, str):
            standardize_with_groq = standardize_with_groq.strip().lower() in {'1', 'true', 'yes', 'on'}
        else:
            standardize_with_groq = bool(standardize_with_groq)

        # Server-side validation
        errors = {}

        # Validate grad_year (must be int if provided)
        grad_year = data.get('grad_year')
        if grad_year is not None:
            try:
                grad_year = int(grad_year)
                if grad_year < 1900 or grad_year > 2100:
                    errors['grad_year'] = 'Graduation year must be between 1900 and 2100'
            except (ValueError, TypeError):
                errors['grad_year'] = 'Graduation year must be a number'

        # Validate location length
        location = data.get('location')
        if location and len(str(location)) > 255:
            errors['location'] = 'Location must be 255 characters or less'

        # Common strict limits to keep update payloads DB-safe and LLM-safe.
        field_limits = {
            'job_start_date': 50,
            'job_end_date': 50,
            'working_while_studying_status': 20,
        }
        for key, max_len in field_limits.items():
            value = data.get(key)
            if value is not None and len(str(value)) > max_len:
                errors[key] = f'{key} must be {max_len} characters or less'

        # Validate first_name and last_name
        first_name = data.get('first_name')
        if first_name and len(str(first_name)) > 100:
            errors['first_name'] = 'First name must be 100 characters or less'

        last_name = data.get('last_name')
        if last_name and len(str(last_name)) > 100:
            errors['last_name'] = 'Last name must be 100 characters or less'

        # Validate job title
        job_title = data.get('current_job_title')
        if job_title and len(str(job_title)) > 255:
            errors['current_job_title'] = 'Job title must be 255 characters or less'

        # Validate company
        company = data.get('company')
        if company and len(str(company)) > 255:
            errors['company'] = 'Company name must be 255 characters or less'

        # Validate degree and major
        degree = data.get('degree')
        if degree and len(str(degree)) > 255:
            errors['degree'] = 'Degree must be 255 characters or less'

        major = data.get('major')
        if major and len(str(major)) > 255:
            errors['major'] = 'Major must be 255 characters or less'

        # Validate headline
        headline = data.get('headline')
        if headline and len(str(headline)) > 500:
            errors['headline'] = 'Headline must be 500 characters or less'

        # Validate working_while_studying_status
        wws_status = data.get('working_while_studying_status')
        if wws_status and wws_status not in ['yes', 'no', 'currently', '']:
            errors['working_while_studying_status'] = 'Invalid working while studying status'

        if errors:
            return jsonify({"error": "Validation failed", "errors": errors}), 400

        # Update database
        if DISABLE_DB:
            return jsonify({"error": "Database access disabled"}), 503

        conn = get_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        try:
            cur = conn.cursor(dictionary=True)

            # Verify alumni exists and fetch current values.
            cur.execute("SELECT id, current_job_title, company FROM alumni WHERE id = %s", (alumni_id,))
            existing = cur.fetchone()
            if not existing:
                return jsonify({"error": "Alumni not found"}), 404

            # Build update query dynamically
            update_fields = []
            update_values = []
            
            field_mapping = {
                'first_name': 'first_name',
                'last_name': 'last_name',
                'grad_year': 'grad_year',
                'degree': 'degree',
                'major': 'major',
                'location': 'location',
                'headline': 'headline',
                'current_job_title': 'current_job_title',
                'company': 'company',
                'school_start_date': 'school_start_date',
                'job_start_date': 'job_start_date',
                'job_end_date': 'job_end_date',
                'working_while_studying_status': 'working_while_studying_status',
            }

            for key, db_column in field_mapping.items():
                if key in data and data[key] is not None:
                    update_fields.append(f"{db_column} = %s")
                    update_values.append(data[key])

            current_job_title_raw = (
                str(data.get('current_job_title', existing.get('current_job_title') if isinstance(existing, dict) else existing[1]) or '').strip()
            )
            company_raw = (
                str(data.get('company', existing.get('company') if isinstance(existing, dict) else existing[2]) or '').strip()
            )

            warnings = []
            if _looks_like_location_text(current_job_title_raw):
                warning_msg = f"Job title looks like a location for alumni_id={alumni_id}: {current_job_title_raw}"
                app.logger.warning(warning_msg)
                warnings.append(warning_msg)

            # Sync normalized title/company IDs with the requested standardization mode.
            if 'current_job_title' in data:
                normalized_title = ""
                if standardize_with_groq:
                    if normalize_title_deterministic:
                        normalized_title = normalize_title_deterministic(current_job_title_raw)
                    if normalize_title_with_groq and get_all_normalized_titles and current_job_title_raw:
                        try:
                            existing_titles_rows = get_all_normalized_titles(conn) or []
                            existing_titles = [r.get('normalized_title') for r in existing_titles_rows if isinstance(r, dict) and r.get('normalized_title')]
                            normalized_title = normalize_title_with_groq(current_job_title_raw, existing_titles) or normalized_title
                        except Exception as norm_err:
                            app.logger.warning(f"Groq title standardization failed for alumni_id={alumni_id}: {norm_err}")
                else:
                    normalized_title = current_job_title_raw

                normalized_title_id = _get_or_create_normalized_entity_id(
                    cur,
                    'normalized_job_titles',
                    'normalized_title',
                    normalized_title,
                )
                update_fields.append("normalized_job_title_id = %s")
                update_values.append(normalized_title_id)

            if 'company' in data:
                normalized_company = ""
                if standardize_with_groq:
                    if normalize_company_deterministic:
                        normalized_company = normalize_company_deterministic(company_raw)
                    if normalize_company_with_groq and get_all_normalized_companies and company_raw:
                        try:
                            existing_companies_rows = get_all_normalized_companies(conn) or []
                            existing_companies = [r.get('normalized_company') for r in existing_companies_rows if isinstance(r, dict) and r.get('normalized_company')]
                            normalized_company = normalize_company_with_groq(company_raw, existing_companies) or normalized_company
                        except Exception as norm_err:
                            app.logger.warning(f"Groq company standardization failed for alumni_id={alumni_id}: {norm_err}")
                else:
                    normalized_company = company_raw

                normalized_company_id = _get_or_create_normalized_entity_id(
                    cur,
                    'normalized_companies',
                    'normalized_company',
                    normalized_company,
                )
                update_fields.append("normalized_company_id = %s")
                update_values.append(normalized_company_id)

            if not update_fields:
                return jsonify({"error": "No fields to update"}), 400

            update_values.append(alumni_id)
            query = f"UPDATE alumni SET {', '.join(update_fields)} WHERE id = %s"
            
            cur.execute(query, update_values)
            conn.commit()

            return jsonify({"success": True, "message": "Alumni record updated", "warnings": warnings}), 200

        except Exception as err:
            app.logger.error(f"❌ Database error updating alumni {alumni_id}: {err}")
            return jsonify({"error": f"Database error: {str(err)}"}), 500
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    except Exception as e:
        app.logger.error(f"❌ Error updating alumni: {e}")
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
        if not user_id:
            return jsonify({"error": "User not found"}), 401
        
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
        if not user_id:
            return jsonify({"error": "User not found"}), 401
        
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


@app.route('/api/notes/summary', methods=['GET'])
@api_login_required
def get_notes_summary():
    """
    Return note existence flags for a batch of alumni IDs.
    This avoids N note-detail requests during list rendering.
    Query params:
      - ids: comma-separated alumni IDs (or repeated ids params)
    """
    try:
        user_id = get_current_user_id()
        if not user_id:
            return jsonify({"success": False, "error": "User not found"}), 401

        alumni_ids = _parse_int_list_param('ids')
        alumni_ids = alumni_ids[:1000]

        if not alumni_ids:
            return jsonify({"success": True, "summary": {}, "count": 0}), 200

        if DISABLE_DB and not USE_SQLITE_FALLBACK:
            summary = {str(aid): False for aid in alumni_ids}
            return jsonify({"success": True, "summary": summary, "count": 0}), 200

        conn = get_connection()
        use_sqlite = DISABLE_DB and USE_SQLITE_FALLBACK

        try:
            rows = []
            if use_sqlite:
                placeholders = ",".join(["?"] * len(alumni_ids))
                sql = f"""
                    SELECT alumni_id
                    FROM notes
                    WHERE user_id = ?
                      AND alumni_id IN ({placeholders})
                      AND note_content IS NOT NULL
                      AND TRIM(note_content) <> ''
                """
                params = tuple([user_id] + alumni_ids)
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute(sql, params)
                    rows = cursor.fetchall() or []
            else:
                placeholders = ",".join(["%s"] * len(alumni_ids))
                sql = f"""
                    SELECT alumni_id
                    FROM notes
                    WHERE user_id = %s
                      AND alumni_id IN ({placeholders})
                      AND note_content IS NOT NULL
                      AND TRIM(note_content) <> ''
                """
                params = tuple([user_id] + alumni_ids)
                with conn.cursor(dictionary=True) as cur:
                    cur.execute(sql, params)
                    rows = cur.fetchall() or []

            summary = {str(aid): False for aid in alumni_ids}
            for row in rows:
                aid = row.get('alumni_id')
                if aid is not None:
                    summary[str(aid)] = True

            return jsonify({
                "success": True,
                "summary": summary,
                "count": sum(1 for has_note in summary.values() if has_note),
            }), 200
        except Exception as err:
            app.logger.error(f"❌ Database error getting notes summary: {err}")
            return jsonify({"success": False, "error": f"Database error: {str(err)}"}), 500
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
    except Exception as e:
        app.logger.error(f"❌ Error getting notes summary: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/notes/<int:alumni_id>', methods=['POST'])
@api_login_required
def save_notes(alumni_id):
    """Save or update notes for a specific alumni"""
    try:
        user_id = get_current_user_id()
        if not user_id:
            return jsonify({"error": "User not found"}), 401

        data = request.get_json()
        if not isinstance(data, dict):
            return jsonify({"error": "Invalid JSON body"}), 400
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


@app.route('/api/scraper-activity', methods=['GET'])
@api_login_required
def get_scraper_activity_api():
    """Return who scraped and profile counts for accountability tracking."""
    try:
        from database import get_scraper_activity

        activity_rows = get_scraper_activity() or []
        users_by_email = {}

        conn = None
        try:
            conn = get_connection()
            try:
                with conn.cursor(dictionary=True) as cur:
                    cur.execute(
                        """
                        SELECT email, first_name, last_name
                        FROM users
                        WHERE email IS NOT NULL AND email != ''
                        """
                    )
                    user_rows = cur.fetchall() or []
            except Exception:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT email, first_name, last_name
                        FROM users
                        WHERE email IS NOT NULL AND email != ''
                        """
                    )
                    raw_rows = cur.fetchall() or []
                    user_rows = []
                    for row in raw_rows:
                        if isinstance(row, dict):
                            user_rows.append(row)
                        elif isinstance(row, (list, tuple)) and len(row) >= 3:
                            user_rows.append({
                                "email": row[0],
                                "first_name": row[1],
                                "last_name": row[2],
                            })

            for row in user_rows:
                email = (row.get("email") or "").strip().lower()
                if email:
                    users_by_email[email] = row
        except Exception as user_err:
            app.logger.warning(f"Could not resolve scraper user names: {user_err}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        activity = []
        total_profiles_scraped = 0
        for row in activity_rows:
            email = (row.get("email") or "").strip().lower()
            profiles_scraped = int(row.get("profiles_scraped") or 0)
            total_profiles_scraped += profiles_scraped

            last_scraped_at = row.get("last_scraped_at")
            if hasattr(last_scraped_at, "isoformat"):
                last_scraped_at = last_scraped_at.isoformat()

            activity.append({
                "email": email,
                "display_name": _resolve_scraper_display_name(email, users_by_email),
                "profiles_scraped": profiles_scraped,
                "last_scraped_at": last_scraped_at,
            })

        activity.sort(key=lambda item: (-item["profiles_scraped"], item["display_name"].lower()))

        return jsonify({
            "success": True,
            "total_profiles_scraped": total_profiles_scraped,
            "activity": activity,
        }), 200
    except Exception as e:
        app.logger.error(f"Error fetching scraper activity: {e}")
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
    try:
        unt_alumni_status_filter = _parse_unt_alumni_status_filter(request.args.get("unt_alumni_status", ""))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    # Optional graduation year range filter (passed from Analytics page redirect)
    try:
        grad_year_from = _parse_optional_non_negative_int("grad_year_from")
        grad_year_to = _parse_optional_non_negative_int("grad_year_to")
        _validate_min_max(grad_year_from, grad_year_to, "grad_year_from", "grad_year_to")
    except ValueError as e:
        message = str(e)
        if "grad_year_from" in message:
            field = "grad_year_from"
        elif "grad_year_to" in message:
            field = "grad_year_to"
        else:
            field = "grad_year_from"
        return _validation_error(message, field=field)
    heatmap_major_filters = [m for m in _parse_multi_value_param('standardized_major') if m in _APPROVED_UNT_MAJORS_SET]
    heatmap_degree_filters_raw = [d.strip().lower() for d in _parse_multi_value_param('degree') if (d or '').strip()]
    heatmap_seniority_filters_raw = _parse_multi_value_param('seniority')

    # Normalize degree filters to UI bucket labels used by classify_degree.
    heatmap_degree_filter_set = set()
    for d in heatmap_degree_filters_raw:
        if d in ('undergraduate', 'bachelors'):
            heatmap_degree_filter_set.add('Undergraduate')
        elif d in ('graduate', 'masters'):
            heatmap_degree_filter_set.add('Graduate')
        elif d in ('phd',):
            heatmap_degree_filter_set.add('PhD')
        else:
            return jsonify({"error": "Invalid degree. Use Bachelors, Masters, or PhD."}), 400

    try:
        heatmap_seniority_filters = _parse_seniority_filters(heatmap_seniority_filters_raw)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    heatmap_seniority_filter_set = set(heatmap_seniority_filters)

    if DISABLE_DB:
        if not USE_SQLITE_FALLBACK:
            return jsonify({"success": True, "locations": [], "total_alumni": 0, "max_count": 0}), 200

    # Skip cache for filtered requests (ephemeral analytical view)
    use_cache = (
        grad_year_from is None
        and grad_year_to is None
        and not heatmap_major_filters
        and not heatmap_degree_filter_set
        and not heatmap_seniority_filter_set
    )

    # --- Check cache ---
    cache_key = (
        f"{continent_filter or '__all__'}"
        f"|{unt_alumni_status_filter or '__all__'}"
    )
    if use_cache:
        cached = _heatmap_cache.get(cache_key)
        if cached and (_time.time() - cached["ts"]) < _HEATMAP_CACHE_TTL:
            return jsonify(cached["data"]), 200

    try:
        conn = get_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                # Build dynamic WHERE clause for grad_year
                year_clauses = ["latitude IS NOT NULL", "longitude IS NOT NULL"]
                year_params = []
                if grad_year_from is not None:
                    year_clauses.append("grad_year >= %s")
                    year_params.append(grad_year_from)
                if grad_year_to is not None:
                    year_clauses.append("grad_year <= %s")
                    year_params.append(grad_year_to)

                if heatmap_major_filters:
                    mf_parts = []
                    for mf in heatmap_major_filters:
                        mf_parts.append("(standardized_major = %s OR standardized_major_alt = %s)")
                        year_params.extend([mf, mf])
                    year_clauses.append("(" + " OR ".join(mf_parts) + ")")

                where_sql = " AND ".join(year_clauses)
                cur.execute(f"""
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
                           created_at,
                           grad_year,
                           school,
                           school2,
                           school3,
                           degree,
                           degree2,
                           degree3,
                           major,
                           major2,
                           major3,
                           standardized_major,
                           standardized_major_alt
                    FROM alumni
                    WHERE {where_sql}
                    ORDER BY location ASC
                """, year_params if year_params else ())
                rows = cur.fetchall()

            # ----- city-level clustering -----
            location_clusters = {}
            location_details = {}
            total_alumni = 0

            for row in rows:
                unt_alumni_status = compute_unt_alumni_status_from_row(row)
                if unt_alumni_status_filter and unt_alumni_status != unt_alumni_status_filter:
                    continue

                degree_level = classify_degree(row.get("degree"), row.get("headline", ""))
                if heatmap_degree_filter_set and degree_level not in heatmap_degree_filter_set:
                    continue

                seniority_bucket = classify_seniority_bucket(row.get("current_job_title"), None)
                if heatmap_seniority_filter_set and seniority_bucket not in heatmap_seniority_filter_set:
                    continue

                lat = row["latitude"]
                lon = row["longitude"]

                continent = get_continent(lat, lon)
                if continent_filter and continent != continent_filter:
                    continue

                total_alumni += 1
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
                    "created_at": row["created_at"].isoformat() if hasattr(row.get("created_at"), 'isoformat') else row.get("created_at"),
                    "unt_alumni_status": unt_alumni_status,
                    "degree": degree_level,
                    "seniority_level": seniority_bucket,
                    "seniority_bucket": seniority_bucket,
                    "standardized_major": (row.get("standardized_major") or "").strip(),
                    "standardized_major_alt": (row.get("standardized_major_alt") or "").strip(),
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

    results = search_location_candidates(query)

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
    Return canonical filter labels for both majors and engineering disciplines.
    """
    resp = jsonify({
        "success": True,
        "majors": sorted(APPROVED_UNT_MAJORS),
        "disciplines": sorted(APPROVED_ENGINEERING_DISCIPLINES),
    })
    resp.headers['Cache-Control'] = 'public, max-age=3600'
    return resp, 200


@app.route('/api/alumni/filter-options', methods=['GET'])
def api_alumni_filter_options():
    """
    Return backend-ranked filter options for analytics-style autocomplete.

    Query params:
      - field: location | company (default: location)
      - q: optional query string
      - limit: max results (default: 15, max: 100)
      - exclude: optional, repeated or comma-separated values to omit
    """
    if DISABLE_DB and not USE_SQLITE_FALLBACK:
        return jsonify({"success": True, "field": "location", "query": "", "options": [], "count": 0}), 200

    field = (request.args.get('field', 'location') or 'location').strip().lower()
    q = (request.args.get('q', '') or '').strip()
    try:
        limit = int(request.args.get('limit', 15))
    except Exception:
        limit = 15
    limit = max(1, min(100, limit))

    field_map = {
        'location': 'location',
        'company': 'company'
    }
    column = field_map.get(field)
    if not column:
        return jsonify({"success": False, "error": "Invalid field. Use 'location' or 'company'."}), 400

    excludes = set()
    for raw in request.args.getlist('exclude'):
        for part in (raw or '').split(','):
            cleaned = part.strip()
            if cleaned:
                excludes.add(cleaned.lower())

    conn = None
    try:
        conn = get_connection()
        with conn.cursor(dictionary=True) as cur:
            sql = f"""
                SELECT a.{column} AS option_value
                FROM alumni a
                WHERE a.{column} IS NOT NULL
                  AND TRIM(a.{column}) <> ''
            """
            params = []
            if q:
                sql += f" AND LOWER(a.{column}) LIKE %s"
                params.append(f"%{q.lower()}%")

            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []

        counts = Counter()
        for row in rows:
            value = (row.get('option_value') or '').strip()
            if not value:
                continue
            if value.lower() in excludes:
                continue
            counts[value] += 1

        options = _rank_filter_option_counts(counts, query=q, limit=limit)
        return jsonify({
            "success": True,
            "field": field,
            "query": q,
            "options": options,
            "count": len(options)
        }), 200
    except Exception as e:
        app.logger.error(f"Error loading filter options for field={field}: {e}")
        return jsonify({"success": False, "error": f"Server error: {str(e)}"}), 500
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


@app.route('/api/alumni/filter', methods=['GET'])
def api_filter_alumni():
    """
    Filter alumni by engineering discipline and other criteria.
    NOTE: Public endpoint - no authentication required
    Query params:
      - major: filter by engineering discipline (legacy query param name)
      - location, company, job_title, grad_year, degree: other filters
      - limit (default 10000), offset (default 0)
    """
    if DISABLE_DB:
        if not USE_SQLITE_FALLBACK:
            return jsonify({"success": True, "alumni": []}), 200

    try:
        # Get filter parameters
        discipline_filter = _normalize_requested_discipline(request.args.get('major', '').strip())
        location = request.args.get('location', '').strip()
        company = request.args.get('company', '').strip()
        job_title = request.args.get('job_title', '').strip()
        grad_year_raw = request.args.get('grad_year', '').strip()
        grad_year = None
        if grad_year_raw:
            if not (grad_year_raw.isdigit() and len(grad_year_raw) == 4):
                return jsonify({"error": "Invalid grad_year. Use a 4-digit year."}), 400
            grad_year = int(grad_year_raw)
        degree_filter = request.args.get('degree', '').strip()
        try:
            unt_alumni_status_filter = _parse_unt_alumni_status_filter(request.args.get('unt_alumni_status', ''))
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

        # Validate engineering discipline filter.
        raw_discipline_filter = request.args.get('major', '').strip()
        if raw_discipline_filter and not discipline_filter:
            return jsonify({"error": f"Invalid engineering discipline: {raw_discipline_filter}"}), 400

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
                # Build dynamic WHERE clause.
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
                if grad_year is not None:
                    where_clauses.append("a.grad_year = %s")
                    params.append(grad_year)
                if degree_filter:
                    if degree_filter.lower() in ('bachelors', 'undergraduate'):
                        where_clauses.append("(a.degree LIKE '%Bachelor%' OR a.degree LIKE '%B.S.%' OR a.degree LIKE '%B.A.%')")
                    elif degree_filter.lower() in ('masters', 'graduate'):
                        where_clauses.append("(a.degree LIKE '%Master%' OR a.degree LIKE '%M.S.%' OR a.degree LIKE '%MBA%')")
                    elif degree_filter.lower() == 'phd':
                        where_clauses.append("(a.degree LIKE '%Ph.D%' OR a.degree LIKE '%PhD%' OR a.degree LIKE '%Doctor%')")

                where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

                query = f"""
                    SELECT a.id, a.first_name, a.last_name, a.grad_year, a.degree, a.major, a.discipline,
                           a.standardized_major, a.standardized_major_alt, a.linkedin_url,
                           a.current_job_title, a.company, a.location, a.headline,
                           njt.normalized_title, nc.normalized_company,
                           a.school, a.school2, a.school3,
                           a.degree2, a.degree3, a.major2, a.major3
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
                    unt_alumni_status = compute_unt_alumni_status_from_row(r)
                    if unt_alumni_status_filter and unt_alumni_status != unt_alumni_status_filter:
                        continue

                    full_degree = r.get('degree') or ''
                    degree_level = classify_degree(full_degree, r.get('headline', ''))
                    major = _resolve_major(r)
                    full_major = _resolve_full_major(r)
                    standardized_majors = _resolve_majors_list(r)
                    discipline = _resolve_discipline(r)

                    # Apply discipline filter in Python.
                    if discipline_filter and discipline != discipline_filter:
                        continue

                    alumni.append({
                        "id": r.get('id'),
                        "name": f"{r.get('first_name','').strip()} {r.get('last_name','').strip()}".strip(),
                        "current_job_title": r.get('current_job_title'),
                        "company": r.get('company'),
                        "grad_year": r.get('grad_year'),
                        "major": major,
                        "standardized_majors": standardized_majors,
                        "discipline": discipline,
                        "role": r.get('current_job_title'),
                        "headline": r.get('headline'),
                        "class": r.get('grad_year'),
                        "location": r.get('location'),
                        "linkedin": r.get('linkedin_url'),
                        "degree": degree_level,
                        "full_degree": full_degree,
                        "full_major": full_major,
                        "normalized_title": r.get('normalized_title'),
                        "normalized_company": r.get('normalized_company'),
                        "unt_alumni_status": unt_alumni_status
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
    from database import (
        init_db,
        seed_alumni_data,
        has_alumni_records,
        normalize_existing_grad_years,
        normalize_single_date_education_semantics,
        ensure_all_alumni_schema_migrations,
    )
    if not DISABLE_DB:
        try:
            init_db()
            # ALTER TABLE migrations for existing DBs (idempotent; see database.py)
            ensure_all_alumni_schema_migrations()

            # Auth system migration: add auth columns to users table, seed admins
            try:
                import sys as _sys
                _sys.path.insert(0, os.path.dirname(__file__))
                from migrations.migrate_auth_system import migrate as migrate_auth
                migrate_auth()
            except Exception as auth_err:
                app.logger.warning(f"Auth migration skipped: {auth_err}")
            # Startup seed strategy:
            # - Default ("0"): skip CSV sync in app startup for faster boot.
            # - "auto": seed only when alumni table is empty.
            # - Force sync with SEED_ON_STARTUP=1/true/yes/sync.
            seed_mode = os.getenv("SEED_ON_STARTUP", "0").strip().lower()
            if seed_mode in {"1", "true", "yes", "sync", "force"}:
                app.logger.info("Seeding alumni on startup (CSV sync explicitly enabled)")
                seed_alumni_data()
            elif seed_mode in {"0", "false", "no", "off", "skip"}:
                app.logger.info("Skipping alumni seed on startup (SEED_ON_STARTUP=0)")
            elif seed_mode == "auto":
                if has_alumni_records():
                    app.logger.info("Skipping alumni seed on startup (alumni table already populated)")
                else:
                    app.logger.info("Seeding alumni on startup (alumni table is empty)")
                    seed_alumni_data()
            else:
                app.logger.info(f"Skipping alumni seed on startup (unknown SEED_ON_STARTUP={seed_mode})")

            # Always apply normalization passes so legacy date semantics stay consistent
            # even when startup seeding is disabled.
            normalize_existing_grad_years()
            normalize_single_date_education_semantics()
        except Exception as e:
            app.logger.error(f"Failed to initialize database: {e}")
            if not USE_SQLITE_FALLBACK:
                exit(1)
            else:
                app.logger.info("Continuing with SQLite fallback...")

    app.run()
