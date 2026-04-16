import datetime
import logging
import re
from functools import wraps

from flask import jsonify, redirect, request, session

from database import get_connection, get_user_by_email

logger = logging.getLogger(__name__)


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

        return False


def configure_werkzeug_access_logging() -> None:
    werkzeug_logger = logging.getLogger("werkzeug")
    werkzeug_logger.setLevel(logging.INFO)
    if not any(getattr(f, "_quiet_http_filter", False) for f in werkzeug_logger.filters):
        access_filter = _SuppressWerkzeugAccessLogFilter()
        access_filter._quiet_http_filter = True
        werkzeug_logger.addFilter(access_filter)


def is_authorized_user(email: str, authorized_domains=None) -> bool:
    if not email:
        return False

    email_lower = email.lower().strip()
    domains = authorized_domains or ["@unt.edu"]

    try:
        from database import is_email_authorized

        if is_email_authorized(email_lower):
            logger.info("User %s authorized via database whitelist", email_lower)
            return True
    except Exception as exc:
        logger.error("Error checking authorized emails from database: %s", exc)

    if email_lower.endswith("@my.unt.edu"):
        logger.warning("Student email blocked: %s", email_lower)
        return False

    for domain in domains:
        if email_lower.endswith(domain.lower()):
            logger.info("User %s authorized via domain %s", email_lower, domain)
            return True

    logger.warning("Unauthorized email attempted access: %s", email_lower)
    return False


def _is_logged_in() -> bool:
    return "linkedin_token" in session or "user_email" in session


def _get_session_email():
    if "user_email" in session:
        return session["user_email"]

    profile = session.get("linkedin_profile")
    if profile:
        return profile.get("email")
    return None


def get_current_user_id(disable_db=False, use_sqlite_fallback=True):
    if not _is_logged_in():
        return None

    if "user_email" in session:
        email = session["user_email"]
        conn = None
        try:
            conn = get_connection()
            with conn.cursor() as cur:
                try:
                    cur.execute("SELECT id FROM users WHERE LOWER(email) = LOWER(?)", (email,))
                except Exception:
                    cur.execute("SELECT id FROM users WHERE LOWER(email) = LOWER(%s)", (email,))
                result = cur.fetchone()
                return (result["id"] if isinstance(result, dict) else result[0]) if result else None
        except Exception as exc:
            logger.error("Error getting user ID by email: %s", exc)
            return None
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    linkedin_profile = session.get("linkedin_profile")
    if not linkedin_profile:
        return None

    linkedin_id = linkedin_profile.get("sub")

    if disable_db and not use_sqlite_fallback:
        session.setdefault("_dev_user_id", 1)
        return session["_dev_user_id"]

    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users WHERE linkedin_id = %s", (linkedin_id,))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as exc:
        logger.error("Error getting user ID: %s", exc)
        return None
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not _is_logged_in():
            return redirect("/login")
        if session.get("must_change_password") and request.path != "/change-password":
            return redirect("/change-password")
        return f(*args, **kwargs)

    return decorated_function


def api_login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not _is_logged_in():
            return jsonify({"error": "Not authenticated"}), 401
        return f(*args, **kwargs)

    return decorated_function


def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        email = _get_session_email()
        if not email:
            return jsonify({"error": "Not authenticated"}), 401

        user = get_user_by_email(email)
        if not user or user.get("role") != "admin":
            return jsonify({"error": "Admin access required"}), 403

        return f(*args, **kwargs)

    return decorated_function
