"""
Authentication & Authorization Module
======================================
Centralizes password hashing, validation, rate limiting, and role checks.
"""

import re
import time
import threading
import logging
import bcrypt

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Password Policy
# ---------------------------------------------------------------------------
PASSWORD_MIN_LENGTH = 10
PASSWORD_SPECIAL_CHARS = r"""!@#$%^&*()_+\-=\[\]{}|;:'",.<>?/"""

_POLICY_RULES = [
    (
        lambda p: len(p) >= PASSWORD_MIN_LENGTH,
        f"Password must be at least {PASSWORD_MIN_LENGTH} characters.",
    ),
    (
        lambda p: bool(re.search(r"[A-Z]", p)),
        "Password must contain at least 1 uppercase letter.",
    ),
    (
        lambda p: bool(re.search(r"[a-z]", p)),
        "Password must contain at least 1 lowercase letter.",
    ),
    (
        lambda p: bool(re.search(r"\d", p)),
        "Password must contain at least 1 number.",
    ),
    (
        lambda p: bool(re.search(r"""[!@#$%^&*()\\_+\-=\[\]{}|;:'",.<>?/]""", p)),
        "Password must contain at least 1 special character.",
    ),
]


def validate_password_policy(password):
    """
    Validate *password* against the security policy.

    Returns ``(True, [])`` when the password is acceptable, otherwise
    ``(False, [list of human-readable failure reasons])``.
    """
    if not password:
        return False, ["Password is required."]

    failures = [msg for check, msg in _POLICY_RULES if not check(password)]
    return (len(failures) == 0, failures)


# ---------------------------------------------------------------------------
# Password Hashing  (bcrypt)
# ---------------------------------------------------------------------------

def hash_password(password):
    """Hash *password* with bcrypt and return the hash as a UTF-8 string."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password, password_hash):
    """Return ``True`` if *password* matches *password_hash*."""
    if not password or not password_hash:
        return False
    try:
        return bcrypt.checkpw(
            password.encode("utf-8"), password_hash.encode("utf-8")
        )
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Rate Limiting  (in-memory, per-email)
# ---------------------------------------------------------------------------
_LOGIN_ATTEMPTS = {}          # email -> [timestamp, ...]
_RATE_LIMIT_LOCK = threading.Lock()
RATE_LIMIT_WINDOW = 15 * 60  # 15 minutes
RATE_LIMIT_MAX = 5            # max attempts per window


def check_rate_limit(email):
    """
    Return ``True`` if the email is within the allowed attempt window.
    Return ``False`` (blocked) if the limit has been exceeded.
    """
    email = (email or "").strip().lower()
    if not email:
        return False

    now = time.time()
    cutoff = now - RATE_LIMIT_WINDOW

    with _RATE_LIMIT_LOCK:
        attempts = _LOGIN_ATTEMPTS.get(email, [])
        # Prune old attempts
        attempts = [t for t in attempts if t > cutoff]
        _LOGIN_ATTEMPTS[email] = attempts

        if len(attempts) >= RATE_LIMIT_MAX:
            logger.warning(f"Rate limit exceeded for {email}")
            return False

        attempts.append(now)
        _LOGIN_ATTEMPTS[email] = attempts
        return True


def clear_rate_limit(email):
    """Clear rate-limit history for *email* (e.g. after a successful login)."""
    email = (email or "").strip().lower()
    with _RATE_LIMIT_LOCK:
        _LOGIN_ATTEMPTS.pop(email, None)


# ---------------------------------------------------------------------------
# Default Admins & Whitelist Seed Data
# ---------------------------------------------------------------------------
DEFAULT_ADMIN_EMAILS = [
    "paul.krueger@unt.edu",
    "seifollah.nasrazadani@unt.edu",
    "aashishs421@gmail.com",
    "ashrish63@gmail.com",
    "lamichhaneabishek451@gmail.com",
    "sachinbanjade@my.unt.edu",
    "sangambartaula4@gmail.com",
]
