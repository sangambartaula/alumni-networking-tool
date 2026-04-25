import datetime
import secrets

import requests
from flask import Blueprint, current_app, jsonify, redirect, request, send_from_directory, session

from auth import (
    check_rate_limit,
    clear_rate_limit,
    hash_password,
    validate_password_policy,
    verify_password,
)
from database import (
    create_user_with_password,
    get_connection,
    get_user_by_email,
    update_user_password,
)
from middleware import _get_session_email, _is_logged_in, api_login_required, is_authorized_user
def _fetch_user_auth_snapshot(cur, user_email: str) -> tuple[bool, str]:
    """
    Return (has_password, auth_type) for existing user with broad schema fallback.
    Avoid '?' fallback on MySQL, which causes parameter-style errors.
    """
    has_password = False
    auth_type = "linkedin_only"
    # Newer schema
    try:
        cur.execute(
            "SELECT password_hash, auth_type FROM users WHERE LOWER(email) = LOWER(%s)",
            (user_email,),
        )
        row = cur.fetchone()
        if row:
            has_password = bool(row[0] if not isinstance(row, dict) else row.get("password_hash"))
            auth_type = (row[1] if not isinstance(row, dict) else row.get("auth_type")) or auth_type
            return has_password, str(auth_type)
    except Exception:
        pass

    # Older schema: auth_type missing
    try:
        cur.execute(
            "SELECT password_hash FROM users WHERE LOWER(email) = LOWER(%s)",
            (user_email,),
        )
        row = cur.fetchone()
        if row:
            has_password = bool(row[0] if not isinstance(row, dict) else row.get("password_hash"))
            return has_password, auth_type
    except Exception:
        pass

    # Minimal schema: just detect existence.
    try:
        cur.execute(
            "SELECT id FROM users WHERE LOWER(email) = LOWER(%s)",
            (user_email,),
        )
        row = cur.fetchone()
        if row:
            return False, auth_type
    except Exception:
        pass

    return False, auth_type




auth_bp = Blueprint("auth", __name__)


def _oauth_available() -> bool:
    return bool(
        current_app.config.get("LINKEDIN_CLIENT_ID")
        and current_app.config.get("LINKEDIN_CLIENT_SECRET")
        and current_app.config.get("LINKEDIN_REDIRECT_URI")
    )


@auth_bp.route("/login")
def login_page():
    if _is_logged_in():
        return redirect("/alumni")
    return send_from_directory("../frontend/public", "index.html")


@auth_bp.route("/register")
def register_page():
    if _is_logged_in():
        return redirect("/alumni")
    return send_from_directory("../frontend/public", "register.html")


@auth_bp.route("/change-password")
def change_password_page():
    if not _is_logged_in():
        return redirect("/login")
    return send_from_directory("../frontend/public", "change_password.html")


@auth_bp.route("/logout")
def logout():
    session.clear()
    return redirect("/")


@auth_bp.route("/access-denied")
def access_denied():
    return send_from_directory("../frontend/public", "access_denied.html"), 403


@auth_bp.route("/api/auth/login", methods=["POST"])
def api_auth_login():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not email or not password:
        return jsonify({"error": "Email and password are required."}), 400

    user = get_user_by_email(email)
    generic_error = "Invalid credentials"

    if user and user.get("lock_until"):
        lock_until_val = user["lock_until"]
        if isinstance(lock_until_val, str):
            try:
                lock_dt = datetime.datetime.strptime(lock_until_val, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                lock_dt = datetime.datetime.utcnow()
        else:
            lock_dt = lock_until_val

        if lock_dt > datetime.datetime.utcnow():
            delta = lock_dt - datetime.datetime.utcnow()
            mins = int(delta.total_seconds() / 60) + 1
            return jsonify({"error": f"Too many failed attempts. Try again after {mins} minutes."}), 401

    if not user:
        return jsonify({"error": generic_error}), 401

    if not is_authorized_user(email, current_app.config.get("AUTHORIZED_DOMAINS")):
        return jsonify({"error": generic_error}), 401

    if not user.get("password_hash"):
        if not check_rate_limit(email):
            return jsonify({"error": "Too many failed attempts. Try again later."}), 401
        return jsonify({"error": generic_error}), 401

    if not verify_password(password, user["password_hash"]):
        if not check_rate_limit(email):
            return jsonify({"error": "Too many failed attempts. Try again later."}), 401
        return jsonify({"error": generic_error}), 401

    clear_rate_limit(email)
    session["user_email"] = email
    session["user_role"] = user.get("role", "user")
    must_change = bool(user.get("must_change_password"))
    session["must_change_password"] = must_change

    return jsonify(
        {
            "success": True,
            "must_change_password": must_change,
            "redirect": "/change-password" if must_change else "/alumni",
        }
    ), 200


@auth_bp.route("/api/auth/register", methods=["POST"])
def api_auth_register():
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not email or not password:
        return jsonify({"error": "Email and password are required."}), 400

    if not is_authorized_user(email, current_app.config.get("AUTHORIZED_DOMAINS")):
        return jsonify({"error": "Your email is not authorized to register. Please contact an admin."}), 403

    existing = get_user_by_email(email)
    if existing:
        if existing.get("password_hash"):
            return jsonify({"error": "An account with this email already exists. Please log in."}), 409
        return jsonify(
            {
                "error": "An account with this email already exists. Log in with LinkedIn and create a password in Settings."
            }
        ), 409

    valid, failures = validate_password_policy(password)
    if not valid:
        return jsonify({"error": "Password does not meet requirements.", "details": failures}), 400

    pw_hash = hash_password(password)
    success = create_user_with_password(email, pw_hash, role="user")
    if not success:
        return jsonify({"error": "Failed to create account. Please try again."}), 500

    return jsonify({"success": True, "message": "Account created. Please log in."}), 201


@auth_bp.route("/api/auth/me", methods=["GET"])
@api_login_required
def api_auth_me():
    email = _get_session_email()
    if not email:
        return jsonify({"error": "Not authenticated"}), 401

    user = get_user_by_email(email)
    if not user:
        return jsonify({"error": "User not found"}), 404

    return jsonify(
        {
            "email": user.get("email", ""),
            "first_name": user.get("first_name", ""),
            "last_name": user.get("last_name", ""),
            "role": user.get("role", "user"),
            "auth_type": user.get("auth_type", ""),
            "must_change_password": bool(user.get("must_change_password")),
        }
    ), 200


@auth_bp.route("/api/auth/change-password", methods=["POST"])
@api_login_required
def api_auth_change_password():
    email = _get_session_email()
    data = request.get_json(silent=True) or {}
    current_pw = data.get("current_password") or ""
    new_pw = data.get("new_password") or ""

    if not current_pw or not new_pw:
        return jsonify({"error": "Current password and new password are required."}), 400

    user = get_user_by_email(email)
    if not user or not user.get("password_hash"):
        return jsonify({"error": "No password set on this account."}), 400

    if not verify_password(current_pw, user["password_hash"]):
        return jsonify({"error": "Current password is incorrect."}), 401

    valid, failures = validate_password_policy(new_pw)
    if not valid:
        return jsonify({"error": "New password does not meet requirements.", "details": failures}), 400

    update_user_password(email, hash_password(new_pw))
    session.pop("must_change_password", None)
    return jsonify({"success": True, "message": "Password changed."}), 200


@auth_bp.route("/api/auth/create-password", methods=["POST"])
@api_login_required
def api_auth_create_password():
    email = _get_session_email()
    data = request.get_json(silent=True) or {}
    new_pw = data.get("new_password") or ""

    if not new_pw:
        return jsonify({"error": "Password is required."}), 400

    user = get_user_by_email(email)
    if not user:
        return jsonify({"error": "User not found."}), 404

    valid, failures = validate_password_policy(new_pw)
    if not valid:
        return jsonify({"error": "Password does not meet requirements.", "details": failures}), 400

    current_type = user.get("auth_type", "linkedin_only")
    new_type = "both" if current_type == "linkedin_only" else current_type

    updated = update_user_password(email, hash_password(new_pw), auth_type=new_type)
    if not updated:
        return jsonify({"error": "Could not save password due to a database schema issue. Contact admin."}), 500
    return jsonify(
        {"success": True, "message": "Password created. You can now log in with email and password."}
    ), 200


@auth_bp.route("/api/auth/force-change-password", methods=["POST"])
@api_login_required
def api_auth_force_change_password():
    email = _get_session_email()
    data = request.get_json(silent=True) or {}
    new_pw = data.get("new_password") or ""

    if not new_pw:
        return jsonify({"error": "Password is required."}), 400

    user = get_user_by_email(email)
    if not user:
        return jsonify({"error": "User not found."}), 404

    if not user.get("must_change_password"):
        return jsonify({"error": "Password change not required."}), 400

    valid, failures = validate_password_policy(new_pw)
    if not valid:
        return jsonify({"error": "Password does not meet requirements.", "details": failures}), 400

    updated = update_user_password(email, hash_password(new_pw))
    if not updated:
        return jsonify({"error": "Could not save password due to a database schema issue. Contact admin."}), 500
    session.pop("must_change_password", None)
    return jsonify({"success": True, "message": "Password set successfully.", "redirect": "/alumni"}), 200


@auth_bp.route("/api/auth/linkedin-available", methods=["GET"])
def api_auth_linkedin_available():
    return jsonify({"available": _oauth_available()}), 200


@auth_bp.route("/login/linkedin")
def login_linkedin():
    if not _oauth_available():
        return jsonify({"error": "LinkedIn OAuth is not configured."}), 503

    state = secrets.token_urlsafe(16)
    session["oauth_state"] = state

    client_id = current_app.config.get("LINKEDIN_CLIENT_ID")
    redirect_uri = current_app.config.get("LINKEDIN_REDIRECT_URI")
    scope = "openid profile email"

    auth_url = (
        "https://www.linkedin.com/oauth/v2/authorization?"
        f"response_type=code&client_id={client_id}&redirect_uri={redirect_uri}"
        f"&scope={scope}&state={state}&prompt=login"
    )
    return redirect(auth_url)


@auth_bp.route("/auth/linkedin/callback")
def linkedin_callback():
    if not _oauth_available():
        return "Error: LinkedIn OAuth is not configured", 503

    code = request.args.get("code")
    state = request.args.get("state")

    if state != session.get("oauth_state"):
        return "Error: State mismatch. Potential CSRF attack.", 400

    if not code:
        return "Error: No code returned from LinkedIn", 400

    token_url = "https://www.linkedin.com/oauth/v2/accessToken"
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": current_app.config.get("LINKEDIN_REDIRECT_URI"),
        "client_id": current_app.config.get("LINKEDIN_CLIENT_ID"),
        "client_secret": current_app.config.get("LINKEDIN_CLIENT_SECRET"),
    }

    # Use explicit timeouts so a stalled LinkedIn upstream cannot hang a worker.
    try:
        resp = requests.post(
            token_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
    except requests.RequestException as exc:
        current_app.logger.error("LinkedIn token request failed: %s", exc)
        return "Error contacting LinkedIn for access token", 502
    if resp.status_code != 200:
        return f"Error fetching access token: {resp.text}", 400

    access_token = resp.json().get("access_token")
    session["linkedin_token"] = access_token

    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        userinfo_resp = requests.get(
            "https://api.linkedin.com/v2/userinfo",
            headers=headers,
            timeout=30,
        )
    except requests.RequestException as exc:
        current_app.logger.error("LinkedIn userinfo request failed: %s", exc)
        return "Error contacting LinkedIn for user info", 502
    if userinfo_resp.status_code != 200:
        return f"Error fetching LinkedIn user info: {userinfo_resp.text}", 400

    linkedin_profile = userinfo_resp.json()
    session["linkedin_profile"] = linkedin_profile

    user_email = linkedin_profile.get("email")
    if not is_authorized_user(user_email, current_app.config.get("AUTHORIZED_DOMAINS")):
        current_app.logger.warning("Unauthorized access attempt by: %s", user_email)
        session.clear()
        return redirect("/access-denied")

    if user_email:
        session["user_email"] = user_email.lower().strip()

    if current_app.config.get("DISABLE_DB"):
        return redirect("/alumni")

    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            has_password, existing_auth_type = _fetch_user_auth_snapshot(cur, user_email)
            new_auth_type = "both" if has_password else (existing_auth_type or "linkedin_only")

            upsert_params = (
                linkedin_profile.get("sub"),
                linkedin_profile.get("email"),
                linkedin_profile.get("given_name"),
                linkedin_profile.get("family_name"),
                new_auth_type,
            )

            try:
                cur.execute(
                    """
                    INSERT INTO users (linkedin_id, email, first_name, last_name, auth_type)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        email = VALUES(email),
                        first_name = VALUES(first_name),
                        last_name = VALUES(last_name),
                        auth_type = VALUES(auth_type),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    upsert_params,
                )
            except Exception as upsert_err:
                err_text = str(upsert_err).lower()
                if "auth_type" not in err_text:
                    raise

                # Backward compatibility for older users table schema without auth_type.
                cur.execute(
                    """
                    INSERT INTO users (linkedin_id, email, first_name, last_name)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        email = VALUES(email),
                        first_name = VALUES(first_name),
                        last_name = VALUES(last_name),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    upsert_params[:4],
                )
            conn.commit()

            session["user_role"] = "user"
            db_user = get_user_by_email(user_email)
            if db_user:
                session["user_role"] = db_user.get("role", "user")
                if db_user.get("must_change_password"):
                    session["must_change_password"] = True

    except Exception as exc:
        current_app.logger.error("Error saving user to database: %s", exc)
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    if session.get("must_change_password"):
        return redirect("/change-password")
    return redirect("/alumni")
