import importlib

from flask import Blueprint, jsonify

import database as backend_db
from middleware import api_login_required


scraper_bp = Blueprint("scraper", __name__)

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


def _app_mod():
    return importlib.import_module("app")


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


@scraper_bp.route("/api/scraper-activity", methods=["GET"])
@api_login_required
def get_scraper_activity_api():
    try:
        activity_rows = backend_db.get_scraper_activity() or []

        conn = _app_mod().get_connection()
        users_by_email = {}
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute("SELECT email, first_name, last_name FROM users")
                for row in (cur.fetchall() or []):
                    email = (row.get("email") or "").strip().lower()
                    if email:
                        users_by_email[email] = row
        finally:
            try:
                conn.close()
            except Exception:
                pass

        total_profiles_scraped = 0
        activity = []

        for row in activity_rows:
            email = (row.get("email") or "").strip().lower()
            profiles_scraped = int(row.get("profiles_scraped") or 0)
            total_profiles_scraped += profiles_scraped

            last_scraped_at = row.get("last_scraped_at")
            if hasattr(last_scraped_at, "isoformat"):
                last_scraped_at = last_scraped_at.isoformat()

            activity.append(
                {
                    "email": email,
                    "display_name": _resolve_scraper_display_name(email, users_by_email),
                    "profiles_scraped": profiles_scraped,
                    "last_scraped_at": last_scraped_at,
                }
            )

        activity.sort(key=lambda item: (-item["profiles_scraped"], item["display_name"].lower()))

        return jsonify(
            {
                "success": True,
                "total_profiles_scraped": total_profiles_scraped,
                "activity": activity,
            }
        ), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
