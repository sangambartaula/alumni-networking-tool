from flask import Blueprint, current_app, jsonify, redirect, send_from_directory

from middleware import _is_logged_in


core_bp = Blueprint("core", __name__)


@core_bp.route("/")
def home():
    if _is_logged_in():
        return redirect("/alumni")
    return send_from_directory("../frontend/public", "index.html")


@core_bp.route("/about")
def about():
    return send_from_directory("../frontend/public", "about.html")


@core_bp.route("/alumni_style.css")
def alumni_css():
    return send_from_directory("../frontend/public", "alumni_style.css")


@core_bp.route("/app.js")
def serve_js():
    return send_from_directory("../frontend/public", "app.js")


@core_bp.route("/assets/<path:filename>")
def assets(filename):
    return send_from_directory("../frontend/public/assets", filename)


@core_bp.route("/profile_modal.js")
def serve_profile_modal_js():
    return send_from_directory("../frontend/public", "profile_modal.js")


@core_bp.route("/profile_modal.css")
def serve_profile_modal_css():
    return send_from_directory("../frontend/public", "profile_modal.css")


@core_bp.route("/profile_modal_test.js")
def serve_profile_modal_test_js():
    return send_from_directory("../frontend/public", "profile_modal_test.js")


@core_bp.route("/api/fallback-status", methods=["GET"])
def get_fallback_status_api():
    if not current_app.config.get("USE_SQLITE_FALLBACK"):
        return jsonify({"success": True, "enabled": False, "message": "SQLite fallback is disabled"}), 200

    try:
        from sqlite_fallback import get_fallback_status

        status = get_fallback_status()
        return jsonify({"success": True, "enabled": True, **status}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
