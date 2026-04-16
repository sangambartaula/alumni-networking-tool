from flask import Blueprint, redirect, send_from_directory

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
