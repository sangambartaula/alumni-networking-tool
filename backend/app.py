from flask import Flask, redirect, send_from_directory

from config import Config
from middleware import (
    _get_session_email,
    _is_logged_in,
    admin_required,
    api_login_required,
    configure_werkzeug_access_logging,
    get_current_user_id,
    is_authorized_user,
    login_required,
)
from routes.admin_routes import admin_bp
from routes.alumni_routes import alumni_bp
from routes.auth_routes import auth_bp
from routes.interaction_routes import interaction_bp

# Backward-compat import exposed at module scope for existing tests/patches.
from database import get_connection


app = Flask(
    __name__,
    static_folder="../frontend/public",
    static_url_path="",
)

Config.apply(app)

if app.config.get("QUIET_HTTP_LOGS", True):
    configure_werkzeug_access_logging()


@app.route("/")
def home():
    if _is_logged_in():
        return redirect("/alumni")
    return send_from_directory("../frontend/public", "index.html")


@app.route("/about")
def about():
    return send_from_directory("../frontend/public", "about.html")


@app.route("/alumni_style.css")
def alumni_css():
    return send_from_directory("../frontend/public", "alumni_style.css")


@app.route("/app.js")
def serve_js():
    return send_from_directory("../frontend/public", "app.js")


@app.route("/assets/<path:filename>")
def assets(filename):
    return send_from_directory("../frontend/public/assets", filename)


app.register_blueprint(auth_bp)
app.register_blueprint(alumni_bp)
app.register_blueprint(interaction_bp)
app.register_blueprint(admin_bp)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
