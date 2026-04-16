from flask import Flask, request

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
from routes.analytics_routes import analytics_bp, _heatmap_cache
from routes.core_routes import core_bp
from routes.auth_routes import auth_bp
from routes.interaction_routes import interaction_bp
from routes.scraper_routes import scraper_bp, _resolve_scraper_display_name
from geocoding import search_location_candidates

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

app.register_blueprint(core_bp)
app.register_blueprint(auth_bp)
app.register_blueprint(alumni_bp)
app.register_blueprint(analytics_bp)
app.register_blueprint(interaction_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(scraper_bp)


def _parse_multi_value_param(param_name):
    values = []
    for raw in request.args.getlist(param_name):
        if raw is None:
            continue
        cleaned = str(raw).strip()
        if cleaned:
            values.append(cleaned)
    return values


def _parse_int_list_param(param_name, strict=False):
    values = []
    for raw in request.args.getlist(param_name):
        if raw is None:
            continue
        for piece in str(raw).split(","):
            cleaned = piece.strip()
            if not cleaned:
                continue
            try:
                values.append(int(cleaned))
            except Exception:
                if strict:
                    raise ValueError(f"Invalid integer value for {param_name}: {cleaned}")
    return values


@app.errorhandler(404)
def not_found(_err):
    return {"error": "Not found"}, 404


@app.errorhandler(500)
def server_error(_err):
    return {"error": "Internal server error"}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
