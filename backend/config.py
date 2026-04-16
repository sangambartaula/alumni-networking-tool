import os
import secrets
from pathlib import Path

from dotenv import load_dotenv


class Config:
    """Centralized runtime configuration for the backend app."""

    @staticmethod
    def load_environment() -> None:
        env_path = Path(__file__).resolve().parent.parent / ".env"
        for enc in ("utf-8", "latin-1"):
            try:
                load_dotenv(env_path, encoding=enc)
                break
            except Exception:
                continue

    @staticmethod
    def is_production_mode() -> bool:
        env_value = (
            os.getenv("APP_ENV")
            or os.getenv("FLASK_ENV")
            or os.getenv("ENV")
            or ""
        ).strip().lower()
        return env_value in {"prod", "production"}

    @staticmethod
    def configure_secret_key(flask_app) -> None:
        secret_key = (os.getenv("SECRET_KEY") or "").strip()
        if Config.is_production_mode():
            if not secret_key:
                raise RuntimeError("SECRET_KEY environment variable is required in production.")
            if len(secret_key) < 32:
                raise RuntimeError("SECRET_KEY must be at least 32 characters in production.")
            flask_app.config["SECRET_KEY"] = secret_key
            return

        flask_app.config["SECRET_KEY"] = secret_key or secrets.token_urlsafe(32)

    @staticmethod
    def apply(flask_app) -> None:
        Config.load_environment()
        Config.configure_secret_key(flask_app)

        flask_app.config["DISABLE_DB"] = os.getenv("DISABLE_DB", "0") == "1"
        flask_app.config["USE_SQLITE_FALLBACK"] = os.getenv("USE_SQLITE_FALLBACK", "1") == "1"
        flask_app.config["QUIET_HTTP_LOGS"] = os.getenv("QUIET_HTTP_LOGS", "1") == "1"

        flask_app.config["LINKEDIN_CLIENT_ID"] = os.getenv("LINKEDIN_CLIENT_ID")
        flask_app.config["LINKEDIN_CLIENT_SECRET"] = os.getenv("LINKEDIN_CLIENT_SECRET")
        flask_app.config["LINKEDIN_REDIRECT_URI"] = os.getenv("LINKEDIN_REDIRECT_URI")

        flask_app.config["AUTHORIZED_DOMAINS"] = ["@unt.edu"]
