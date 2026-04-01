import os
import sys
from pathlib import Path


project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "backend"))
os.chdir(project_root)

import app as backend_app
import database as backend_db


def _set_authenticated_session(client):
    with client.session_transaction() as sess:
        sess["linkedin_token"] = "test-token"


def test_resolve_scraper_display_name_uses_user_row_names():
    users_by_email = {
        "sangam@unt.edu": {
            "first_name": "Sangam",
            "last_name": "Bartaula",
        }
    }

    display_name = backend_app._resolve_scraper_display_name("sangam@unt.edu", users_by_email)

    assert display_name == "Sangam Bartaula"


def test_resolve_scraper_display_name_uses_team_hints_when_user_missing():
    display_name = backend_app._resolve_scraper_display_name("niranjan.paudel@unt.edu", {})

    assert display_name == "Niranjan Paudel"


def test_scraper_activity_api_returns_name_mapped_totals(monkeypatch):
    activity_rows = [
        {
            "email": "sachin.banjade@unt.edu",
            "profiles_scraped": 12,
            "last_scraped_at": None,
            "created_at": None,
        },
        {
            "email": "shrish.acharya@unt.edu",
            "profiles_scraped": 3,
            "last_scraped_at": None,
            "created_at": None,
        },
    ]
    user_rows = [
        {"email": "sachin.banjade@unt.edu", "first_name": "Sachin", "last_name": "Banjade"},
        {"email": "shrish.acharya@unt.edu", "first_name": "Shrish", "last_name": "Acharya"},
    ]

    class _FakeCursor:
        def __init__(self, rows):
            self._rows = rows

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, _query):
            return None

        def fetchall(self):
            return self._rows

    class _FakeConn:
        def cursor(self, dictionary=False):
            return _FakeCursor(user_rows)

        def close(self):
            return None

    monkeypatch.setattr(backend_db, "get_scraper_activity", lambda: activity_rows)
    monkeypatch.setattr(backend_app, "get_connection", lambda: _FakeConn())

    client = backend_app.app.test_client()
    _set_authenticated_session(client)

    resp = client.get("/api/scraper-activity")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["success"] is True
    assert payload["total_profiles_scraped"] == 15
    assert payload["activity"][0]["display_name"] == "Sachin Banjade"
    assert payload["activity"][0]["profiles_scraped"] == 12
    assert payload["activity"][1]["display_name"] == "Shrish Acharya"


def test_scraper_activity_api_requires_authentication():
    client = backend_app.app.test_client()

    resp = client.get("/api/scraper-activity")
    payload = resp.get_json()

    assert resp.status_code == 401
    assert payload["error"] == "Not authenticated"
