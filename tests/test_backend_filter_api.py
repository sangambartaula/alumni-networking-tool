import os
import sys
from pathlib import Path


project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "backend"))
os.chdir(project_root)

import app as backend_app


class _FakeCursor:
    def __init__(self, rows, sink):
        self._rows = rows
        self._sink = sink

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params):
        self._sink["query"] = query
        self._sink["params"] = params

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows, sink):
        self._rows = rows
        self._sink = sink

    def cursor(self, dictionary=False):
        return _FakeCursor(self._rows, self._sink)

    def close(self):
        return None


def test_filter_api_rejects_invalid_grad_year(monkeypatch):
    def _unexpected_get_connection():
        raise AssertionError("Database should not be hit for invalid grad_year")

    monkeypatch.setattr(backend_app, "get_connection", _unexpected_get_connection)
    client = backend_app.app.test_client()

    resp = client.get("/api/alumni/filter?grad_year=not-a-year")
    payload = resp.get_json()

    assert resp.status_code == 400
    assert payload["error"] == "Invalid grad_year. Use a 4-digit year."


def test_filter_api_selects_major_and_returns_it(monkeypatch):
    executed = {}
    rows = [
        {
            "id": 1,
            "first_name": "Ada",
            "last_name": "Lovelace",
            "grad_year": 2022,
            "degree": "Bachelor of Science",
            "major": "Software, Data & AI Engineering",
            "linkedin_url": "https://www.linkedin.com/in/ada",
            "current_job_title": "Software Engineer",
            "company": "UNT",
            "location": "Denton, TX",
            "headline": "Software Engineer",
            "normalized_title": "Software Engineer",
            "normalized_company": "University of North Texas",
        }
    ]

    monkeypatch.setattr(
        backend_app,
        "get_connection",
        lambda: _FakeConn(rows=rows, sink=executed),
    )
    client = backend_app.app.test_client()

    resp = client.get("/api/alumni/filter?grad_year=2022")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert "a.major" in executed["query"]
    assert executed["params"] == [2022]
    assert payload["alumni"][0]["major"] == "Software, Data & AI Engineering"
