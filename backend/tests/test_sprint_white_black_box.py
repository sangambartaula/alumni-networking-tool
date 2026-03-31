import pytest

import app as backend_app


class _FilterOptionsCursor:
    def __init__(self, rows, sink):
        self._rows = rows
        self._sink = sink

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self._sink["query"] = query
        self._sink["params"] = tuple(params or ())

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows, sink):
        self._rows = rows
        self._sink = sink

    def cursor(self, dictionary=False):
        return _FilterOptionsCursor(self._rows, self._sink)

    def close(self):
        return None


def test_white_box_parse_multi_value_param_preserves_comma_values():
    with backend_app.app.test_request_context(
        "/api/alumni?location=Austin,%20TX&location=Dallas,%20TX"
    ):
        values = backend_app._parse_multi_value_param("location")

    assert values == ["Austin, TX", "Dallas, TX"]


def test_white_box_parse_int_list_param_strict_rejects_invalid_values():
    with backend_app.app.test_request_context("/api/alumni?grad_year=2020,2021&grad_year=oops"):
        with pytest.raises(ValueError, match="Invalid integer value for grad_year: oops"):
            backend_app._parse_int_list_param("grad_year", strict=True)


def test_black_box_filter_options_ranks_and_excludes_values(client, monkeypatch):
    executed = {}
    rows = [
        {"option_value": "Austin, TX"},
        {"option_value": "Austin, TX"},
        {"option_value": "Austin Metro"},
        {"option_value": "Remote"},
    ]
    monkeypatch.setattr(
        backend_app,
        "get_connection",
        lambda: _FakeConn(rows, executed),
    )

    resp = client.get(
        "/api/alumni/filter-options?field=location&q=aus&exclude=Remote&limit=2"
    )
    payload = resp.get_json()

    assert resp.status_code == 200
    assert "SELECT a.location AS option_value" in executed["query"]
    assert executed["params"] == ("%aus%",)
    assert payload["success"] is True
    assert payload["count"] == 2
    assert payload["options"] == [
        {"value": "Austin, TX", "count": 2},
        {"value": "Austin Metro", "count": 1},
    ]


def test_black_box_geocode_returns_candidate_list(client, monkeypatch):
    monkeypatch.setattr(
        backend_app,
        "search_location_candidates",
        lambda query: [
            {
                "display_name": f"{query}, Texas, United States",
                "lat": "33.2148",
                "lon": "-97.1331",
            }
        ],
    )

    with client.session_transaction() as sess:
        sess["linkedin_token"] = "token"

    resp = client.get("/api/geocode?q=Denton")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload == {
        "success": True,
        "count": 1,
        "results": [
            {
                "display_name": "Denton, Texas, United States",
                "lat": "33.2148",
                "lon": "-97.1331",
            }
        ],
    }
