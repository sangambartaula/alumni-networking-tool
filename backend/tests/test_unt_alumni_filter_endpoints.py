import app as backend_app


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, _query, _params=None):
        return None

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self, dictionary=False):
        return _FakeCursor(self._rows)

    def close(self):
        return None


def _rows_for_filters():
    return [
        {
            "id": 1,
            "first_name": "Alice",
            "last_name": "Past",
            "grad_year": 2020,
            "degree": "Bachelor of Science",
            "major": "Software, Data & AI Engineering",
            "discipline": "Software, Data & AI Engineering",
            "standardized_major": "Computer Science",
            "linkedin_url": "https://www.linkedin.com/in/alice",
            "current_job_title": "Engineer",
            "company": "Acme",
            "location": "Austin, TX",
            "headline": "Engineer",
            "updated_at": None,
            "normalized_title": "Engineer",
            "normalized_company": "Acme",
            "working_while_studying": None,
            "working_while_studying_status": None,
            "school": "University of North Texas",
            "school2": None,
            "school3": None,
            "degree2": None,
            "degree3": None,
            "major2": None,
            "major3": None,
            "latitude": 33.2148,
            "longitude": -97.1331,
            "created_at": None,
        },
        {
            "id": 2,
            "first_name": "Bob",
            "last_name": "Future",
            "grad_year": 2028,
            "degree": "Master of Science",
            "major": "Software, Data & AI Engineering",
            "discipline": "Software, Data & AI Engineering",
            "standardized_major": "Computer Science",
            "linkedin_url": "https://www.linkedin.com/in/bob",
            "current_job_title": "Engineer",
            "company": "Acme",
            "location": "Austin, TX",
            "headline": "Engineer",
            "updated_at": None,
            "normalized_title": "Engineer",
            "normalized_company": "Acme",
            "working_while_studying": None,
            "working_while_studying_status": None,
            "school": "University of North Texas",
            "school2": None,
            "school3": None,
            "degree2": None,
            "degree3": None,
            "major2": None,
            "major3": None,
            "latitude": 33.2148,
            "longitude": -97.1331,
            "created_at": None,
        },
        {
            "id": 3,
            "first_name": "Cara",
            "last_name": "Unknown",
            "grad_year": None,
            "degree": "Bachelor of Arts",
            "major": "Software, Data & AI Engineering",
            "discipline": "Software, Data & AI Engineering",
            "standardized_major": "Computer Science",
            "linkedin_url": "https://www.linkedin.com/in/cara",
            "current_job_title": "Analyst",
            "company": "Beta",
            "location": "Dallas, TX",
            "headline": "Analyst",
            "updated_at": None,
            "normalized_title": "Analyst",
            "normalized_company": "Beta",
            "working_while_studying": None,
            "working_while_studying_status": None,
            "school": "Texas A&M University",
            "school2": "University of North Texas",
            "school3": None,
            "degree2": "MBA",
            "degree3": None,
            "major2": "Business",
            "major3": None,
            "latitude": 32.7767,
            "longitude": -96.7970,
            "created_at": None,
        },
    ]


def test_dashboard_list_endpoint_filters_with_unt_status_and_location(client, monkeypatch):
    monkeypatch.setattr(backend_app, "get_connection", lambda: _FakeConn(_rows_for_filters()))

    resp = client.get("/api/alumni?location=Austin&unt_alumni_status=no")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["success"] is True
    assert len(payload["alumni"]) == 1
    assert payload["alumni"][0]["id"] == 2
    assert payload["alumni"][0]["unt_alumni_status"] == "no"


def test_analytics_data_path_filters_unknown_status_with_location(client, monkeypatch):
    monkeypatch.setattr(backend_app, "get_connection", lambda: _FakeConn(_rows_for_filters()))

    resp = client.get("/api/alumni?location=Dallas&unt_alumni_status=unknown")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["success"] is True
    assert len(payload["alumni"]) == 1
    assert payload["alumni"][0]["id"] == 3
    assert payload["alumni"][0]["unt_alumni_status"] == "unknown"


def test_heatmap_endpoint_filters_with_continent_and_unt_status(client, monkeypatch):
    backend_app._heatmap_cache.clear()
    monkeypatch.setattr(backend_app, "get_connection", lambda: _FakeConn(_rows_for_filters()))

    resp = client.get("/api/heatmap?continent=North%20America&unt_alumni_status=no")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["success"] is True
    assert payload["total_alumni"] == 1
    assert len(payload["locations"]) == 1
    sample_alumni = payload["locations"][0]["sample_alumni"]
    assert len(sample_alumni) == 1
    assert sample_alumni[0]["id"] == 2
    assert sample_alumni[0]["unt_alumni_status"] == "no"

