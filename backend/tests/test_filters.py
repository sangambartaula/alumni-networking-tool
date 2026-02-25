from flask import json
import app as backend_app

mock_alumni_data = [
    {"id": 1, "name": "Alice Johnson", "year": 2020, "role": "Software Engineer", "company": "Google", "location": "Austin"},
    {"id": 2, "name": "Bob Smith", "year": 2021, "role": "Data Scientist", "company": "Meta", "location": "Dallas"},
    {"id": 3, "name": "Charlie Lee", "year": 2020, "role": "Software Engineer", "company": "Amazon", "location": "Seattle"},
    {"id": 4, "name": "Diana Patel", "year": 2019, "role": "Systems Analyst", "company": "Google", "location": "Houston"},
    {"id": 5, "name": "Ethan Nguyen", "year": 2021, "role": "Software Engineer", "company": "Tesla", "location": "Austin"},
]

def apply_filters(data, year=None, role=None, company=None, location=None):
    filtered = data
    if year:
        filtered = [d for d in filtered if d["year"] == year]
    if role:
        filtered = [d for d in filtered if role.lower() in d["role"].lower()]
    if company:
        filtered = [d for d in filtered if company.lower() in d["company"].lower()]
    if location:
        filtered = [d for d in filtered if location.lower() in d["location"].lower()]
    return filtered

def sort_alumni(data, sort_key="name", reverse=False):
    return sorted(data, key=lambda x: str(x.get(sort_key, "")).lower(), reverse=reverse)

def test_filter_by_year():
    result = apply_filters(mock_alumni_data, year=2020)
    assert len(result) == 2
    assert all(a["year"] == 2020 for a in result)

def test_filter_by_company():
    result = apply_filters(mock_alumni_data, company="Google")
    assert len(result) == 2
    assert all("Google" in a["company"] for a in result)

def test_combined_filter():
    result = apply_filters(mock_alumni_data, year=2021, location="Austin")
    assert len(result) == 1
    assert result[0]["name"] == "Ethan Nguyen"

def test_case_insensitive_filter():
    result = apply_filters(mock_alumni_data, company="meta")
    assert len(result) == 1
    assert result[0]["name"] == "Bob Smith"

def test_sort_by_name():
    result = sort_alumni(mock_alumni_data, sort_key="name")
    assert [a["name"] for a in result] == sorted([a["name"] for a in mock_alumni_data])

def test_sort_by_year_descending():
    result = sort_alumni(mock_alumni_data, sort_key="year", reverse=True)
    years = [a["year"] for a in result]
    assert years == sorted(years, reverse=True)

def test_partial_name_search():
    query = "Cha"
    matches = [a for a in mock_alumni_data if query.lower() in a["name"].lower()]
    assert any("Charlie" in a["name"] for a in matches)

def test_empty_filter_returns_all():
    result = apply_filters(mock_alumni_data)
    assert len(result) == len(mock_alumni_data)

def test_invalid_filter_returns_empty():
    result = apply_filters(mock_alumni_data, company="Nonexistent")
    assert result == []

def test_api_filter_endpoint(client, monkeypatch):
    class _FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, _query, _params):
            return None

        def fetchall(self):
            return [
                {
                    "id": 1,
                    "first_name": "Alice",
                    "last_name": "Johnson",
                    "grad_year": 2020,
                    "degree": "Bachelor of Science",
                    "major": "Software, Data & AI Engineering",
                    "linkedin_url": "https://www.linkedin.com/in/alice",
                    "current_job_title": "Software Engineer",
                    "company": "Google",
                    "location": "Austin",
                    "headline": "Software Engineer",
                    "updated_at": None,
                    "normalized_title": "Software Engineer",
                    "normalized_company": "Google",
                    "working_while_studying": None,
                    "working_while_studying_status": None,
                }
            ]

    class _FakeConn:
        def cursor(self, dictionary=False):
            return _FakeCursor()

        def close(self):
            return None

    monkeypatch.setattr(backend_app, "get_connection", lambda: _FakeConn())

    response = client.get("/api/alumni?limit=1&offset=0")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["success"] is True
    assert len(data["alumni"]) == 1
    assert data["alumni"][0]["grad_year"] == 2020
