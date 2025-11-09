import pytest
from flask import json

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

@pytest.mark.skip(reason="Enable after /api/alumni implemented")
def test_api_filter_endpoint(client):
    response = client.get("/api/alumni?year=2020")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert all(a["year"] == 2020 for a in data)
