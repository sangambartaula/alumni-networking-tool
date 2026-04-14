"""Tests for education Groq extractor cleanup — activities/societies stripping."""

import os
import sys
import re
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "scraper"))
os.chdir(project_root)

import groq_extractor_education


# ========== Fake Groq infrastructure ==========

class _FakeMessage:
    def __init__(self, content):
        self.content = content

class _FakeChoice:
    def __init__(self, content):
        self.message = _FakeMessage(content)

class _FakeUsage:
    def __init__(self, total_tokens):
        self.total_tokens = total_tokens

class _FakeResponse:
    def __init__(self, content, tokens=100):
        self.choices = [_FakeChoice(content)]
        self.usage = _FakeUsage(tokens)


# ========== Tests ==========

def test_activities_stripped_from_major_raw(monkeypatch):
    """Groq returning activities text in major_raw should be cleaned."""
    import json

    class _Completions:
        @staticmethod
        def create(**_kwargs):
            return _FakeResponse(json.dumps({"education": [{
                "school": "University of North Texas",
                "degree_raw": "Bachelor of Science - BS",
                "major_raw": "Mechanical & Energy Engineering\nActivities and societies: Engineering Ambassador & Peer Mentor for The College of Engineering at UNT",
                "start_year": "2021",
                "end_year": "2025"
            }]}))

    class _Chat:
        completions = _Completions()

    class _Client:
        chat = _Chat()

    monkeypatch.setattr(groq_extractor_education, "_get_client", lambda: _Client())
    monkeypatch.setattr(groq_extractor_education, "is_groq_available", lambda: True)

    entries, _ = groq_extractor_education.extract_education_with_groq(
        "<section><li>University of North Texas</li></section>",
        profile_name="Test User",
    )

    assert entries
    assert entries[0]["major_raw"] == "Mechanical & Energy Engineering"
    assert "Activities" not in entries[0]["major_raw"]


def test_clean_major_untouched(monkeypatch):
    """Major without activities text should not be modified."""
    import json

    class _Completions:
        @staticmethod
        def create(**_kwargs):
            return _FakeResponse(json.dumps({"education": [{
                "school": "University of North Texas",
                "degree_raw": "Bachelor of Science",
                "major_raw": "Computer Science",
                "start_year": "2020",
                "end_year": "2024"
            }]}))

    class _Chat:
        completions = _Completions()

    class _Client:
        chat = _Chat()

    monkeypatch.setattr(groq_extractor_education, "_get_client", lambda: _Client())
    monkeypatch.setattr(groq_extractor_education, "is_groq_available", lambda: True)

    entries, _ = groq_extractor_education.extract_education_with_groq(
        "<section><li>University of North Texas</li></section>",
        profile_name="Test User",
    )

    assert entries
    assert entries[0]["major_raw"] == "Computer Science"


def test_trailing_em_dash_stripped_from_school(monkeypatch):
    """School name with trailing em-dash should be cleaned."""
    import json

    class _Completions:
        @staticmethod
        def create(**_kwargs):
            return _FakeResponse(json.dumps({"education": [{
                "school": "University of North Texas —",
                "degree_raw": "BS",
                "major_raw": "Engineering",
                "start_year": "2021",
                "end_year": "2025"
            }]}))

    class _Chat:
        completions = _Completions()

    class _Client:
        chat = _Chat()

    monkeypatch.setattr(groq_extractor_education, "_get_client", lambda: _Client())
    monkeypatch.setattr(groq_extractor_education, "is_groq_available", lambda: True)

    entries, _ = groq_extractor_education.extract_education_with_groq(
        "<section><li>University of North Texas</li></section>",
        profile_name="Test User",
    )

    assert entries
    assert entries[0]["school"] == "University of North Texas"


def test_description_blob_not_treated_as_school(monkeypatch):
    """Long narrative text should never survive as an education school value."""
    import json

    class _Completions:
        @staticmethod
        def create(**_kwargs):
            return _FakeResponse(json.dumps({"education": [
                {
                    "school": "Cornell University",
                    "degree_raw": "Bachelor of Science",
                    "major_raw": "Computer Science",
                    "start_year": "2016",
                    "end_year": "2020"
                },
                {
                    "school": "Staying fit at Cornell is easier than ever! With Uplift, you can: - Check gym hours and occupancy - Explore fitness classes and add them to your calendar",
                    "degree_raw": "",
                    "major_raw": "",
                    "start_year": "",
                    "end_year": ""
                }
            ]}))

    class _Chat:
        completions = _Completions()

    class _Client:
        chat = _Chat()

    monkeypatch.setattr(groq_extractor_education, "_get_client", lambda: _Client())
    monkeypatch.setattr(groq_extractor_education, "is_groq_available", lambda: True)

    entries, _ = groq_extractor_education.extract_education_with_groq(
        "<section><li>Cornell University</li></section>",
        profile_name="Blob Test",
    )

    assert len(entries) == 1
    assert entries[0]["school"] == "Cornell University"


def test_university_high_school_degree_is_cleared(monkeypatch):
    """University entries should not retain High School Diploma degrees."""
    import json

    class _Completions:
        @staticmethod
        def create(**_kwargs):
            return _FakeResponse(json.dumps({"education": [{
                "school": "University of North Texas",
                "degree_raw": "Advanced High School Diploma",
                "major_raw": "Computer Science",
                "start_year": "2014",
                "end_year": "2016"
            }]}))

    class _Chat:
        completions = _Completions()

    class _Client:
        chat = _Chat()

    monkeypatch.setattr(groq_extractor_education, "_get_client", lambda: _Client())
    monkeypatch.setattr(groq_extractor_education, "is_groq_available", lambda: True)

    entries, _ = groq_extractor_education.extract_education_with_groq(
        "<section><li>University of North Texas</li></section>",
        profile_name="HS Degree Test",
    )

    assert len(entries) == 1
    assert entries[0]["school"] == "University of North Texas"
    assert entries[0]["degree_raw"] == ""
