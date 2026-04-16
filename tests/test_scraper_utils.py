"""Focused tests for reusable Groq scraper utilities."""

import os
import sys


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scraper"))


def test_parse_groq_date():
    from groq_client import parse_groq_date

    result = parse_groq_date("Oct 2024")
    assert result is not None
    assert result["year"] == 2024
    assert result["month"] == 10

    result = parse_groq_date("Present")
    assert result is not None
    assert result["is_present"] is True

    result = parse_groq_date("2023")
    assert result is not None
    assert result["year"] == 2023

    result = parse_groq_date("")
    assert result is None


def test_clean_doubled():
    from groq_client import _clean_doubled

    assert _clean_doubled("EngineerEngineer") == "Engineer"
    assert _clean_doubled("Hello World Hello World") == "Hello World"
    assert _clean_doubled("Normal") == "Normal"
    assert _clean_doubled("") == ""
    assert _clean_doubled(None) is None
