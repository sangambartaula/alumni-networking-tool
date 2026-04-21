import os
import sys
from pathlib import Path


project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "scraper"))
os.chdir(project_root)

import degree_normalization
import major_normalization
import company_normalization


def test_degree_uses_groq_first_when_available(monkeypatch):
    monkeypatch.setenv("USE_GROQ", "true")
    monkeypatch.setenv("GROQ_API_KEY", "dummy")
    monkeypatch.setattr(degree_normalization, "_standardize_degree_with_llm", lambda raw: "Masters")

    # Deterministic mapping for BS would be Bachelors; Groq-first should win.
    assert degree_normalization.standardize_degree("BS") == "Masters"


def test_degree_falls_back_to_deterministic_when_groq_is_other(monkeypatch):
    monkeypatch.setenv("USE_GROQ", "true")
    monkeypatch.setenv("GROQ_API_KEY", "dummy")
    monkeypatch.setattr(degree_normalization, "_standardize_degree_with_llm", lambda raw: "Other")

    # BS should still resolve via deterministic fallback.
    assert degree_normalization.standardize_degree("BS") == "Bachelors"


def test_major_uses_groq_first_when_available(monkeypatch):
    monkeypatch.setenv("USE_GROQ", "true")
    monkeypatch.setenv("GROQ_API_KEY", "dummy")
    monkeypatch.setenv("MAJOR_USE_GROQ_FALLBACK", "1")
    monkeypatch.setattr(major_normalization, "_standardize_major_with_llm", lambda raw, job: "Electrical Engineering")

    # Deterministic mapping for "Computer Science" would be Computer Science;
    # Groq-first behavior should honor the LLM output when valid.
    assert major_normalization.standardize_major_list("Computer Science", "Software Engineer") == ["Electrical Engineering"]


def test_major_falls_back_to_deterministic_when_groq_returns_other(monkeypatch):
    monkeypatch.setenv("USE_GROQ", "true")
    monkeypatch.setenv("GROQ_API_KEY", "dummy")
    monkeypatch.setenv("MAJOR_USE_GROQ_FALLBACK", "1")
    monkeypatch.setattr(major_normalization, "_standardize_major_with_llm", lambda raw, job: "Other")

    assert major_normalization.standardize_major_list("Computer Science", "Software Engineer") == ["Computer Science"]


def test_company_uses_groq_first_when_available(monkeypatch):
    monkeypatch.setenv("USE_GROQ", "true")
    monkeypatch.setattr(company_normalization, "GROQ_API_KEY", "dummy")
    monkeypatch.setattr(company_normalization, "normalize_company_with_groq", lambda raw, existing: "OpenAI")

    assert company_normalization.normalize_company_deterministic("Open AI, Inc.") == "OpenAI"


def test_company_falls_back_when_groq_is_disabled(monkeypatch):
    monkeypatch.setenv("USE_GROQ", "false")

    assert company_normalization.normalize_company_deterministic("Google Inc.") == "Google"
