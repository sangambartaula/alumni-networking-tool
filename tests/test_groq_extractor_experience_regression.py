import os
import sys
from pathlib import Path


project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "scraper"))
os.chdir(project_root)

import groq_extractor_experience


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
    def __init__(self, content, tokens=123):
        self.choices = [_FakeChoice(content)]
        self.usage = _FakeUsage(tokens)


class _FakeCompletions:
    @staticmethod
    def create(**_kwargs):
        return _FakeResponse(
            '{"jobs":[{"company":"ACME Corp","job_title":"Software Engineer","start_date":"Jan 2024","end_date":"Present"}]}'
        )


class _FakeChat:
    completions = _FakeCompletions()


class _FakeClient:
    chat = _FakeChat()


def test_extract_experiences_with_groq_does_not_crash_on_length_metrics(monkeypatch):
    monkeypatch.setattr(groq_extractor_experience, "_get_client", lambda: _FakeClient())
    monkeypatch.setattr(groq_extractor_experience, "is_groq_available", lambda: True)

    jobs, token_count = groq_extractor_experience.extract_experiences_with_groq(
        "<section><li>Software Engineer | ACME Corp | Jan 2024 - Present</li></section>",
        max_jobs=3,
        profile_name="Test User",
    )

    assert jobs
    assert jobs[0]["job_title"] == "Software Engineer"
    assert jobs[0]["company"] == "ACME Corp"
    assert token_count == 123


def test_extract_experiences_with_groq_skips_title_company_collisions(monkeypatch):
    class _CollisionCompletions:
        @staticmethod
        def create(**_kwargs):
            return _FakeResponse(
                '{"jobs":['
                '{"company":"UNT College of Engineering","job_title":"UNT College of Engineering","start_date":"2021","end_date":"2025"},'
                '{"company":"RWB Consulting Engineers","job_title":"Mechanical Engineer","start_date":"2025","end_date":"Present"}'
                ']}'
            )

    class _CollisionChat:
        completions = _CollisionCompletions()

    class _CollisionClient:
        chat = _CollisionChat()

    monkeypatch.setattr(groq_extractor_experience, "_get_client", lambda: _CollisionClient())
    monkeypatch.setattr(groq_extractor_experience, "is_groq_available", lambda: True)

    jobs, _ = groq_extractor_experience.extract_experiences_with_groq(
        "<section><li>dummy</li></section>",
        max_jobs=3,
        profile_name="Collision Test",
    )

    assert len(jobs) == 1
    assert jobs[0]["company"] == "RWB Consulting Engineers"
    assert jobs[0]["job_title"] == "Mechanical Engineer"


def test_extract_experiences_with_groq_prompt_stays_lean(monkeypatch):
    captured = {}

    class _PromptCompletions:
        @staticmethod
        def create(**kwargs):
            captured["messages"] = kwargs.get("messages", [])
            return _FakeResponse(
                '{"jobs":[{"company":"ACME Corp","job_title":"Software Engineer","start_date":"Jan 2024","end_date":"Present"}]}'
            )

    class _PromptChat:
        completions = _PromptCompletions()

    class _PromptClient:
        chat = _PromptChat()

    monkeypatch.setattr(groq_extractor_experience, "_get_client", lambda: _PromptClient())
    monkeypatch.setattr(groq_extractor_experience, "is_groq_available", lambda: True)

    jobs, _ = groq_extractor_experience.extract_experiences_with_groq(
        "<section><li>dummy</li></section>",
        max_jobs=1,
        profile_name="Prompt Test",
    )

    assert jobs
    user_prompt = ""
    for msg in captured.get("messages", []):
        if msg.get("role") == "user":
            user_prompt = msg.get("content", "")
            break

    assert "Standardized job titles" not in user_prompt


def test_extract_experiences_strips_trailing_employment_type(monkeypatch):
    """Company names with trailing employment types should be cleaned."""
    class _EmpTypeCompletions:
        @staticmethod
        def create(**_kwargs):
            return _FakeResponse(
                '{"jobs":['
                '{"company":"UNT College of Engineering Part-time","job_title":"Peer Mentor","start_date":"Oct 2021","end_date":"Apr 2025"},'
                '{"company":"RWB Consulting Engineers","job_title":"Mechanical Design Engineer","start_date":"Jun 2025","end_date":"Present"}'
                ']}'
            )

    class _EmpTypeChat:
        completions = _EmpTypeCompletions()

    class _EmpTypeClient:
        chat = _EmpTypeChat()

    monkeypatch.setattr(groq_extractor_experience, "_get_client", lambda: _EmpTypeClient())
    monkeypatch.setattr(groq_extractor_experience, "is_groq_available", lambda: True)

    jobs, _ = groq_extractor_experience.extract_experiences_with_groq(
        "<section><li>dummy</li></section>",
        max_jobs=3,
        profile_name="Employment Type Test",
    )

    assert len(jobs) == 2
    assert jobs[0]["company"] == "RWB Consulting Engineers"  # no change needed
    assert jobs[1]["company"] == "UNT College of Engineering"  # "Part-time" stripped
    assert "Part-time" not in jobs[1]["company"]
