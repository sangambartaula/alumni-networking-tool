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
