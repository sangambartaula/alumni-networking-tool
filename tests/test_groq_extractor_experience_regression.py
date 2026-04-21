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
    assert 'Return only JSON' in user_prompt or 'Return ONLY JSON' in user_prompt
    assert 'Never guess missing titles, companies, or dates' in user_prompt


def test_html_to_structured_text_keeps_parent_company_for_grouped_roles():
    html = """
    <section>
      <div componentkey="entity-collection-item-1">
        <p>Civil Engineer -EIT</p>
        <p>Stantec · Full-time</p>
        <p>Aug 2025 - Present · 9 mos</p>
        <p>On-site</p>
      </div>
      <div componentkey="entity-collection-item-2">
        <p>HVJ Associates®</p>
        <p>1 yr 1 mo</p>
        <ul>
          <li>
            <p>Field Inspector</p>
            <p>Oct 2024 - Jul 2025 · 10 mos</p>
          </li>
          <li>
            <p>Site Engineer</p>
            <p>Full-time</p>
            <p>Jul 2024 - Oct 2024 · 4 mos</p>
            <p>Dallas, Texas, United States · On-site</p>
          </li>
        </ul>
      </div>
      <div componentkey="entity-collection-item-3">
        <p>Research Assistant</p>
        <p>University of North Texas · Part-time</p>
        <p>Dec 2022 - Dec 2023 · 1 yr 1 mo</p>
        <p>Denton, Texas, United States</p>
      </div>
    </section>
    """

    structured = groq_extractor_experience._html_to_structured_text(html, "Structured Text Test")

    assert "Civil Engineer -EIT | Stantec · Full-time | Aug 2025 - Present · 9 mos | On-site" in structured
    assert "HVJ Associates® | Field Inspector | Oct 2024 - Jul 2025 · 10 mos" in structured
    assert (
        "HVJ Associates® | Site Engineer | Full-time | Jul 2024 - Oct 2024 · 4 mos | Dallas, Texas, United States · On-site"
        in structured
    )
    assert (
        "Research Assistant | University of North Texas · Part-time | Dec 2022 - Dec 2023 · 1 yr 1 mo | Denton, Texas, United States"
        in structured
    )


def test_extract_experiences_with_groq_skips_oversized_entries(monkeypatch):
    class _OversizedCompletions:
        @staticmethod
        def create(**_kwargs):
            return _FakeResponse(
                '{"jobs":['
                '{"company":"' + ('A' * 300) + '","job_title":"Software Engineer","start_date":"Jan 2024","end_date":"Present"},'
                '{"company":"RWB Consulting Engineers","job_title":"Mechanical Engineer","start_date":"Jun 2025","end_date":"Present"}'
                ']}'
            )

    class _OversizedChat:
        completions = _OversizedCompletions()

    class _OversizedClient:
        chat = _OversizedChat()

    monkeypatch.setattr(groq_extractor_experience, "_get_client", lambda: _OversizedClient())
    monkeypatch.setattr(groq_extractor_experience, "is_groq_available", lambda: True)

    jobs, _ = groq_extractor_experience.extract_experiences_with_groq(
        "<section><li>dummy</li></section>",
        max_jobs=3,
        profile_name="Oversized Test",
    )

    assert len(jobs) == 1
    assert jobs[0]["company"] == "RWB Consulting Engineers"
    assert jobs[0]["job_title"] == "Mechanical Engineer"


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


def test_extract_experiences_skips_year_range_education_title(monkeypatch):
    """Date-range education snippets must not be treated as job titles."""
    class _YearRangeCompletions:
        @staticmethod
        def create(**_kwargs):
            return _FakeResponse(
                '{"jobs":['
                '{"company":"University of North Texas","job_title":"2023 - 2025 - Engineering Management","start_date":"2023","end_date":"2025"},'
                '{"company":"Acme PMO","job_title":"Project Manager","start_date":"2025","end_date":"Present"}'
                ']}'
            )

    class _YearRangeChat:
        completions = _YearRangeCompletions()

    class _YearRangeClient:
        chat = _YearRangeChat()

    monkeypatch.setattr(groq_extractor_experience, "_get_client", lambda: _YearRangeClient())
    monkeypatch.setattr(groq_extractor_experience, "is_groq_available", lambda: True)

    jobs, _ = groq_extractor_experience.extract_experiences_with_groq(
        "<section><li>dummy</li></section>",
        max_jobs=3,
        profile_name="Year Range Title Test",
    )

    assert len(jobs) == 1
    assert jobs[0]["job_title"] == "Project Manager"
    assert jobs[0]["company"] == "Acme PMO"
