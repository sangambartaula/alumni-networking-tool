import os
import sys
import types
from datetime import datetime, timezone
from pathlib import Path


project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "scraper"))
os.chdir(project_root)

import main as scraper_main


class _DummyScraper:
    def scroll_full_page(self):
        return None

    def extract_profile_urls_from_page(self):
        return []


class _DummyNav:
    def __init__(self, ok=True):
        self.ok = ok
        self.urls = []

    def get(self, url):
        self.urls.append(url)
        return self.ok


class _DummyHistory:
    def should_skip(self, _url):
        return False


def test_run_search_mode_resets_state_to_page1_when_no_results(monkeypatch):
    calls = []
    nav = _DummyNav(ok=True)

    monkeypatch.setattr(scraper_main, "load_scrape_state", lambda: None)
    monkeypatch.setattr(scraper_main, "save_scrape_state", lambda m, u, p: calls.append((m, u, p)))
    monkeypatch.setattr(scraper_main.time, "sleep", lambda _s: None)

    scraper_main.exit_requested = False
    scraper_main.force_exit = False

    scraper_main.run_search_mode(_DummyScraper(), nav, _DummyHistory())

    assert nav.urls, "Search page should be visited at least once"
    assert calls[0][2] == 1, "First checkpoint should save current page"
    assert calls[-1][2] == 1, "No-results path should reset next run to page 1"


def test_run_search_mode_resumes_recent_state(monkeypatch):
    calls = []
    nav = _DummyNav(ok=False)  # stop immediately after first navigation
    base_url = (
        "https://www.linkedin.com/search/results/people/"
        "?network=%5B%22O%22%5D&schoolFilter=%5B%226464%22%5D"
    )

    monkeypatch.setattr(
        scraper_main,
        "load_scrape_state",
        lambda: {
            "mode": "search",
            "search_url": base_url,
            "page": 5,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    monkeypatch.setattr(scraper_main, "save_scrape_state", lambda m, u, p: calls.append((m, u, p)))
    monkeypatch.setattr(scraper_main.time, "sleep", lambda _s: None)

    scraper_main.exit_requested = False
    scraper_main.force_exit = False

    scraper_main.run_search_mode(_DummyScraper(), nav, _DummyHistory())

    assert nav.urls == [f"{base_url}&page=5"]
    assert calls and calls[0][2] == 5


def test_run_names_mode_default_input_csv(monkeypatch):
    captured = {}

    def _fake_load_names(csv_path):
        captured["csv_path"] = csv_path
        return []

    monkeypatch.delenv("INPUT_CSV", raising=False)
    monkeypatch.setattr(scraper_main.utils, "load_names_from_csv", _fake_load_names)

    scraper_main.run_names_mode(_DummyScraper(), _DummyNav(ok=True), _DummyHistory())

    assert "csv_path" in captured
    assert captured["csv_path"].name == "engineering_graduates.csv"


def test_remove_dead_urls_uses_exact_delete_query(monkeypatch, tmp_path):
    executed = []

    class _FakeCursor:
        def execute(self, query, params):
            executed.append((query, params))

    class _FakeConn:
        def __init__(self):
            self._cursor = _FakeCursor()

        def cursor(self):
            return self._cursor

        def commit(self):
            return None

        def close(self):
            return None

    fake_db_module = types.SimpleNamespace(get_connection=lambda: _FakeConn())
    monkeypatch.setitem(sys.modules, "database", fake_db_module)
    monkeypatch.setattr(scraper_main, "PROJECT_ROOT", tmp_path)

    out_dir = tmp_path / "scraper" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "visited_history.csv").write_text(
        "profile_url,saved,visited_at,update_needed,last_db_update\n",
        encoding="utf-8",
    )
    (out_dir / "UNT_Alumni_Data.csv").write_text(
        "linkedin_url\n",
        encoding="utf-8",
    )

    flagged = out_dir / "flagged_for_review.txt"
    scraper_main._remove_dead_urls(["https://www.linkedin.com/in/john"], flagged, _DummyHistory())

    assert executed, "Expected at least one SQL statement"
    delete_query, params = executed[0]
    assert "LIKE" not in delete_query.upper()
    assert delete_query == "DELETE FROM alumni WHERE linkedin_url = %s OR linkedin_url = %s"
    assert params == ("https://www.linkedin.com/in/john", "https://www.linkedin.com/in/john/")


def test_remove_dead_urls_cleans_slash_variants_from_files(monkeypatch, tmp_path):
    class _FakeCursor:
        def execute(self, _query, _params):
            return None

    class _FakeConn:
        def __init__(self):
            self._cursor = _FakeCursor()

        def cursor(self):
            return self._cursor

        def commit(self):
            return None

        def close(self):
            return None

    fake_db_module = types.SimpleNamespace(get_connection=lambda: _FakeConn())
    monkeypatch.setitem(sys.modules, "database", fake_db_module)
    monkeypatch.setattr(scraper_main, "PROJECT_ROOT", tmp_path)

    out_dir = tmp_path / "scraper" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)

    flagged = out_dir / "flagged_for_review.txt"
    flagged.write_text(
        "\n".join(
            [
                "https://www.linkedin.com/in/remove-me # flagged",
                "https://www.linkedin.com/in/remove-me/",
                "https://www.linkedin.com/in/keep-me",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (out_dir / "visited_history.csv").write_text(
        "\n".join(
            [
                "profile_url,saved,visited_at,update_needed,last_db_update",
                "https://www.linkedin.com/in/remove-me,yes,2025-01-01,no,2025-01-01",
                "https://www.linkedin.com/in/keep-me,yes,2025-01-01,no,2025-01-01",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (out_dir / "UNT_Alumni_Data.csv").write_text(
        "\n".join(
            [
                "linkedin_url",
                "https://www.linkedin.com/in/remove-me/",
                "https://www.linkedin.com/in/keep-me",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    scraper_main._remove_dead_urls(["https://www.linkedin.com/in/remove-me"], flagged, _DummyHistory())

    flagged_after = flagged.read_text(encoding="utf-8")
    assert "remove-me" not in flagged_after
    assert "keep-me" in flagged_after

    visited_after = (out_dir / "visited_history.csv").read_text(encoding="utf-8")
    assert "remove-me" not in visited_after
    assert "keep-me" in visited_after

    alumni_after = (out_dir / "UNT_Alumni_Data.csv").read_text(encoding="utf-8")
    assert "remove-me" not in alumni_after
    assert "keep-me" in alumni_after
