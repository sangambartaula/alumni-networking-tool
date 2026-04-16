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


def test_canonicalize_redirect_url_deletes_old_url_and_marks_alias(monkeypatch, tmp_path):
    executed = []

    class _FakeCursor:
        def __init__(self):
            self.rowcount = 1

        def execute(self, query, params):
            executed.append((query, params))

        def close(self):
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

    class _History:
        def __init__(self):
            self.calls = []

        def mark_as_visited(self, url, saved=False):
            self.calls.append((url, saved))

    out_dir = tmp_path / "scraper" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(scraper_main, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr("backend.database.get_connection", lambda: _FakeConn())

    history = _History()
    scraper_main._canonicalize_redirect_url(
        "https://www.linkedin.com/in/old-user",
        "https://www.linkedin.com/in/new-user",
        history,
    )

    assert executed
    query, params = executed[0]
    assert query == "DELETE FROM alumni WHERE linkedin_url = %s OR linkedin_url = %s"
    assert params == (
        "https://www.linkedin.com/in/old-user",
        "https://www.linkedin.com/in/old-user/",
    )
    assert history.calls == [("https://www.linkedin.com/in/old-user", True)]


def test_parse_search_disciplines_handles_case_whitespace_and_duplicates():
    selected = scraper_main._parse_search_disciplines(" software , MECHANICAL,software ")
    assert selected == ["software", "mechanical"]


def test_get_selected_search_disciplines_all_invalid_falls_back(monkeypatch):
    monkeypatch.setattr(scraper_main.config, "SEARCH_DISCIPLINES", "invalid,unknown")
    selected = scraper_main._get_selected_search_disciplines()
    assert selected == []


def test_canonicalize_search_base_url_strips_sid_and_page():
    raw_url = (
        "https://www.linkedin.com/search/results/people/"
        "?schoolFilter=%5B%226464%22%5D&keywords=software&page=4&sid=abc123"
    )
    normalized = scraper_main._canonicalize_search_base_url(
        raw_url,
        scraper_main.UNT_DISCIPLINE_SEARCH_BASE_URL,
    )
    assert "sid=" not in normalized
    assert "page=" not in normalized
    assert "keywords=software" in normalized


def test_save_and_track_increments_scraper_activity_with_config_email(monkeypatch):
    calls = []
    upsert_calls = []

    class _History:
        def should_skip(self, _url):
            return False

        def mark_as_visited(self, _url, saved=False):
            return None

    monkeypatch.setattr(scraper_main.database_handler, "save_profile_to_csv", lambda _data: True)
    monkeypatch.setattr(
        scraper_main,
        "upsert_scraped_profile",
        lambda data, allow_cloud=True, run_id=None: upsert_calls.append((data.get("profile_url"), allow_cloud)) or {
            "cloud_attempted": True,
            "cloud_written": True,
            "sqlite_written": True,
        },
    )
    monkeypatch.setattr(scraper_main, "increment_scraper_activity", lambda email: calls.append(email))
    monkeypatch.setattr(scraper_main.config, "LINKEDIN_EMAIL", "scraper@unt.edu")

    ok = scraper_main._save_and_track(
        {"profile_url": "https://www.linkedin.com/in/test-user"},
        "https://www.linkedin.com/in/test-user",
        _History(),
    )

    assert ok is True
    assert upsert_calls == [("https://www.linkedin.com/in/test-user", True)]
    assert calls == ["scraper@unt.edu"]


def test_save_and_track_passes_blank_email_through_to_increment(monkeypatch):
    calls = []
    upsert_calls = []

    class _History:
        def should_skip(self, _url):
            return False

        def mark_as_visited(self, _url, saved=False):
            return None

    monkeypatch.setattr(scraper_main.database_handler, "save_profile_to_csv", lambda _data: True)
    monkeypatch.setattr(
        scraper_main,
        "upsert_scraped_profile",
        lambda data, allow_cloud=True, run_id=None: upsert_calls.append((data.get("profile_url"), allow_cloud)) or {
            "cloud_attempted": True,
            "cloud_written": True,
            "sqlite_written": True,
        },
    )
    monkeypatch.setattr(scraper_main, "increment_scraper_activity", lambda email: calls.append(email))
    monkeypatch.setattr(scraper_main.config, "LINKEDIN_EMAIL", "")

    ok = scraper_main._save_and_track(
        {"profile_url": "https://www.linkedin.com/in/test-user"},
        "https://www.linkedin.com/in/test-user",
        _History(),
    )

    assert ok is True
    assert upsert_calls == [("https://www.linkedin.com/in/test-user", True)]
    assert calls == [""]


def test_save_and_track_continues_when_upsert_fails(monkeypatch):
    calls = []

    class _History:
        def should_skip(self, _url):
            return False

        def mark_as_visited(self, _url, saved=False):
            return None

    monkeypatch.setattr(scraper_main.database_handler, "save_profile_to_csv", lambda _data: True)
    monkeypatch.setattr(
        scraper_main,
        "upsert_scraped_profile",
        lambda _data, allow_cloud=True, run_id=None: (_ for _ in ()).throw(RuntimeError("db down")),
    )
    monkeypatch.setattr(scraper_main, "increment_scraper_activity", lambda email: calls.append(email))
    monkeypatch.setattr(scraper_main.config, "LINKEDIN_EMAIL", "scraper@unt.edu")

    ok = scraper_main._save_and_track(
        {"profile_url": "https://www.linkedin.com/in/test-user"},
        "https://www.linkedin.com/in/test-user",
        _History(),
    )

    # Save remains successful because CSV is authoritative backup path.
    assert ok is True
    assert calls == ["scraper@unt.edu"]


def test_save_and_track_disables_cloud_after_five_consecutive_failures(monkeypatch):
    class _History:
        def should_skip(self, _url):
            return False

        def mark_as_visited(self, _url, saved=False):
            return None

    monkeypatch.setattr(scraper_main.database_handler, "save_profile_to_csv", lambda _data: True)
    monkeypatch.setattr(
        scraper_main,
        "upsert_scraped_profile",
        lambda _data, allow_cloud=True, run_id=None: {
            "cloud_attempted": allow_cloud,
            "cloud_written": False,
            "sqlite_written": True,
        },
    )
    monkeypatch.setattr(scraper_main, "increment_scraper_activity", lambda _email: None)
    monkeypatch.setattr(scraper_main.config, "LINKEDIN_EMAIL", "scraper@unt.edu")
    monkeypatch.setattr(scraper_main, "_cloud_upsert_consecutive_failures", 0)
    monkeypatch.setattr(scraper_main, "_cloud_upsert_disabled_for_run", False)

    for _ in range(5):
        ok = scraper_main._save_and_track(
            {"profile_url": "https://www.linkedin.com/in/test-user"},
            "https://www.linkedin.com/in/test-user",
            _History(),
        )
        assert ok is True

    assert scraper_main._cloud_upsert_disabled_for_run is True


def test_save_and_track_uses_sqlite_only_after_cloud_disabled(monkeypatch):
    cloud_flags = []

    class _History:
        def should_skip(self, _url):
            return False

        def mark_as_visited(self, _url, saved=False):
            return None

    monkeypatch.setattr(scraper_main.database_handler, "save_profile_to_csv", lambda _data: True)

    def _fake_upsert(_data, allow_cloud=True, run_id=None):
        cloud_flags.append(allow_cloud)
        return {
            "cloud_attempted": allow_cloud,
            "cloud_written": False,
            "sqlite_written": True,
        }

    monkeypatch.setattr(scraper_main, "upsert_scraped_profile", _fake_upsert)
    monkeypatch.setattr(scraper_main, "increment_scraper_activity", lambda _email: None)
    monkeypatch.setattr(scraper_main.config, "LINKEDIN_EMAIL", "scraper@unt.edu")
    monkeypatch.setattr(scraper_main, "_cloud_upsert_consecutive_failures", 5)
    monkeypatch.setattr(scraper_main, "_cloud_upsert_disabled_for_run", True)

    ok = scraper_main._save_and_track(
        {"profile_url": "https://www.linkedin.com/in/test-user"},
        "https://www.linkedin.com/in/test-user",
        _History(),
    )

    assert ok is True
    assert cloud_flags == [False]


def test_save_and_track_records_geocode_miss_without_crashing(monkeypatch):
    class _History:
        def should_skip(self, _url):
            return False

        def mark_as_visited(self, _url, saved=False):
            return None

    monkeypatch.setattr(scraper_main.database_handler, "save_profile_to_csv", lambda _data: True)
    monkeypatch.setattr(
        scraper_main,
        "upsert_scraped_profile",
        lambda _data, allow_cloud=True, run_id=None: {
            "cloud_attempted": allow_cloud,
            "cloud_written": True,
            "sqlite_written": True,
        },
    )
    monkeypatch.setattr(scraper_main, "increment_scraper_activity", lambda _email: None)
    monkeypatch.setattr(scraper_main, "geocode_location_with_status", lambda _location: (None, "unknown_location"))
    monkeypatch.setattr(scraper_main.config, "LINKEDIN_EMAIL", "scraper@unt.edu")
    monkeypatch.setattr(scraper_main, "_geocode_failures_this_run", 0)
    monkeypatch.setattr(scraper_main, "_geocode_failure_locations", set())
    monkeypatch.setattr(scraper_main, "_geocode_network_failures_this_run", 0)

    ok = scraper_main._save_and_track(
        {"profile_url": "https://www.linkedin.com/in/test-user", "location": "Eastern Region"},
        "https://www.linkedin.com/in/test-user",
        _History(),
    )

    assert ok is True
    assert scraper_main._geocode_failures_this_run == 1
    assert scraper_main._geocode_network_failures_this_run == 0
    assert "Eastern Region" in scraper_main._geocode_failure_locations


def test_save_and_track_retries_geocode_with_groq_normalized_location(monkeypatch):
    class _History:
        def should_skip(self, _url):
            return False

        def mark_as_visited(self, _url, saved=False):
            return None

    monkeypatch.setattr(scraper_main.database_handler, "save_profile_to_csv", lambda _data: True)
    monkeypatch.setattr(
        scraper_main,
        "upsert_scraped_profile",
        lambda _data, allow_cloud=True, run_id=None: {
            "cloud_attempted": allow_cloud,
            "cloud_written": True,
            "sqlite_written": True,
        },
    )
    monkeypatch.setattr(scraper_main, "increment_scraper_activity", lambda _email: None)
    monkeypatch.setattr(scraper_main.config, "LINKEDIN_EMAIL", "scraper@unt.edu")
    monkeypatch.setattr(scraper_main, "_geocode_failures_this_run", 0)
    monkeypatch.setattr(scraper_main, "_geocode_failure_locations", set())
    monkeypatch.setattr(scraper_main, "_geocode_network_failures_this_run", 0)
    monkeypatch.setattr(scraper_main, "_geocode_success_this_run", 0)
    monkeypatch.setattr(
        scraper_main,
        "_normalize_location_for_geocoding",
        lambda location: "Austin, Texas, United States",
    )

    calls = {"count": 0}

    def _fake_geocode(location):
        calls["count"] += 1
        if calls["count"] == 1:
            return None, "unknown_location"
        assert location == "Austin, Texas, United States"
        return (30.2672, -97.7431), "ok"

    monkeypatch.setattr(scraper_main, "geocode_location_with_status", _fake_geocode)

    payload = {
        "profile_url": "https://www.linkedin.com/in/test-user",
        "location": "Austin, Texas Metropolitan Area",
    }
    ok = scraper_main._save_and_track(
        payload,
        "https://www.linkedin.com/in/test-user",
        _History(),
    )

    assert ok is True
    assert payload["location"] == "Austin, Texas, United States"
    assert payload["latitude"] == 30.2672
    assert payload["longitude"] == -97.7431
    assert scraper_main._geocode_success_this_run == 1


def test_save_and_track_clears_location_when_groq_returns_unknown(monkeypatch):
    class _History:
        def should_skip(self, _url):
            return False

        def mark_as_visited(self, _url, saved=False):
            return None

    monkeypatch.setattr(scraper_main.database_handler, "save_profile_to_csv", lambda _data: True)
    monkeypatch.setattr(
        scraper_main,
        "upsert_scraped_profile",
        lambda _data, allow_cloud=True, run_id=None: {
            "cloud_attempted": allow_cloud,
            "cloud_written": True,
            "sqlite_written": True,
        },
    )
    monkeypatch.setattr(scraper_main, "increment_scraper_activity", lambda _email: None)
    monkeypatch.setattr(scraper_main.config, "LINKEDIN_EMAIL", "scraper@unt.edu")
    monkeypatch.setattr(scraper_main, "_geocode_failures_this_run", 0)
    monkeypatch.setattr(scraper_main, "_geocode_failure_locations", set())
    monkeypatch.setattr(scraper_main, "_geocode_network_failures_this_run", 0)
    monkeypatch.setattr(scraper_main, "_geocode_success_this_run", 0)
    monkeypatch.setattr(scraper_main, "_normalize_location_for_geocoding", lambda _location: "unknown")
    monkeypatch.setattr(scraper_main, "geocode_location_with_status", lambda _location: (None, "unknown_location"))

    payload = {
        "profile_url": "https://www.linkedin.com/in/test-user",
        "location": "Eastern Region",
    }
    ok = scraper_main._save_and_track(
        payload,
        "https://www.linkedin.com/in/test-user",
        _History(),
    )

    assert ok is True
    assert payload["location"] is None
    assert scraper_main._geocode_failures_this_run == 1
    assert "Eastern Region" in scraper_main._geocode_failure_locations


def test_save_and_track_records_geocode_exception_without_crashing(monkeypatch):
    class _History:
        def should_skip(self, _url):
            return False

        def mark_as_visited(self, _url, saved=False):
            return None

    monkeypatch.setattr(scraper_main.database_handler, "save_profile_to_csv", lambda _data: True)
    monkeypatch.setattr(
        scraper_main,
        "upsert_scraped_profile",
        lambda _data, allow_cloud=True, run_id=None: {
            "cloud_attempted": allow_cloud,
            "cloud_written": True,
            "sqlite_written": True,
        },
    )
    monkeypatch.setattr(scraper_main, "increment_scraper_activity", lambda _email: None)
    monkeypatch.setattr(
        scraper_main,
        "geocode_location_with_status",
        lambda _location: (_ for _ in ()).throw(RuntimeError("geocode unavailable")),
    )
    monkeypatch.setattr(scraper_main.config, "LINKEDIN_EMAIL", "scraper@unt.edu")
    monkeypatch.setattr(scraper_main, "_geocode_failures_this_run", 0)
    monkeypatch.setattr(scraper_main, "_geocode_failure_locations", set())
    monkeypatch.setattr(scraper_main, "_geocode_network_failures_this_run", 0)

    ok = scraper_main._save_and_track(
        {"profile_url": "https://www.linkedin.com/in/test-user", "location": "Eastern Region"},
        "https://www.linkedin.com/in/test-user",
        _History(),
    )

    assert ok is True
    assert scraper_main._geocode_failures_this_run == 0
    assert scraper_main._geocode_network_failures_this_run == 1
    assert "Eastern Region" not in scraper_main._geocode_failure_locations


def test_save_and_track_not_found_location_emits_warning_and_flags(monkeypatch):
    warnings = []
    run_flags = []

    class _History:
        def should_skip(self, _url):
            return False

        def mark_as_visited(self, _url, saved=False):
            return None

    monkeypatch.setattr(scraper_main.database_handler, "save_profile_to_csv", lambda _data: True)
    monkeypatch.setattr(
        scraper_main,
        "upsert_scraped_profile",
        lambda _data, allow_cloud=True, run_id=None: {
            "cloud_attempted": allow_cloud,
            "cloud_written": True,
            "sqlite_written": True,
        },
    )
    monkeypatch.setattr(scraper_main, "increment_scraper_activity", lambda _email: None)
    monkeypatch.setattr(scraper_main.config, "LINKEDIN_EMAIL", "scraper@unt.edu")
    monkeypatch.setattr(scraper_main, "_current_scrape_run_id", 99)
    monkeypatch.setattr(scraper_main, "_flagged_urls_this_run", set())
    monkeypatch.setattr(scraper_main, "_emit_not_found_location_warning", lambda profile_url, location: warnings.append((profile_url, location)))
    monkeypatch.setattr(scraper_main, "_append_flagged_review_line", lambda _url, _reason: True)
    monkeypatch.setattr(
        scraper_main,
        "record_scrape_run_flag",
        lambda run_id, linkedin_url, reason: run_flags.append((run_id, linkedin_url, reason)) or True,
    )
    monkeypatch.setattr(scraper_main, "geocode_location_with_status", lambda _location: ((32.0, -97.0), "ok"))

    payload = {
        "profile_url": "https://www.linkedin.com/in/test-user",
        "location": "Not Found",
    }
    ok = scraper_main._save_and_track(
        payload,
        "https://www.linkedin.com/in/test-user",
        _History(),
    )

    assert ok is True
    assert warnings == [("https://www.linkedin.com/in/test-user", "Not Found")]
    assert run_flags == [(99, "https://www.linkedin.com/in/test-user", "Location Not Found")]
    assert "https://www.linkedin.com/in/test-user" in scraper_main._flagged_urls_this_run


def test_save_and_track_propagates_run_id_and_records_flags(monkeypatch):
    upsert_calls = []
    flag_calls = []

    class _History:
        def should_skip(self, _url):
            return False

        def mark_as_visited(self, _url, saved=False):
            return None

    monkeypatch.setattr(scraper_main.database_handler, "save_profile_to_csv", lambda _data: True)

    def _fake_upsert(data, allow_cloud=True, run_id=None):
        upsert_calls.append((data.get("profile_url"), allow_cloud, run_id))
        return {
            "cloud_attempted": allow_cloud,
            "cloud_written": True,
            "sqlite_written": True,
        }

    monkeypatch.setattr(scraper_main, "upsert_scraped_profile", _fake_upsert)
    monkeypatch.setattr(scraper_main, "increment_scraper_activity", lambda _email: None)
    monkeypatch.setattr(scraper_main, "_collect_profile_flag_reasons", lambda _data: ["Missing Grad Year"])
    monkeypatch.setattr(
        scraper_main,
        "record_scrape_run_flag",
        lambda run_id, linkedin_url, reason: flag_calls.append((run_id, linkedin_url, reason)) or True,
    )
    monkeypatch.setattr(scraper_main, "_current_scrape_run_id", 42)
    monkeypatch.setattr(scraper_main.config, "LINKEDIN_EMAIL", "scraper@unt.edu")
    monkeypatch.setattr(scraper_main, "session_profiles_scraped", 0)

    ok = scraper_main._save_and_track(
        {"profile_url": "https://www.linkedin.com/in/test-user"},
        "https://www.linkedin.com/in/test-user",
        _History(),
    )

    assert ok is True
    assert upsert_calls == [("https://www.linkedin.com/in/test-user", True, 42)]
    assert flag_calls == [(42, "https://www.linkedin.com/in/test-user", "Missing Grad Year")]


def test_save_and_track_truncates_oversized_education_fields_for_cloud(monkeypatch):
    captured = {}

    class _History:
        def should_skip(self, _url):
            return False

        def mark_as_visited(self, _url, saved=False):
            return None

    monkeypatch.setattr(scraper_main.database_handler, "save_profile_to_csv", lambda _data: True)

    def _fake_upsert(data, allow_cloud=True, run_id=None):
        captured["data"] = data
        return {
            "cloud_attempted": allow_cloud,
            "cloud_written": True,
            "sqlite_written": True,
        }

    monkeypatch.setattr(scraper_main, "upsert_scraped_profile", _fake_upsert)
    monkeypatch.setattr(scraper_main, "increment_scraper_activity", lambda _email: None)
    monkeypatch.setattr(scraper_main.config, "LINKEDIN_EMAIL", "scraper@unt.edu")

    long_text = "X" * 400
    ok = scraper_main._save_and_track(
        {
            "profile_url": "https://www.linkedin.com/in/test-user",
            "school3": long_text,
            "major3": long_text,
        },
        "https://www.linkedin.com/in/test-user",
        _History(),
    )

    assert ok is True
    assert len(captured["data"]["school3"]) == 255
    assert len(captured["data"]["major3"]) == 255


def test_format_linkedin_keyword_query_uses_comma_separated_terms():
    formatted = scraper_main._format_linkedin_keyword_query(
        ' computer science,  software engineer , , data science '
    )
    assert formatted == "computer science, software engineer, data science"


def test_build_discipline_search_base_url_keeps_comma_keyword_format():
    keyword_query = scraper_main._format_linkedin_keyword_query(
        "computer science, software developer, machine learning"
    )
    url = scraper_main._build_discipline_search_base_url(
        scraper_main.UNT_DISCIPLINE_SEARCH_BASE_URL,
        keyword_query,
    )
    assert "keywords=computer+science%2C+software+developer%2C+machine+learning" in url
    keywords_value = url.split("keywords=", 1)[1]
    assert "%22" not in keywords_value
    assert "+OR+" not in keywords_value
