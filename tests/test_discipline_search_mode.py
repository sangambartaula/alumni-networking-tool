import os
import sys
import types
import urllib.parse
from pathlib import Path

import pytest


project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "scraper"))
os.chdir(project_root)

import main as scraper_main


@pytest.fixture(autouse=True)
def _isolate_scrape_persistence(monkeypatch):
    # Keep discipline tests fully local and side-effect free.
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
    monkeypatch.setattr(scraper_main, "increment_scrape_run_profiles", lambda _run_id, _delta=1: True)
    monkeypatch.setattr(scraper_main, "_current_scrape_run_id", None)
    monkeypatch.setattr(scraper_main, "session_profiles_scraped", 0)


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


class _MainHistoryManager:
    def sync_with_db(self):
        return None


class _MainScraper:
    def __init__(self):
        self.driver = object()

    def setup_driver(self):
        return None

    def login(self):
        return True

    def quit(self):
        return None


class _TrackingHistory:
    def __init__(self):
        self.visited = set()
        self.marked = []
        self.skip_checks = []
        self.visited_history = {}

    def should_skip(self, url):
        normalized = scraper_main._normalize_profile_url(url)
        self.skip_checks.append(normalized)
        return normalized in self.visited

    def mark_as_visited(self, url, saved=False, update_needed=False):
        normalized = scraper_main._normalize_profile_url(url)
        self.visited.add(normalized)
        self.marked.append((normalized, saved, update_needed))
        self.visited_history[normalized] = {"saved": "yes"}
        return True

    def save_history_csv(self):
        return None


class _NoResultScraper:
    def __init__(self, batches):
        self.driver = types.SimpleNamespace(current_url=scraper_main.UNT_DISCIPLINE_SEARCH_BASE_URL)
        self._batches = list(batches)
        self.scraped_urls = []

    def scroll_full_page(self):
        return None

    def extract_profile_urls_from_page(self):
        if self._batches:
            return self._batches.pop(0)
        return []

    def scrape_profile_page(self, url):
        self.scraped_urls.append(url)
        return {
            "profile_url": url,
            "name": "Test Person",
        }


def _patch_minimal_cluster_groups(monkeypatch):
    monkeypatch.setattr(
        scraper_main,
        "DISCIPLINE_SEARCH_GROUPS",
        {
            "software": ["software cluster one", "software cluster two"],
            "mechanical": ["mechanical cluster one", "mechanical cluster two"],
            "embedded": ["embedded cluster one", "embedded cluster two"],
        },
    )
    monkeypatch.setattr(
        scraper_main,
        "DISCIPLINE_ALIAS_LABELS",
        {
            "software": "Software",
            "mechanical": "Mechanical",
            "embedded": "Embedded",
        },
    )
    monkeypatch.setattr(scraper_main, "load_discipline_rotation", lambda _alias: None)
    monkeypatch.setattr(scraper_main, "save_discipline_rotation", lambda *_args: None)
    monkeypatch.setattr(scraper_main, "load_keyword_state", lambda *_args: None)
    monkeypatch.setattr(scraper_main, "save_keyword_state", lambda *_args: None)


def _patch_main_entry_dependencies(monkeypatch):
    monkeypatch.setattr(scraper_main.database_handler, "HistoryManager", lambda: _MainHistoryManager())
    monkeypatch.setattr(scraper_main, "LinkedInScraper", lambda: _MainScraper())
    monkeypatch.setattr(scraper_main, "SafeNavigator", lambda _driver: object())
    monkeypatch.setattr(scraper_main, "start_exit_listener", lambda: None)
    monkeypatch.setattr(scraper_main, "stop_exit_listener", lambda: None)
    monkeypatch.setattr(scraper_main, "create_scrape_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(scraper_main, "finalize_scrape_run", lambda *args, **kwargs: True)
    monkeypatch.setattr(scraper_main.config, "SCRAPER_MODE", "search")


@pytest.mark.parametrize(
    "raw_value",
    ["__UNSET__", "", "   ", ",,,"],
)
def test_main_search_mode_uses_default_flow_when_disciplines_blank(monkeypatch, raw_value):
    _patch_main_entry_dependencies(monkeypatch)
    calls = {"search": 0, "discipline": 0, "submit": 0}

    if raw_value == "__UNSET__":
        monkeypatch.delattr(scraper_main.config, "SEARCH_DISCIPLINES", raising=False)
    else:
        monkeypatch.setattr(scraper_main.config, "SEARCH_DISCIPLINES", raw_value)

    monkeypatch.setattr(scraper_main, "run_search_mode", lambda *_args: calls.__setitem__("search", calls["search"] + 1))
    monkeypatch.setattr(
        scraper_main,
        "run_discipline_search_mode",
        lambda *_args: calls.__setitem__("discipline", calls["discipline"] + 1),
    )
    monkeypatch.setattr(
        scraper_main,
        "_submit_discipline_keywords",
        lambda *_args: calls.__setitem__("submit", calls["submit"] + 1) or True,
    )

    scraper_main.main()

    assert calls["search"] == 1
    assert calls["discipline"] == 0
    assert calls["submit"] == 0


def test_main_search_mode_falls_back_when_all_disciplines_invalid(monkeypatch):
    _patch_main_entry_dependencies(monkeypatch)
    calls = {"search": 0, "discipline": 0}
    monkeypatch.setattr(scraper_main.config, "SEARCH_DISCIPLINES", "banana,blah")
    monkeypatch.setattr(scraper_main, "run_search_mode", lambda *_args: calls.__setitem__("search", calls["search"] + 1))
    monkeypatch.setattr(
        scraper_main,
        "run_discipline_search_mode",
        lambda *_args: calls.__setitem__("discipline", calls["discipline"] + 1),
    )

    scraper_main.main()

    assert calls["search"] == 1
    assert calls["discipline"] == 0


def test_parse_search_disciplines_keeps_valid_and_warns_for_invalid(monkeypatch):
    warnings = []
    monkeypatch.setattr(scraper_main.logger, "warning", lambda msg: warnings.append(str(msg)))

    selected = scraper_main._parse_search_disciplines("software,banana,mechanical,blah")

    assert selected == ["software", "mechanical"]
    assert any("banana" in msg for msg in warnings)
    assert any("blah" in msg for msg in warnings)


def test_parse_search_disciplines_case_insensitive_trim_and_dedup():
    selected = scraper_main._parse_search_disciplines(" SOFTWARE , mechanical , software ")
    assert selected == ["software", "mechanical"]


def test_run_discipline_search_mode_processes_selected_aliases_in_order(monkeypatch):
    _patch_minimal_cluster_groups(monkeypatch)
    nav = _DummyNav(ok=True)
    history = _DummyHistory()
    scraper = types.SimpleNamespace(driver=types.SimpleNamespace(current_url=scraper_main.UNT_DISCIPLINE_SEARCH_BASE_URL))
    submit_calls = []
    run_calls = []

    def _fake_submit(scraper_obj, keyword_query):
        submit_calls.append(keyword_query)
        scraper_obj.driver.current_url = (
            "https://www.linkedin.com/search/results/people/"
            f"?schoolFilter=%5B%226464%22%5D&keywords={urllib.parse.quote_plus(keyword_query)}&sid=temp123"
        )
        return True

    def _fake_run(*, scraper, nav, history_mgr, base_url, state_mode_key, mode_label, max_profiles_for_mode):
        run_calls.append(
            {
                "base_url": base_url,
                "state_mode_key": state_mode_key,
                "mode_label": mode_label,
                "max_profiles_for_mode": max_profiles_for_mode,
            }
        )
        return "threshold_reached", max_profiles_for_mode

    monkeypatch.setattr(scraper_main, "_submit_discipline_keywords", _fake_submit)
    monkeypatch.setattr(scraper_main, "_run_search_results_mode", _fake_run)
    monkeypatch.setattr(scraper_main.time, "sleep", lambda _s: None)

    scraper_main.run_discipline_search_mode(scraper, nav, history, ["software", "mechanical"])

    assert submit_calls == [
        "software cluster one",
        "mechanical cluster one",
    ]
    assert nav.urls == [
        scraper_main.UNT_DISCIPLINE_SEARCH_BASE_URL,
        scraper_main.UNT_DISCIPLINE_SEARCH_BASE_URL,
    ]
    assert [call["state_mode_key"] for call in run_calls] == [
        "discipline:software:cluster_1",
        "discipline:mechanical:cluster_1",
    ]
    assert all("sid=" not in call["base_url"] for call in run_calls)
    assert run_calls[0]["base_url"] != run_calls[1]["base_url"]


def test_submit_discipline_keywords_clicks_clears_types_and_enters(monkeypatch):
    class _FakeInput:
        def __init__(self, driver):
            self.driver = driver
            self.actions = []

        def click(self):
            self.actions.append("click")

        def send_keys(self, *keys):
            self.actions.append(keys)
            if keys == (scraper_main.Keys.ENTER,):
                self.driver.current_url = (
                    "https://www.linkedin.com/search/results/people/"
                    "?schoolFilter=%5B%226464%22%5D&keywords=software"
                )

    class _FakeDriver:
        def __init__(self):
            self.current_url = scraper_main.UNT_DISCIPLINE_SEARCH_BASE_URL

        def execute_script(self, *_args, **_kwargs):
            return True

    driver = _FakeDriver()
    fake_input = _FakeInput(driver)
    scraper = types.SimpleNamespace(driver=driver)

    monkeypatch.setattr(scraper_main, "_find_visible_people_search_input", lambda _scraper: fake_input)
    monkeypatch.setattr(scraper_main.time, "sleep", lambda _s: None)
    monkeypatch.setattr(scraper_main.random, "uniform", lambda _a, _b: 0.01)

    ok = scraper_main._submit_discipline_keywords(scraper, "software engineering")

    assert ok is True
    assert fake_input.actions[0] == "click"
    assert (scraper_main.Keys.CONTROL, "a") in fake_input.actions
    assert (scraper_main.Keys.DELETE,) in fake_input.actions
    assert ("software engineering",) in fake_input.actions
    assert (scraper_main.Keys.ENTER,) in fake_input.actions


def test_submit_discipline_keywords_uses_command_on_macos(monkeypatch):
    class _FakeInput:
        def __init__(self, driver):
            self.driver = driver
            self.actions = []

        def click(self):
            self.actions.append("click")

        def send_keys(self, *keys):
            self.actions.append(keys)
            if keys == (scraper_main.Keys.ENTER,):
                self.driver.current_url = (
                    "https://www.linkedin.com/search/results/people/"
                    "?schoolFilter=%5B%226464%22%5D&keywords=software"
                )

    class _FakeDriver:
        def __init__(self):
            self.current_url = scraper_main.UNT_DISCIPLINE_SEARCH_BASE_URL

        def execute_script(self, *_args, **_kwargs):
            return True

    driver = _FakeDriver()
    fake_input = _FakeInput(driver)
    scraper = types.SimpleNamespace(driver=driver)

    monkeypatch.setattr(scraper_main, "_find_visible_people_search_input", lambda _scraper: fake_input)
    monkeypatch.setattr(scraper_main.time, "sleep", lambda _s: None)
    monkeypatch.setattr(scraper_main.random, "uniform", lambda _a, _b: 0.01)
    monkeypatch.setattr(scraper_main.sys, "platform", "darwin")

    ok = scraper_main._submit_discipline_keywords(scraper, "software engineering")

    assert ok is True
    assert (scraper_main.Keys.COMMAND, "a") in fake_input.actions
    assert (scraper_main.Keys.CONTROL, "a") not in fake_input.actions


def test_find_visible_people_search_input_uses_js_fallback(monkeypatch):
    fake_input = object()

    class _FakeDriver:
        def find_elements(self, *_args):
            return []

        def execute_script(self, script):
            assert "document.querySelectorAll('input, textarea')" in script
            return fake_input

    monkeypatch.setattr(scraper_main.time, "sleep", lambda _s: None)

    scraper = types.SimpleNamespace(driver=_FakeDriver())

    found = scraper_main._find_visible_people_search_input(scraper, timeout_seconds=0.1)

    assert found is fake_input


def test_submit_discipline_keywords_returns_false_when_search_bar_missing(monkeypatch):
    scraper = types.SimpleNamespace(driver=types.SimpleNamespace(current_url=scraper_main.UNT_DISCIPLINE_SEARCH_BASE_URL))
    monkeypatch.setattr(scraper_main, "_find_visible_people_search_input", lambda _scraper: None)
    assert scraper_main._submit_discipline_keywords(scraper, "software") is False


def test_submit_discipline_keywords_returns_false_when_submission_errors(monkeypatch):
    class _BrokenInput:
        def click(self):
            return None

        def send_keys(self, *_args):
            raise RuntimeError("send failed")

    scraper = types.SimpleNamespace(driver=types.SimpleNamespace(current_url=scraper_main.UNT_DISCIPLINE_SEARCH_BASE_URL))
    monkeypatch.setattr(scraper_main, "_find_visible_people_search_input", lambda _scraper: _BrokenInput())
    monkeypatch.setattr(scraper_main.time, "sleep", lambda _s: None)
    monkeypatch.setattr(scraper_main.random, "uniform", lambda _a, _b: 0.01)
    assert scraper_main._submit_discipline_keywords(scraper, "software") is False


def test_run_discipline_search_mode_no_results_continues_to_next_discipline(monkeypatch):
    _patch_minimal_cluster_groups(monkeypatch)
    nav = _DummyNav(ok=True)
    history = _DummyHistory()
    scraper = _NoResultScraper(batches=[[], []])
    submit_calls = []

    def _fake_submit(scraper_obj, keyword_query):
        submit_calls.append(keyword_query)
        scraper_obj.driver.current_url = (
            "https://www.linkedin.com/search/results/people/"
            f"?schoolFilter=%5B%226464%22%5D&keywords={urllib.parse.quote_plus(keyword_query)}&sid=s123"
        )
        return True

    monkeypatch.setattr(scraper_main, "_submit_discipline_keywords", _fake_submit)
    monkeypatch.setattr(
        scraper_main,
        "_run_search_results_mode",
        lambda **_kwargs: ("no_results", 0),
    )
    monkeypatch.setattr(scraper_main.time, "sleep", lambda _s: None)

    scraper_main.run_discipline_search_mode(scraper, nav, history, ["software", "mechanical"])

    assert submit_calls == [
        "software cluster one",
        "software cluster two",
        "mechanical cluster one",
        "mechanical cluster two",
    ]
    assert scraper.scraped_urls == []


def test_save_and_track_marks_visited_only_after_success(monkeypatch):
    history = _TrackingHistory()
    data = {
        "name": "Test Person",
        "profile_url": "https://www.linkedin.com/in/john-doe/?trk=abc",
    }
    monkeypatch.setattr(scraper_main.database_handler, "save_profile_to_csv", lambda _data: True)
    monkeypatch.setattr(scraper_main, "increment_scraper_activity", lambda _email: None)

    ok = scraper_main._save_and_track(data, "https://www.linkedin.com/in/john-doe?miniProfileUrn=xyz", history)

    assert ok is True
    assert len(history.marked) == 1
    assert history.marked[0][0] == "https://www.linkedin.com/in/john-doe"


def test_save_and_track_does_not_mark_visited_when_save_fails(monkeypatch):
    history = _TrackingHistory()
    data = {
        "name": "Test Person",
        "profile_url": "https://www.linkedin.com/in/john-doe",
    }
    monkeypatch.setattr(scraper_main.database_handler, "save_profile_to_csv", lambda _data: False)
    monkeypatch.setattr(scraper_main, "increment_scraper_activity", lambda _email: None)

    ok = scraper_main._save_and_track(data, "https://www.linkedin.com/in/john-doe", history)

    assert ok is False
    assert history.marked == []


@pytest.mark.parametrize("data", [None, "PAGE_NOT_FOUND"])
def test_save_and_track_does_not_mark_visited_for_invalid_data(monkeypatch, data):
    history = _TrackingHistory()
    monkeypatch.setattr(scraper_main.database_handler, "save_profile_to_csv", lambda _data: True)
    ok = scraper_main._save_and_track(data, "https://www.linkedin.com/in/john-doe", history)
    assert ok is False
    assert history.marked == []


def test_normalize_profile_url_collapses_query_and_slash_variants():
    variants = [
        "https://www.linkedin.com/in/john-doe",
        "https://www.linkedin.com/in/john-doe/",
        "https://www.linkedin.com/in/john-doe?trk=abc",
        "https://www.linkedin.com/in/john-doe?miniProfileUrn=xyz",
    ]
    normalized = {scraper_main._normalize_profile_url(url) for url in variants}
    assert normalized == {"https://www.linkedin.com/in/john-doe"}


def test_same_profile_across_disciplines_processed_once_after_live_visit_update(monkeypatch):
    _patch_minimal_cluster_groups(monkeypatch)
    profile_url = "https://www.linkedin.com/in/same-person?trk=foo"
    scraper = _NoResultScraper(
        batches=[
            [profile_url],
            [],
            [profile_url],
            [],
        ]
    )
    nav = _DummyNav(ok=True)
    history = _TrackingHistory()

    def _fake_submit(scraper_obj, keyword_query):
        scraper_obj.driver.current_url = (
            "https://www.linkedin.com/search/results/people/"
            f"?schoolFilter=%5B%226464%22%5D&keywords={urllib.parse.quote_plus(keyword_query)}"
        )
        return True

    monkeypatch.setattr(scraper_main, "_submit_discipline_keywords", _fake_submit)
    monkeypatch.setattr(scraper_main.database_handler, "save_profile_to_csv", lambda _data: True)
    monkeypatch.setattr(scraper_main, "increment_scraper_activity", lambda _email: None)
    monkeypatch.setattr(scraper_main, "wait_between_profiles", lambda: None)
    monkeypatch.setattr(scraper_main.time, "sleep", lambda _s: None)

    scraper_main.run_discipline_search_mode(scraper, nav, history, ["software", "embedded"])

    assert scraper.scraped_urls == ["https://www.linkedin.com/in/same-person"]
    assert len(history.marked) == 1
    assert history.skip_checks.count("https://www.linkedin.com/in/same-person") >= 2


def test_run_discipline_search_mode_falls_back_to_url_when_submission_fails(monkeypatch):
    _patch_minimal_cluster_groups(monkeypatch)
    nav = _DummyNav(ok=True)
    history = _DummyHistory()
    scraper = types.SimpleNamespace(driver=types.SimpleNamespace(current_url=scraper_main.UNT_DISCIPLINE_SEARCH_BASE_URL))
    run_calls = []
    submit_outcomes = iter([False, True])

    def _fake_submit(scraper_obj, keyword_query):
        outcome = next(submit_outcomes)
        if outcome:
            scraper_obj.driver.current_url = (
                "https://www.linkedin.com/search/results/people/"
                f"?schoolFilter=%5B%226464%22%5D&keywords={urllib.parse.quote_plus(keyword_query)}"
            )
        return outcome

    monkeypatch.setattr(scraper_main, "_submit_discipline_keywords", _fake_submit)
    monkeypatch.setattr(
        scraper_main,
        "_run_search_results_mode",
        lambda **kwargs: run_calls.append(kwargs) or ("threshold_reached", kwargs["max_profiles_for_mode"]),
    )
    monkeypatch.setattr(scraper_main.time, "sleep", lambda _s: None)

    scraper_main.run_discipline_search_mode(scraper, nav, history, ["software", "mechanical"])

    assert len(run_calls) == 2
    assert run_calls[0]["state_mode_key"] == "discipline:software:cluster_1"
    assert run_calls[1]["state_mode_key"] == "discipline:mechanical:cluster_1"
    assert "keywords=software+cluster+one" in run_calls[0]["base_url"]
