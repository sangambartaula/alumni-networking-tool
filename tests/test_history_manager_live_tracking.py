import os
import sys
from pathlib import Path


project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "scraper"))
os.chdir(project_root)

import database_handler


def _build_history_manager(monkeypatch, tmp_path):
    visited_csv = tmp_path / "visited_history.csv"
    monkeypatch.setattr(database_handler, "VISITED_HISTORY_FILE", visited_csv)
    return database_handler.HistoryManager()


def test_mark_as_visited_attempts_live_db_persistence_immediately(monkeypatch, tmp_path):
    calls = []

    def _fake_save_visited_profile(url, is_unt_alum=False, notes=None):
        calls.append((url, is_unt_alum, notes))
        return True

    monkeypatch.setattr(database_handler, "save_visited_profile", _fake_save_visited_profile)
    history = _build_history_manager(monkeypatch, tmp_path)

    ok = history.mark_as_visited("https://www.linkedin.com/in/john-doe?trk=abc", saved=True)

    assert ok is True
    assert calls == [("https://www.linkedin.com/in/john-doe", True, None)]
    assert "https://www.linkedin.com/in/john-doe" in history.visited_history


def test_mark_as_visited_logs_warning_when_db_persistence_returns_false(monkeypatch, tmp_path):
    warnings = []
    monkeypatch.setattr(database_handler, "save_visited_profile", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(database_handler.logger, "warning", lambda msg: warnings.append(str(msg)))
    history = _build_history_manager(monkeypatch, tmp_path)

    ok = history.mark_as_visited("https://www.linkedin.com/in/john-doe?miniProfileUrn=xyz", saved=True)

    assert ok is False
    assert any("Could not persist visited profile to DB immediately" in msg for msg in warnings)
    assert "https://www.linkedin.com/in/john-doe" in history.visited_history


def test_mark_as_visited_logs_warning_when_db_persistence_raises(monkeypatch, tmp_path):
    warnings = []

    def _raise_on_save(*_args, **_kwargs):
        raise RuntimeError("db is offline")

    monkeypatch.setattr(database_handler, "save_visited_profile", _raise_on_save)
    monkeypatch.setattr(database_handler.logger, "warning", lambda msg: warnings.append(str(msg)))
    history = _build_history_manager(monkeypatch, tmp_path)

    ok = history.mark_as_visited("https://www.linkedin.com/in/john-doe/?trk=abc", saved=True)

    assert ok is False
    assert any("db is offline" in msg for msg in warnings)
    assert "https://www.linkedin.com/in/john-doe" in history.visited_history


def test_history_manager_should_skip_uses_normalized_profile_url(monkeypatch, tmp_path):
    monkeypatch.setattr(database_handler, "save_visited_profile", lambda *_args, **_kwargs: True)
    history = _build_history_manager(monkeypatch, tmp_path)
    history.mark_as_visited("https://www.linkedin.com/in/john-doe?trk=abc", saved=True)

    variants = [
        "https://www.linkedin.com/in/john-doe",
        "https://www.linkedin.com/in/john-doe/",
        "https://www.linkedin.com/in/john-doe?trk=abc",
        "https://www.linkedin.com/in/john-doe?miniProfileUrn=xyz",
    ]
    assert all(history.should_skip(url) for url in variants)
