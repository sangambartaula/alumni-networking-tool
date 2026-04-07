import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtWidgets import QApplication

import scraper_gui


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance() or QApplication([])
    yield app


def _silence_message_boxes(monkeypatch):
    monkeypatch.setattr(scraper_gui.QMessageBox, "information", lambda *args, **kwargs: None)
    monkeypatch.setattr(scraper_gui.QMessageBox, "warning", lambda *args, **kwargs: None)
    monkeypatch.setattr(scraper_gui.QMessageBox, "critical", lambda *args, **kwargs: None)


def test_update_env_many_round_trips_special_values(tmp_path, monkeypatch):
    monkeypatch.setattr(scraper_gui, "get_base_dir", lambda: str(tmp_path))

    updates = {
        "LINKEDIN_PASSWORD": 'pa ss#word "quoted"',
        "INPUT_CSV": str(tmp_path / "Folder With Spaces" / "Connections #1.csv"),
    }
    scraper_gui.update_env_many(updates)

    loaded = scraper_gui._safe_load_dotenv_values(tmp_path / ".env")
    assert loaded["LINKEDIN_PASSWORD"] == updates["LINKEDIN_PASSWORD"]
    assert loaded["INPUT_CSV"] == updates["INPUT_CSV"]


def test_settings_reset_to_defaults_restores_default_values(qapp, tmp_path, monkeypatch):
    _silence_message_boxes(monkeypatch)
    monkeypatch.setattr(scraper_gui, "get_base_dir", lambda: str(tmp_path))

    dialog = scraper_gui.SettingsDialog()
    dialog._fields["MYSQLPORT"]["widget"].setText("9999")
    dialog._fields["MYSQLHOST"]["widget"].setText("db.example.edu")
    dialog._fields["HEADLESS"]["widget"].setChecked(True)

    dialog.reset_to_defaults()

    assert dialog._fields["MYSQLPORT"]["widget"].text() == "3306"
    assert dialog._fields["MYSQLHOST"]["widget"].text() == "localhost"
    assert dialog._fields["HEADLESS"]["widget"].isChecked() is False


def test_save_all_settings_clears_password_and_persists_total_runtime(qapp, tmp_path, monkeypatch):
    _silence_message_boxes(monkeypatch)
    env_path = Path(tmp_path) / ".env"
    env_path.write_text("LINKEDIN_PASSWORD=oldpass\n", encoding="utf-8")
    monkeypatch.setattr(scraper_gui, "get_base_dir", lambda: str(tmp_path))
    monkeypatch.setattr(scraper_gui.ScraperApp, "refresh_run_history", lambda self: None)
    monkeypatch.setattr(scraper_gui.ScraperApp, "refresh_scrape_count", lambda self: None)
    monkeypatch.setattr(scraper_gui.ScraperApp, "_start_gui_autoreload_watcher", lambda self: None)

    app = scraper_gui.ScraperApp()
    app.email_input.setText("tester@example.com")
    app.password_input.clear()
    app.hours_input.setText("2")
    app.mins_input.setText("30")

    app.save_all_settings_to_env()

    loaded = scraper_gui._safe_load_dotenv_values(env_path)
    assert loaded.get("LINKEDIN_PASSWORD", "") == ""
    assert loaded["GUI_MAX_RUNTIME_HOURS"] == "2"
    assert loaded["GUI_MAX_RUNTIME_DISPLAY_MINUTES"] == "30"
    assert loaded["GUI_MAX_RUNTIME_MINUTES"] == "150"


def test_refresh_preflight_status_uses_background_worker(qapp, tmp_path, monkeypatch):
    _silence_message_boxes(monkeypatch)
    monkeypatch.setattr(scraper_gui, "get_base_dir", lambda: str(tmp_path))
    monkeypatch.setattr(scraper_gui.ScraperApp, "refresh_run_history", lambda self: None)
    monkeypatch.setattr(scraper_gui.ScraperApp, "refresh_scrape_count", lambda self: None)
    monkeypatch.setattr(scraper_gui.ScraperApp, "_start_gui_autoreload_watcher", lambda self: None)

    worker_calls = []

    class _FakeSignal:
        def __init__(self):
            self._callback = None

        def connect(self, callback):
            self._callback = callback

        def emit(self, payload):
            if self._callback:
                self._callback(payload)

    class _FakeWorker:
        def __init__(self, force_cloud=False, force_geo=False, cloud_cache=None, geo_cache=None):
            worker_calls.append({
                "force_cloud": force_cloud,
                "force_geo": force_geo,
                "cloud_cache": cloud_cache,
                "geo_cache": geo_cache,
            })
            self.finished_signal = _FakeSignal()

        def isRunning(self):
            return False

        def start(self):
            self.finished_signal.emit({
                "cloud": ("green", "Cloud DB connected", "ok"),
                "geo": ("green", "Geocoding reachable", "ok"),
            })

        def deleteLater(self):
            return None

    monkeypatch.setattr(scraper_gui, "PreflightStatusWorker", _FakeWorker)

    app = scraper_gui.ScraperApp()
    app._cloud_status_cache = None
    app._geo_status_cache = None

    app.refresh_preflight_status(force_cloud_probe=True, force_geo_probe=True)

    assert worker_calls[-1]["force_cloud"] is True
    assert worker_calls[-1]["force_geo"] is True
    assert "Cloud DB connected" in app.cloud_status_label.text()
    assert "Geocoding reachable" in app.geo_status_label.text()
    assert app.refresh_status_btn.isEnabled() is True
