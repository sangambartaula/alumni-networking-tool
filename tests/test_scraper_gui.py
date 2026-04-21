import os
import zipfile
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


def _build_app(monkeypatch, tmp_path, stub_preflight=True):
    monkeypatch.setattr(scraper_gui, "get_base_dir", lambda: str(tmp_path))
    monkeypatch.setattr(scraper_gui.ScraperApp, "refresh_run_history", lambda self: None)
    monkeypatch.setattr(scraper_gui.ScraperApp, "refresh_scrape_count", lambda self: None)
    monkeypatch.setattr(scraper_gui.ScraperApp, "_start_gui_autoreload_watcher", lambda self: None)
    if stub_preflight:
        monkeypatch.setattr(scraper_gui.ScraperApp, "refresh_preflight_status", lambda self, *args, **kwargs: None)
    return scraper_gui.ScraperApp()


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
    app = _build_app(monkeypatch, tmp_path, stub_preflight=False)
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

    app = _build_app(monkeypatch, tmp_path, stub_preflight=False)
    app._cloud_status_cache = None
    app._geo_status_cache = None

    app.refresh_preflight_status(force_cloud_probe=True, force_geo_probe=True)

    assert worker_calls[-1]["force_cloud"] is True
    assert worker_calls[-1]["force_geo"] is True
    assert "Cloud DB connected" in app.cloud_status_label.text()
    assert "Geocoding reachable" in app.geo_status_label.text()
    assert "Cloud DB" in app.preflight_details_box.toPlainText()
    assert app.refresh_status_btn.isEnabled() is True


def test_settings_tabs_split_operator_and_admin(qapp, tmp_path, monkeypatch):
    _silence_message_boxes(monkeypatch)
    monkeypatch.setattr(scraper_gui, "get_base_dir", lambda: str(tmp_path))

    dialog = scraper_gui.SettingsDialog()

    assert dialog.tabs.tabText(0) == "Operator"
    assert dialog.tabs.tabText(1) == "Admin & Dev"


def test_validate_inputs_warns_when_profile_limit_exceeds_safe_default(qapp, tmp_path, monkeypatch):
    _silence_message_boxes(monkeypatch)
    app = _build_app(monkeypatch, tmp_path)
    app.email_input.setText("tester@example.com")
    app.max_profiles.setText("55")

    monkeypatch.setattr(
        scraper_gui.QMessageBox,
        "question",
        lambda *args, **kwargs: scraper_gui.QMessageBox.StandardButton.No,
    )

    assert app.validate_inputs() is False


def test_export_diagnostics_bundle_redacts_secrets(qapp, tmp_path, monkeypatch):
    _silence_message_boxes(monkeypatch)
    env_path = Path(tmp_path) / ".env"
    env_path.write_text(
        "\n".join([
            "LINKEDIN_EMAIL=tester@example.com",
            "LINKEDIN_PASSWORD=secretpass",
            "GROQ_API_KEY=abc123",
            "MYSQLHOST=db.example.edu",
            "",
        ]),
        encoding="utf-8",
    )
    app = _build_app(monkeypatch, tmp_path)
    app.console.setPlainText("console test line")
    app._cloud_status_cache = ("yellow", "Cloud DB setup needed", "Missing module")
    app._geo_status_cache = ("green", "Geocoding reachable", "ok")

    dialog = scraper_gui.SettingsDialog(app)
    dialog.export_diagnostics_bundle()

    bundles = sorted((Path(tmp_path) / "diagnostics_exports").glob("diagnostics-*.zip"))
    assert bundles
    with zipfile.ZipFile(bundles[-1]) as zf:
        env_redacted = zf.read("env.redacted").decode("utf-8")
        summary = zf.read("summary.txt").decode("utf-8")
        console_log = zf.read("console.log").decode("utf-8")

    assert "LINKEDIN_PASSWORD=***REDACTED***" in env_redacted
    assert "GROQ_API_KEY=***REDACTED***" in env_redacted
    assert "Cloud Status: Cloud DB setup needed" in summary
    assert "console test line" in console_log


def test_cloud_probe_guidance_suggests_network_fix():
    tip = scraper_gui._build_probe_guidance(
        "cloud",
        "Cloud DB unavailable",
        "Cloud probe failed: InterfaceError: 2003: Can't connect to MySQL server on 'db.example.edu' (timed out)",
    )

    assert "Wi-Fi/VPN" in tip
    assert "blocked" in tip


def test_probe_setup_guidance_stays_short_for_hover_text():
    tip = scraper_gui._build_probe_guidance(
        "cloud",
        "Probe setup needed",
        "Could not execute probe with 'python3': [Errno 2] No such file or directory. Install dependencies or configure Python path.",
    )

    assert tip == "Install Dependencies, then refresh status. If it still fails, reopen the app from the project environment."


def test_preflight_status_ready_keeps_friendly_guidance_and_raw_detail(qapp, tmp_path, monkeypatch):
    _silence_message_boxes(monkeypatch)
    app = _build_app(monkeypatch, tmp_path, stub_preflight=False)

    app._on_preflight_status_ready({
        "cloud": {
            "state": "red",
            "text": "Cloud DB unavailable",
            "tip": "Connect to a different Wi-Fi/VPN or check the database host/port; the cloud database connection looks blocked from this network.",
            "detail": "Cloud probe failed: InterfaceError: 2003: Can't connect to MySQL server on 'db.example.edu' (timed out)",
        },
        "geo": {
            "state": "green",
            "text": "Geocoding reachable",
            "tip": "Geocoding is reachable.",
            "detail": "Fort Worth probe resolved to expected metro coordinates.",
        },
    })

    assert "Wi-Fi/VPN" in app._cloud_status_cache[2]
    assert "timed out" in app._cloud_status_detail_cache
    assert "Guidance:" in app.preflight_details_box.toPlainText()
    assert "Details: Cloud probe failed" in app.preflight_details_box.toPlainText()


def test_flag_manager_shows_every_txt_entry_even_when_csv_is_missing_rows(qapp, tmp_path, monkeypatch):
    output_dir = Path(tmp_path) / "scraper" / "output"
    output_dir.mkdir(parents=True)

    (output_dir / "flagged_for_review.txt").write_text(
        "\n".join([
            "https://www.linkedin.com/in/alpha # Missing Grad Year",
            "https://www.linkedin.com/in/beta # Missing Company but Job Title Present",
            "",
        ]),
        encoding="utf-8",
    )
    (output_dir / "UNT_Alumni_Data.csv").write_text(
        "\n".join([
            "linkedin_url,first,last,scraped_at,grad_year",
            "https://www.linkedin.com/in/alpha,Alpha,User,2026-04-01T12:00:00,2024",
            "",
        ]),
        encoding="utf-8",
    )

    monkeypatch.setattr(scraper_gui.FlagManagerDialog, "load_runs", lambda self: None)

    def _raise_no_db(self):
        raise RuntimeError("database unavailable in test")

    monkeypatch.setattr(scraper_gui.FlagManagerDialog, "_get_connection", _raise_no_db)

    dialog = scraper_gui.FlagManagerDialog(str(tmp_path))
    try:
        assert dialog.table.rowCount() == 2
        assert dialog.table.item(0, 5).text() == "https://www.linkedin.com/in/alpha"
        assert dialog.table.item(1, 5).text() == "https://www.linkedin.com/in/beta"
        assert dialog.table.item(0, 3).text() == "Alpha User"
        assert dialog.table.item(1, 3).text() == ""
        assert dialog.table.item(1, 4).text() == "Missing Company but Job Title Present"
    finally:
        dialog.close()
