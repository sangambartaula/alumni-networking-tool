"""Tests for alumni CSV schema creation and migration."""

import sys
from pathlib import Path

import pandas as pd

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / "scraper"))

import database_handler  # noqa: E402


def test_ensure_creates_missing_csv(monkeypatch, tmp_path):
    out = tmp_path / "UNT_Alumni_Data.csv"
    monkeypatch.setattr(database_handler, "OUTPUT_CSV", out)
    database_handler.ensure_alumni_output_csv()
    assert out.exists()
    df = pd.read_csv(out, encoding="utf-8")
    assert list(df.columns) == database_handler.CSV_COLUMNS
    assert len(df) == 0


def test_ensure_migrates_partial_schema(monkeypatch, tmp_path):
    out = tmp_path / "UNT_Alumni_Data.csv"
    monkeypatch.setattr(database_handler, "OUTPUT_CSV", out)
    legacy = pd.DataFrame(
        [
            {
                "first": "Ali",
                "last": "B",
                "linkedin_url": "https://www.linkedin.com/in/example",
                "school": "University of North Texas",
                "degree": "B.S.",
                "major": "Computer Science",
            }
        ]
    )
    legacy.to_csv(out, index=False, encoding="utf-8")

    database_handler.ensure_alumni_output_csv()
    df = pd.read_csv(out, encoding="utf-8")
    assert list(df.columns) == database_handler.CSV_COLUMNS
    assert len(df) == 1
    assert df.iloc[0]["first"] == "Ali"
    jet = df.iloc[0]["job_employment_type"]
    assert jet == "" or (isinstance(jet, float) and pd.isna(jet))
