import os
import sys
import warnings
from pathlib import Path


project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "scraper"))
os.chdir(project_root)

import database_handler


def test_save_profile_to_csv_avoids_futurewarning_on_append(monkeypatch, tmp_path):
    output_csv = tmp_path / "UNT_Alumni_Data.csv"
    monkeypatch.setattr(database_handler, "OUTPUT_CSV", output_csv)
    monkeypatch.setattr(database_handler, "flag_profile_for_review", lambda _data: None)

    profile = {
        "name": "Test Person",
        "profile_url": "https://www.linkedin.com/in/test-person",
        "school": "University of North Texas",
        "degree": "Master's degree",
        "major": "Computer Science",
        "graduation_year": "2026",
        "job_title": "Software Engineer",
        "company": "ACME",
        "location": "Denton, TX",
        "scraped_at": "2026-02-25 00:00:00",
    }

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        ok = database_handler.save_profile_to_csv(profile)

    assert ok is True
    future_warnings = [w for w in caught if issubclass(w.category, FutureWarning)]
    assert not future_warnings
