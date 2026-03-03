import os
import sys
from pathlib import Path

import pytest


project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "scraper"))
os.chdir(project_root)

from job_title_normalization import normalize_title_deterministic


def test_data_owner_normalizes_to_data_analyst():
    assert normalize_title_deterministic("Data Owner") == "Data Analyst"


@pytest.mark.parametrize(
    ("raw_title", "expected"),
    [
        ("Senior Project Manager", "Project Manager"),
        ("Senior Software Engineer", "Software Engineer"),
        ("Software Developer", "Software Engineer"),
        ("Software Dev", "Software Engineer"),
        ("Application Engineer II", "Application Engineer"),
        ("Senior Accountant", "Accountant"),
    ],
)
def test_title_standardization_removes_seniority_and_soft_dev_variants(raw_title, expected):
    assert normalize_title_deterministic(raw_title) == expected
