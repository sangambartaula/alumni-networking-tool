import os
import sys
from pathlib import Path


project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "scraper"))
os.chdir(project_root)

from job_title_normalization import normalize_title_deterministic


def test_data_owner_normalizes_to_data_analyst():
    assert normalize_title_deterministic("Data Owner") == "Data Analyst"

