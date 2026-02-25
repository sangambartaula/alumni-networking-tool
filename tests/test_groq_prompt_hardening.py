import os
import sys
from pathlib import Path


project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "scraper"))
os.chdir(project_root)

from company_normalization import _coerce_existing_company_choice
from discipline_classification import _coerce_llm_discipline_choice
from job_title_normalization import _coerce_existing_title_choice
from major_normalization import _coerce_llm_major_choice, CANONICAL_MAJORS


def test_major_choice_prefers_id_mapping():
    assert _coerce_llm_major_choice({"major_id": 1}) == CANONICAL_MAJORS[0]
    assert _coerce_llm_major_choice({"major_id": "2"}) == CANONICAL_MAJORS[1]


def test_major_choice_accepts_exact_name_case_insensitive():
    assert _coerce_llm_major_choice({"major": "computer science"}) == "Computer Science"


def test_major_choice_invalid_payload_returns_other():
    assert _coerce_llm_major_choice({"major": "something made up"}) == "Other"
    assert _coerce_llm_major_choice(None) == "Other"


def test_discipline_choice_accepts_id_and_name():
    assert _coerce_llm_discipline_choice({"discipline_id": 1}) == "Software, Data & AI Engineering"
    assert _coerce_llm_discipline_choice({"discipline": "other"}) == "Other"


def test_title_choice_restores_existing_casing():
    existing = ["Software Engineer", "Graduate Assistant"]
    assert _coerce_existing_title_choice("software engineer", existing) == "Software Engineer"
    assert _coerce_existing_title_choice("Graduate Assistant.", existing) == "Graduate Assistant"


def test_company_choice_restores_existing_casing():
    existing = ["Amazon Web Services", "University of North Texas"]
    assert _coerce_existing_company_choice("amazon web services", existing) == "Amazon Web Services"
    assert _coerce_existing_company_choice("University of North Texas,", existing) == "University of North Texas"
