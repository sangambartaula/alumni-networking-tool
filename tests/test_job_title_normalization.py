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
        ("Senior Software Developer", "Software Engineer"),
        ("Software Engineer II", "Software Engineer"),
        ("Lead Software Developer", "Software Engineer"),
        ("Director of Software Engineering", "Director"),
        ("Software Developer", "Software Engineer"),
        ("Software Dev", "Software Engineer"),
        ("Application Engineer II", "Application Engineer"),
        ("Senior Accountant", "Finance / Accounting"),
    ],
)
def test_title_standardization_removes_seniority_and_soft_dev_variants(raw_title, expected):
    assert normalize_title_deterministic(raw_title) == expected


# ── Executive / C-Level titles ──────────────────────────────────────────────
@pytest.mark.parametrize(
    ("raw_title", "expected"),
    [
        ("CEO", "CEO"),
        ("Chief Executive Officer", "CEO"),
        ("CTO", "CTO"),
        ("Chief Technology Officer", "CTO"),
        ("COO", "COO"),
        ("Chief Operations Officer", "COO"),
        ("CFO", "CFO"),
        ("Chief Financial Officer", "CFO"),
        ("CMO", "CMO"),
        ("Chief Marketing Officer", "CMO"),
        ("VP of Engineering", "VP"),
        ("Vice President of Engineering", "VP"),
        ("Executive Vice President of Operations", "VP"),
    ],
)
def test_executive_titles_map_to_specific_buckets(raw_title, expected):
    assert normalize_title_deterministic(raw_title) == expected


# ── IT Infrastructure titles ────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("raw_title", "expected"),
    [
        ("Database Administrator", "Database Administrator"),
        ("DBA", "Database Administrator"),
        ("Systems Administrator", "Systems Administrator"),
        ("System Administrator", "Systems Administrator"),
        ("Sys Admin", "Systems Administrator"),
        ("Sysadmin", "Systems Administrator"),
        ("Network Engineer", "Network Engineer"),
        ("Network Administrator", "Network Engineer"),
    ],
)
def test_it_infrastructure_titles_remain_distinct(raw_title, expected):
    assert normalize_title_deterministic(raw_title) == expected


# ── Civil / Field / Project Engineers ───────────────────────────────────────
@pytest.mark.parametrize(
    ("raw_title", "expected"),
    [
        ("Civil Engineer", "Civil Engineer"),
        ("Structural Engineer", "Civil Engineer"),
        ("Site Engineer", "Site Engineer"),
        ("Field Engineer", "Field Engineer"),
        ("Project Engineer", "Project Engineer"),
        ("FEO Project Engineer", "Project Engineer"),
    ],
)
def test_distinct_engineering_titles(raw_title, expected):
    assert normalize_title_deterministic(raw_title) == expected


# ── Director titles ────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("raw_title", "expected"),
    [
        ("Director of Engineering", "Director"),
        ("Director of Southwest Region", "Director"),
        ("Director of Strategic Initiatives", "Director"),
        ("Sr. Director of Software Engineering", "Director"),
    ],
)
def test_director_titles_map_to_director(raw_title, expected):
    assert normalize_title_deterministic(raw_title) == expected


# ── Member of Technical Staff ───────────────────────────────────────────────
def test_member_of_technical_staff():
    assert normalize_title_deterministic("Member of Technical Staff") == "Software Engineer"


# ── Graduate student without assistant title ────────────────────────────────
def test_graduate_student_maps_to_student():
    assert normalize_title_deterministic("Graduate Student") == "Student"
    assert normalize_title_deterministic("Graduate Student at University of North Texas") == "Student"


@pytest.mark.parametrize(
    ("raw_title", "expected"),
    [
        ("Summer Intern", "Intern"),
        ("Software Engineer Intern", "Software Engineer"),
        ("Associate Engineer", "Engineer"),
        ("Workday Engineer", "Software Engineer"),
        ("Apple Engineer", "Software Engineer"),
    ],
)
def test_intern_associate_and_company_prefixed_titles(raw_title, expected):
    assert normalize_title_deterministic(raw_title) == expected


@pytest.mark.parametrize(
    "raw_title",
    ["Denton, Texas", "Denton County, Texas", "United States", "Texas"],
)
def test_location_strings_do_not_become_normalized_titles(raw_title):
    assert normalize_title_deterministic(raw_title) == ""
