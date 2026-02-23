import os
import sys
from pathlib import Path


# Add scraper to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "scraper"))
os.chdir(project_root)

from scraper import LinkedInScraper


def _unt_education_entries():
    return [{"school": "University of North Texas"}]


def _non_unt_education_entries():
    return [{"school": "University of Houston"}]


def _ga_exp(raw_company):
    return [{"standardized_title": "Graduate Assistant", "raw_company": raw_company}]


def test_missing_dates_fallback_true_for_unt_full_name():
    scraper = LinkedInScraper()
    result = scraper._apply_missing_dates_unt_ga_fallback(
        best_wws="",
        all_experiences=_ga_exp("University of North Texas"),
        edu_entries=_unt_education_entries(),
    )
    assert result == "yes"


def test_missing_dates_fallback_true_for_unt_prefix_unit():
    scraper = LinkedInScraper()
    result = scraper._apply_missing_dates_unt_ga_fallback(
        best_wws="",
        all_experiences=_ga_exp("UNT Libraries"),
        edu_entries=_unt_education_entries(),
    )
    assert result == "yes"


def test_missing_dates_fallback_false_for_non_unt_employer():
    scraper = LinkedInScraper()
    result = scraper._apply_missing_dates_unt_ga_fallback(
        best_wws="",
        all_experiences=_ga_exp("University of Houston"),
        edu_entries=_unt_education_entries(),
    )
    assert result == "no"


def test_missing_dates_fallback_false_for_blank_raw_company():
    scraper = LinkedInScraper()
    result_blank = scraper._apply_missing_dates_unt_ga_fallback(
        best_wws="",
        all_experiences=_ga_exp(""),
        edu_entries=_unt_education_entries(),
    )
    result_none = scraper._apply_missing_dates_unt_ga_fallback(
        best_wws="",
        all_experiences=_ga_exp(None),
        edu_entries=_unt_education_entries(),
    )
    assert result_blank == "no"
    assert result_none == "no"


def test_missing_dates_fallback_false_without_unt_education():
    scraper = LinkedInScraper()
    result = scraper._apply_missing_dates_unt_ga_fallback(
        best_wws="",
        all_experiences=_ga_exp("University of North Texas"),
        edu_entries=_non_unt_education_entries(),
    )
    assert result == "no"


def test_computable_false_not_overridden_by_fallback():
    scraper = LinkedInScraper()
    result = scraper._apply_missing_dates_unt_ga_fallback(
        best_wws="no",
        all_experiences=_ga_exp("University of North Texas"),
        edu_entries=_unt_education_entries(),
    )
    assert result == "no"


def test_computable_true_not_changed():
    scraper = LinkedInScraper()
    result = scraper._apply_missing_dates_unt_ga_fallback(
        best_wws="yes",
        all_experiences=_ga_exp("University of North Texas"),
        edu_entries=_unt_education_entries(),
    )
    assert result == "yes"


def test_hunt_does_not_match_unt_employer():
    scraper = LinkedInScraper()
    result = scraper._apply_missing_dates_unt_ga_fallback(
        best_wws="",
        all_experiences=_ga_exp("HUNT Oil Company"),
        edu_entries=_unt_education_entries(),
    )
    assert result == "no"
