from datetime import date

from unt_alumni_status import (
    UNT_ALUMNI_STATUS_NO,
    UNT_ALUMNI_STATUS_UNKNOWN,
    UNT_ALUMNI_STATUS_YES,
    compute_unt_alumni_status,
    compute_unt_alumni_status_from_row,
)


def test_past_grad_year_yes():
    status = compute_unt_alumni_status(
        [{"school": "University of North Texas", "end_year": 2020}],
        today=date(2026, 2, 26),
    )
    assert status == UNT_ALUMNI_STATUS_YES


def test_future_grad_year_no():
    status = compute_unt_alumni_status(
        [{"school": "University of North Texas", "end_year": 2028}],
        today=date(2026, 2, 26),
    )
    assert status == UNT_ALUMNI_STATUS_NO


def test_no_grad_date_unknown():
    status = compute_unt_alumni_status(
        [{"school": "UNT", "end_year": None}],
        today=date(2026, 2, 26),
    )
    assert status == UNT_ALUMNI_STATUS_UNKNOWN


def test_no_unt_education_unknown():
    status = compute_unt_alumni_status([], today=date(2026, 2, 26))
    assert status == UNT_ALUMNI_STATUS_UNKNOWN


def test_bs_past_ms_future_no():
    status = compute_unt_alumni_status(
        [
            {"school": "UNT", "end_year": 2021},
            {"school": "UNT", "end_year": 2027},
        ],
        today=date(2026, 2, 26),
    )
    assert status == UNT_ALUMNI_STATUS_NO


def test_bs_past_ms_missing_yes():
    status = compute_unt_alumni_status(
        [
            {"school": "UNT", "end_year": 2021},
            {"school": "UNT", "end_year": None},
        ],
        today=date(2026, 2, 26),
    )
    assert status == UNT_ALUMNI_STATUS_YES


def test_bs_missing_ms_future_no():
    status = compute_unt_alumni_status(
        [
            {"school": "UNT", "end_year": None},
            {"school": "UNT", "end_year": 2027},
        ],
        today=date(2026, 2, 26),
    )
    assert status == UNT_ALUMNI_STATUS_NO


def test_multiple_past_degrees_yes():
    status = compute_unt_alumni_status(
        [
            {"school": "UNT", "end_year": 2018},
            {"school": "UNT", "end_year": 2022},
        ],
        today=date(2026, 2, 26),
    )
    assert status == UNT_ALUMNI_STATUS_YES


def test_current_year_without_date_unknown():
    status = compute_unt_alumni_status(
        [{"school": "UNT", "end_year": 2026}],
        today=date(2026, 2, 26),
    )
    assert status == UNT_ALUMNI_STATUS_UNKNOWN


def test_row_mapping_uses_only_unt_entries():
    row = {
        "school": "University of North Texas",
        "grad_year": 2023,
        "degree": "Bachelor of Science",
        "major": "Computer Science",
        "school2": "Texas A&M",
        "degree2": "Master of Science 2027",
        "major2": "CS",
        "school3": None,
        "degree3": None,
        "major3": None,
    }
    status = compute_unt_alumni_status_from_row(row, today=date(2026, 2, 26))
    assert status == UNT_ALUMNI_STATUS_YES

