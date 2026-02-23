"""
Tests for the UNT-aware work_while_studying module (v2).

Covers every branch of the spec:
  1. UNT school filter (only matching rows used)
  2. unt_start / unt_end window computation (direct date, year fallback, is_expected, null)
  3. Job-date overlap logic (inside, outside, boundary, partial, current-job)
  4. Evidence list contains ONLY overlapping jobs
  5. Edge cases: null start_date, null end_date, no UNT row, no experience
"""

import unittest
from datetime import date
from unittest.mock import MagicMock

from work_while_studying import (
    _is_unt_school,
    _compute_unt_window,
    _get_graduation_date,
    _get_graduated_status,
    computeWorkWhileStudying,
    ensure_work_while_studying_schema,
)

# ---------------------------------------------------------------------------
# Constants used across tests
# ---------------------------------------------------------------------------
TODAY = date(2026, 2, 23)   # freeze "today" for deterministic tests


# ---------------------------------------------------------------------------
# Helper: build a mock DB connection
# ---------------------------------------------------------------------------

def _make_conn(edu_rows=None, exp_rows=None):
    """
    Return a mock connection whose cursor yields:
      - first fetchall()  →  edu_rows
      - second fetchall() →  exp_rows
    Both default to empty lists.
    """
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_conn.cursor.return_value.__exit__.return_value = None

    # fetchall is called twice: once for education, once for experience
    mock_cur.fetchall.side_effect = [
        edu_rows if edu_rows is not None else [],
        exp_rows if exp_rows is not None else [],
    ]
    return mock_conn


def _get_conn_factory(edu_rows=None, exp_rows=None):
    """Return a zero-arg callable that produces a fresh mock connection."""
    conn = _make_conn(edu_rows, exp_rows)
    return lambda: conn


# ---------------------------------------------------------------------------
# 1. _is_unt_school
# ---------------------------------------------------------------------------

class TestIsUntSchool(unittest.TestCase):

    def test_exact_full_name(self):
        self.assertTrue(_is_unt_school("University of North Texas"))

    def test_full_name_mixed_case(self):
        self.assertTrue(_is_unt_school("university of north texas"))

    def test_abbreviation_unt(self):
        self.assertTrue(_is_unt_school("UNT"))

    def test_abbreviation_lowercase(self):
        self.assertTrue(_is_unt_school("unt"))

    def test_partial_match_in_longer_name(self):
        self.assertTrue(_is_unt_school("UNT Health Science Center"))

    def test_non_unt_school(self):
        self.assertFalse(_is_unt_school("Texas A&M University"))

    def test_none_returns_false(self):
        self.assertFalse(_is_unt_school(None))

    def test_empty_string_returns_false(self):
        self.assertFalse(_is_unt_school(""))


# ---------------------------------------------------------------------------
# 2. _compute_unt_window
# ---------------------------------------------------------------------------

class TestComputeUntWindow(unittest.TestCase):

    def test_exact_dates_used_when_present(self):
        edu = {
            "school_start_date": date(2018, 9, 1),
            "school_start_year": 2018,
            "graduation_date": date(2022, 5, 7),
            "graduation_year": 2022,
            "is_expected": False,
        }
        start, end = _compute_unt_window(edu, TODAY)
        self.assertEqual(start, date(2018, 9, 1))
        self.assertEqual(end, date(2022, 5, 7))

    def test_start_year_fallback_aug15(self):
        edu = {
            "school_start_date": None,
            "school_start_year": 2019,
            "graduation_date": date(2023, 5, 15),
            "graduation_year": 2023,
            "is_expected": False,
        }
        start, end = _compute_unt_window(edu, TODAY)
        self.assertEqual(start, date(2019, 8, 15))

    def test_grad_year_fallback_may15(self):
        edu = {
            "school_start_date": date(2019, 8, 15),
            "school_start_year": None,
            "graduation_date": None,
            "graduation_year": 2023,
            "is_expected": False,
        }
        start, end = _compute_unt_window(edu, TODAY)
        self.assertEqual(end, date(2023, 5, 15))

    def test_is_expected_sets_unt_end_to_today(self):
        edu = {
            "school_start_date": date(2022, 8, 15),
            "school_start_year": None,
            "graduation_date": None,
            "graduation_year": 2026,
            "is_expected": True,
        }
        start, end = _compute_unt_window(edu, TODAY)
        self.assertEqual(end, TODAY)

    def test_null_grad_info_sets_unt_end_to_today(self):
        edu = {
            "school_start_date": date(2020, 1, 1),
            "school_start_year": None,
            "graduation_date": None,
            "graduation_year": None,
            "is_expected": False,
        }
        _, end = _compute_unt_window(edu, TODAY)
        self.assertEqual(end, TODAY)

    def test_null_start_returns_none_start(self):
        edu = {
            "school_start_date": None,
            "school_start_year": None,
            "graduation_date": date(2023, 5, 15),
            "graduation_year": None,
            "is_expected": False,
        }
        start, _ = _compute_unt_window(edu, TODAY)
        self.assertIsNone(start)


# ---------------------------------------------------------------------------
# 3. Legacy helpers (_get_graduation_date, _get_graduated_status)
# ---------------------------------------------------------------------------

class TestLegacyHelpers(unittest.TestCase):

    def test_graduation_date_preferred(self):
        d = date(2021, 6, 1)
        self.assertEqual(_get_graduation_date(d, 2021, 5), d)

    def test_graduation_year_fallback_may15(self):
        self.assertEqual(_get_graduation_date(None, 2021, None), date(2021, 5, 15))

    def test_all_null_returns_none(self):
        self.assertIsNone(_get_graduation_date(None, None, None))

    def test_graduated_status_past_year(self):
        self.assertEqual(_get_graduated_status(2020, None, False, 2026), "graduated")

    def test_graduated_status_expected_flag(self):
        self.assertEqual(_get_graduated_status(2026, None, True, 2026), "not_yet_graduated")

    def test_graduated_status_future_year(self):
        self.assertEqual(_get_graduated_status(2027, None, False, 2026), "not_yet_graduated")

    def test_graduated_status_unknown(self):
        self.assertEqual(_get_graduated_status(None, None, False, 2026), "unknown")


# ---------------------------------------------------------------------------
# 4. computeWorkWhileStudying — UNT filter
# ---------------------------------------------------------------------------

class TestUntSchoolFilter(unittest.TestCase):

    def test_no_unt_row_returns_false(self):
        """No UNT education row → worked_while_at_unt=False."""
        edu_rows = [
            {
                "school_name": "Texas A&M University",
                "school_start_date": date(2018, 8, 15),
                "school_start_year": None,
                "graduation_date": date(2022, 5, 15),
                "graduation_year": 2022,
                "graduation_month": 5,
                "is_expected": False,
            }
        ]
        result = computeWorkWhileStudying(1, _get_conn_factory(edu_rows, []), today=TODAY)
        self.assertIsNotNone(result)
        self.assertFalse(result["worked_while_at_unt"])
        self.assertFalse(result["is_working_while_studying"])
        self.assertEqual(result["evidence_jobs"], [])

    def test_unt_row_used_other_schools_ignored(self):
        """Only the UNT row drives the window; other rows are irrelevant."""
        edu_rows = [
            {
                "school_name": "Some Community College",
                "school_start_date": date(2016, 8, 15),
                "school_start_year": None,
                "graduation_date": date(2018, 5, 15),
                "graduation_year": 2018,
                "graduation_month": 5,
                "is_expected": False,
            },
            {
                "school_name": "University of North Texas",
                "school_start_date": date(2018, 8, 15),
                "school_start_year": None,
                "graduation_date": date(2022, 5, 15),
                "graduation_year": 2022,
                "graduation_month": 5,
                "is_expected": False,
            },
        ]
        # A job that overlaps UNT window but NOT the community college window
        exp_rows = [
            {
                "company": "Tech Corp",
                "title": "Intern",
                "start_date": date(2020, 6, 1),
                "end_date": date(2021, 8, 1),
                "is_current": False,
            }
        ]
        result = computeWorkWhileStudying(1, _get_conn_factory(edu_rows, exp_rows), today=TODAY)
        self.assertTrue(result["worked_while_at_unt"])
        self.assertEqual(len(result["evidence_jobs"]), 1)

    def test_empty_edu_rows_returns_false(self):
        result = computeWorkWhileStudying(1, _get_conn_factory([], []), today=TODAY)
        self.assertFalse(result["worked_while_at_unt"])
        self.assertEqual(result["evidence_jobs"], [])


# ---------------------------------------------------------------------------
# 5. computeWorkWhileStudying — window / unt_start edge cases
# ---------------------------------------------------------------------------

class TestUntWindowEdgeCases(unittest.TestCase):

    def _unt_edu(self, **overrides):
        base = {
            "school_name": "University of North Texas",
            "school_start_date": date(2018, 8, 15),
            "school_start_year": None,
            "graduation_date": date(2022, 5, 15),
            "graduation_year": 2022,
            "graduation_month": 5,
            "is_expected": False,
        }
        base.update(overrides)
        return [base]

    def test_null_unt_start_returns_false(self):
        """If we cannot determine when UNT attendance started, return False."""
        edu_rows = self._unt_edu(school_start_date=None, school_start_year=None)
        exp_rows = [
            {
                "company": "Acme",
                "title": "Dev",
                "start_date": date(2020, 1, 1),
                "end_date": date(2021, 1, 1),
                "is_current": False,
            }
        ]
        result = computeWorkWhileStudying(1, _get_conn_factory(edu_rows, exp_rows), today=TODAY)
        self.assertFalse(result["worked_while_at_unt"])
        self.assertEqual(result["evidence_jobs"], [])

    def test_start_year_fallback_used_in_overlap(self):
        """school_start_year → Aug 15 is used when school_start_date is null."""
        edu_rows = self._unt_edu(school_start_date=None, school_start_year=2018)
        # Job starts in fall 2018, overlaps
        exp_rows = [
            {
                "company": "Beta Co",
                "title": "RA",
                "start_date": date(2018, 9, 1),
                "end_date": date(2019, 5, 1),
                "is_current": False,
            }
        ]
        result = computeWorkWhileStudying(1, _get_conn_factory(edu_rows, exp_rows), today=TODAY)
        self.assertTrue(result["worked_while_at_unt"])

    def test_is_expected_makes_unt_end_today(self):
        """is_expected=True → unt_end=today; a current job overlaps."""
        edu_rows = self._unt_edu(
            graduation_date=None, graduation_year=2027, is_expected=True
        )
        # job that started recently still overlaps (unt_end = today)
        exp_rows = [
            {
                "company": "Lab",
                "title": "TA",
                "start_date": date(2024, 9, 1),
                "end_date": None,
                "is_current": True,
            }
        ]
        result = computeWorkWhileStudying(1, _get_conn_factory(edu_rows, exp_rows), today=TODAY)
        self.assertTrue(result["worked_while_at_unt"])
        self.assertEqual(result["unt_end"], TODAY)


# ---------------------------------------------------------------------------
# 6. computeWorkWhileStudying — job overlap logic
# ---------------------------------------------------------------------------

class TestJobOverlap(unittest.TestCase):

    def _run(self, exp_rows, edu_overrides=None):
        """Helper: run with a standard UNT window 2018-08-15 → 2022-05-15."""
        base_edu = {
            "school_name": "University of North Texas",
            "school_start_date": date(2018, 8, 15),
            "school_start_year": None,
            "graduation_date": date(2022, 5, 15),
            "graduation_year": 2022,
            "graduation_month": 5,
            "is_expected": False,
        }
        if edu_overrides:
            base_edu.update(edu_overrides)
        return computeWorkWhileStudying(
            1, _get_conn_factory([base_edu], exp_rows), today=TODAY
        )

    # --- Overlapping jobs ---

    def test_job_entirely_within_unt_window(self):
        exp = [{"company": "A", "title": "Dev", "start_date": date(2020, 1, 1), "end_date": date(2021, 1, 1), "is_current": False}]
        result = self._run(exp)
        self.assertTrue(result["worked_while_at_unt"])
        self.assertEqual(len(result["evidence_jobs"]), 1)

    def test_job_starts_before_unt_and_ends_during(self):
        exp = [{"company": "B", "title": "Mgr", "start_date": date(2017, 1, 1), "end_date": date(2019, 6, 1), "is_current": False}]
        result = self._run(exp)
        self.assertTrue(result["worked_while_at_unt"])

    def test_job_starts_during_unt_and_ends_after(self):
        exp = [{"company": "C", "title": "SWE", "start_date": date(2021, 9, 1), "end_date": date(2023, 1, 1), "is_current": False}]
        result = self._run(exp)
        self.assertTrue(result["worked_while_at_unt"])

    def test_job_spans_entire_unt_period(self):
        exp = [{"company": "D", "title": "Eng", "start_date": date(2015, 1, 1), "end_date": date(2024, 1, 1), "is_current": False}]
        result = self._run(exp)
        self.assertTrue(result["worked_while_at_unt"])

    def test_job_starts_on_unt_end_date_is_overlap(self):
        """start_date == unt_end: overlap (boundary inclusive)."""
        exp = [{"company": "E", "title": "PM", "start_date": date(2022, 5, 15), "end_date": date(2023, 1, 1), "is_current": False}]
        result = self._run(exp)
        self.assertTrue(result["worked_while_at_unt"])

    def test_job_ends_on_unt_start_date_is_overlap(self):
        """end_date == unt_start: overlap (boundary inclusive)."""
        exp = [{"company": "F", "title": "TA", "start_date": date(2017, 5, 1), "end_date": date(2018, 8, 15), "is_current": False}]
        result = self._run(exp)
        self.assertTrue(result["worked_while_at_unt"])

    # --- Non-overlapping jobs ---

    def test_job_entirely_before_unt_start(self):
        """Job ends before UNT starts → no overlap."""
        exp = [{"company": "G", "title": "Cashier", "start_date": date(2015, 1, 1), "end_date": date(2018, 8, 14), "is_current": False}]
        result = self._run(exp)
        self.assertFalse(result["worked_while_at_unt"])
        self.assertEqual(result["evidence_jobs"], [])

    def test_job_entirely_after_unt_end(self):
        """Job starts after UNT ends → no overlap."""
        exp = [{"company": "H", "title": "Lead", "start_date": date(2022, 5, 16), "end_date": date(2024, 1, 1), "is_current": False}]
        result = self._run(exp)
        self.assertFalse(result["worked_while_at_unt"])

    # --- Null start_date ---

    def test_job_with_null_start_date_is_skipped(self):
        exp = [{"company": "I", "title": "Unknown", "start_date": None, "end_date": None, "is_current": False}]
        result = self._run(exp)
        self.assertFalse(result["worked_while_at_unt"])
        self.assertEqual(result["evidence_jobs"], [])

    # --- Null end_date (current job) ---

    def test_current_job_with_null_end_overlaps_if_started_during_unt(self):
        """Null end_date → treat job_end as today. If started during UNT, overlaps."""
        exp = [{"company": "J", "title": "Intern", "start_date": date(2021, 6, 1), "end_date": None, "is_current": True}]
        result = self._run(exp)
        self.assertTrue(result["worked_while_at_unt"])
        # The stored end_date in evidence should be None (not today)
        self.assertIsNone(result["evidence_jobs"][0]["end_date"])

    def test_current_job_started_after_unt_does_not_overlap(self):
        """Current job started after graduation → no overlap."""
        exp = [{"company": "K", "title": "SRE", "start_date": date(2023, 1, 1), "end_date": None, "is_current": True}]
        result = self._run(exp)
        self.assertFalse(result["worked_while_at_unt"])

    # --- Multiple jobs ---

    def test_only_overlapping_jobs_in_evidence(self):
        """evidence_jobs contains ONLY jobs that overlap the UNT window."""
        exp = [
            # Overlapping
            {"company": "AlphaCo", "title": "TA",   "start_date": date(2020, 1, 1), "end_date": date(2021, 1, 1), "is_current": False},
            # Not overlapping (after graduation)
            {"company": "BetaCo",  "title": "SWE",  "start_date": date(2023, 1, 1), "end_date": date(2024, 1, 1), "is_current": False},
            # Not overlapping (before start)
            {"company": "GammaCo", "title": "Asst", "start_date": date(2016, 1, 1), "end_date": date(2017, 1, 1), "is_current": False},
        ]
        result = self._run(exp)
        self.assertTrue(result["worked_while_at_unt"])
        self.assertEqual(len(result["evidence_jobs"]), 1)
        self.assertEqual(result["evidence_jobs"][0]["company"], "AlphaCo")

    def test_multiple_overlapping_jobs_all_in_evidence(self):
        exp = [
            {"company": "Co1", "title": "R1", "start_date": date(2019, 1, 1), "end_date": date(2020, 1, 1), "is_current": False},
            {"company": "Co2", "title": "R2", "start_date": date(2021, 1, 1), "end_date": date(2022, 1, 1), "is_current": False},
        ]
        result = self._run(exp)
        self.assertTrue(result["worked_while_at_unt"])
        self.assertEqual(len(result["evidence_jobs"]), 2)

    def test_no_experience_rows(self):
        result = self._run([])
        self.assertFalse(result["worked_while_at_unt"])
        self.assertEqual(result["evidence_jobs"], [])


# ---------------------------------------------------------------------------
# 7. computeWorkWhileStudying — return structure
# ---------------------------------------------------------------------------

class TestReturnStructure(unittest.TestCase):

    def _base_edu(self):
        return {
            "school_name": "University of North Texas",
            "school_start_date": date(2018, 8, 15),
            "school_start_year": None,
            "graduation_date": date(2022, 5, 15),
            "graduation_year": 2022,
            "graduation_month": 5,
            "is_expected": False,
        }

    def test_all_expected_keys_present(self):
        result = computeWorkWhileStudying(1, _get_conn_factory([self._base_edu()], []), today=TODAY)
        expected_keys = {
            "alumni_id", "unt_start", "unt_end",
            "worked_while_at_unt", "graduation_year",
            "graduation_date_used", "graduated_status",
            "is_working_while_studying", "evidence_jobs",
        }
        self.assertEqual(set(result.keys()), expected_keys)

    def test_backwards_compat_is_working_while_studying_matches(self):
        exp = [{"company": "X", "title": "Y", "start_date": date(2020, 1, 1), "end_date": date(2021, 1, 1), "is_current": False}]
        result = computeWorkWhileStudying(1, _get_conn_factory([self._base_edu()], exp), today=TODAY)
        self.assertEqual(result["worked_while_at_unt"], result["is_working_while_studying"])

    def test_database_error_returns_none(self):
        def bad_conn():
            raise RuntimeError("DB down")
        result = computeWorkWhileStudying(1, bad_conn, today=TODAY)
        self.assertIsNone(result)

    def test_evidence_job_fields(self):
        exp = [{"company": "Acme", "title": "Engineer", "start_date": date(2020, 1, 1), "end_date": date(2021, 1, 1), "is_current": False}]
        result = computeWorkWhileStudying(1, _get_conn_factory([self._base_edu()], exp), today=TODAY)
        job = result["evidence_jobs"][0]
        self.assertIn("company", job)
        self.assertIn("title", job)
        self.assertIn("start_date", job)
        self.assertIn("end_date", job)


# ---------------------------------------------------------------------------
# 8. ensure_work_while_studying_schema
# ---------------------------------------------------------------------------

class TestEnsureSchema(unittest.TestCase):

    def test_schema_creation_succeeds(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_conn.cursor.return_value.__exit__.return_value = None

        result = ensure_work_while_studying_schema(lambda: mock_conn)

        self.assertTrue(result)
        mock_conn.commit.assert_called_once()
        # CREATE TABLE IF NOT EXISTS should be called twice (education + experience)
        create_calls = [
            call for call in mock_cur.execute.call_args_list
            if "CREATE TABLE" in str(call)
        ]
        self.assertEqual(len(create_calls), 2)

    def test_schema_creation_handles_connection_error(self):
        def bad():
            raise RuntimeError("no DB")
        result = ensure_work_while_studying_schema(bad)
        self.assertFalse(result)


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(verbosity=2)
