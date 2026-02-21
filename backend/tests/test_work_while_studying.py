"""
Comprehensive tests for the work_while_studying module.

Tests cover:
- Missing graduation info
- Missing start_date
- Current jobs (is_current=true)
- Null end_dates
- Multiple jobs with mixed statuses
- Edge cases with date calculations
"""

import unittest
from datetime import datetime, date
from unittest.mock import Mock, MagicMock, patch, call
from io import StringIO
import sys

# Import the module under test
from work_while_studying import (
    _get_graduation_date,
    _get_graduated_status,
    _classify_job_experience,
    computeWorkWhileStudying,
    ensure_work_while_studying_schema
)


class TestGetGraduationDate(unittest.TestCase):
    """Test graduation date calculation logic."""
    
    def test_prefer_graduation_date_if_exists(self):
        """Rule 1: Prefer graduation_date if it exists."""
        grad_date = date(2020, 6, 1)
        result = _get_graduation_date(grad_date, 2020, 5)
        self.assertEqual(result, grad_date)
    
    def test_fallback_to_year_with_may_15(self):
        """Rule 2: Use May 15 if only year is provided."""
        result = _get_graduation_date(None, 2021, None)
        expected = date(2021, 5, 15)
        self.assertEqual(result, expected)
    
    def test_ignore_month_without_date_and_year(self):
        """Month without year should be ignored."""
        result = _get_graduation_date(None, None, 5)
        self.assertIsNone(result)
    
    def test_return_none_if_all_null(self):
        """Return None if no graduation info."""
        result = _get_graduation_date(None, None, None)
        self.assertIsNone(result)
    
    def test_handle_invalid_year(self):
        """Handle invalid year gracefully."""
        result = _get_graduation_date(None, "invalid", None)
        self.assertIsNone(result)
    
    def test_handle_year_zero(self):
        """Handle edge case of year 0."""
        result = _get_graduation_date(None, 0, None)
        self.assertIsNone(result)


class TestGetGraduatedStatus(unittest.TestCase):
    """Test graduated status determination."""
    
    def test_is_expected_true_means_not_yet_graduated(self):
        """Rule 1: is_expected=true → not_yet_graduated."""
        result = _get_graduated_status(
            graduation_year=2025,
            graduation_date=None,
            is_expected=True,
            current_year=2024
        )
        self.assertEqual(result, "not_yet_graduated")
    
    def test_future_year_means_not_yet_graduated(self):
        """Rule 2: graduation_year > current_year → not_yet_graduated."""
        result = _get_graduated_status(
            graduation_year=2026,
            graduation_date=None,
            is_expected=False,
            current_year=2024
        )
        self.assertEqual(result, "not_yet_graduated")
    
    def test_past_year_means_graduated(self):
        """Past graduation year with no is_expected flag → graduated."""
        result = _get_graduated_status(
            graduation_year=2020,
            graduation_date=None,
            is_expected=False,
            current_year=2024
        )
        self.assertEqual(result, "graduated")
    
    def test_current_year_equals_graduation_year_means_graduated(self):
        """When current_year == graduation_year → graduated."""
        result = _get_graduated_status(
            graduation_year=2024,
            graduation_date=None,
            is_expected=False,
            current_year=2024
        )
        self.assertEqual(result, "graduated")
    
    def test_graduation_date_with_no_year_means_graduated(self):
        """If graduation_date exists but no year → graduated."""
        result = _get_graduated_status(
            graduation_year=None,
            graduation_date=date(2020, 6, 1),
            is_expected=False,
            current_year=2024
        )
        self.assertEqual(result, "graduated")
    
    def test_no_info_means_unknown(self):
        """No graduation info → unknown."""
        result = _get_graduated_status(
            graduation_year=None,
            graduation_date=None,
            is_expected=False,
            current_year=2024
        )
        self.assertEqual(result, "unknown")
    
    def test_unknown_status_ignores_date_comparison(self):
        """Unknown status takes precedence over date checks."""
        result = _get_graduated_status(
            graduation_year=None,
            graduation_date=date(2020, 6, 1),
            is_expected=None,  # None, not False
            current_year=2024
        )
        # Should still be "graduated" because graduation_date exists
        self.assertEqual(result, "graduated")


class TestClassifyJobExperience(unittest.TestCase):
    """Test job experience classification."""
    
    def test_job_starts_before_graduation_worked_while_studying(self):
        """Job start < graduation date → worked_while_studying."""
        result = _classify_job_experience(
            date(2020, 1, 15),
            date(2020, 6, 1)
        )
        self.assertEqual(result, "worked_while_studying")
    
    def test_job_starts_after_graduation_worked_after(self):
        """Job start > graduation date → worked_after_graduation."""
        result = _classify_job_experience(
            date(2020, 8, 15),
            date(2020, 6, 1)
        )
        self.assertEqual(result, "worked_after_graduation")
    
    def test_job_starts_on_graduation_date_worked_after(self):
        """Job start == graduation date → worked_after_graduation."""
        grad_date = date(2020, 6, 1)
        result = _classify_job_experience(grad_date, grad_date)
        self.assertEqual(result, "worked_after_graduation")
    
    def test_null_start_date_cannot_classify(self):
        """Null start_date → cannot classify (returns None)."""
        result = _classify_job_experience(None, date(2020, 6, 1))
        self.assertIsNone(result)
    
    def test_null_graduation_date_cannot_classify(self):
        """Null graduation_date → cannot classify (returns None)."""
        result = _classify_job_experience(date(2020, 1, 15), None)
        self.assertIsNone(result)
    
    def test_both_dates_null_cannot_classify(self):
        """Both dates null → cannot classify."""
        result = _classify_job_experience(None, None)
        self.assertIsNone(result)


class TestComputeWorkWhileStudying(unittest.TestCase):
    """Integration tests for computeWorkWhileStudying function."""
    
    def _mock_connection(self, edu_record=None, exp_records=None):
        """Create a mock database connection."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Set up the context manager for cursor
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = None
        
        # Configure cursor to return education record, then experience records
        mock_cursor.fetchone.return_value = edu_record
        mock_cursor.fetchall.return_value = exp_records or []
        
        return mock_conn
    
    def test_user_not_found_returns_unknown_status(self):
        """User with no education record → unknown status."""
        mock_conn = self._mock_connection(edu_record=None)
        mock_get_conn = lambda: mock_conn
        
        result = computeWorkWhileStudying(999, mock_get_conn)
        
        self.assertIsNotNone(result)
        self.assertEqual(result["user_id"], 999)
        self.assertEqual(result["graduated_status"], "unknown")
        self.assertFalse(result["is_working_while_studying"])
        self.assertEqual(result["evidence_jobs"], [])
    
    def test_successful_result_structure(self):
        """Result has correct structure for valid user."""
        edu_record = {
            "graduation_year": 2020,
            "graduation_month": 5,
            "graduation_date": None,
            "is_expected": False
        }
        exp_records = [
            {
                "company": "Tech Corp",
                "title": "Engineer",
                "start_date": date(2019, 1, 15),
                "end_date": None,
                "is_current": False
            }
        ]
        
        mock_conn = self._mock_connection(edu_record, exp_records)
        mock_get_conn = lambda: mock_conn
        
        result = computeWorkWhileStudying(1, mock_get_conn)
        
        self.assertEqual(result["user_id"], 1)
        self.assertEqual(result["graduation_year"], 2020)
        self.assertEqual(result["graduation_date_used"], date(2020, 5, 15))
        self.assertEqual(result["graduated_status"], "graduated")
        self.assertTrue(result["is_working_while_studying"])
        self.assertEqual(len(result["evidence_jobs"]), 1)
        self.assertEqual(result["evidence_jobs"][0]["status"], "worked_while_studying")
    
    def test_null_start_date_excluded_from_evidence(self):
        """Jobs with null start_date excluded from evidence."""
        edu_record = {
            "graduation_year": 2020,
            "graduation_month": 5,
            "graduation_date": None,
            "is_expected": False
        }
        exp_records = [
            {
                "company": "Unknown Job",
                "title": "Unknown",
                "start_date": None,  # NULL start_date
                "end_date": None,
                "is_current": False
            }
        ]
        
        mock_conn = self._mock_connection(edu_record, exp_records)
        mock_get_conn = lambda: mock_conn
        
        result = computeWorkWhileStudying(1, mock_get_conn)
        
        # Job should not be in evidence_jobs
        self.assertEqual(result["evidence_jobs"], [])
        self.assertFalse(result["is_working_while_studying"])
    
    def test_multiple_jobs_mixed_statuses(self):
        """Multiple jobs with different statuses handled correctly."""
        edu_record = {
            "graduation_year": 2020,
            "graduation_month": 5,
            "graduation_date": None,
            "is_expected": False
        }
        exp_records = [
            {
                "company": "Tech Corp",
                "title": "Engineer",
                "start_date": date(2019, 1, 15),  # Before graduation
                "end_date": date(2020, 5, 1),
                "is_current": False
            },
            {
                "company": "Google",
                "title": "Senior Engineer",
                "start_date": date(2021, 1, 1),  # After graduation
                "end_date": None,
                "is_current": True
            }
        ]
        
        mock_conn = self._mock_connection(edu_record, exp_records)
        mock_get_conn = lambda: mock_conn
        
        result = computeWorkWhileStudying(1, mock_get_conn)
        
        self.assertTrue(result["is_working_while_studying"])
        self.assertEqual(len(result["evidence_jobs"]), 2)
        self.assertEqual(result["evidence_jobs"][0]["status"], "worked_while_studying")
        self.assertEqual(result["evidence_jobs"][1]["status"], "worked_after_graduation")
    
    def test_is_expected_true_not_yet_graduated(self):
        """User with is_expected=true → not_yet_graduated."""
        edu_record = {
            "graduation_year": 2025,
            "graduation_month": 5,
            "graduation_date": None,
            "is_expected": True
        }
        exp_records = []
        
        mock_conn = self._mock_connection(edu_record, exp_records)
        mock_get_conn = lambda: mock_conn
        
        result = computeWorkWhileStudying(1, mock_get_conn)
        
        self.assertEqual(result["graduated_status"], "not_yet_graduated")
    
    def test_current_job_with_null_end_date(self):
        """Current job with null end_date handled correctly."""
        edu_record = {
            "graduation_year": 2020,
            "graduation_month": 5,
            "graduation_date": None,
            "is_expected": False
        }
        exp_records = [
            {
                "company": "Current Employer",
                "title": "Manager",
                "start_date": date(2021, 6, 1),
                "end_date": None,  # Null for current job
                "is_current": True
            }
        ]
        
        mock_conn = self._mock_connection(edu_record, exp_records)
        mock_get_conn = lambda: mock_conn
        
        result = computeWorkWhileStudying(1, mock_get_conn)
        
        self.assertEqual(len(result["evidence_jobs"]), 1)
        self.assertIsNone(result["evidence_jobs"][0]["end_date"])
        self.assertEqual(result["evidence_jobs"][0]["status"], "worked_after_graduation")
    
    def test_no_classifiable_jobs_is_false(self):
        """If all jobs have null start_date, is_working_while_studying=false."""
        edu_record = {
            "graduation_year": 2020,
            "graduation_month": 5,
            "graduation_date": None,
            "is_expected": False
        }
        exp_records = [
            {
                "company": "Job 1",
                "title": "Role 1",
                "start_date": None,
                "end_date": None,
                "is_current": False
            },
            {
                "company": "Job 2",
                "title": "Role 2",
                "start_date": None,
                "end_date": None,
                "is_current": False
            }
        ]
        
        mock_conn = self._mock_connection(edu_record, exp_records)
        mock_get_conn = lambda: mock_conn
        
        result = computeWorkWhileStudying(1, mock_get_conn)
        
        self.assertFalse(result["is_working_while_studying"])
        self.assertEqual(result["evidence_jobs"], [])
    
    def test_database_error_returns_none(self):
        """Database error → function returns None."""
        mock_get_conn = lambda: (_ for _ in ()).throw(Exception("DB Connection Failed"))
        
        result = computeWorkWhileStudying(1, mock_get_conn)
        
        self.assertIsNone(result)


class TestEnsureSchema(unittest.TestCase):
    """Test schema initialization."""
    
    def test_ensure_schema_creates_tables(self):
        """ensure_work_while_studying_schema creates required tables."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = None
        
        mock_get_conn = lambda: mock_conn
        
        result = ensure_work_while_studying_schema(mock_get_conn)
        
        self.assertTrue(result)
        # Verify cursor.execute was called twice (education and experience tables)
        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_conn.commit.assert_called_once()
    
    def test_ensure_schema_handles_error(self):
        """ensure_work_while_studying_schema handles exceptions."""
        def failing_get_conn():
            raise Exception("Connection failed")
        
        result = ensure_work_while_studying_schema(failing_get_conn)
        
        self.assertFalse(result)


# ============================================================
# EDGE CASES & BOUNDARY TESTS
# ============================================================

class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""
    
    def test_leap_year_date(self):
        """Handle leap year dates correctly."""
        grad_date = date(2020, 2, 29)  # Leap year
        result = _get_graduation_date(grad_date, None, None)
        self.assertEqual(result, grad_date)
    
    def test_year_2000_y2k(self):
        """Handle year 2000."""
        result = _get_graduation_date(None, 2000, None)
        self.assertEqual(result, date(2000, 5, 15))
    
    def test_far_future_year(self):
        """Handle far future year."""
        result = _get_graduated_status(
            graduation_year=2100,
            graduation_date=None,
            is_expected=False,
            current_year=2024
        )
        self.assertEqual(result, "not_yet_graduated")
    
    def test_empty_experience_records(self):
        """User with no experience records."""
        edu_record = {
            "graduation_year": 2020,
            "graduation_month": 5,
            "graduation_date": None,
            "is_expected": False
        }
        exp_records = []
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = None
        
        mock_cursor.fetchone.return_value = edu_record
        mock_cursor.fetchall.return_value = []
        
        mock_get_conn = lambda: mock_conn
        
        result = computeWorkWhileStudying(1, mock_get_conn)
        
        self.assertFalse(result["is_working_while_studying"])
        self.assertEqual(result["evidence_jobs"], [])


if __name__ == "__main__":
    # Run tests with verbose output
    unittest.main(verbosity=2)
