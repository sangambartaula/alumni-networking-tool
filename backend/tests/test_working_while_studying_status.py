import unittest

from backend.working_while_studying_status import (
    is_unt_employer,
    is_unt_school_name,
    recompute_working_while_studying_status,
    status_to_bool,
)


def _base_row():
    return {
        "grad_year": None,
        "school": "University of North Texas",
        "school2": None,
        "school3": None,
        "current_job_title": None,
        "company": None,
        "job_start_date": None,
        "job_end_date": None,
        "exp2_title": None,
        "exp2_company": None,
        "exp2_dates": None,
        "exp3_title": None,
        "exp3_company": None,
        "exp3_dates": None,
    }


class WorkingWhileStudyingStatusTests(unittest.TestCase):
    def test_is_unt_school_name(self):
        self.assertTrue(is_unt_school_name("University of North Texas"))
        self.assertTrue(is_unt_school_name("UNT"))
        self.assertFalse(is_unt_school_name("University of Houston"))

    def test_is_unt_employer_word_boundary(self):
        self.assertTrue(is_unt_employer("University of North Texas"))
        self.assertTrue(is_unt_employer("UNT Libraries"))
        self.assertFalse(is_unt_employer("HUNT Oil Company"))
        self.assertFalse(is_unt_employer("University of Houston"))

    def test_missing_dates_fallback_yes_for_unt_ga(self):
        row = _base_row()
        row["current_job_title"] = "Graduate Research Assistant"
        row["company"] = "UNT Libraries"
        self.assertEqual(recompute_working_while_studying_status(row), "yes")

    def test_missing_dates_fallback_no_for_non_unt_employer(self):
        row = _base_row()
        row["current_job_title"] = "Graduate Assistant"
        row["company"] = "University of Houston"
        self.assertEqual(recompute_working_while_studying_status(row), "no")

    def test_missing_dates_fallback_no_when_raw_company_missing(self):
        row = _base_row()
        row["current_job_title"] = "Graduate Assistant"
        row["company"] = ""
        self.assertEqual(recompute_working_while_studying_status(row), "no")

    def test_missing_dates_fallback_no_without_unt_education(self):
        row = _base_row()
        row["school"] = "University of Houston"
        row["current_job_title"] = "Graduate Assistant"
        row["company"] = "University of North Texas"
        self.assertEqual(recompute_working_while_studying_status(row), "no")

    def test_computable_false_not_overridden(self):
        row = _base_row()
        row["grad_year"] = 2020
        row["job_start_date"] = "2022"
        row["job_end_date"] = "Present"
        row["current_job_title"] = "Graduate Research Assistant"
        row["company"] = "University of North Texas"
        self.assertEqual(recompute_working_while_studying_status(row), "no")

    def test_computable_true_not_changed(self):
        row = _base_row()
        row["grad_year"] = 2024
        row["job_start_date"] = "2022"
        row["job_end_date"] = "2023"
        self.assertEqual(recompute_working_while_studying_status(row), "yes")

    def test_status_to_bool(self):
        self.assertTrue(status_to_bool("yes"))
        self.assertTrue(status_to_bool("currently"))
        self.assertFalse(status_to_bool("no"))
        self.assertIsNone(status_to_bool(""))


if __name__ == "__main__":
    unittest.main()
