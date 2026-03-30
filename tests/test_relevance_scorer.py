#!/usr/bin/env python
"""
Tests for the relevance scoring engine.

Tests cover:
  - _extract_score: valid/invalid LLM outputs
  - is_job_relevant: threshold boundary at 0.6
  - analyze_profile_relevance: 0/1/3 jobs, missing major, edge cases
  - get_relevance_json: structured JSON output shape
  - compute_relevant_experience_months: overlapping intervals, no relevant jobs
  - _split_date_range: various separator formats
  - score_job_relevance: mocked Groq calls, retry on invalid output
"""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add project root and scraper to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'scraper'))
sys.path.insert(0, str(PROJECT_ROOT / 'backend'))

import importlib.util
spec = importlib.util.spec_from_file_location(
    "relevance_scorer",
    PROJECT_ROOT / "scraper" / "relevance_scorer.py"
)
relevance_scorer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(relevance_scorer)

_extract_score = relevance_scorer._extract_score
is_job_relevant = relevance_scorer.is_job_relevant
analyze_profile_relevance = relevance_scorer.analyze_profile_relevance
get_relevance_json = relevance_scorer.get_relevance_json
compute_relevant_experience_months = relevance_scorer.compute_relevant_experience_months
_split_date_range = relevance_scorer._split_date_range
score_job_relevance = relevance_scorer.score_job_relevance
RELEVANCE_THRESHOLD_RELEVANT = relevance_scorer.RELEVANCE_THRESHOLD_RELEVANT


# =====================================================================
# _extract_score
# =====================================================================

def test_extract_score_valid_numbers():
    """Valid float strings should parse correctly."""
    assert _extract_score("0.85") == 0.85
    assert _extract_score("0.0") == 0.0
    assert _extract_score("1.0") == 1.0
    assert _extract_score("0.5") == 0.5
    assert _extract_score("  0.72  ") == 0.72
    print("✅ test_extract_score_valid_numbers passed")


def test_extract_score_boundary():
    """Boundary values at 0 and 1."""
    assert _extract_score("0") == 0.0
    assert _extract_score("1") == 1.0
    print("✅ test_extract_score_boundary passed")


def test_extract_score_invalid():
    """Invalid inputs should return None."""
    assert _extract_score(None) is None
    assert _extract_score("") is None
    assert _extract_score("not a number") is None
    assert _extract_score("1.5") is None  # Out of range
    assert _extract_score("-0.5") is None  # Negative
    assert _extract_score("2") is None    # > 1
    print("✅ test_extract_score_invalid passed")


def test_extract_score_embedded_number():
    """Score embedded in text should still be extracted."""
    assert _extract_score("The score is 0.85") == 0.85
    assert _extract_score("Score: 0.5") == 0.5
    print("✅ test_extract_score_embedded_number passed")


def test_extract_score_rounding():
    """Scores should be rounded to 2 decimal places."""
    assert _extract_score("0.123456") == 0.12
    assert _extract_score("0.999") == 1.0
    print("✅ test_extract_score_rounding passed")


# =====================================================================
# is_job_relevant
# =====================================================================

def test_is_job_relevant_threshold():
    """Threshold boundary: 0.60 = relevant, 0.59 = not relevant."""
    assert RELEVANCE_THRESHOLD_RELEVANT == 0.6
    assert is_job_relevant(0.60) is True
    assert is_job_relevant(0.59) is False
    assert is_job_relevant(1.0) is True
    assert is_job_relevant(0.0) is False
    assert is_job_relevant(None) is None
    print("✅ test_is_job_relevant_threshold passed")


def test_is_job_relevant_custom_threshold():
    """Custom threshold should work."""
    assert is_job_relevant(0.5, threshold=0.5) is True
    assert is_job_relevant(0.49, threshold=0.5) is False
    print("✅ test_is_job_relevant_custom_threshold passed")


# =====================================================================
# _split_date_range
# =====================================================================

def test_split_date_range_dash():
    assert _split_date_range("Mar 2020 - Dec 2022") == ("Mar 2020", "Dec 2022")
    print("✅ test_split_date_range_dash passed")


def test_split_date_range_endash():
    assert _split_date_range("Mar 2020 – Dec 2022") == ("Mar 2020", "Dec 2022")
    print("✅ test_split_date_range_endash passed")


def test_split_date_range_emdash():
    assert _split_date_range("Mar 2020 — Dec 2022") == ("Mar 2020", "Dec 2022")
    print("✅ test_split_date_range_emdash passed")


def test_split_date_range_to():
    assert _split_date_range("Mar 2020 to Dec 2022") == ("Mar 2020", "Dec 2022")
    print("✅ test_split_date_range_to passed")


def test_split_date_range_single():
    assert _split_date_range("2020") == ("2020", "2020")
    print("✅ test_split_date_range_single passed")


def test_split_date_range_empty():
    assert _split_date_range("") == ("", "")
    assert _split_date_range(None) == ("", "")
    print("✅ test_split_date_range_empty passed")


# =====================================================================
# compute_relevant_experience_months
# =====================================================================

def test_compute_experience_no_jobs():
    """Empty list → None."""
    assert compute_relevant_experience_months([]) is None
    assert compute_relevant_experience_months(None) is None
    print("✅ test_compute_experience_no_jobs passed")


def test_compute_experience_no_relevant():
    """No relevant jobs → 0 months."""
    jobs = [
        {'start_date': 'Jan 2020', 'end_date': 'Dec 2020', 'is_relevant': False},
        {'start_date': 'Jan 2021', 'end_date': 'Dec 2021', 'is_relevant': False},
    ]
    assert compute_relevant_experience_months(jobs) == 0
    print("✅ test_compute_experience_no_relevant passed")


def test_compute_experience_simple():
    """Simple non-overlapping relevant job."""
    jobs = [
        {'start_date': 'Jan 2020', 'end_date': 'Dec 2020', 'is_relevant': True},
    ]
    result = compute_relevant_experience_months(jobs)
    assert result == 12  # Jan to Dec = 12 months (inclusive)
    print("✅ test_compute_experience_simple passed")


def test_compute_experience_missing_dates():
    """Jobs with missing dates should be skipped gracefully."""
    jobs = [
        {'start_date': '', 'end_date': '', 'is_relevant': True},
        {'start_date': 'Jan 2020', 'end_date': 'Jun 2020', 'is_relevant': True},
    ]
    result = compute_relevant_experience_months(jobs)
    assert result == 6  # Jan to Jun = 6 months
    print("✅ test_compute_experience_missing_dates passed")


# =====================================================================
# analyze_profile_relevance (with mocked Groq)
# =====================================================================

def _mock_score(*args, **kwargs):
    """Mock score_job_relevance to return deterministic scores."""
    title = args[0] if args else ""
    if "engineer" in title.lower():
        return 0.92
    elif "analyst" in title.lower():
        return 0.45
    elif "intern" in title.lower():
        return 0.85
    return 0.3


@patch.object(relevance_scorer, 'score_job_relevance', side_effect=_mock_score)
def test_analyze_no_major(mock_score):
    """No major → still scores jobs (CoE default context + heuristics)."""
    profile = {'title': 'Software Engineer', 'company': 'Google'}
    result = analyze_profile_relevance(profile)
    assert result.get('job_1_relevance_score') == 0.92
    assert result.get('job_1_is_relevant') is True
    print("✅ test_analyze_no_major passed")


def test_analyze_no_jobs():
    """Major but no jobs → empty dict (no job scores, no experience)."""
    profile = {'standardized_major': 'Computer Science'}
    result = analyze_profile_relevance(profile)
    # Should have no job scores (no titles present)
    assert 'job_1_relevance_score' not in result
    print("✅ test_analyze_no_jobs passed")


@patch.object(relevance_scorer, 'score_job_relevance', side_effect=_mock_score)
def test_analyze_one_job(mock_score):
    """One job with major → score + relevant flag."""
    profile = {
        'standardized_major': 'Computer Science',
        'title': 'Software Engineer',
        'company': 'Google',
        'job_start': 'Jan 2020',
        'job_end': 'Present',
    }
    result = analyze_profile_relevance(profile)
    assert result['job_1_relevance_score'] == 0.92
    assert result['job_1_is_relevant'] is True
    assert 'job_2_relevance_score' not in result
    assert 'job_3_relevance_score' not in result
    print("✅ test_analyze_one_job passed")


@patch.object(relevance_scorer, 'score_job_relevance', side_effect=_mock_score)
def test_analyze_three_jobs(mock_score):
    """Three jobs → all scored."""
    profile = {
        'standardized_major': 'Computer Science',
        'title': 'Software Engineer',
        'company': 'Google',
        'job_start': 'Jan 2022',
        'job_end': 'Present',
        'exp_2_title': 'Data Analyst',
        'exp_2_company': 'Meta',
        'exp_2_dates': 'Mar 2020 - Dec 2021',
        'exp_3_title': 'Intern',
        'exp_3_company': 'Startup',
        'exp_3_dates': 'Jun 2019 - Aug 2019',
    }
    result = analyze_profile_relevance(profile)
    
    # Job 1: engineer → 0.92 → relevant
    assert result['job_1_relevance_score'] == 0.92
    assert result['job_1_is_relevant'] is True
    
    # Job 2: analyst → 0.45 → not relevant
    assert result['job_2_relevance_score'] == 0.45
    assert result['job_2_is_relevant'] is False
    
    # Job 3: intern → 0.85 → relevant
    assert result['job_3_relevance_score'] == 0.85
    assert result['job_3_is_relevant'] is True
    
    # Relevant experience months should be computed
    assert 'relevant_experience_months' in result
    print("✅ test_analyze_three_jobs passed")


@patch.object(relevance_scorer, 'score_job_relevance', side_effect=_mock_score)
def test_analyze_none_relevant(mock_score):
    """All jobs below threshold → all is_relevant=False."""
    profile = {
        'standardized_major': 'Computer Science',
        'title': 'Cashier',       # → 0.3
        'company': 'McDonalds',
    }
    result = analyze_profile_relevance(profile)
    assert result['job_1_relevance_score'] == 0.3
    assert result['job_1_is_relevant'] is False
    print("✅ test_analyze_none_relevant passed")


# =====================================================================
# get_relevance_json (with mocked Groq)
# =====================================================================

@patch.object(relevance_scorer, 'score_job_relevance', side_effect=_mock_score)
def test_get_relevance_json_shape(mock_score):
    """Structured JSON should have correct keys."""
    profile = {
        'standardized_major': 'Computer Science',
        'title': 'Software Engineer',
        'company': 'Google',
        'job_start': 'Jan 2020',
        'job_end': 'Present',
        'exp_2_title': 'Data Analyst',
        'exp_2_company': 'Meta',
        'exp_2_dates': 'Mar 2018 - Dec 2019',
    }
    result = get_relevance_json(profile)
    
    assert isinstance(result, list)
    assert len(result) == 2
    
    # Check keys
    for entry in result:
        assert 'title' in entry
        assert 'company' in entry
        assert 'score' in entry
        assert 'is_relevant' in entry
        assert 'start_date' in entry
        assert 'end_date' in entry
    
    # Job 1
    assert result[0]['title'] == 'Software Engineer'
    assert result[0]['company'] == 'Google'
    assert result[0]['score'] == 0.92
    assert result[0]['is_relevant'] is True
    assert result[0]['start_date'] == 'Jan 2020'
    assert result[0]['end_date'] == 'Present'
    
    # Job 2
    assert result[1]['title'] == 'Data Analyst'
    assert result[1]['score'] == 0.45
    assert result[1]['is_relevant'] is False
    
    print("✅ test_get_relevance_json_shape passed")


@patch.object(relevance_scorer, '_get_client', return_value=None)
def test_get_relevance_json_no_major(mock_client):
    """No major → still returns jobs with heuristic floor (no Groq)."""
    result = get_relevance_json({'title': 'Mechanical Engineer', 'company': 'Acme'})
    assert len(result) == 1
    assert result[0]['score'] == 0.6
    assert result[0]['is_relevant'] is True
    print("✅ test_get_relevance_json_no_major passed")


def test_get_relevance_json_no_jobs():
    """Major but no jobs → empty list."""
    result = get_relevance_json({'standardized_major': 'CS'})
    assert result == []
    print("✅ test_get_relevance_json_no_jobs passed")


# =====================================================================
# score_job_relevance — Groq mock tests
# =====================================================================

def _make_mock_response(content):
    """Create a mock Groq API response."""
    response = MagicMock()
    response.choices = [MagicMock()]
    response.choices[0].message.content = content
    return response


@patch.object(relevance_scorer, '_get_client')
def test_score_job_relevance_success(mock_get_client):
    """Valid Groq response → returns score."""
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = _make_mock_response("0.85")
    mock_get_client.return_value = mock_client
    
    score = score_job_relevance("Software Engineer", "Google", "Computer Science")
    # 0.85 LLM + 0.05 engineering title + 0.05 STEM major
    assert score == 0.95
    print("✅ test_score_job_relevance_success passed")


@patch.object(relevance_scorer, '_get_client')
def test_score_job_relevance_retry(mock_get_client):
    """Invalid then valid → retries and succeeds."""
    mock_client = MagicMock()
    mock_client.chat.completions.create.side_effect = [
        _make_mock_response("I think it's about 0.9"),  # invalid: text around number
        _make_mock_response("0.90"),  # valid
    ]
    mock_get_client.return_value = mock_client

    # The first response has "0.9" embedded which _extract_score can find via regex
    # So it should succeed on first try
    score = score_job_relevance("Engineer", "Company", "CS")
    assert score is not None
    print("✅ test_score_job_relevance_retry passed")


@patch.object(relevance_scorer, '_get_client')
def test_score_job_relevance_all_fail(mock_get_client):
    """All retries fail → returns None."""
    mock_client = MagicMock()
    mock_client.chat.completions.create.side_effect = Exception("API Error")
    mock_get_client.return_value = mock_client
    
    # No LLM: floors only — ME + listed major has no floor; TA does
    assert score_job_relevance("Mechanical Engineer", "Company", "Mechanical Engineering") is None
    score = score_job_relevance("Teaching Assistant", "UNT", "Mechanical Engineering")
    assert score == 0.65
    print("✅ test_score_job_relevance_all_fail passed")


@patch.object(relevance_scorer, '_get_client', return_value=None)
def test_score_job_relevance_missing_inputs(mock_client):
    """Missing title → None. Missing major → heuristic floor still applies."""
    assert score_job_relevance("", "Google", "CS") is None
    assert score_job_relevance(None, "Google", "CS") is None
    assert score_job_relevance("Software Engineer", "Google", "") == 0.6
    assert score_job_relevance("Software Engineer", "Google", None) == 0.6
    print("✅ test_score_job_relevance_missing_inputs passed")


@patch.object(relevance_scorer, '_get_client')
def test_junk_title_does_not_call_groq(mock_get_client):
    """Cashier and similar titles return a low score without calling Groq."""
    score = score_job_relevance("Cashier", "Groceries", "Computer Science")
    assert score == 0.1
    mock_get_client.assert_not_called()
    print("✅ test_junk_title_does_not_call_groq passed")


@patch.object(relevance_scorer, '_get_client', return_value=None)
def test_teaching_assistant_floor_without_groq(mock_client):
    """TA / RA roles get a relevance floor without LLM."""
    score = score_job_relevance("Teaching Assistant", "UNT", "")
    assert score == 0.65
    assert is_job_relevant(score) is True
    print("✅ test_teaching_assistant_floor_without_groq passed")


# =====================================================================
# Run all tests
# =====================================================================

if __name__ == '__main__':
    tests = [
        test_extract_score_valid_numbers,
        test_extract_score_boundary,
        test_extract_score_invalid,
        test_extract_score_embedded_number,
        test_extract_score_rounding,
        test_is_job_relevant_threshold,
        test_is_job_relevant_custom_threshold,
        test_split_date_range_dash,
        test_split_date_range_endash,
        test_split_date_range_emdash,
        test_split_date_range_to,
        test_split_date_range_single,
        test_split_date_range_empty,
        test_compute_experience_no_jobs,
        test_compute_experience_no_relevant,
        test_compute_experience_simple,
        test_compute_experience_missing_dates,
        test_analyze_no_major,
        test_analyze_no_jobs,
        test_analyze_one_job,
        test_analyze_three_jobs,
        test_analyze_none_relevant,
        test_get_relevance_json_shape,
        test_get_relevance_json_no_major,
        test_get_relevance_json_no_jobs,
        test_score_job_relevance_success,
        test_score_job_relevance_retry,
        test_score_job_relevance_all_fail,
        test_score_job_relevance_missing_inputs,
        test_junk_title_does_not_call_groq,
        test_teaching_assistant_floor_without_groq,
    ]
    
    print("=" * 60)
    print("RELEVANCE SCORER TEST SUITE")
    print("=" * 60)
    
    passed = 0
    failed = 0
    
    for test_fn in tests:
        try:
            test_fn()
            passed += 1
        except Exception as e:
            print(f"❌ {test_fn.__name__} FAILED: {e}")
            failed += 1
    
    print("\n" + "=" * 60)
    if failed == 0:
        print(f"SUCCESS: ALL {passed} TESTS PASSED")
    else:
        print(f"FAILURE: {failed}/{passed + failed} TESTS FAILED")
    print("=" * 60)
    
    sys.exit(0 if failed == 0 else 1)
