"""
Tests for degree normalization logic.
Run with: python3 tests/test_degree_normalization.py
"""

import sys
import os

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "backend"))

from backend.degree_normalization import normalize_degree_deterministic


def test_exact_matches():
    """Test exact canonical lookup."""
    assert normalize_degree_deterministic("Bachelor of Science") == "Bachelor of Science"
    assert normalize_degree_deterministic("Master of Science") == "Master of Science"
    assert normalize_degree_deterministic("Doctor of Philosophy") == "Doctor of Philosophy"
    assert normalize_degree_deterministic("Master of Business Administration") == "Master of Business Administration"
    print("  ✓ Exact matches")


def test_abbreviations():
    """Test abbreviation normalization."""
    assert normalize_degree_deterministic("BS") == "Bachelor of Science"
    assert normalize_degree_deterministic("B.S.") == "Bachelor of Science"
    assert normalize_degree_deterministic("BA") == "Bachelor of Arts"
    assert normalize_degree_deterministic("MS") == "Master of Science"
    assert normalize_degree_deterministic("M.S.") == "Master of Science"
    assert normalize_degree_deterministic("MBA") == "Master of Business Administration"
    assert normalize_degree_deterministic("PhD") == "Doctor of Philosophy"
    assert normalize_degree_deterministic("Ph.D.") == "Doctor of Philosophy"
    print("  ✓ Abbreviations")


def test_prefix_extraction():
    """Test degree prefix before 'in' or comma."""
    assert normalize_degree_deterministic("BS in Computer Science") == "Bachelor of Science"
    assert normalize_degree_deterministic("M.S. in Electrical Engineering") == "Master of Science"
    assert normalize_degree_deterministic("Bachelor of Science, Computer Science") == "Bachelor of Science"
    assert normalize_degree_deterministic("Bachelor of Arts - English") == "Bachelor of Arts"
    print("  ✓ Prefix extraction")


def test_specialty_abbreviations():
    """Test specialty degree abbreviations."""
    assert normalize_degree_deterministic("BSME") == "Bachelor of Science in Mechanical Engineering"
    assert normalize_degree_deterministic("BSEE") == "Bachelor of Science in Electrical Engineering"
    assert normalize_degree_deterministic("BSCS") == "Bachelor of Science in Computer Science"
    print("  ✓ Specialty abbreviations")


def test_empty_and_none():
    """Test empty and None inputs."""
    assert normalize_degree_deterministic("") == ""
    assert normalize_degree_deterministic(None) == ""
    assert normalize_degree_deterministic("   ") == ""
    print("  ✓ Empty/None handling")


def test_idempotency():
    """Test that normalizing a normalized result gives the same result."""
    for raw in ["BS", "M.S.", "PhD", "MBA", "Bachelor of Science in CS"]:
        first = normalize_degree_deterministic(raw)
        second = normalize_degree_deterministic(first)
        assert first == second, f"Not idempotent: {raw} → {first} → {second}"
    print("  ✓ Idempotency")


def test_case_insensitive():
    """Test case insensitivity."""
    assert normalize_degree_deterministic("bachelor of science") == "Bachelor of Science"
    assert normalize_degree_deterministic("BACHELOR OF SCIENCE") == "Bachelor of Science"
    assert normalize_degree_deterministic("bs") == "Bachelor of Science"
    assert normalize_degree_deterministic("Bs") == "Bachelor of Science"
    print("  ✓ Case insensitivity")


def test_unknown_degree():
    """Test unknown degree returns cleaned original."""
    result = normalize_degree_deterministic("Certificate in Data Analytics")
    assert result != "", "Should return non-empty for unknown degrees"
    print(f"  ✓ Unknown degree: '{result}'")


if __name__ == "__main__":
    print("Testing degree normalization...")
    test_exact_matches()
    test_abbreviations()
    test_prefix_extraction()
    test_specialty_abbreviations()
    test_empty_and_none()
    test_idempotency()
    test_case_insensitive()
    test_unknown_degree()
    print("\n✅ All degree normalization tests passed!")
