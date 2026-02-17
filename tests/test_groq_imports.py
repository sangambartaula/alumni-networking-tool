"""
Tests for Groq module imports after refactoring.
Run with: python3 tests/test_groq_imports.py
"""

import sys
import os

# Add project root and scraper to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scraper"))


def test_groq_client_imports():
    """Test shared groq_client module imports."""
    from groq_client import is_groq_available, parse_groq_date, _clean_doubled, save_debug_html
    assert callable(is_groq_available)
    assert callable(parse_groq_date)
    assert callable(_clean_doubled)
    assert callable(save_debug_html)
    print("  ✓ groq_client imports")


def test_experience_extractor_imports():
    """Test experience extractor module imports."""
    from groq_extractor_experience import extract_experiences_with_groq
    assert callable(extract_experiences_with_groq)
    print("  ✓ groq_extractor_experience imports")


def test_education_extractor_imports():
    """Test education extractor module imports."""
    from groq_extractor_education import extract_education_with_groq
    assert callable(extract_education_with_groq)
    print("  ✓ groq_extractor_education imports")


def test_old_module_removed():
    """Test that old groq_extractor module no longer exists."""
    old_path = os.path.join(PROJECT_ROOT, "scraper", "groq_extractor.py")
    assert not os.path.exists(old_path), f"Old module still exists: {old_path}"
    print("  ✓ Old groq_extractor.py removed")


def test_parse_groq_date():
    """Test date parsing utility."""
    from groq_client import parse_groq_date
    
    result = parse_groq_date("Oct 2024")
    assert result is not None
    assert result["year"] == 2024
    assert result["month"] == 10
    
    result = parse_groq_date("Present")
    assert result is not None
    assert result["is_present"] is True
    
    result = parse_groq_date("2023")
    assert result is not None
    assert result["year"] == 2023
    
    result = parse_groq_date("")
    assert result is None
    
    print("  ✓ parse_groq_date")


def test_clean_doubled():
    """Test doubled text cleaning utility."""
    from groq_client import _clean_doubled
    
    assert _clean_doubled("EngineerEngineer") == "Engineer"
    assert _clean_doubled("Hello World Hello World") == "Hello World"
    assert _clean_doubled("Normal") == "Normal"
    assert _clean_doubled("") == ""
    assert _clean_doubled(None) is None
    print("  ✓ _clean_doubled")


if __name__ == "__main__":
    print("Testing Groq module imports and utilities...")
    test_groq_client_imports()
    test_experience_extractor_imports()
    test_education_extractor_imports()
    test_old_module_removed()
    test_parse_groq_date()
    test_clean_doubled()
    print("\n✅ All Groq import and utility tests passed!")
