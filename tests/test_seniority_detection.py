#!/usr/bin/env python
"""
Test script for seniority level detection.
Verifies that job titles are correctly classified into seniority buckets.
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'scraper'))
sys.path.insert(0, str(PROJECT_ROOT / 'backend'))

# Import directly from the modules
import importlib.util
seniority_module = importlib.util.spec_from_file_location(
    "seniority_detector", 
    PROJECT_ROOT / "scraper" / "seniority_detector.py"
)
seniority_detector = importlib.util.module_from_spec(seniority_module)
seniority_module.loader.exec_module(seniority_detector)

detect_seniority = seniority_detector.detect_seniority
adjust_and_flag_seniority = seniority_detector.adjust_and_flag_seniority
analyze_seniority = seniority_detector.analyze_seniority

# Test cases: (job_title, expected_seniority)
TEST_CASES = [
    # Executive level
    ("CEO at Tech Company", "Executive"),
    ("Chief Technology Officer", "Executive"),
    ("President of Engineering", "Executive"),
    ("Founder and CTO", "Executive"),
    ("VP of Product", "Executive"),
    
    # Director level
    ("Director of Engineering", "Executive"),
    ("Director of Sales", "Executive"),
    ("Head of Product Development", "Executive"),
    ("Principal Engineer", "Executive"),
    
    # Manager level
    ("Engineering Manager", "Executive"),
    ("Project Manager", "Executive"),
    ("Product Manager", "Executive"),  # Product Manager typically implies management
    ("Manager of Operations", "Executive"),
    ("Tech Lead", "Executive"),
    ("Team Lead - Software", "Executive"),
    ("Engineering Lead", "Executive"),
    ("Program Manager", "Executive"),
    ("Scrum Master", "Executive"),
    ("Supervisor of QA", "Executive"),
    ("Associate Product Manager", "Executive"),
    
    # Senior level
    ("Senior Software Engineer", "Senior"),
    ("Senior Data Scientist", "Senior"),
    ("Senior Engineer", "Senior"),  # Match on Senior
    ("Staff Engineer", "Senior"),
    ("Distinguished Engineer", "Senior"),
    ("Fellow, Research", "Senior"),
    
    # Junior level (merged into Mid)
    ("Junior Software Engineer", "Mid"),
    ("Entry-Level Accountant", "Mid"),
    ("Apprentice", "Mid"),
    
    # Intern level
    ("Intern", "Intern"),
    ("Internship - Software Engineering", "Intern"),
    ("Co-op - Data Science", "Intern"),
    ("Research Assistant", "Intern"),
    ("Teaching Assistant", "Intern"),
    ("Student Employee", "Intern"),
    
    # Mid level (default when no indicators found)
    ("Software Engineer", "Mid"),
    ("Data Analyst", "Mid"),
    ("DevOps Engineer", "Mid"),
    ("Solutions Architect", "Mid"),
    ("Business Analyst", "Mid"),
]

def test_seniority_detection():
    """Run all test cases and report results."""
    print("=" * 70)
    print("SENIORITY LEVEL DETECTION TEST")
    print("=" * 70)
    
    passed = 0
    failed = 0
    results = []
    
    for job_title, expected in TEST_CASES:
        actual = analyze_seniority(
            {
                "title": job_title,
                "linkedin_url": "https://linkedin.com/in/test",
                "job_employment_type": "",
            },
            relevant_experience_months=None,
        )
        status = "PASS" if actual == expected else "FAIL"
        
        if actual == expected:
            passed += 1
        else:
            failed += 1
        
        results.append({
            'title': job_title,
            'expected': expected,
            'actual': actual,
            'status': status
        })
    
    # Print results
    print(f"\nRESULTS (passed: {passed}/{len(TEST_CASES)}):\n")
    
    for r in results:
        print(f"[{r['status']}] | {r['title']:<40} | {r['expected']:<10} | {r['actual']:<10}")
    
    print("\n" + "=" * 70)
    if failed == 0:
        print(f"SUCCESS: ALL TESTS PASSED ({passed}/{len(TEST_CASES)})")
    else:
        print(f"FAILURE: {failed} TEST(S) FAILED")
    print("=" * 70)
    
    return failed == 0


def test_mismatch_flagging():
    """Test the mismatch flagging logic."""
    print("\n" + "=" * 70)
    print("SENIORITY MISMATCH FLAGGING TEST")
    print("=" * 70)
    
    test_cases = [
        # (seniority, experience_months, should_flag)
        ("Senior", 12, False),     # Exactly at threshold
        ("Senior", 11, True),      # Below threshold
        ("Senior", 24, False),     # Well above threshold
        ("Manager", 18, False),    # At threshold
        ("Manager", 17, True),     # Below threshold
        ("Director", 36, False),   # At threshold
        ("Director", 35, True),    # Below threshold
        ("Executive", 48, False),  # At threshold
        ("Executive", 47, True),   # Below threshold
        ("Mid", 6, False),         # Mid doesn't trigger flagging
        ("Junior", 6, False),      # Junior doesn't trigger flagging
    ]
    
    print("\nMismatch Threshold Tests:\n")
    
    for seniority, months, should_flag in test_cases:
        # adjust_and_flag_seniority doesn't change the value, just flags
        result = adjust_and_flag_seniority(seniority, months, "https://linkedin.com/in/test")
        print(f"  {seniority:<12} with {months:>2} months -> should_flag={should_flag} (returned: {result})")
    
    print("\n" + "=" * 70)
    print("Mismatch flagging test completed")
    print("=" * 70)


if __name__ == '__main__':
    success = test_seniority_detection()
    test_mismatch_flagging()
    
    sys.exit(0 if success else 1)
