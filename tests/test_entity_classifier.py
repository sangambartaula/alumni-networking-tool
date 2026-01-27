"""
Tests for the Entity Classifier module.

Run with: pytest tests/test_entity_classifier.py -v
"""

import sys
import os
from pathlib import Path

# Add scraper to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'scraper'))
os.chdir(project_root)  # Change to project root for relative paths

import pytest
from entity_classifier import (
    EntityClassifier, 
    classify_entity, 
    is_location, 
    is_university,
    get_classifier
)


class TestEntityClassifier:
    """Tests for the EntityClassifier class."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.classifier = get_classifier()
    
    # === Company Classification Tests ===
    
    def test_classify_known_company_from_database(self):
        """Companies in the database should be detected with high confidence."""
        companies = ["ServiceNow", "State Farm", "Vanguard", "Google", "Microsoft"]
        for company in companies:
            entity_type, confidence = classify_entity(company)
            assert entity_type == "company", f"'{company}' should be classified as company"
            assert confidence >= 0.95, f"'{company}' should have high confidence"
    
    def test_classify_company_with_suffix(self):
        """Companies with common suffixes should be detected."""
        test_cases = [
            "Ethos Group",
            "Linbeck Group, LLC",
            "Tata Consultancy Services",
            "SPEED MONEY TRANSFER SERVICE PRIVATE LIMITED"
        ]
        for company in test_cases:
            entity_type, confidence = classify_entity(company)
            assert entity_type == "company", f"'{company}' should be classified as company"
    
    # === Job Title Classification Tests ===
    
    def test_classify_job_titles(self):
        """Common job titles should be detected as job_title."""
        titles = [
            "Software Engineer",
            "Software Engineer II",
            "Senior Technical Engineer",
            "Data Analyst",
            "Research Assistant",
            "Student",
            "Intern",
            "Project Engineer"
        ]
        for title in titles:
            entity_type, confidence = classify_entity(title)
            assert entity_type == "job_title", f"'{title}' should be classified as job_title"
    
    def test_classify_job_title_with_level(self):
        """Job titles with levels (I, II, Senior, etc.) should work."""
        titles = [
            "Software Engineer II",
            "Senior Software Engineer",
            "Junior Developer",
            "Lead Architect"
        ]
        for title in titles:
            entity_type, confidence = classify_entity(title)
            assert entity_type == "job_title", f"'{title}' should be classified as job_title"
    
    # === Location Classification Tests ===
    
    def test_classify_location_city_state(self):
        """City, State format should be detected as location."""
        locations = [
            "Orlando, Florida",
            "Denton, Texas, United States",
            "Dallas, Texas"
        ]
        for loc in locations:
            entity_type, confidence = classify_entity(loc)
            assert entity_type == "location", f"'{loc}' should be classified as location"
    
    def test_is_location_function(self):
        """The is_location helper should correctly identify locations."""
        assert is_location("Orlando, Florida") == True
        assert is_location("Dallas-Fort Worth Metroplex") == True
        assert is_location("United States") == True
        assert is_location("Remote") == True
        
        # These should NOT be locations
        assert is_location("ServiceNow") == False
        assert is_location("Software Engineer") == False
        assert is_location("State Farm") == False
    
    # === University Classification Tests ===
    
    def test_classify_university(self):
        """Universities should be detected as university or company (universities can be employers)."""
        unis = [
            "University of North Texas",
            "Stanford University",
        ]
        for uni in unis:
            entity_type, confidence = classify_entity(uni)
            # Universities are typically classified as 'university' but may also be 'company' 
            # since they can be employers. Either is acceptable.
            assert entity_type in ("university", "company"), f"'{uni}' should be university or company"
    
    def test_is_university_function(self):
        """The is_university helper should correctly identify universities."""
        assert is_university("University of North Texas") == True
        assert is_university("Texas A&M") == True
        
        assert is_university("Google") == False
        assert is_university("Software Engineer") == False
    
    # === Edge Cases ===
    
    def test_empty_input(self):
        """Empty or None input should return unknown."""
        entity_type, confidence = classify_entity("")
        assert entity_type == "unknown"
        assert confidence == 0.0
        
        entity_type, confidence = classify_entity(None)
        assert entity_type == "unknown"
    
    def test_mixed_case_input(self):
        """Classification should handle different cases."""
        # Database lookup is case-insensitive
        entity_type, _ = classify_entity("servicenow")
        assert entity_type == "company"
        
        entity_type, _ = classify_entity("GOOGLE")
        assert entity_type == "company"


class TestDatabaseHandler:
    """Tests for the database_handler normalize_text function."""
    
    def test_normalize_text_removes_newlines(self):
        """Newlines should be replaced with pipe separator."""
        from database_handler import normalize_text
        
        text = "Line 1\nLine 2\nLine 3"
        result = normalize_text(text)
        assert "\n" not in result
        assert "|" in result or " " in result
    
    def test_normalize_text_removes_carriage_returns(self):
        """Carriage returns should be handled."""
        from database_handler import normalize_text
        
        text = "Line 1\r\nLine 2"
        result = normalize_text(text)
        assert "\r" not in result
        assert "\n" not in result
    
    def test_normalize_text_replaces_unicode_quotes(self):
        """Fancy Unicode quotes should become ASCII."""
        from database_handler import normalize_text
        
        # Use Unicode escapes for the smart quotes
        text = "It\u2019s a \u201ctest\u201d"
        result = normalize_text(text)
        assert "'" in result  # ASCII apostrophe
        assert '"' in result  # ASCII quote
    
    def test_normalize_text_collapses_spaces(self):
        """Multiple spaces should become single space."""
        from database_handler import normalize_text
        
        text = "Too    many     spaces"
        result = normalize_text(text)
        assert "  " not in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
