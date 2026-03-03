
import sys
import os
import pytest
from pathlib import Path

# Add scraper to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'scraper'))
sys.modules.pop('scraper', None)
os.chdir(project_root)

import scraper as scraper_module
from scraper import (
    LinkedInScraper,
    _is_company_title_collision,
    _resolve_standardized_title,
)

class TestScraperLogic:
    def test_is_company_title_collision(self):
        assert _is_company_title_collision("UNT College of Engineering", "UNT College of Engineering")
        assert _is_company_title_collision("UNT College of Engineering.", "UNT College of Engineering")
        assert not _is_company_title_collision("Peer Mentor", "UNT College of Engineering")

    def test_split_context_line_pipes(self):
        """Test splitting by pipe symbol."""
        scraper = LinkedInScraper()
        
        text = "Instructional Assistant | University of North Texas"
        parts = scraper._split_context_line(text)
        
        # This is what we WANT:
        expected = ["Instructional Assistant", "University of North Texas"]
        
        # This assert captures the current failure if logic is missing
        assert len(parts) == 2, f"Should split into 2 parts, got {parts}"
        assert parts[0] == expected[0]
        assert parts[1] == expected[1]

    def test_split_context_line_dashes(self):
        """Test splitting by space-dash-space."""
        scraper = LinkedInScraper()
        
        text = "Junior Engineer - Worley"
        parts = scraper._split_context_line(text)
        
        assert len(parts) == 2
        assert "Junior Engineer" in parts
        assert "Worley" in parts
        
    def test_split_context_line_existing_logic(self):
        """Ensure existing logic (at/dots) still works."""
        scraper = LinkedInScraper()
        
        text1 = "Software Engineer at Google"
        parts1 = scraper._split_context_line(text1)
        assert "Google" in parts1
        
        text2 = "Developer · Microsoft"
        parts2 = scraper._split_context_line(text2)
        assert "Microsoft" in parts2

    def test_extract_education_top_card_handles_non_string_button_text(self):
        scraper = LinkedInScraper()

        class _BadButton:
            def get(self, _key, default=""):
                return default

            def get_text(self, *_args, **_kwargs):
                return None

        class _FakeSoup:
            def find_all(self, tag):
                if tag == "button":
                    return [_BadButton()]
                return []

        entries = scraper._extract_education_from_top_card(_FakeSoup())
        assert entries == []

    def test_resolve_standardized_title_prefers_exact_lookup(self):
        lookup = {
            "peer mentor": "Peer Mentor",
            "software engineer": "Software Engineer",
        }
        resolved, score = _resolve_standardized_title("peer mentor", lookup)
        assert resolved == "Peer Mentor"
        assert score == 3

    def test_resolve_standardized_title_does_not_preserve_jr_variant_from_lookup(self):
        lookup = {
            "jr. devops engineer": "Jr. DevOps Engineer",
            "devops engineer": "DevOps Engineer",
        }

        resolved, score = _resolve_standardized_title("Jr. DevOps Engineer", lookup)

        assert resolved == "DevOps Engineer"
        assert score == 2

    def test_apply_experience_display_normalization_reuses_best_same_entry_title(self, monkeypatch):
        scraper = LinkedInScraper()
        monkeypatch.setattr(
            scraper_module,
            "_load_standardized_title_lookup",
            lambda: {"peer mentor": "Peer Mentor"},
        )
        data = {
            "job_title": "Peer Mentor",
            "company": "UNT College of Engineering",
            "job_start_date": "2021",
            "job_end_date": "2025",
            "exp2_title": "Mentor",
            "exp2_company": "UNT College of Engineering",
            "exp2_dates": "2021 - 2025",
            "exp3_title": "",
            "exp3_company": "",
            "exp3_dates": "",
        }

        scraper._apply_experience_display_normalization(data)

        assert data["normalized_job_title"] == "Peer Mentor"
        assert data["normalized_exp2_title"] == "Peer Mentor"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
