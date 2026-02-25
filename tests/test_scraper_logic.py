
import sys
import os
import pytest
from pathlib import Path

# Add scraper to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'scraper'))
sys.modules.pop('scraper', None)
os.chdir(project_root)

from scraper import LinkedInScraper

class TestScraperLogic:
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

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
