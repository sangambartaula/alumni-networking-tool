
import sys
import os
import pytest
from pathlib import Path
from bs4 import BeautifulSoup

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

    def test_extract_education_entries_recovers_unt_from_school_link_id(self):
        scraper = LinkedInScraper()
        soup = BeautifulSoup(
            """
            <section componentkey="profileEducationTopLevelSection">
              <h2>Education</h2>
              <div>
                <a href="https://www.linkedin.com/school/6464/"><figure></figure></a>
                <p>Bachelor of Science - BS, Biomedical Engineering</p>
                <p>2021 - 2025</p>
              </div>
            </section>
            """,
            "html.parser",
        )

        entries = scraper._extract_education_entries(soup)

        assert entries
        assert entries[0]["school"] == "University of North Texas"
        assert entries[0]["graduation_year"] == "2025"

    def test_find_section_root_falls_back_to_school_link_when_heading_is_missing(self):
        scraper = LinkedInScraper()
        soup = BeautifulSoup(
            """
            <div componentkey="com.linkedin.sdui.profile.card.refEducationTopLevelSection">
              <div>
                <a href="https://www.linkedin.com/school/6464/">University of North Texas</a>
              </div>
            </div>
            """,
            "html.parser",
        )

        root = scraper._find_section_root(soup, "Education")

        assert root is not None
        assert "EducationTopLevelSection".lower() in str(root.get("componentkey", "")).lower()

    def test_scrape_profile_page_merges_inline_unt_education_when_detailed_view_is_partial(self, monkeypatch):
        profile_url = "https://www.linkedin.com/in/test-user"
        scraper = LinkedInScraper()

        class _FakeDriver:
            def __init__(self, html, url):
                self._html = html
                self.current_url = url
                self.title = "Test User | LinkedIn"
                self.page_source = html

            def get(self, url):
                self.current_url = url

            def execute_script(self, script, *_args):
                if "return document.body.innerHTML;" in script:
                    return self._html
                return None

        scraper.driver = _FakeDriver("<main></main>", profile_url)
        monkeypatch.setattr(scraper, "_force_focus", lambda: None)
        monkeypatch.setattr(scraper, "_page_block_reason", lambda: None)
        monkeypatch.setattr(scraper, "_page_not_found", lambda: False)
        monkeypatch.setattr(scraper, "_wait_for_top_card", lambda timeout=10: True)
        monkeypatch.setattr(scraper, "_wait_for_education_ready", lambda timeout=10: True)
        monkeypatch.setattr(scraper, "_extract_top_card", lambda _soup: ("Test User", "", "Denton, Texas"))
        monkeypatch.setattr(scraper, "_extract_all_experiences", lambda *_args, **_kwargs: [])
        monkeypatch.setattr(
            scraper,
            "_extract_education_entries_from_detailed_view",
            lambda *_args, **_kwargs: (
                [{
                    "school": "Google Career Certificate",
                    "degree": "Certificate",
                    "major": "",
                    "graduation_year": "2025",
                    "school_start": None,
                    "school_end": None,
                }],
                0,
            ),
        )
        monkeypatch.setattr(
            scraper,
            "_extract_education_entries",
            lambda _soup: [{
                "school": "University of North Texas",
                "degree": "Bachelor of Science",
                "major": "Biomedical Engineering",
                "raw_degree": "Bachelor of Science",
                "graduation_year": "2024",
                "school_start": None,
                "school_end": {"year": 2024, "month": 5, "is_present": False},
            }],
        )
        monkeypatch.setattr(scraper, "_extract_education_from_top_card", lambda _soup: [])
        monkeypatch.setattr(scraper, "_apply_education_and_discipline_normalization", lambda _data: None)
        monkeypatch.setattr(scraper, "_apply_experience_display_normalization", lambda _data: None)
        monkeypatch.setattr(scraper, "_log_missing_data_warnings", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(scraper_module, "is_groq_available", lambda: False)
        monkeypatch.setattr(scraper_module, "print_profile_summary", lambda *_args, **_kwargs: None)

        result = scraper.scrape_profile_page(profile_url)

        assert isinstance(result, dict)
        assert result.get("__status__") != "NOT_UNT_ALUM"
        assert result["school"] == "University of North Texas"
        assert result["education"] == "University of North Texas"

    def test_scroll_full_page_moves_down_then_back_up(self, monkeypatch):
        scraper = LinkedInScraper()
        deltas = []
        edges = []

        monkeypatch.setattr(scraper, "_scroll_active_surfaces", lambda delta: deltas.append(delta))
        monkeypatch.setattr(scraper, "_scroll_surfaces_to_edge", lambda edge: edges.append(edge))
        monkeypatch.setattr(scraper_module.time, "sleep", lambda _s: None)
        monkeypatch.setattr(scraper_module.random, "uniform", lambda _a, _b: 0.01)

        scraper.scroll_full_page()

        assert deltas[:5] == [900, 900, 900, 900, 900]
        assert deltas[-2:] == [-1200, -1200]
        assert edges == ["bottom", "top"]

    def test_scrape_profile_page_uses_shared_scroll_routine(self, monkeypatch):
        profile_url = "https://www.linkedin.com/in/test-user"
        scraper = LinkedInScraper()
        scroll_calls = []

        class _FakeDriver:
            def __init__(self, html, url):
                self._html = html
                self.current_url = url
                self.title = "Test User | LinkedIn"
                self.page_source = html

            def get(self, url):
                self.current_url = url

            def execute_script(self, script, *_args):
                if "return document.body.innerHTML;" in script:
                    return self._html
                return None

        scraper.driver = _FakeDriver("<main></main>", profile_url)
        monkeypatch.setattr(scraper, "_force_focus", lambda: None)
        monkeypatch.setattr(scraper, "_page_block_reason", lambda: None)
        monkeypatch.setattr(scraper, "_page_not_found", lambda: False)
        monkeypatch.setattr(scraper, "scroll_full_page", lambda: scroll_calls.append("scroll"))
        monkeypatch.setattr(scraper, "_wait_for_top_card", lambda timeout=10: True)
        monkeypatch.setattr(scraper, "_wait_for_education_ready", lambda timeout=10: True)
        monkeypatch.setattr(scraper, "_extract_top_card", lambda _soup: ("Test User", "", "Denton, Texas"))
        monkeypatch.setattr(scraper, "_extract_all_experiences", lambda *_args, **_kwargs: [])
        monkeypatch.setattr(scraper, "_extract_education_entries_from_detailed_view", lambda *_args, **_kwargs: ([], 0))
        monkeypatch.setattr(scraper, "_extract_education_entries", lambda _soup: [])
        monkeypatch.setattr(scraper, "_extract_education_from_top_card", lambda _soup: [])
        monkeypatch.setattr(scraper, "scrape_all_education", lambda *_args, **_kwargs: ([], None))
        monkeypatch.setattr(scraper, "_apply_education_and_discipline_normalization", lambda _data: None)
        monkeypatch.setattr(scraper, "_apply_experience_display_normalization", lambda _data: None)
        monkeypatch.setattr(scraper, "_log_missing_data_warnings", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(scraper_module, "is_groq_available", lambda: False)
        monkeypatch.setattr(scraper_module, "print_profile_summary", lambda *_args, **_kwargs: None)

        result = scraper.scrape_profile_page(profile_url)

        assert result["__status__"] == "NOT_UNT_ALUM"
        assert scroll_calls == ["scroll"]

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
