
import sys
import os
import json
import pytest
from pathlib import Path
from bs4 import BeautifulSoup

# Add scraper to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'scraper'))
sys.modules.pop('scraper', None)
os.chdir(project_root)

import scraper as scraper_module
import groq_client
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

    def test_resolve_standardized_title_prefers_exact_lookup(self, monkeypatch):
        monkeypatch.setattr(scraper_module.config, "USE_GROQ", False)
        lookup = {
            "peer mentor": "Peer Mentor",
            "software engineer": "Software Engineer",
        }
        resolved, score = _resolve_standardized_title("peer mentor", lookup)
        assert resolved == "Peer Mentor"
        assert score == 3

    def test_resolve_standardized_title_does_not_preserve_jr_variant_from_lookup(self, monkeypatch):
        monkeypatch.setattr(scraper_module.config, "USE_GROQ", False)
        lookup = {
            "jr. devops engineer": "Jr. DevOps Engineer",
            "devops engineer": "DevOps Engineer",
        }

        resolved, score = _resolve_standardized_title("Jr. DevOps Engineer", lookup)

        assert resolved == "DevOps Engineer"
        assert score == 2

    def test_resolve_standardized_title_prefers_groq_before_deterministic_fallback(self, monkeypatch):
        lookup = {
            "software engineer": "Software Engineer",
            "site engineer": "Site Engineer",
        }

        monkeypatch.setattr(scraper_module.config, "USE_GROQ", True)
        monkeypatch.setattr(scraper_module, "normalize_title_with_groq", lambda raw, existing: "Site Engineer")
        monkeypatch.setattr(scraper_module, "normalize_title_deterministic", lambda raw: "Software Engineer")

        resolved, score = _resolve_standardized_title("Site Engineer", lookup)

        assert resolved == "Site Engineer"
        assert score >= 2

    def test_apply_experience_display_normalization_reuses_best_same_entry_title(self, monkeypatch):
        scraper = LinkedInScraper()
        monkeypatch.setattr(scraper_module.config, "USE_GROQ", False)
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

    def test_extract_all_experiences_falls_back_to_css_when_groq_returns_empty(self, monkeypatch):
        scraper = LinkedInScraper()
        soup = BeautifulSoup(
            """
            <section componentkey="profileExperienceTopLevelSection">
              <h2>Experience</h2>
              <div data-view-name="profile-component-entity">
                <div class="t-bold"><span aria-hidden="true">Embedded Systems Intern</span></div>
                <span class="t-14 t-normal"><span aria-hidden="true">Acme Robotics · Internship</span></span>
                <span class="pvs-entity__caption-wrapper" aria-hidden="true">Jan 2024 - Present</span>
              </div>
            </section>
            """,
            "html.parser",
        )

        monkeypatch.setattr(scraper_module, "is_groq_available", lambda: True)
        monkeypatch.setattr(
            scraper_module,
            "extract_experiences_with_groq",
            lambda *_args, **_kwargs: ([], 77),
        )
        warnings = []
        monkeypatch.setattr(
            scraper,
            "_log_groq_accuracy_risk",
            lambda section, reason, profile_name="unknown", raw_debug_payloads=None: warnings.append(
                (section, reason, profile_name, bool(raw_debug_payloads))
            ),
        )

        experiences = scraper._extract_all_experiences(soup, max_entries=3, profile_name="Test User")

        assert scraper._last_exp_tokens == 77
        assert len(experiences) == 1
        assert experiences[0]["title"] == "Embedded Systems Intern"
        assert experiences[0]["company"] == "Acme Robotics · Internship"
        assert experiences[0]["employment_type"] == "Internship"
        assert experiences[0]["end"]["is_present"] is True
        assert warnings == [
            (
                "experience",
                "Groq saw the Experience section but returned no usable jobs. Falling back to CSS/text extraction.",
                "Test User",
                True,
            )
        ]

    def test_extract_all_experiences_warns_when_groq_is_unavailable(self, monkeypatch):
        scraper = LinkedInScraper()
        soup = BeautifulSoup(
            """
            <section componentkey="profileExperienceTopLevelSection">
              <h2>Experience</h2>
              <div data-view-name="profile-component-entity">
                <div class="t-bold"><span aria-hidden="true">Embedded Systems Intern</span></div>
                <span class="t-14 t-normal"><span aria-hidden="true">Acme Robotics · Internship</span></span>
                <span class="pvs-entity__caption-wrapper" aria-hidden="true">Jan 2024 - Present</span>
              </div>
            </section>
            """,
            "html.parser",
        )

        warnings = []
        monkeypatch.setattr(scraper_module, "is_groq_available", lambda: False)
        monkeypatch.setattr(
            scraper,
            "_log_groq_accuracy_risk",
            lambda section, reason, profile_name="unknown", raw_debug_payloads=None: warnings.append(
                (section, reason, profile_name, bool(raw_debug_payloads))
            ),
        )

        experiences = scraper._extract_all_experiences(soup, max_entries=3, profile_name="Test User")

        assert len(experiences) == 1
        assert warnings == [
            (
                "experience",
                "Groq was unavailable for Experience extraction. Using CSS/text fallback only.",
                "Test User",
                True,
            )
        ]

    def test_detect_experience_count_mismatch_identifies_missing_companies(self):
        scraper = LinkedInScraper()

        mismatch = scraper._detect_experience_count_mismatch(
            {
                "name": "Test User",
                "profile_url": "https://www.linkedin.com/in/test-user",
                "job_title": "Civil Engineer",
                "company": "",
                "exp2_title": "Site Engineer",
                "exp2_company": "",
                "exp3_title": "Research Assistant",
                "exp3_company": "University of North Texas",
            }
        )

        assert mismatch is not None
        assert mismatch["title_count"] == 3
        assert mismatch["company_count"] == 1
        assert mismatch["missing_company_slots"] == ["Experience 1", "Experience 2"]
        assert mismatch["missing_title_slots"] == []
        assert "Experience count mismatch: 3 title value(s) vs 1 company value(s)" in mismatch["reason"]

    def test_log_groq_accuracy_risk_persists_audit_and_forced_debug_dump(self, monkeypatch):
        temp_root = project_root / "scraper" / "output" / f"_groq_accuracy_test_{os.getpid()}"
        temp_root.mkdir(parents=True, exist_ok=True)
        try:
            audit_file = temp_root / "groq_accuracy_audit.jsonl"
            debug_dir = temp_root / "debug_html"

            monkeypatch.setattr(groq_client, "GROQ_ACCURACY_AUDIT_FILE", audit_file)
            monkeypatch.setattr(groq_client, "DEBUG_HTML_DIR", debug_dir)
            monkeypatch.setattr(groq_client, "SCRAPER_DEBUG_HTML", False)
            monkeypatch.setenv("SCRAPE_RUN_UUID", "run-123")
            monkeypatch.setenv("LINKEDIN_EMAIL", "Ashri@example.com")

            groq_client.reset_groq_accuracy_risk_events()
            groq_client.log_groq_accuracy_risk(
                section="experience",
                reason="Groq was unavailable for Experience extraction.",
                profile_name="Test User",
                profile_url="https://www.linkedin.com/in/test-user",
                debug_payloads={"raw_html_non_groq": "<section>Experience</section>"},
            )

            audit_lines = audit_file.read_text(encoding="utf-8").splitlines()
            assert len(audit_lines) == 1
            event = json.loads(audit_lines[0])
            assert event["run_uuid"] == "run-123"
            assert event["scraper_email"] == "ashri@example.com"
            assert event["section"] == "experience"
            assert event["profile_name"] == "Test User"
            assert event["profile_url"] == "https://www.linkedin.com/in/test-user"

            in_memory_events = groq_client.get_groq_accuracy_risk_events()
            assert len(in_memory_events) == 1
            assert in_memory_events[0]["reason"] == "Groq was unavailable for Experience extraction."

            debug_files = list(debug_dir.glob("Test_User_experience_raw_html_non_groq_*.html"))
            assert len(debug_files) == 1
            assert debug_files[0].read_text(encoding="utf-8") == "<section>Experience</section>"
        finally:
            for path in sorted(temp_root.rglob("*"), reverse=True):
                if path.is_file():
                    path.unlink(missing_ok=True)
                elif path.is_dir():
                    path.rmdir()
            temp_root.rmdir()

    def test_extract_top_card_keeps_metropolitan_area_location(self):
        scraper = LinkedInScraper()
        soup = BeautifulSoup(
            """
            <main>
              <h1>Test User</h1>
              <div class="text-body-medium">Engineer</div>
              <span class="text-body-small inline t-black--light">New York City Metropolitan Area</span>
            </main>
            """,
            "html.parser",
        )

        _name, _headline, location = scraper._extract_top_card(soup)

        assert location == "New York City Metropolitan Area"

    def test_extract_top_card_contact_fallback_keeps_metro_location(self):
        scraper = LinkedInScraper()
        soup = BeautifulSoup(
            """
            <main>
              <h1>Test User</h1>
              <div class="text-body-medium">Engineer</div>
              <div>
                <span>Charlotte Metro</span>
                <a>Contact info</a>
              </div>
            </main>
            """,
            "html.parser",
        )

        _name, _headline, location = scraper._extract_top_card(soup)

        assert location == "Charlotte Metro"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
