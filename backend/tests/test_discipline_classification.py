"""
Unit tests for discipline classification logic.

Tests the infer_discipline function from backfill_disciplines.py
to ensure correct keyword matching and ordering.

Run: python -m pytest backend/tests/test_discipline_classification.py -v
"""
import pytest
import sys
from pathlib import Path

# Add the backend directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backfill_disciplines import infer_discipline


class TestDisciplineClassification:
    """Test the discipline inference logic."""
    
    # ==========================================================================
    # SOFTWARE, DATA & AI ENGINEERING TESTS
    # ==========================================================================
    
    def test_software_engineer_keyword(self):
        """Software engineer should match Software, Data & AI Engineering."""
        result = infer_discipline(None, "Software Engineer", None)
        assert result == "Software, Data & AI Engineering"
    
    def test_python_developer(self):
        """Python keyword should match Software, Data & AI Engineering."""
        result = infer_discipline(None, "Python Developer", None)
        assert result == "Software, Data & AI Engineering"
    
    def test_machine_learning_engineer(self):
        """Machine learning should match Software, Data & AI Engineering."""
        result = infer_discipline(None, "Machine Learning Engineer", None)
        assert result == "Software, Data & AI Engineering"
    
    def test_data_scientist(self):
        """Data scientist should match Software, Data & AI Engineering."""
        result = infer_discipline(None, "Data Scientist", None)
        assert result == "Software, Data & AI Engineering"
    
    def test_cybersecurity_analyst(self):
        """Cybersecurity should match Software, Data & AI Engineering."""
        result = infer_discipline(None, "Cybersecurity Analyst", None)
        assert result == "Software, Data & AI Engineering"
    
    def test_devops_engineer(self):
        """DevOps should match Software, Data & AI Engineering."""
        result = infer_discipline(None, "DevOps Engineer", None)
        assert result == "Software, Data & AI Engineering"
    
    # ==========================================================================
    # EMBEDDED, ELECTRICAL & HARDWARE TESTS
    # ==========================================================================
    
    def test_embedded_systems(self):
        """Embedded systems should match Embedded, Electrical & Hardware."""
        result = infer_discipline(None, "Embedded Systems Engineer", None)
        assert result == "Embedded, Electrical & Hardware Engineering"
    
    def test_firmware_engineer(self):
        """Firmware should match Embedded, Electrical & Hardware."""
        result = infer_discipline(None, "Firmware Engineer", None)
        assert result == "Embedded, Electrical & Hardware Engineering"
    
    def test_fpga_developer(self):
        """FPGA should match Embedded, Electrical & Hardware."""
        result = infer_discipline(None, "FPGA Developer", None)
        assert result == "Embedded, Electrical & Hardware Engineering"
    
    def test_electrical_engineer(self):
        """Electrical engineer should match Embedded, Electrical & Hardware."""
        result = infer_discipline(None, "Electrical Engineer", None)
        assert result == "Embedded, Electrical & Hardware Engineering"
    
    # ==========================================================================
    # MECHANICAL & ENERGY ENGINEERING TESTS
    # ==========================================================================
    
    def test_mechanical_engineer(self):
        """Mechanical engineer should match Mechanical & Energy."""
        result = infer_discipline(None, "Mechanical Engineer", None)
        assert result == "Mechanical & Energy Engineering"
    
    def test_hvac_specialist(self):
        """HVAC should match Mechanical & Energy."""
        result = infer_discipline(None, "HVAC Specialist", None)
        assert result == "Mechanical & Energy Engineering"
    
    def test_solidworks_designer(self):
        """SolidWorks should match Mechanical & Energy."""
        result = infer_discipline(None, "SolidWorks Designer", None)
        assert result == "Mechanical & Energy Engineering"
    
    # ==========================================================================
    # BIOMEDICAL ENGINEERING TESTS
    # ==========================================================================
    
    def test_biomedical_engineer(self):
        """Biomedical engineer should match Biomedical Engineering."""
        result = infer_discipline(None, "Biomedical Engineer", None)
        assert result == "Biomedical Engineering"
    
    def test_medical_device_engineer(self):
        """Medical device should match Biomedical Engineering."""
        result = infer_discipline(None, "Medical Device Engineer", None)
        assert result == "Biomedical Engineering"
    
    def test_bioinformatics_specialist(self):
        """Bioinformatics should match Biomedical Engineering."""
        result = infer_discipline(None, "Bioinformatics Specialist", None)
        assert result == "Biomedical Engineering"
    
    # ==========================================================================
    # MATERIALS SCIENCE & MANUFACTURING TESTS
    # ==========================================================================
    
    def test_materials_engineer(self):
        """Materials engineer should match Materials Science & Manufacturing."""
        result = infer_discipline(None, "Materials Engineer", None)
        assert result == "Materials Science & Manufacturing"
    
    def test_metallurgy_specialist(self):
        """Metallurgy should match Materials Science & Manufacturing."""
        result = infer_discipline(None, "Metallurgy Specialist", None)
        assert result == "Materials Science & Manufacturing"
    
    def test_quality_engineer_six_sigma(self):
        """Six sigma should match Materials Science & Manufacturing."""
        result = infer_discipline(None, "Six Sigma Quality Engineer", None)
        assert result == "Materials Science & Manufacturing"
    
    # ==========================================================================
    # CONSTRUCTION & ENGINEERING MANAGEMENT TESTS
    # ==========================================================================
    
    def test_construction_manager(self):
        """Construction manager should match Construction & Engineering Management."""
        result = infer_discipline(None, "Construction Manager", None)
        assert result == "Construction & Engineering Management"
    
    def test_civil_engineer(self):
        """Civil engineer should match Construction & Engineering Management."""
        result = infer_discipline(None, "Civil Engineer", None)
        assert result == "Construction & Engineering Management"
    
    def test_site_engineer(self):
        """Site engineer should match Construction & Engineering Management."""
        result = infer_discipline(None, "Site Engineer", None)
        assert result == "Construction & Engineering Management"
    
    # ==========================================================================
    # ORDERING TESTS (CRITICAL)
    # ==========================================================================
    
    def test_order_software_before_construction_for_project_manager(self):
        """Project manager alone should NOT match Construction (too generic)."""
        # Note: plain "project manager" doesn't match anything in our list
        # because we removed generic IT terms from Construction
        result = infer_discipline(None, "Project Manager", None)
        # Should be Unknown since "project manager" alone is too generic
        assert result == "Unknown"
    
    def test_order_software_before_construction_for_it_project_manager(self):
        """IT Project Manager should match Software (checked first)."""
        result = infer_discipline(None, "IT Project Manager", None)
        assert result == "Software, Data & AI Engineering"
    
    def test_construction_project_manager(self):
        """Construction Project Manager should match Construction."""
        result = infer_discipline(None, "Project Manager Construction", None)
        assert result == "Construction & Engineering Management"
    
    # ==========================================================================
    # PRIORITY TESTS (Job Title > Degree > Headline)
    # ==========================================================================
    
    def test_job_priority_over_degree(self):
        """Job title should take priority over degree (Changed based on user feedback)."""
        # Job: Construction Manager (Construction)
        # Degree: Computer Science (Software)
        # Should be Construction (Job wins)
        result = infer_discipline("Computer Science", "Construction Manager", None)
        assert result == "Construction & Engineering Management"

    def test_job_priority_over_degree_user_case(self):
        """User Case: Lead Software Engineer (Software) vs Computer Engineering (Embedded)."""
        # Job: Lead Software Engineer (Software)
        # Degree: Computer Engineering (Embedded)
        # Should be Software (Job wins)
        result = infer_discipline("Computer Engineering", "Lead Software Engineer", None)
        assert result == "Software, Data & AI Engineering"
    
    def test_job_priority_over_headline(self):
        """Job title should take priority over headline."""
        result = infer_discipline(None, "Software Engineer", "Construction Manager")
        assert result == "Software, Data & AI Engineering"
    
    def test_headline_used_when_no_job(self):
        """Headline should be used when no job title."""
        result = infer_discipline(None, None, "Experienced Software Developer")
        assert result == "Software, Data & AI Engineering"
    
    # ==========================================================================
    # UNKNOWN TESTS
    # ==========================================================================
    
    def test_unknown_for_no_match(self):
        """Should return Unknown when no keywords match."""
        result = infer_discipline(None, "Sales Manager", None)
        assert result == "Unknown"
    
    def test_unknown_for_empty_input(self):
        """Should return Unknown for empty input."""
        result = infer_discipline(None, None, None)
        assert result == "Unknown"
    
    def test_unknown_for_generic_title(self):
        """Should return Unknown for generic titles."""
        result = infer_discipline(None, "Manager", None)
        assert result == "Unknown"


class TestCaseInsensitivity:
    """Test that matching is case-insensitive."""
    
    def test_lowercase_match(self):
        """Should match lowercase keywords."""
        result = infer_discipline(None, "software engineer", None)
        assert result == "Software, Data & AI Engineering"
    
    def test_uppercase_match(self):
        """Should match uppercase keywords."""
        result = infer_discipline(None, "SOFTWARE ENGINEER", None)
        assert result == "Software, Data & AI Engineering"
    
    def test_mixed_case_match(self):
        """Should match mixed case keywords."""
        result = infer_discipline(None, "SoFtWaRe EnGiNeEr", None)
        assert result == "Software, Data & AI Engineering"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
