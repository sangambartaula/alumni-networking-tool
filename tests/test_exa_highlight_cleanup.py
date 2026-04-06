import importlib.util
import sys
import types
from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent
EXA_PATH = PROJECT_ROOT / "exa.py"


def _load_exa_module():
    fake_exa = types.ModuleType("exa_py")

    class _Exa:
        pass

    fake_exa.Exa = _Exa
    sys.modules["exa_py"] = fake_exa

    if "dotenv" not in sys.modules:
        fake_dotenv = types.ModuleType("dotenv")
        fake_dotenv.load_dotenv = lambda: None
        sys.modules["dotenv"] = fake_dotenv

    spec = importlib.util.spec_from_file_location("exa_test_module", EXA_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_clean_highlight_text_strips_noise_sections_and_keeps_core_profile_data():
    exa = _load_exa_module()

    raw = """# Mohammad Farhat Aspiring Software & AI Developer | B.S. in Computer Science - University of North Texas | Skilled in Python Walmart Associate at Walmart

Carrollton, Texas, United States (US)

134 connections • 135 followers ## About I am a UNT student majoring in Computer Science with a focus on Game Programming.

## Experience

### Walmart Associate at Walmart (Current) Aug 2024 - Present • 1 year and 5 months

Plano, Texas, United States Company: 10,001+ employees • Founded 1962 • Public Company • Retail

## Education ### Bachelor of Science - BS, Computer Science at University of North Texas

2021 - 2025 • 4 years Denton, Texas, US

## Skills computer science • game programming • artificial intelligence

## Courses Algorithms by CSCE 4110
"""

    cleaned = exa.clean_highlight_text(raw)

    assert "## Skills" not in cleaned
    assert "## Courses" not in cleaned
    assert "followers" not in cleaned.lower()
    assert "## Experience" in cleaned
    assert "## Education" in cleaned
    assert "Walmart Associate at Walmart" in cleaned
    assert "Bachelor of Science - BS, Computer Science at University of North Texas" in cleaned


def test_clean_highlight_text_drops_certificate_rows_and_inline_education_noise():
    exa = _load_exa_module()

    raw = """# Rithvik Kuthuru

CS @ UNT | AI/ML, Cybersecurity & Data Science

## Experience

### Data Analytics Extern at TruBridge Jan 2025 - May 2025 • 4 months

## Education ### Bachelor of Engineering - BE, Computer Science, 4.0 at UNT College of Engineering

2024 - 2021 Denton, Texas, US Honors: Dean’s List, President’s List Activities and societies: IEEE, Data Science Club Coursework: Assembly Language

### Security certificate Program, ENGINEERING at UNT College of Engineering

2024 -

### Artificial Intelligence (AI) certificate program, ENGINEERING at UNT College of Engineering

2024 -
"""

    cleaned = exa.clean_highlight_text(raw)

    assert "Security certificate Program" not in cleaned
    assert "Artificial Intelligence (AI) certificate program" not in cleaned
    assert "Honors:" not in cleaned
    assert "Activities and societies:" not in cleaned
    assert "Coursework:" not in cleaned
    assert "2024 - 2021 Denton, Texas, US" in cleaned


def test_highlight_has_profile_signal_rejects_low_signal_noise():
    exa = _load_exa_module()

    noise = """## Activity

### Shared a post about hiring

## Skills python • leadership • teamwork
"""

    assert exa.highlight_has_profile_signal(exa.clean_highlight_text(noise)) is False


def test_profile_validators_require_real_name_and_unt_signal():
    exa = _load_exa_module()

    valid_text = """# Jane Doe

## Experience

### Software Engineer at Acme Jan 2024 - Present

## Education

### Bachelor of Science - BS, Computer Science at University of North Texas
2020 - 2024
"""

    assert exa.is_valid_name("Jane Doe | Software Engineer") is True
    assert exa.is_valid_name("LinkedIn Jobs") is False
    assert exa.is_valid_unt_profile("Jane Doe | Software Engineer", valid_text) is True
    assert exa.is_valid_unt_profile("Jane Doe | Software Engineer", "## Experience\n\n### Software Engineer at Acme Jan 2024 - Present") is False


def test_clean_highlight_text_drops_blank_education_rows_and_student_noise():
    exa = _load_exa_module()

    raw = """# Ngan Tran

Interested in Data Science and Machine Learning applications. Seeking Internship/Part-time/Full-time position Research Assistant at University of North Texas

## About

I graduated from UNT with a Bachelor in Computer Science and I am currently pursuing a PhD in Information Science.

## Experience

### Research Assistant at University of North Texas (Current)
Dec 2020 - Present • 5 years and 2 months

### Student at University of North Texas
2020 - Present • 5 years

## Education

### at University of North Texas
2018 - 2020 • 2 years

### Doctor of Philosophy - PhD, Computer and Information Sciences, General, 4.0 at University of North Texas
2020 - 2026 • 6 years

### Bachelor of Applied Science - BASc, Computer Science, 4.0 at University of North Texas
2018 - 2020 • 2 years
"""

    cleaned = exa.clean_highlight_text(raw)

    assert "### at University of North Texas" not in cleaned
    assert "### Student at University of North Texas" not in cleaned
    assert "Doctor of Philosophy - PhD, Computer and Information Sciences, General" in cleaned
    assert "Bachelor of Applied Science - BASc, Computer Science" in cleaned
    assert "Research Assistant at University of North Texas" in cleaned


def test_clean_highlight_text_promotes_inline_role_lines_to_headings():
    exa = _load_exa_module()

    raw = """# Hema Tummapala

## Experience

Data Scientist/ AI-ML Engineer at Fynite Corp.
Jul 2025 - Present • 10 mos

Instructional Assistant - Fundamentals of Database Systems at University of North Texas
Jan 2025 - Aug 2025 • 8 mos

Bootcamp Instructor - AI & Machine Learning at Explore STEM Summer Program
Jun 2024 - Jul 2024 • 2 mos

## Education

Master of Science - MS, Computer Science at University of North Texas
Aug 2023 - May 2025
"""

    cleaned = exa.clean_highlight_text(raw)

    assert "### Data Scientist/ AI-ML Engineer at Fynite Corp." in cleaned
    assert "Jul 2025 - Present" in cleaned
    assert "### Instructional Assistant - Fundamentals of Database Systems at University of North Texas" in cleaned
    assert "### Bootcamp Instructor - AI & Machine Learning at Explore STEM Summer Program" in cleaned
    assert "### Master of Science - MS, Computer Science at University of North Texas" in cleaned


def test_clean_highlight_text_rejects_narrative_and_award_rows_in_education():
    exa = _load_exa_module()

    raw = """# Navya sri Alapati

## Education

### Master's degree, Data Science, A at University of North Texas
2025 - 2026 • 1 year

### Denton, Texas, US Data Science Master's student at UNT with a strong background in DevOps engineering at TCS. Passionate about leveraging ...

### Bachelor's degree, Electrical and Electronics Engineering at Velagapudi Ramakrishna Siddhartha Engineering College
2017 - 2021 • 4 years

### District Level Inspire Science Exhibition-2013 by Inspire
"""

    cleaned = exa.clean_highlight_text(raw)

    assert "### Master's degree, Data Science, A at University of North Texas" in cleaned
    assert "### Bachelor's degree, Electrical and Electronics Engineering at Velagapudi Ramakrishna Siddhartha Engineering College" in cleaned
    assert "Passionate about leveraging" not in cleaned
    assert "District Level Inspire Science Exhibition-2013 by Inspire" not in cleaned


def test_clean_highlight_text_trims_narrative_tail_from_education_heading():
    exa = _load_exa_module()

    raw = """# Mayuri Gevaria

## Education

### Bachelor's degree, Mechanical Engineering at SR University ... Thrilled to announce that I've graduated with a Bachelor of Science in Mechanical and Energy Engineering from the ... I'm
"""

    cleaned = exa.clean_highlight_text(raw)

    assert "### Bachelor's degree, Mechanical Engineering at SR University ..." in cleaned
    assert "Thrilled to announce" not in cleaned
