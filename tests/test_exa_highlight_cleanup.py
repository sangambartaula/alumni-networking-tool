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
