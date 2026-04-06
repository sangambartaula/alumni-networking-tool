import importlib.util
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[5]
ARCHIVE_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

EXA_GROQ_PATH = ARCHIVE_ROOT / "exa_groq.py"


def _load_exa_groq_module():
    spec = importlib.util.spec_from_file_location("archived_exa_groq", EXA_GROQ_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


exa_groq = _load_exa_groq_module()


MICHELLE_RAW = """# Michelle R. Vargas

Simulation Senior Associate Engineer at Caterpillar, Inc. Simulation Senior Associate Engineer at Caterpillar Inc.

Peoria, Illinois, United States (US) 307 connections • 313 followers

## About Hello there! My name is Michelle and I am currently a student at the University of North Texas pursuing a major in Mechanical and Energy Engineering. I expect to graduate in May 2022.
Total Experience: 7 years and 6 months

## Experience ### Simulation Senior Associate Engineer at Caterpillar Inc. (Current) Jan 2024 - Present • 1 year and 10 months

Illinois, United States Company: 10,001+ employees • Founded 1925 • Public Company • Machinery Manufacturing Department: Engineering and Technical • Level: Senior

### Developmental Program Engineer at Caterpillar Inc.

Jun 2022 - Jan 2024 • 1 year and 7 months

### EMF Lab Assistant at UNT College of Engineering

Jan 2021 - May 2022 • 1 year and 4 months Denton, Texas, United States

## Education ### Mechanical and Energy Engineering, Mechanical Engineering, 3.621 at University of North Texas 2018 - 2022 • 4 years

Denton, Texas, US
"""


CELESTE_RAW = """# Celeste Aucoin

Biomedical Engineering Master’s Student at the University of North Texas | Validation Engineer Pharmacy Technician Trainee at Walgreens

Denton, Texas, United States (US) 252 connections • 252 followers

## About I recently graduated from the University of North Texas, receiving my B.S. in biomedical engineering with minors in materials science and mathematics.
Total Experience: 6 years and 11 months

## Experience ### Pharmacy Technician Trainee at Walgreens (Current)

Sep 2024 - Present • 1 year and 4 months Company: 10,001+ employees • Founded 1901 • Privately Held • Retail Pharmacies

### Validation Engineer II at PSC Biotech Corporation

Oct 2022 - Jun 2024 • 1 year and 8 months

## Education ### Bachelor of Science - BS Biomedical Engineering at University of North Texas

2025 - 2027 • 2 years Denton, Texas, US

### Bachelor of Science - BS, Biomedical Engineering at University of North Texas 2017 - 2021 • 4 years
"""


RITHVIK_RAW = """# Rithvik Kuthuru

CS @ UNT | AI/ML, Cybersecurity & Data Science

Dallas-Fort Worth Metroplex (US) 346 connections • 347 followers

## Experience

### Data Analytics Extern at TruBridge Jan 2025 - May 2025 • 4 months

### Undergraduate Laboratory Assistant at The University of Texas System Aug 2022 - May 2024 • 1 year and 9 months

## Education ### Bachelor of Engineering - BE, Computer Science, 4.0 at UNT College of Engineering

2024 - 2021 Denton, Texas, US

### Security certificate Program, ENGINEERING at UNT College of Engineering

2024 -

### Artificial Intelligence (AI) certificate program, ENGINEERING at UNT College of Engineering

2024 -

### Bachelor of Engineering - BE, Electrical Engineering at [Cockrell School of Engineering, The University of Texas at Austin] 2021 - 2024 • 3 years
"""


def test_build_groq_payload_keeps_major_signal_and_recent_jobs():
    payload = exa_groq.build_groq_payload(
        "Michelle R. Vargas | Simulation Senior Associate Engineer at Caterpillar, Inc.",
        MICHELLE_RAW,
    )

    assert any(
        candidate["school_hint"] == "University of North Texas"
        and candidate["major_hint"] == "Mechanical and Energy Engineering"
        for candidate in payload["education_candidates"]
    )
    assert [candidate["title_hint"] for candidate in payload["experience_candidates"][:3]] == [
        "Simulation Senior Associate Engineer",
        "Developmental Program Engineer",
        "EMF Lab Assistant",
    ]


def test_merge_job_payloads_uses_local_current_role_when_model_misses_it():
    payload = exa_groq.build_groq_payload(
        "Celeste Aucoin | Biomedical Engineering Master’s Student at the University of North Texas | Validation Engineer",
        CELESTE_RAW,
    )

    groq_jobs = [
        {
            "title": "Validation Engineer",
            "company": "N/A",
            "start_date": "Oct 2022",
            "end_date": "Jun 2024",
        }
    ]

    merged = exa_groq.merge_job_payloads(
        groq_jobs,
        exa_groq.job_records_from_candidates(payload["experience_candidates"]),
    )
    jobs = exa_groq.normalize_jobs(merged)

    assert jobs[0]["title"] == "Pharmacy Technician Trainee"
    assert jobs[0]["company"] == "Walgreens"
    assert jobs[1]["title"] == "Validation Engineer"


def test_normalize_education_keeps_highest_unt_first_without_triple_unt():
    normalized = exa_groq.normalize_education(
        [
            {
                "school": "UNT College of Engineering",
                "degree": "Bachelor of Science",
                "major": "Computer Science",
                "start": "2016",
                "end": "2020",
            },
            {
                "school": "University of North Texas",
                "degree": "Bachelor of Science",
                "major": "Computer Science",
                "start": "2016",
                "end": "2020",
            },
            {
                "school": "University of North Texas",
                "degree": "Master of Science",
                "major": "Computer Science",
                "start": "2024",
                "end": "2026",
            },
            {
                "school": "Texas A&M University",
                "degree": "Bachelor of Science",
                "major": "Mechanical Engineering",
                "start": "2012",
                "end": "2016",
            },
        ]
    )

    assert normalized[0]["school"] == "University of North Texas"
    assert normalized[0]["degree"] == "M.S."
    assert normalized[1]["school"] == "University of North Texas"
    assert normalized[1]["degree"] == "B.S."
    assert normalized[2]["school"] == "Texas A&M University"


def test_build_groq_payload_drops_certificate_only_education_noise():
    payload = exa_groq.build_groq_payload(
        "Rithvik Kuthuru | CS @ UNT | AI/ML, Cybersecurity & Data Science",
        RITHVIK_RAW,
    )

    education_text = " ".join(candidate["evidence"] for candidate in payload["education_candidates"]).lower()
    assert "security certificate" not in education_text
    assert "artificial intelligence (ai) certificate" not in education_text
