"""
Seniority level detection from job titles.

Determines seniority from the MOST RECENT job title (original, not normalized),
then validates against relevant experience months. Flags mismatches for review.

External output buckets (merged):
  Intern / Mid / Senior / Executive
"""

import re
from pathlib import Path
from typing import Optional

from config import logger

FLAGGED_PROFILES_FILE = Path(__file__).parent / "output" / "flagged_for_review.txt"

# Seniority keyword patterns — order matters (most specific first)
# Each tuple: (seniority_level, compiled_regex_pattern)
SENIORITY_PATTERNS = [
    # Executive level
    ("Executive", re.compile(
        r'\b(CEO|CTO|CFO|COO|CIO|CMO|CPO|CISO|Chief|President|Founder|Co-Founder|'
        r'Vice\s*President|VP|EVP|SVP)\b', re.IGNORECASE
    )),
    # Director level
    ("Director", re.compile(
        r'\b(Director|Head\s+of|Principal)\b', re.IGNORECASE
    )),
    # Manager level
    ("Manager", re.compile(
        r'\b(Manager|Supervisor|Program\s+Manager|Scrum\s+Master)\b', re.IGNORECASE
    )),
    # Senior level
    ("Senior", re.compile(
        r'\b(Senior|Sr\.?|Staff|Distinguished|Fellow|Team\s+Lead|Tech\s+Lead|Lead\s+Engineer|'
        r'Engineering\s+Lead|Project\s+Lead|Lead)\b', re.IGNORECASE
    )),
    # Junior level (Associate only when NOT followed by senior-role words)
    ("Junior", re.compile(
        r'\b(Junior|Jr\.?|Entry[\s-]?Level|Associate(?!\s+(?:Director|VP|Vice|Manager|Principal))|Apprentice)\b', re.IGNORECASE
    )),
    # Intern level
    ("Intern", re.compile(
        r'\b(Intern|Internship|Co-op|Coop|Trainee|Student\s+Worker|'
        r'Student\s+Employee|Research\s+Assistant|Teaching\s+Assistant)\b', re.IGNORECASE
    )),
]

# Min experience thresholds for flagging (in months)
SENIORITY_MIN_EXPERIENCE = {
    "Senior": 12,      # Flag if Senior but < 12 months relevant experience
    "Manager": 18,     # Flag if Manager but < 18 months
    "Director": 36,    # Flag if Director but < 36 months
    "Executive": 48,   # Flag if Executive but < 48 months
}


def _merge_seniority_level(seniority: str) -> str:
    """Merge fine-grained seniority into UI buckets: Intern/Mid/Senior/Manager/Executive."""
    s = (seniority or "").strip()
    if s == "Intern":
        return "Intern"
    if s in {"Junior", "Mid"}:
        return "Mid"
    if s == "Senior":
        return "Senior"
    if s == "Manager":
        return "Manager"
    if s in {"Director", "Executive"}:
        return "Executive"
    # Safe default: if we ever get an unknown value, treat as Mid.
    return "Mid"

_EMP_TYPE_INTERN = re.compile(
    r"\b(intern(ship)?|co-?op|trainee|student\s+worker|student\s+employee)\b",
    re.IGNORECASE,
)
_EMP_TYPE_JUNIOR = re.compile(
    r"\b(entry[\s-]?level|apprentice(ship)?)\b",
    re.IGNORECASE,
)


def _seniority_hint_from_employment_type(employment_type: str) -> Optional[str]:
    """LinkedIn subtitle after '·' (e.g. Full-time, Contract, Internship)."""
    et = (employment_type or "").strip()
    if not et:
        return None
    if _EMP_TYPE_INTERN.search(et):
        return "Intern"
    if _EMP_TYPE_JUNIOR.search(et):
        return "Junior"
    return None


def detect_seniority(job_title, employment_type=None):
    """
    Determine seniority level from the original (not normalized) job title,
    optionally refined by LinkedIn employment_type (e.g. Internship on the company line).
    """
    title = str(job_title).strip() if job_title else ""
    et_hint = _seniority_hint_from_employment_type(employment_type)

    if not title:
        if et_hint == "Intern":
            return "Intern"
        if et_hint == "Junior":
            return "Junior"
        return "Mid"

    for seniority, pattern in SENIORITY_PATTERNS:
        if pattern.search(title):
            return seniority

    if et_hint == "Intern":
        return "Intern"
    if et_hint == "Junior":
        return "Junior"
    return "Mid"


def adjust_and_flag_seniority(seniority, experience_months, linkedin_url):
    """
    Previously validated seniority against experience months.
    Now disabled — seniority flagging removed per user request.
    Returns the seniority level unchanged.
    """
    return seniority


def _flag_seniority_mismatch(linkedin_url, seniority, experience_months, expected_min):
    """Append a seniority mismatch flag to flagged_for_review.txt."""
    url = str(linkedin_url).strip().rstrip('/')
    if not url:
        return
    
    comment = (
        f"{seniority} but only {experience_months} months relevant experience "
        f"(expected >= {expected_min} months)"
    )
    flag_line = f"{url} # Seniority mismatch: {comment}\n"
    
    try:
        FLAGGED_PROFILES_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # Check if already flagged for seniority
        existing_urls = set()
        if FLAGGED_PROFILES_FILE.exists():
            with open(FLAGGED_PROFILES_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    if 'Seniority mismatch' in line:
                        existing_url = line.split('#')[0].strip()
                        existing_urls.add(existing_url)
        
        if url not in existing_urls:
            with open(FLAGGED_PROFILES_FILE, 'a', encoding='utf-8') as f:
                f.write(flag_line)
            logger.info(f"🚩 Seniority mismatch flagged: {url} - {comment}")
    except Exception as e:
        logger.warning(f"Could not flag seniority mismatch: {e}")


def analyze_seniority(profile_data, relevant_experience_months=None):
    """
    Full seniority analysis for a profile.
    
    Args:
        profile_data: dict with job title and linkedin_url
        relevant_experience_months: pre-computed relevant experience in months
        
    Returns:
        str: seniority level
    """
    # Use the most recent job title (original, not normalized)
    title = (
        profile_data.get('title')
        or profile_data.get('current_job_title')
        or ''
    ).strip()

    employment_type = (
        profile_data.get('job_employment_type')
        or profile_data.get('employment_type')
        or ''
    ).strip()
    
    linkedin_url = (
        profile_data.get('linkedin_url')
        or profile_data.get('profile_url')
        or ''
    ).strip()
    
    raw_seniority = detect_seniority(title, employment_type)
    # Flag mismatches using the fine-grained label, but return the merged bucket.
    validated = adjust_and_flag_seniority(raw_seniority, relevant_experience_months, linkedin_url)
    return _merge_seniority_level(validated)
