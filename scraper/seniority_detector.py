"""
Seniority level detection from job titles.

Determines seniority from the MOST RECENT job title (original, not normalized),
then validates against relevant experience months. Flags mismatches for review.

Buckets: Intern, Junior, Mid, Senior, Manager, Director, Executive
"""

import re
from pathlib import Path
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
        r'\b(Manager|Supervisor|Team\s+Lead|Tech\s+Lead|Lead\s+Engineer|'
        r'Engineering\s+Lead|Project\s+Lead|Program\s+Manager|Scrum\s+Master)\b', re.IGNORECASE
    )),
    # Senior level
    ("Senior", re.compile(
        r'\b(Senior|Sr\.?|Staff|Distinguished|Fellow)\b', re.IGNORECASE
    )),
    # Junior level
    ("Junior", re.compile(
        r'\b(Junior|Jr\.?|Entry[\s-]?Level|Associate|Apprentice)\b', re.IGNORECASE
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


def detect_seniority(job_title):
    """
    Determine seniority level from the original (not normalized) job title.
    
    Args:
        job_title: The original job title string
        
    Returns:
        str: One of "Intern", "Junior", "Mid", "Senior", "Manager", "Director", "Executive"
    """
    if not job_title:
        return "Mid"  # Default when no title available
    
    title = str(job_title).strip()
    if not title:
        return "Mid"
    
    # Check patterns (most specific first)
    for seniority, pattern in SENIORITY_PATTERNS:
        if pattern.search(title):
            return seniority
    
    # Default: Mid-level if no seniority indicators found
    return "Mid"


def adjust_and_flag_seniority(seniority, experience_months, linkedin_url):
    """
    Validate seniority against experience months and flag mismatches.
    
    Does NOT change the seniority assignment — just flags for manual review
    if there's a mismatch.
    
    Args:
        seniority: The detected seniority level
        experience_months: Total relevant experience months (can be None)
        linkedin_url: LinkedIn profile URL for flagging
        
    Returns:
        str: The same seniority level (unchanged)
    """
    if experience_months is None or not linkedin_url:
        return seniority
    
    min_exp = SENIORITY_MIN_EXPERIENCE.get(seniority)
    if min_exp is None:
        return seniority
    
    if experience_months < min_exp:
        _flag_seniority_mismatch(
            linkedin_url, seniority, experience_months, min_exp
        )
    
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
    
    linkedin_url = (
        profile_data.get('linkedin_url')
        or profile_data.get('profile_url')
        or ''
    ).strip()
    
    seniority = detect_seniority(title)
    seniority = adjust_and_flag_seniority(seniority, relevant_experience_months, linkedin_url)
    
    return seniority
