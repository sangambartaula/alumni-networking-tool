"""
Major Normalization Module

Maps raw major/field-of-study strings to canonical UNT College of Engineering program names.

Example:
    "CS" → "Computer Science"
    "Comp Eng" → "Computer Engineering"
    "EE" → "Electrical Engineering"
"""

import re
import logging

logger = logging.getLogger(__name__)

# ── Canonical majors (UNT College of Engineering) ─────────────────────────
# Each entry: (compiled regex pattern, canonical name)
# Order matters — more specific patterns first to avoid false positives.
_MAJOR_PATTERNS = [
    # Computer Science
    (re.compile(
        r'\b(computer\s*science|comp\.?\s*sci\.?|c\.?\s*s\.?|compsci|computing science)\b', re.I),
     "Computer Science"),

    # Computer Engineering
    (re.compile(
        r'\b(computer\s*engineering|comp\.?\s*eng\.?|computer\s*systems?\s*engineering)\b', re.I),
     "Computer Engineering"),

    # Electrical Engineering
    (re.compile(
        r'\b(electrical\s*engineering|electrical\s*&?\s*electronics?\s*engineering|e\.?\s*e\.?)\b', re.I),
     "Electrical Engineering"),

    # Mechanical Engineering
    (re.compile(
        r'\b(mechanical\s*engineering|mech\.?\s*eng\.?)\b', re.I),
     "Mechanical Engineering"),

    # Biomedical Engineering
    (re.compile(
        r'\b(biomedical\s*engineering|biomed\.?\s*eng\.?|bio-?medical\s*eng)\b', re.I),
     "Biomedical Engineering"),

    # Materials Science and Engineering
    (re.compile(
        r'\b(materials?\s*science|materials?\s*eng|materials?\s*science\s*(?:and|&)\s*eng)\b', re.I),
     "Materials Science and Engineering"),

    # Construction Engineering Technology
    (re.compile(
        r'\b(construction\s*(?:engineering|management|technology)|construction\s*eng\.?\s*tech)\b', re.I),
     "Construction Engineering Technology"),

    # Information Technology
    (re.compile(
        r'\b(information\s*technology|info\.?\s*tech\.?|i\.?\s*t\.?)\b', re.I),
     "Information Technology"),

    # Data Science
    (re.compile(
        r'\b(data\s*science|data\s*analytics)\b', re.I),
     "Data Science"),

    # Artificial Intelligence
    (re.compile(
        r'\b(artificial\s*intelligence|a\.?\s*i\.?|machine\s*learning)\b', re.I),
     "Artificial Intelligence"),

    # Cybersecurity
    (re.compile(
        r'\b(cyber\s*security|cybersecurity|information\s*security|info\.?\s*sec)\b', re.I),
     "Cybersecurity"),

    # Software Engineering (not in UNT CoE but common)
    (re.compile(
        r'\b(software\s*engineering|software\s*eng\.?)\b', re.I),
     "Software Engineering"),

    # Civil Engineering (not in UNT CoE but common)
    (re.compile(
        r'\b(civil\s*engineering|civil\s*eng\.?)\b', re.I),
     "Civil Engineering"),

    # Industrial Engineering (not in UNT CoE but common)
    (re.compile(
        r'\b(industrial\s*engineering|industrial\s*eng\.?)\b', re.I),
     "Industrial Engineering"),

    # Broad engineering catch
    (re.compile(
        r'\bengineering\b', re.I),
     "Other Engineering"),
]


def standardize_major(raw_major: str) -> str:
    """
    Map a raw major string to a canonical program name.

    Returns the canonical name if matched, otherwise:
    - "Other Engineering" if 'engineering' appears in the text
    - "Other" for everything else

    Does not modify raw data — used only for `standardized_major*` columns.
    """
    if not raw_major or not raw_major.strip():
        return "Other"

    text = raw_major.strip()

    for pattern, canonical in _MAJOR_PATTERNS:
        if pattern.search(text):
            return canonical

    return "Other"
