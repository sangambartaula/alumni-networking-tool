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
    # --- Computer Science & Engineering Department ---
    (re.compile(r'\b(computer\s*science|comp\.?sci|c\.?s\.?|software\s*engineering)\b', re.I),
     "Computer Science"),
    
    (re.compile(r'\b(computer\s*engineering|comp\.?eng|c\.?e\.?)\b', re.I),
     "Computer Engineering"),
    
    (re.compile(r'\b(information\s*technology|i\.?t\.?)\b', re.I),
     "Information Technology"),
    
    (re.compile(r'\b(cyber\s*security|computer\s*security)\b', re.I),
     "Cybersecurity"),
    
    (re.compile(r'\b(data\s*engineering)\b', re.I),
     "Data Engineering"),
     
    (re.compile(r'\b(artificial\s*intelligence|a\.?i\.?|machine\s*learning|data\s*science)\b', re.I),
     "Artificial Intelligence"),

    # --- Biomedical ---
    (re.compile(r'\b(biomedical\s*engineering|bmen|bio\.?med)\b', re.I),
     "Biomedical Engineering"),

    # --- Electrical ---
    (re.compile(r'\b(electrical\s*engineering|e\.?e\.?)\b', re.I),
     "Electrical Engineering"),

    # --- Materials Science ---
    (re.compile(r'\b(materials?\s*science|materials?\s*engineering|mtse)\b', re.I),
     "Materials Science and Engineering"),

    # --- Mechanical & Energy ---
    # Map generic "Mechanical Engineering" to UNT's "Mechanical and Energy Engineering"
    (re.compile(r'\b(mechanical\s*and\s*energy\s*engineering|mechanical\s*engineering|m\.?e\.?e\.?|mech\.?eng)\b', re.I),
     "Mechanical and Energy Engineering"),

    (re.compile(r'\b(mechanical\s*engineering\s*technology|m\.?e\.?t\.?)\b', re.I),
     "Mechanical Engineering Technology"),
     
    (re.compile(r'\b(construction\s*management|construction\s*engineering)\b', re.I),
     "Construction Management"),
     
    (re.compile(r'\b(engineering\s*management)\b', re.I),
     "Engineering Management"),

    # --- Catch-all Engineering ---
    (re.compile(r'\b(engineering|engr)\b', re.I),
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
