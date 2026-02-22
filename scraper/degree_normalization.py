"""
Degree Normalization Module

Deterministic normalization of degree strings to canonical forms.
Follows the same pattern as job_title_normalization.py.

Example:
    "Bachelor of Science" → "Bachelor of Science"
    "BS" → "Bachelor of Science"
    "M.S." → "Master of Science"
    "BSME" → "Bachelor of Science in Mechanical Engineering"
"""

import re
import logging

logger = logging.getLogger(__name__)

# Official UNT College of Engineering (CENG) Degrees for standardization
OFFICIAL_UNT_DEGREES = [
    "B.S. Biomedical Engineering",
    "B.A. Information Technology",
    "B.S. Computer Engineering",
    "B.S. Computer Science",
    "B.S. Cybersecurity",
    "B.S. Electrical Engineering",
    "B.S. Materials Science and Engineering",
    "B.S. Construction Management",
    "B.S. Mechanical and Energy Engineering",
    "B.S.E.T. Mechanical Engineering Technology",
    "M.S. Biomedical Engineering",
    "M.S. Computer Engineering",
    "M.S. Computer Science",
    "M.S. Cybersecurity",
    "M.S. Data Engineering",
    "M.S. Electrical Engineering",
    "M.S. Materials Science and Engineering",
    "M.S. Mechanical and Energy Engineering",
    "M.S. Engineering Management",
    "M.S. Engineering Technology",
    "Ph.D. Biomedical Engineering",
    "Ph.D. Computer Science and Engineering",
    "Ph.D. Electrical Engineering",
    "Ph.D. Materials Science and Engineering",
    "Ph.D. Mechanical and Energy Engineering"
]

# ── Canonical degree mapping ──────────────────────────────────────────────
# Keys are lowercase patterns; values are canonical display strings.
DEGREE_MAP = {
    # Bachelor of Science variants
    "bachelor of science": "Bachelor of Science",
    "bachelors of science": "Bachelor of Science",
    "bachelor's of science": "Bachelor of Science",
    "b.s.": "Bachelor of Science",
    "b.s": "Bachelor of Science",
    "bs": "Bachelor of Science",
    "bsc": "Bachelor of Science",
    "b.sc.": "Bachelor of Science",
    "b.sc": "Bachelor of Science",

    # Bare "bachelors" / "bachelor's" / "bachelor's degree"
    "bachelor": "Bachelor's Degree",
    "bachelors": "Bachelor's Degree",
    "bachelor's": "Bachelor's Degree",
    "bachelor's degree": "Bachelor's Degree",
    "bachelors degree": "Bachelor's Degree",
    "undergraduate degree": "Bachelor's Degree",
    "undergraduate": "Bachelor's Degree",

    # Bachelor of Arts variants
    "bachelor of arts": "Bachelor of Arts",
    "bachelors of arts": "Bachelor of Arts",
    "bachelor's of arts": "Bachelor of Arts",
    "b.a.": "Bachelor of Arts",
    "b.a": "Bachelor of Arts",
    "ba": "Bachelor of Arts",

    # Bachelor of Engineering variants
    "bachelor of engineering": "Bachelor of Engineering",
    "b.e.": "Bachelor of Engineering",
    "b.eng.": "Bachelor of Engineering",
    "b.eng": "Bachelor of Engineering",
    "beng": "Bachelor of Engineering",

    # Bachelor of Technology variants
    "bachelor of technology": "Bachelor of Technology",
    "bachelor of technology - btech": "Bachelor of Technology",
    "b.tech.": "Bachelor of Technology",
    "b.tech": "Bachelor of Technology",
    "btech": "Bachelor of Technology",

    # Bachelor of Business Administration
    "bachelor of business administration": "Bachelor of Business Administration",
    "bba": "Bachelor of Business Administration",
    "b.b.a.": "Bachelor of Business Administration",
    "b.b.a": "Bachelor of Business Administration",

    # BAAS (Bachelor of Applied Arts and Science)
    "bachelor of applied arts and science": "Bachelor of Applied Arts and Science",
    "bachelor of applied art and science": "Bachelor of Applied Arts and Science",
    "baas": "Bachelor of Applied Arts and Science",

    # BSBA (Bachelor of Science in Business Administration)
    "bsba": "Bachelor of Business Administration",

    # Bachelor of Fine Arts
    "bachelor of fine arts": "Bachelor of Fine Arts",
    "bfa": "Bachelor of Fine Arts",
    "b.f.a.": "Bachelor of Fine Arts",
    "b.f.a": "Bachelor of Fine Arts",

    # BSME / BSEE / BSCE specialty abbreviations
    "bsme": "Bachelor of Science in Mechanical Engineering",
    "bsee": "Bachelor of Science in Electrical Engineering",
    "bsce": "Bachelor of Science in Civil Engineering",
    "bscs": "Bachelor of Science in Computer Science",
    "bsie": "Bachelor of Science in Industrial Engineering",
    "bachelor of science in engineering technology": "Bachelor of Science",
    "bset": "Bachelor of Science",

    # Master of Science variants
    "master of science": "Master of Science",
    "masters of science": "Master of Science",
    "master's of science": "Master of Science",
    "m.s.": "Master of Science",
    "m.s": "Master of Science",
    "ms": "Master of Science",
    "msc": "Master of Science",
    "m.sc.": "Master of Science",
    "m.sc": "Master of Science",

    # Bare "masters" / "master's" / "master's degree"
    "master": "Master's Degree",
    "masters": "Master's Degree",
    "master's": "Master's Degree",
    "master's degree": "Master's Degree",
    "masters degree": "Master's Degree",

    # Master of Arts variants
    "master of arts": "Master of Arts",
    "masters of arts": "Master of Arts",
    "master's of arts": "Master of Arts",
    "m.a.": "Master of Arts",
    "m.a": "Master of Arts",
    "ma": "Master of Arts",

    # Master of Engineering
    "master of engineering": "Master of Engineering",
    "m.eng.": "Master of Engineering",
    "m.eng": "Master of Engineering",
    "meng": "Master of Engineering",
    "m.e.": "Master of Engineering",

    # MBA
    "master of business administration": "Master of Business Administration",
    "mba": "Master of Business Administration",
    "m.b.a.": "Master of Business Administration",
    "m.b.a": "Master of Business Administration",

    # MPA
    "master of public administration": "Master of Public Administration",
    "mpa": "Master of Public Administration",
    "m.p.a.": "Master of Public Administration",
    "m.p.a": "Master of Public Administration",

    # MFA
    "master of fine arts": "Master of Fine Arts",
    "mfa": "Master of Fine Arts",
    "m.f.a.": "Master of Fine Arts",
    "m.f.a": "Master of Fine Arts",

    # Doctor of Philosophy
    "doctor of philosophy": "Doctor of Philosophy",
    "phd": "Doctor of Philosophy",
    "ph.d.": "Doctor of Philosophy",
    "ph.d": "Doctor of Philosophy",
    "doctorate": "Doctor of Philosophy",

    # Doctor of Education
    "doctor of education": "Doctor of Education",
    "edd": "Doctor of Education",
    "ed.d.": "Doctor of Education",
    "ed.d": "Doctor of Education",

    # Associate degrees
    "associate of science": "Associate of Science",
    "associate of arts": "Associate of Arts",
    "associate's degree": "Associate's Degree",
    "associate degree": "Associate's Degree",
    "associated degree": "Associate's Degree",
    "associates degree": "Associate's Degree",
    "a.s.": "Associate of Science",
    "a.a.": "Associate of Arts",
    "aas": "Associate of Applied Science",
    "a.a.s.": "Associate of Applied Science",

    # Juris Doctor
    "juris doctor": "Juris Doctor",
    "jd": "Juris Doctor",
    "j.d.": "Juris Doctor",
    "j.d": "Juris Doctor",

    # Doctor of Medicine
    "doctor of medicine": "Doctor of Medicine",
    "md": "Doctor of Medicine",
    "m.d.": "Doctor of Medicine",
    "m.d": "Doctor of Medicine",
}


def normalize_degree_deterministic(raw_degree: str) -> str:
    """
    Normalize a degree string to its canonical form.

    Steps:
    1. Strip and lowercase
    2. Try full-string exact match in DEGREE_MAP
    3. Try extracting the degree prefix (before "in", ",", or "-") and matching
    4. Return the original cleaned string if no match found

    Args:
        raw_degree: The raw degree string (e.g., "BS in Computer Science")

    Returns:
        Normalized canonical degree string (e.g., "Bachelor of Science")
        Returns "" for empty/None input.
    """
    if not raw_degree:
        return ""

    cleaned = raw_degree.strip()
    if not cleaned:
        return ""

    lower = cleaned.lower()

    # 1. Full exact match
    if lower in DEGREE_MAP:
        return DEGREE_MAP[lower]

    # 2. Try prefix before "in", ",", or "-"
    # Example: "BS in Computer Science" → try "bs"
    # Example: "Bachelor of Science, Computer Science" → try "bachelor of science"
    prefix_match = re.match(r'^(.+?)\s*(?:\bin\b|,|\s*[-–—]\s*)', lower)
    if prefix_match:
        prefix = prefix_match.group(1).strip()
        if prefix in DEGREE_MAP:
            return DEGREE_MAP[prefix]

    # 3. Try matching known patterns anywhere in the string
    # Sort by longest match first to prefer more specific matches
    # Use lookaround to prevent false substring hits (e.g. "ma" in "diploma")
    for pattern in sorted(DEGREE_MAP.keys(), key=len, reverse=True):
        if re.search(r'(?<![a-z])' + re.escape(pattern) + r'(?![a-z])', lower):
            return DEGREE_MAP[pattern]

    # 4. No match — return the original cleaned string (title case)
    return cleaned


def get_or_create_normalized_degree(conn, raw_degree: str) -> int | None:
    """
    Get or create a normalized degree entry in the database.

    Args:
        conn: Database connection (MySQL or SQLite).
        raw_degree: The raw degree string.

    Returns:
        The ID of the normalized_degrees row, or None if raw_degree is empty.
    """
    normalized = normalize_degree_deterministic(raw_degree)
    if not normalized:
        return None

    is_sqlite = hasattr(conn, 'execute') and not hasattr(conn, 'cmd_query')

    try:
        cur = conn.cursor()

        if is_sqlite:
            # SQLite: INSERT OR IGNORE + SELECT
            cur.execute(
                "INSERT OR IGNORE INTO normalized_degrees (normalized_degree) VALUES (?)",
                (normalized,)
            )
            cur.execute(
                "SELECT id FROM normalized_degrees WHERE normalized_degree = ?",
                (normalized,)
            )
        else:
            # MySQL: INSERT IGNORE + SELECT
            cur.execute(
                "INSERT IGNORE INTO normalized_degrees (normalized_degree) VALUES (%s)",
                (normalized,)
            )
            cur.execute(
                "SELECT id FROM normalized_degrees WHERE normalized_degree = %s",
                (normalized,)
            )

        row = cur.fetchone()
        cur.close()

        if row:
            return row[0] if not isinstance(row, dict) else row.get('id')
        return None

    except Exception as e:
        logger.error(f"Error getting/creating normalized degree '{normalized}': {e}")
        return None


def get_all_normalized_degrees(conn) -> dict:
    """
    Fetch all normalized degrees from the database.

    Returns:
        Dict mapping normalized_degree string → id
    """
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, normalized_degree FROM normalized_degrees")
        rows = cur.fetchall()
        cur.close()

        result = {}
        for row in rows:
            if isinstance(row, dict):
                result[row['normalized_degree']] = row['id']
            else:
                result[row[1]] = row[0]
        return result

    except Exception as e:
        logger.error(f"Error fetching normalized degrees: {e}")
        return {}


# ── Simple grouping labels (for standardized_degree column) ───────────────
# Maps canonical display strings → group labels
_DEGREE_GROUP_MAP = {
    # Bachelors
    "Bachelor of Science": "Bachelors",
    "Bachelor of Arts": "Bachelors",
    "Bachelor of Engineering": "Bachelors",
    "Bachelor of Technology": "Bachelors",
    "Bachelor of Business Administration": "Bachelors",
    "Bachelor of Fine Arts": "Bachelors",
    "Bachelor of Applied Arts and Science": "Bachelors",
    "Bachelor's Degree": "Bachelors",
    "Bachelor of Science in Mechanical Engineering": "Bachelors",
    "Bachelor of Science in Electrical Engineering": "Bachelors",
    "Bachelor of Science in Civil Engineering": "Bachelors",
    "Bachelor of Science in Computer Science": "Bachelors",
    "Bachelor of Science in Industrial Engineering": "Bachelors",
    # Masters
    "Master of Science": "Masters",
    "Master of Arts": "Masters",
    "Master of Engineering": "Masters",
    "Master of Business Administration": "Masters",
    "Master of Fine Arts": "Masters",
    "Master of Public Administration": "Masters",
    "Master's Degree": "Masters",
    # Doctorate
    "Doctor of Philosophy": "Doctorate",
    "Doctor of Education": "Doctorate",
    "Doctor of Medicine": "Doctorate",
    "Juris Doctor": "Doctorate",
    # Associate
    "Associate of Science": "Associate",
    "Associate of Arts": "Associate",
    "Associate's Degree": "Associate",
    "Associate of Applied Science": "Associate",
}

# Keyword fallbacks for strings that don't match DEGREE_MAP exactly
_GROUP_KEYWORDS = [
    # Order matters — check most specific first
    # IMPORTANT: high school / diploma MUST come before Masters,
    # because "diploma" contains "ma" which can match m.a. regex
    (re.compile(r'\b(high\s*school|diploma|ged)\b', re.I), "Other"),
    (re.compile(r'\b(certificate|certification|cert)\b', re.I), "Other"),
    (re.compile(r'\b(ph\.?d|doctor|doctorate|ed\.?d|d\.?sc|sc\.?d)\b', re.I), "Doctorate"),
    (re.compile(r'\b(masters?|m\.?s\.?c?|m\.?eng|mba|m\.?b\.?a|m\.?p\.?a|m\.?f\.?a)\b', re.I), "Masters"),
    (re.compile(r'(?<![a-z])m\.?a\.?(?![a-z])', re.I), "Masters"),
    (re.compile(r'\b(bachelors?|b\.?s\.?c?|b\.?a\.?|b\.?eng|b\.?e\.?|b\.?tech|b\.?f\.?a|b\.?s\.?e\.?t)\b', re.I), "Bachelors"),
    (re.compile(r'\b(associates?|a\.?s\.?|a\.?a\.?|a\.?a\.?s)\b', re.I), "Associate"),
]


def standardize_degree(raw_degree: str) -> str:
    """
    Map a raw degree string to a simple group label or an official UNT degree.

    Returns one of: Official UNT Name, Bachelors, Masters, Doctorate, Associate, Unknown.
    """
    if not raw_degree or not raw_degree.strip():
        return "Other"

    lower = raw_degree.lower()
    
    # 1. Try mapping to Official UNT Degree first
    for official in OFFICIAL_UNT_DEGREES:
        # Clean both for fuzzy match
        official_clean = re.sub(r'[^\w\s]', '', official.lower())
        raw_clean = re.sub(r'[^\w\s]', '', lower)
        
        # Exact match of cleaned strings or presence of full official name
        if official_clean == raw_clean or official_clean in raw_clean:
            return official

    # Early exit: high school / diploma / GED are always "Unknown"
    if re.search(r'\b(high\s*school|diploma|ged)\b', lower, re.I):
        return "Unknown"

    # 2. Try canonical resolution via existing DEGREE_MAP
    canonical = normalize_degree_deterministic(raw_degree)

    # If canonical matched in our group map, we're done
    if canonical in _DEGREE_GROUP_MAP:
        result = _DEGREE_GROUP_MAP[canonical]
    else:
        # Keyword fallback on the raw string
        result = "Unknown"
        for pattern, group in _GROUP_KEYWORDS:
            if pattern.search(lower):
                result = group
                break

    # Sanity check: "diploma" never belongs to Masters
    if result == "Masters" and re.search(r'\bdiploma\b', raw_degree, re.I):
        return "Other"
        
    # User requested fallback to Other
    if result == "Other":
        return "Other"

    return result

def extract_hidden_degree(raw_major: str) -> tuple[str, str]:
    """
    If the degree field is blank but the major contains an obvious degree 
    keyword (like PhD, BFA, Master's), extract the standard degree group
    and remove the degree text from the major string.
    
    Returns: (extracted_degree_group, cleaned_major_string)
    """
    if not raw_major:
         return "", ""
         
    lower_maj = raw_major.lower()
    extracted_degree_group = ""
    
    # Check if a degree keyword is present
    for pattern, group in _GROUP_KEYWORDS:
        if pattern.search(lower_maj):
            extracted_degree_group = group # e.g. "Bachelors", "Doctorate"
            break
            
    if not extracted_degree_group:
        return "", raw_major
        
    cleaned = raw_major
    
    # 1. Strip full formal names (Bachelor of Science, Doctor of Philosophy, etc)
    cleaned = re.sub(r'(?i)\b(?:Doctor|Master|Bachelor|Associate)s?\s*(?:of\s+[A-Za-z\s]+)?\b', '', cleaned)
    
    # 2. Strip acronyms (PhD, BFA, MS, etc)
    cleaned = re.sub(r'(?i)\b(ph\.?d|b\.?f\.?a|m\.?b\.?a|m\.?s|b\.?s|b\.?a|m\.?a|b\.?tech|m\.?s\.?c|b\.?s\.?c)\b', '', cleaned)
    
    # 3. Strip prefix/plurals like "2 ", "'s "
    cleaned = re.sub(r"(?i)\b\d+\s*'?s?\b", "", cleaned)
    cleaned = cleaned.replace("'s", "")
    
    # 4. Strip connectors like " in ", " - ", ","
    cleaned = re.sub(r'(?i)^\s*(in|of|in the field of)\s+', '', cleaned)
    
    # Clean up dangling punctuation from stripping
    cleaned = re.sub(r'^[,\-\/\.\s]+', '', cleaned)
    cleaned = re.sub(r'[,\-\/\.\s]+$', '', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    return extracted_degree_group, cleaned
