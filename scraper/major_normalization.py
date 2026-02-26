"""
Major Normalization Module

Strictly maps raw major text to an approved UNT major list or "Other".
"""

import json
import logging
import os
import re

logger = logging.getLogger(__name__)

# Allowed UNT majors provided by product requirements.
UNT_ALLOWED_MAJORS = [
    "Autonomous Systems",
    "Biomedical Engineering",
    "Computer Engineering",
    "Computer Science",
    "Computer Science and Engineering",
    "Construction Management",
    "Cybersecurity",
    "Data Engineering",
    "Electrical Engineering",
    "Engineering Management",
    "Engineering Technology",
    "Information Technology",
    "Machine Learning",
    "Materials Science",
    "Materials Science and Engineering",
    "Mechanical and Energy Engineering",
    "Mechanical Engineering Technology",
    "Other",
]

# For LLM ID mapping (exclude Other; 0 means Other).
CANONICAL_MAJORS = [m for m in UNT_ALLOWED_MAJORS if m != "Other"]
_CANONICAL_MAJOR_BY_LOWER = {m.lower(): m for m in CANONICAL_MAJORS}

# High-signal exact aliases.
_EXACT_MAJOR_MAP = {
    "cse": "Computer Science and Engineering",
    "ece": "Electrical Engineering",
    "computer and information sciences": "Computer Science",
    "computer and information sciences and support services": "Computer Science",
    "computer and information sciences, general": "Computer Science",
    "computer and information systems security/information assurance": "Computer Science",
    "btech": "Other",
    "student": "Other",
    "other": "Other",
}

# Ordered regex mapping; first match wins.
_MAJOR_PATTERNS = [
    # Autonomous/robotics
    (re.compile(r"\b(autonomous\s+systems?|robotics(\s+engineering)?)\b", re.I), "Autonomous Systems"),

    # Computer stack
    (re.compile(r"\b(computer\s+science\s+and\s+engineering|c\.?\s*s\.?\s*and\s*e\.?|cs\s*&\s*e)\b", re.I), "Computer Science and Engineering"),
    (re.compile(r"\b(computer\s+engineering|comp\.?\s*eng|computer\s*hardware\s*engineering)\b", re.I), "Computer Engineering"),
    (re.compile(r"\b(computer\s+science|computer\s+and\s+information\s+sciences?(?:\s+and\s+support\s+services)?(?:,\s*general)?|computer\s+programming(?:,\s*specific\s+applications)?|c\.?\s*s\.?\b)\b", re.I), "Computer Science"),

    # Security / data / AI
    (re.compile(r"\b(cyber\s*security|infosec)\b", re.I), "Cybersecurity"),
    (re.compile(r"\b(data\s+engineering|data\s+science|visual\s+analytics|health\s+data)\b", re.I), "Data Engineering"),
    (re.compile(r"\b(machine\s+learning|artificial\s+intelligence|ai)\b", re.I), "Machine Learning"),

    # IT / info systems
    (re.compile(r"\b(information\s+technology|information\s+systems?\s*(?:and|&)\s*technolog(?:y|ies)|information\s+systems?|information\s+science(?:\s*/\s*information\s+systems)?|information\s+science\/studies|informations\s+systems\s+and\s+technologies)\b", re.I), "Information Technology"),

    # Core engineering
    (re.compile(r"\b(biomedical\s+engineering|biomedical\s+sciences?)\b", re.I), "Biomedical Engineering"),
    (re.compile(r"\b(electrical\s+engineering|electronics\s+engineering|microwaves?\s+and\s+communication)\b", re.I), "Electrical Engineering"),
    (re.compile(r"\b(materials?\s+science\s+and\s+engineering)\b", re.I), "Materials Science and Engineering"),
    (re.compile(r"\b(materials?\s+science)\b", re.I), "Materials Science"),
    (re.compile(r"\b(mechanical\s*(?:and|&)\s*energy\s*engineering|mechanical\s+engineering)\b", re.I), "Mechanical and Energy Engineering"),
    (re.compile(r"\b(mechanical\s+engineering\s+technology|engineering\s+technology)\b", re.I), "Mechanical Engineering Technology"),
    (re.compile(r"\b(construction\s+management|construction\s+engineering\s+technology|building\s+construction\s+technology)\b", re.I), "Construction Management"),

    # Management
    (re.compile(r"\b(engineering\s+management|engineering\s*/\s*industrial\s*management|engineering\s*technology\s*&\s*management|industrial\s+management)\b", re.I), "Engineering Management"),
]


def _strip_minor_noise(text: str) -> str:
    """Drop minor/concentration fragments and normalize separators."""
    t = re.sub(r"\s+", " ", (text or "")).strip()
    if not t:
        return ""

    t = re.sub(r"(?i)^double\s+major\s*:\s*", "", t).strip()
    t = re.sub(r"(?i)^major\s*:\s*", "", t).strip()

    # Remove parenthetical minor/concentration details.
    t = re.sub(r"\(([^)]*(minor|concentration|track|certificate)[^)]*)\)", "", t, flags=re.I).strip()

    # Remove trailing "with ... minor/concentration".
    t = re.sub(r"(?i)\bwith\b[^,;|]*\b(minor|concentration|track|certificate)\b.*$", "", t).strip()

    # Remove explicit minor suffix segments.
    t = re.sub(r"(?i)[,;/|\-]\s*.*\b(minor|concentration|track|certificate)\b.*$", "", t).strip()

    # Normalize ampersand spacing.
    t = re.sub(r"\s*&\s*", " & ", t)
    t = re.sub(r"\s+", " ", t).strip(" ,;|-")
    return t


def _coerce_llm_major_choice(payload: dict | None) -> str:
    """
    Parse Groq JSON payload and coerce it to an allowed major.
    Priority:
      1) major_id (strict numeric)
      2) exact text match against approved majors (case-insensitive)
    Any unknown value defaults to "Other".
    """
    if not isinstance(payload, dict):
        return "Other"

    major_id = payload.get("major_id")
    if isinstance(major_id, str):
        major_id = major_id.strip()
        if major_id.isdigit():
            major_id = int(major_id)

    if isinstance(major_id, int):
        if major_id == 0:
            return "Other"
        if 1 <= major_id <= len(CANONICAL_MAJORS):
            return CANONICAL_MAJORS[major_id - 1]

    # Fallback: allow exact canonical major text from known keys.
    for key in ("major", "major_name", "normalized_major", "major_label"):
        value = payload.get(key)
        if not isinstance(value, str):
            continue
        cleaned = re.sub(r"\s+", " ", value).strip().strip("\"'")
        if not cleaned:
            continue
        if cleaned.lower() == "other":
            return "Other"
        exact = _CANONICAL_MAJOR_BY_LOWER.get(cleaned.lower())
        if exact:
            return exact

    # Last-resort scan: if any string value exactly matches an approved major.
    for value in payload.values():
        if not isinstance(value, str):
            continue
        cleaned = re.sub(r"\s+", " ", value).strip().strip("\"'")
        if not cleaned:
            continue
        exact = _CANONICAL_MAJOR_BY_LOWER.get(cleaned.lower())
        if exact:
            return exact

    return "Other"


def standardize_major(raw_major: str, job_title: str = "") -> str:
    """
    Map raw major to one of UNT_ALLOWED_MAJORS, else "Other".
    """
    if not raw_major or not raw_major.strip():
        return "Other"

    text = _strip_minor_noise(raw_major)
    if not text:
        return "Other"

    exact = _EXACT_MAJOR_MAP.get(text.lower())
    if exact:
        return exact

    direct = _CANONICAL_MAJOR_BY_LOWER.get(text.lower())
    if direct:
        return direct

    for pattern, canonical in _MAJOR_PATTERNS:
        if pattern.search(text):
            return canonical

    # LLM fallback is strict (major_id only) and enabled by default.
    # Any non-integer/invalid output still resolves to "Other".
    use_llm_fallback = os.getenv("MAJOR_USE_GROQ_FALLBACK", "1") == "1"
    if use_llm_fallback and os.getenv("GROQ_API_KEY"):
        llm_result = _standardize_major_with_llm(text, job_title)
        if llm_result in UNT_ALLOWED_MAJORS:
            return llm_result

    return "Other"


def _standardize_major_with_llm(raw_major: str, job_title: str) -> str:
    """Use Groq to map raw major to one of CANONICAL_MAJORS or Other."""
    try:
        from groq_client import _get_client, GROQ_MODEL

        client = _get_client()
        if not client:
            return "Other"

        canonical_list = "\n".join(
            f"{idx}. {name}" for idx, name in enumerate(CANONICAL_MAJORS, start=1)
        )
        major_text = (raw_major or "").strip()[:220]
        job_text = (job_title or "").strip()[:140]

        prompt = f"""
Map this raw major to ONE approved UNT major ID.

Raw Major: "{major_text}"
Job Title Context (weak hint only): "{job_text}"

Approved majors (ID -> name):
{canonical_list}

Rules:
1. Output ONLY one major_id from 0-{len(CANONICAL_MAJORS)}.
2. major_id=0 means Other.
3. Ignore minors/concentrations/certificates and map the PRIMARY major.
4. Do not invent labels outside the approved list.

Return JSON only:
{{ "major_id": <integer 0-{len(CANONICAL_MAJORS)}> }}
        """

        completion = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are a data cleaning assistant. Output valid JSON only."},
                {"role": "user", "content": prompt},
            ],
            model=GROQ_MODEL,
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=24,
        )

        payload = json.loads(completion.choices[0].message.content)
        return _coerce_llm_major_choice(payload)

    except Exception as e:
        logger.warning(f"LLM major standardization failed for '{raw_major}': {e}")
        return "Other"
