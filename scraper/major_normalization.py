"""
Major Normalization Module

Strictly maps raw major text to an approved UNT major list or "Other".

Multi-entry handling:
  "Computer Science and Engineering" is the only raw major that maps to TWO
  canonical majors: "Computer Science" AND "Computer Engineering".  The helper
  ``standardize_major_list()`` returns a list (length 1 for normal majors,
  length 2 for the CS&E case).  The legacy ``standardize_major()`` keeps its
  single-string return for backward compatibility (returns the primary entry).
"""

import json
import logging
import os
import re
from typing import List

logger = logging.getLogger(__name__)

# ── Degree-agnostic normalized majors list (product requirements v2) ─────────
UNT_ALLOWED_MAJORS = [
    "Artificial Intelligence",
    "Biomedical Engineering",
    "Computer Engineering",
    "Computer Science",
    "Construction Engineering Technology",
    "Construction Management",
    "Cybersecurity",
    "Data Engineering",
    "Electrical Engineering",
    "Engineering Management",
    "Geographic Information Systems + Computer Science",
    "Information Technology",
    "Materials Science and Engineering",
    "Mechanical and Energy Engineering",
    "Mechanical Engineering Technology",
    "Semiconductor Manufacturing Engineering",
    "Other",
]

# Internal sentinel for the only multi-entry raw major.
_CSE_MULTI = "__CSE_MULTI__"

# For LLM ID mapping (exclude Other; 0 means Other).
CANONICAL_MAJORS = [m for m in UNT_ALLOWED_MAJORS if m != "Other"]
_CANONICAL_MAJOR_BY_LOWER = {m.lower(): m for m in CANONICAL_MAJORS}

# High-signal exact aliases.
# Values may be a canonical string OR ``_CSE_MULTI`` for the dual-mapped case.
_EXACT_MAJOR_MAP = {
    "cse": _CSE_MULTI,
    "cs&e": _CSE_MULTI,
    "computer science and engineering": _CSE_MULTI,
    "ece": "Electrical Engineering",
    "ee": "Electrical Engineering",
    "computer and information sciences": "Computer Science",
    "computer and information sciences and support services": "Computer Science",
    "computer and information sciences, general": "Computer Science",
    "computer and information systems security/information assurance": "Cybersecurity",
    "gis": "Geographic Information Systems + Computer Science",
    "geographic information systems": "Geographic Information Systems + Computer Science",
    "geographic information science": "Geographic Information Systems + Computer Science",
    "semiconductor manufacturing": "Semiconductor Manufacturing Engineering",
    "semiconductor engineering": "Semiconductor Manufacturing Engineering",
    "autonomous systems": "Other",
    "robotics": "Other",
    "machine learning": "Artificial Intelligence",
    "materials science": "Materials Science and Engineering",
    "engineering technology": "Mechanical Engineering Technology",
    "btech": "Other",
    "student": "Other",
    "other": "Other",
}

# Ordered regex mapping; first match wins.
# Values may be a canonical string OR ``_CSE_MULTI``.
_MAJOR_PATTERNS = [
    # CS&E multi-entry (must precede CS / CE patterns)
    (re.compile(r"\b(computer\s+science\s+and\s+engineering|c\.?\s*s\.?\s*and\s*e\.?|cs\s*&\s*e)\b", re.I), _CSE_MULTI),

    # Computer stack
    (re.compile(r"\b(computer\s+engineering|comp\.?\s*eng|computer\s*hardware\s*engineering)\b", re.I), "Computer Engineering"),
    (re.compile(r"\b(computer\s+science|computer\s+and\s+information\s+sciences?(?:\s+and\s+support\s+services)?(?:,\s*general)?|computer\s+programming(?:,\s*specific\s+applications)?|c\.?\s*s\.?\b)\b", re.I), "Computer Science"),

    # Security / data / AI
    (re.compile(r"\b(cyber\s*security|infosec|information\s+assurance)\b", re.I), "Cybersecurity"),
    (re.compile(r"\b(data\s+engineering|data\s+science|visual\s+analytics|health\s+data)\b", re.I), "Data Engineering"),
    (re.compile(r"\b(artificial\s+intelligence|machine\s+learning|ai(?:\s+engineering)?)\b", re.I), "Artificial Intelligence"),

    # GIS + CS
    (re.compile(r"\b(geographic\s+information\s+(?:systems?|science)|gis)\b", re.I), "Geographic Information Systems + Computer Science"),

    # IT / info systems
    (re.compile(r"\b(information\s+technology|information\s+systems?\s*(?:and|&)\s*technolog(?:y|ies)|information\s+systems?|information\s+science(?:\s*/\s*information\s+systems)?|information\s+science\/studies|informations\s+systems\s+and\s+technologies)\b", re.I), "Information Technology"),

    # Core engineering
    (re.compile(r"\b(biomedical\s+engineering|biomedical\s+sciences?)\b", re.I), "Biomedical Engineering"),
    (re.compile(r"\b(electrical\s+engineering|electronics\s+engineering|microwaves?\s+and\s+communication)\b", re.I), "Electrical Engineering"),
    (re.compile(r"\b(materials?\s+science\s+and\s+engineering|materials?\s+science)\b", re.I), "Materials Science and Engineering"),
    (re.compile(r"\b(mechanical\s*(?:and|&)\s*energy\s*engineering|mechanical\s+engineering)\b", re.I), "Mechanical and Energy Engineering"),
    (re.compile(r"\b(mechanical\s+engineering\s+technology)\b", re.I), "Mechanical Engineering Technology"),
    (re.compile(r"\b(construction\s+engineering\s+technology|building\s+construction\s+technology)\b", re.I), "Construction Engineering Technology"),
    (re.compile(r"\b(construction\s+management)\b", re.I), "Construction Management"),
    (re.compile(r"\b(semiconductor\s+(?:manufacturing\s+)?engineering)\b", re.I), "Semiconductor Manufacturing Engineering"),

    # Management
    (re.compile(r"\b(engineering\s+management|engineering\s*/\s*industrial\s*management|engineering\s*technology\s*&\s*management|industrial\s+management)\b", re.I), "Engineering Management"),
]

# The two canonical majors that "Computer Science and Engineering" expands to.
_CSE_EXPANSION = ["Computer Science", "Computer Engineering"]


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


def standardize_major_list(raw_major: str, job_title: str = "") -> List[str]:
    """
    Map *raw_major* to one or more canonical UNT majors.

    Returns a **list** -- normally length-1.  The only multi-entry case is
    ``"Computer Science and Engineering"`` which expands to
    ``["Computer Science", "Computer Engineering"]``.

    If the raw text cannot be resolved, returns ``["Other"]``.
    """
    if not raw_major or not raw_major.strip():
        return ["Other"]

    text = _strip_minor_noise(raw_major)
    if not text:
        return ["Other"]

    def _resolve(value):
        if value == _CSE_MULTI:
            return list(_CSE_EXPANSION)
        return [value]

    # --- Deterministic matching first (fast, reliable) ---
    exact = _EXACT_MAJOR_MAP.get(text.lower())
    if exact:
        return _resolve(exact)

    direct = _CANONICAL_MAJOR_BY_LOWER.get(text.lower())
    if direct:
        return _resolve(direct)

    for pattern, canonical in _MAJOR_PATTERNS:
        if pattern.search(text):
            return _resolve(canonical)

    # --- Groq LLM fallback for unrecognized raw strings ---
    use_llm_fallback = os.getenv("MAJOR_USE_GROQ_FALLBACK", "1") == "1"
    if use_llm_fallback and os.getenv("GROQ_API_KEY"):
        llm_result = _standardize_major_with_llm(text, job_title)
        if llm_result in UNT_ALLOWED_MAJORS and llm_result != "Other":
            return [llm_result]

    return ["Other"]


def standardize_major(raw_major: str, job_title: str = "") -> str:
    """
    Backward-compatible wrapper: returns the **primary** canonical major.

    For the CS&E multi-entry case this returns ``"Computer Science"``
    (the first element).  Use ``standardize_major_list()`` when you need
    the full expansion.
    """
    return standardize_major_list(raw_major, job_title)[0]


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
