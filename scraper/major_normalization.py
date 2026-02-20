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
import os
import json

logger = logging.getLogger(__name__)

# ── Canonical majors (UNT College of Engineering) ─────────────────────────
# Each entry: (compiled regex pattern, canonical name)
# Order matters — more specific patterns first to avoid false positives.
_MAJOR_PATTERNS = [
    # --- Computer Science & Engineering Department ---
    (re.compile(r'\b(computer\s*science|comp\.?sci|c\.?s\.?|software\s*engineering|swe)\b', re.I),
     "Computer Science"),
    
    (re.compile(r'\b(computer\s*engineering|comp\.?eng|c\.?e\.?)\b', re.I),
     "Computer Engineering"),
    
    (re.compile(r'\b(information\s*technology|i\.?t\.?)\b', re.I),
     "Information Technology"),
    
    (re.compile(r'\b(cyber\s*security|computer\s*security|infosec)\b', re.I),
     "Cybersecurity"),
    
    (re.compile(r'\b(data\s*engineering)\b', re.I),
     "Data Science"),

    (re.compile(r'\b(data\s*science|data\s*analytics)\b', re.I),
     "Data Science"),
     
    (re.compile(r'\b(artificial\s*intelligence|a\.?i\.?|machine\s*learning)\b', re.I),
     "Artificial Intelligence"),

    # --- Biomedical ---
    (re.compile(r'\b(biomedical\s*engineering|bmen|bio\.?med|tissue\s*engineering)\b', re.I),
     "Biomedical Engineering"),

    # --- Electrical ---
    (re.compile(r'\b(electrical\s*engineering|e\.?e\.?)\b', re.I),
     "Electrical Engineering"),

    # --- Materials Science ---
    (re.compile(r'\b(materials?\s*science|materials?\s*engineering|mtse|metallurgy)\b', re.I),
     "Materials Science and Engineering"),

    # --- Mechanical & Energy ---
    (re.compile(r'\b(mechanical\s*(?:and|&)\s*energy\s*engineering)\b', re.I),
     "Mechanical and Energy Engineering"),

    (re.compile(r'\b(mechanical\s*engineering|m\.?e\.?|mech\.?eng)\b', re.I),
     "Mechanical Engineering"),

    (re.compile(r'\b(mechanical\s*engineering\s*technology|m\.?e\.?t\.?)\b', re.I),
     "Mechanical Engineering Technology"),
     
    (re.compile(r'\b(construction\s*management|construction\s*engineering|con\.?eng)\b', re.I),
     "Construction Management"),
     
    (re.compile(r'\b(engineering\s*management)\b', re.I),
     "Engineering Management"),

    # --- Civil & Environmental ---
    (re.compile(r'\b(civil\s*engineering|civil\s*eng|c\.?e\.?)\b', re.I),
     "Civil Engineering"),
     
    (re.compile(r'\b(environmental\s*engineering|env\.?eng)\b', re.I),
     "Environmental Engineering"),
     
    (re.compile(r'\b(structural\s*engineering)\b', re.I),
     "Structural Engineering"),

    # --- Chemical ---
    (re.compile(r'\b(chemical\s*engineering|chem\.?eng)\b', re.I),
     "Chemical Engineering"),

    # --- Aerospace ---
    (re.compile(r'\b(aerospace\s*engineering|aero\s*eng|aeronautical)\b', re.I),
     "Aerospace Engineering"),

    # --- Industrial & Systems ---
    (re.compile(r'\b(industrial\s*engineering|i\.?e\.?)\b', re.I),
     "Industrial Engineering"),
     
    (re.compile(r'\b(systems\s*engineering)\b', re.I),
     "Systems Engineering"),
     
    (re.compile(r'\b(manufacturing\s*engineering)\b', re.I),
     "Manufacturing Engineering"),

    # --- Other Specific Engineering Disciplines ---
    (re.compile(r'\b(projects?\s*engineering)\b', re.I), "Project Engineering"),
    (re.compile(r'\b(petroleum\s*engineering)\b', re.I), "Petroleum Engineering"),
    (re.compile(r'\b(nuclear\s*engineering)\b', re.I), "Nuclear Engineering"),
    (re.compile(r'\b(marine\s*engineering|naval\s*architecture)\b', re.I), "Marine Engineering"),
    (re.compile(r'\b(agricultural\s*engineering|biological\s*engineering)\b', re.I), "Agricultural Engineering"),
    (re.compile(r'\b(mining\s*engineering)\b', re.I), "Mining Engineering"),
    (re.compile(r'\b(geological\s*engineering)\b', re.I), "Geological Engineering"),
    (re.compile(r'\b(robotics(\s*engineering)?)\b', re.I), "Robotics Engineering"),
    (re.compile(r'\b(mechatronics)\b', re.I), "Mechatronics"),
    (re.compile(r'\b(electronics(\s*engineering)?)\b', re.I), "Electronics Engineering"),
    (re.compile(r'\b(telecommunications(\s*engineering)?)\b', re.I), "Telecommunications Engineering"),
    (re.compile(r'\b(optical\s*engineering|optics)\b', re.I), "Optical Engineering"),
    (re.compile(r'\b(automotive\s*engineering)\b', re.I), "Automotive Engineering"),

    # --- Catch-all Engineering ---
    (re.compile(r'\b(engineering|engr)\b', re.I),
     "Other Engineering"),
]

# Unique set of canonical major names for LLM validation
CANONICAL_MAJORS = sorted(list({c for _, c in _MAJOR_PATTERNS if c not in ["Other", "Other Engineering"]}))


def standardize_major(raw_major: str, job_title: str = "") -> str:
    """
    Map a raw major string to a canonical program name.
    
    Args:
        raw_major: The major/degree string from LinkedIn
        job_title: Optional job title for context (used by LLM fallback)

    Returns the canonical name if matched by explicit regex, otherwise
    preserves the raw major as-is.  LLM is only invoked when the text
    contains 'engineering' but didn't match a specific pattern.

    Logic:
    1. Regex Match against explicit engineering taxonomy
    2. LLM Fallback ONLY for ambiguous engineering strings
    3. Preserve raw value for everything else (no silent mutation)
    """
    if not raw_major or not raw_major.strip():
        return ""

    text = raw_major.strip()
    
    # 1. Regex Match against explicit engineering taxonomy
    for pattern, canonical in _MAJOR_PATTERNS:
        if pattern.search(text):
            if canonical not in ("Other", "Other Engineering"):
                if canonical != text:
                    logger.debug(f"Major standardized: \"{text}\" → \"{canonical}\"")
                return canonical
            # Matched catch-all "engineering" — try LLM below
            break
            
    # 2. LLM Fallback — ONLY for strings containing 'engineering'
    #    that didn't match a specific pattern above.
    if re.search(r'\bengineering\b', text, re.I) and os.getenv("GROQ_API_KEY"):
        llm_result = _standardize_major_with_llm(text, job_title)
        if llm_result in CANONICAL_MAJORS:
            logger.debug(f"Major standardized (LLM): \"{text}\" → \"{llm_result}\"")
            return llm_result

    # 3. No match — preserve raw value exactly as-is.
    #    Non-engineering fields (Business, General Studies, etc.)
    #    must not be silently mutated.
    return text


def _standardize_major_with_llm(raw_major: str, job_title: str) -> str:
    """Use Groq to map raw major to one of the CANONICAL_MAJORS."""
    try:
        from groq import Groq
        client = Groq(api_key=os.getenv("GROQ_API_KEY"))
        
        prompt = f"""
        Map this raw major to EXACTLY one of the canonical majors in the list below.
        
        Raw Major: "{raw_major}"
        Context (Job Title): "{job_title}"
        
        Canonical Majors:
        {json.dumps(CANONICAL_MAJORS)}
        
        Instructions:
        1. If the major clearly maps to one of the canonical names, return that name.
        2. Use the job title ONLY to disambiguate (e.g. "Health Data" + "Software Eng" -> "Data Science").
        3. If it does not clearly match any, return "Other".
        
        Return JSON: {{ "major": "Exact Name" }}
        """
        
        completion = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are a data cleaning assistant. Output valid JSON only."},
                {"role": "user", "content": prompt}
            ],
            model="llama-3.1-8b-instant",
            response_format={"type": "json_object"},
            temperature=0
        )
        
        result = json.loads(completion.choices[0].message.content)
        return result.get("major", "Other")
        
    except Exception as e:
        logger.warning(f"LLM major standardization failed for '{raw_major}': {e}")
        return "Other"
