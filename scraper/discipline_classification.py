import re
import os
import json
import logging
from typing import Optional, List, Tuple

logger = logging.getLogger(__name__)

# Approved engineering disciplines (only these will appear in the filter).
# Rule: Any profile that doesn't fit these categories or is identified as 
# non-engineering is mapped to 'Other' to maintain a clean, professional filter list
# while ensuring no alumni are excluded from the database entirely.
APPROVED_ENGINEERING_DISCIPLINES = [
    'Software, Data & AI Engineering',
    'Embedded, Electrical & Hardware Engineering',
    'Mechanical & Energy Engineering',
    'Biomedical Engineering',
    'Materials Science & Manufacturing',
    'Construction & Engineering Management',
    'Other'
]

# UNT College of Engineering (CENG) Official Major Mapping (Reference for classification)
UNT_CENG_MAJORS = {
    "Biomedical Engineering": [
        "B.S. Biomedical Engineering", "M.S. Biomedical Engineering", 
        "Ph.D. Biomedical Engineering", "Biomedical Engineering (Concentration)"
    ],
    "Software, Data & AI Engineering": [
        "B.A. Information Technology", "B.S. Computer Engineering", "B.S. Computer Science", "B.S. Cybersecurity",
        "M.S. Computer Engineering", "M.S. Computer Science", "M.S. Cybersecurity", "M.S. Data Engineering",
        "Autonomous Systems (Concentration)", "Machine Learning (Concentration)", "Ph.D. Computer Science and Engineering"
    ],
    "Embedded, Electrical & Hardware Engineering": [
        "B.S. Electrical Engineering", "M.S. Electrical Engineering", "Ph.D. Electrical Engineering"
    ],
    "Materials Science & Manufacturing": [
        "B.S. Materials Science and Engineering", "M.S. Materials Science and Engineering", 
        "Ph.D. Materials Science and Engineering"
    ],
    "Mechanical & Energy Engineering": [
        "B.S. Mechanical and Energy Engineering", "B.S.E.T. Mechanical Engineering Technology",
        "M.S. Mechanical and Energy Engineering", "M.S. Engineering Management (General, Energy)",
        "Ph.D. Mechanical and Energy Engineering", "Mechanical Engineering"
    ],
    "Construction & Engineering Management": [
        "B.S. Construction Management", "M.S. Engineering Management (Construction Management)", 
        "M.S. Engineering Technology (Construction Management)"
    ]
}

# =============================================================================
# DISCIPLINE CLASSIFICATION RULES
# Evaluated strictly for fallback keyword match: first match wins.
# Priority: Degree > Job Title > Headline (Wait: explicit check for non-engineering degrees first!)
# =============================================================================

# Keywords that heavily indicate a non-engineering degree
NON_ENGINEERING_DEGREE_KEYWORDS = [
    "business", "arts", "fine arts", "history", "marketing", "sales", 
    "finance", "accounting", "nursing", "english", "literature", "music", 
    "theatre", "education", "sociology", "psychology", "political science", 
    "law", "communications", "journalism", "kinesiology", "biology", 
    "chemistry", "physics" 
]

DISCIPLINES = [
    # 1. SOFTWARE, DATA & AI ENGINEERING
    ("Software, Data & AI Engineering", [
        "cs", "computer science", "computing", "software", "software engineer",
        "software developer", "software architect", "backend engineer",
        "frontend engineer", "full stack", "full-stack", "web developer",
        "application developer", "platform engineer", "systems engineer",
        "devops", "site reliability engineer", "sre", "cloud engineer",
        "aws", "azure", "gcp", "api", "microservices", "programmer",
        "coding", "programming", "python", "java", "javascript", "typescript",
        "c++", "go", "rust", "data engineer", "data scientist", "data science",
        "data analyst", "data analytics", "business analyst", "business intelligence", 
        "bi developer", "ui developer", "ux developer", "ui/ux", "web design",
        "machine learning", "ml", "artificial intelligence", "ai",
        "deep learning", "cyber", "cybersecurity", "information technology",
        "it", "infosec", "appsec", "security engineer", "computer applications",
        "computer engineering", "autonomous systems"
    ]),
    # 2. EMBEDDED, ELECTRICAL & HARDWARE ENGINEERING
    ("Embedded, Electrical & Hardware Engineering", [
        "embedded systems engineer", "embedded", "embedded systems", "embedded engineer", "firmware",
        "firmware engineer", "rtos", "real-time systems", "electrical engineer",
        "electrical engineering", "hardware",
        "hardware engineer", "electronics", "circuit design", "pcb",
        "schematic", "altium", "orcad", "cadence", "fpga", "verilog",
        "vhdl", "asic", "silicon", "semiconductor", "microcontroller",
        "arm", "stm32", "esp32",
        "bare metal", "low level", "device driver",
        "linux kernel", "robotics", "controls engineer", "mechatronics",
        "signal processing", "digital design", "analog design", "power systems",
    ]),
    # 3. MECHANICAL & ENERGY ENGINEERING
    ("Mechanical & Energy Engineering", [
        "mechanical engineering", "mechanical engineer", "mech engineer",
        "mechanics", "machine design", "cad", "solidworks", "catia",
        "autocad", "ansys", "manufacturing engineer", "manufacturing engineering",
        "hvac", "thermal engineering", "thermodynamics", "heat transfer",
        "fluids", "fluid mechanics", "energy engineering", "energy systems",
        "renewable energy", "power generation", "turbines", "combustion",
        "automotive engineering", "aerospace engineering", "structural analysis",
        "stress analysis", "finite element", "fea", "industrial engineering",
        "industrial engineer", "industrial", "manufacturing engineer", 
        "manufacturing engineering", "manufacturing", "plant engineer",
        "mechanical and energy engineering", "engineering management", "engineering technology",
    ]),
    # 4. BIOMEDICAL ENGINEERING
    ("Biomedical Engineering", [
        "biomedical", "biomedical engineering", "biomedical engineer",
        "bioengineering", "medical engineering", "medical devices",
        "medical device engineer", "clinical engineer", "clinical engineering",
        "healthcare engineering", "biotech", "biotechnology", "bioinformatics",
        "health informatics", "clinical informatics", "medical informatics",
        "pharmacy informatics", "drug safety", "pharmacovigilance",
        "clinical pharmacist",
        "computational biology", "medical imaging", "imaging engineer",
        "mri", "ct scan", "ultrasound", "x-ray", "biosensors", "prosthetics",
        "implants", "rehabilitation engineering", "neural engineering",
        "tissue engineering", "biomaterials", "physiological systems",
        "regulatory affairs", "fda",
    ]),
    # 5. MATERIALS SCIENCE & MANUFACTURING
    ("Materials Science & Manufacturing", [
        "materials science", "materials engineering", "materials engineer",
        "metallurgy", "metallurgical engineering", "polymers", "polymer science",
        "ceramics", "composites", "nanomaterials", "nanotechnology",
        "thin films", "surface science", "crystallography", "solid state materials",
        "semiconductor materials", "process engineering", "process engineer",
        "manufacturing science", "manufacturing process", "quality engineering",
        "quality engineer", "quality", "six sigma", "lean manufacturing", 
        "failure analysis", "corrosion",
        "heat treatment", "additive manufacturing", "3d printing",
        "powder metallurgy", "materials characterization", "xrd", "sem", "tem",
    ]),
    # 6. CONSTRUCTION & ENGINEERING MANAGEMENT
    ("Construction & Engineering Management", [
        "construction management", "construction manager", "construction engineer",
        "civil engineering", "civil engineer", "structural engineering",
        "structural engineer", "project manager construction",
        "engineering manager construction", "site engineer", "field engineer",
        "general contractor", "subcontractor", "estimating", "cost estimation",
        "quantity surveying", "scheduler", "scheduling", "primavera", "p6",
        "ms project", "bim", "revit", "autodesk construction", "project controls",
        "capital projects", "infrastructure projects", "transportation engineering",
        "geotechnical engineering", "surveying", "land development",
        "building systems", "facilities engineering", "osha"
    ])
]


_DISCIPLINE_ID_MAP = {
    idx: name for idx, name in enumerate(APPROVED_ENGINEERING_DISCIPLINES, start=1)
}
_DISCIPLINE_BY_LOWER = {name.lower(): name for name in APPROVED_ENGINEERING_DISCIPLINES}
_GENERIC_DEGREE_MARKERS = {
    "bachelor's degree",
    "bachelors degree",
    "master's degree",
    "masters degree",
    "doctorate",
    "doctor of philosophy",
    "master of science",
    "bachelor of science",
}
_ENGINEERING_SIGNAL_KEYWORDS = sorted(
    {
        kw
        for _, keywords in DISCIPLINES
        for kw in keywords
        if len(kw) > 2
    }
)


def _coerce_llm_discipline_choice(payload: dict | None) -> str:
    """
    Parse Groq JSON payload into an approved discipline label.
    Accepts either:
      - {"discipline_id": <1-based index>}
      - {"discipline": "<exact label>"}
    Returns "Unknown" when payload is invalid.
    """
    if not isinstance(payload, dict):
        return "Unknown"

    discipline_id = payload.get("discipline_id")
    if isinstance(discipline_id, str):
        text = discipline_id.strip()
        if text.isdigit():
            discipline_id = int(text)
    if isinstance(discipline_id, int) and discipline_id in _DISCIPLINE_ID_MAP:
        return _DISCIPLINE_ID_MAP[discipline_id]

    discipline_name = payload.get("discipline")
    if isinstance(discipline_name, str):
        cleaned = discipline_name.strip().strip('"\'')
        return _DISCIPLINE_BY_LOWER.get(cleaned.lower(), "Unknown")

    return "Unknown"


def _has_engineering_signal(text_lower: str) -> bool:
    """
    Return True if text contains concrete engineering/technical signals.
    """
    if not text_lower:
        return False
    if " engineering" in f" {text_lower}" or "engineer" in text_lower:
        return True
    return any(kw in text_lower for kw in _ENGINEERING_SIGNAL_KEYWORDS)


def _infer_discipline_with_llm(text: str, job_title_unused: str = "", headline_unused: str = "") -> str:
    """
    Uses Groq LLM to classify discipline for a specific piece of text.
    """
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return "Unknown"

    try:
        from groq_client import _get_client, GROQ_MODEL
        client = _get_client()
        if not client:
            return "Unknown"

        text_value = (text or "").strip()[:240]
        categories = "\n".join(
            f"{idx}. {name}" for idx, name in _DISCIPLINE_ID_MAP.items()
        )
        major_hints = "\n".join(
            f"- {discipline}: {', '.join(majors[:3])}"
            for discipline, majors in UNT_CENG_MAJORS.items()
        )

        prompt = f"""
Classify this single text into one discipline ID.

Text: "{text_value}"

Discipline IDs:
{categories}

Reference hints from UNT engineering majors:
{major_hints}

Rules:
1. If text is generic/non-engineering (e.g. manager, sales, business, arts), choose Other.
2. Use specific engineering signals over generic words.
3. If uncertain, choose Other.
4. Do not output explanations.

Return JSON only:
{{ "discipline_id": <integer 1-{len(_DISCIPLINE_ID_MAP)}> }}
"""
        
        completion = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are an automated engineering discipline classifier. Output strictly valid JSON."},
                {"role": "user", "content": prompt}
            ],
            model=GROQ_MODEL,
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=24
        )
        
        payload = json.loads(completion.choices[0].message.content)
        discipline = _coerce_llm_discipline_choice(payload)
        
        if discipline in APPROVED_ENGINEERING_DISCIPLINES:
            return discipline
        return "Unknown"

    except Exception as e:
        logger.warning(f"LLM discipline inference failed: {e}")
        return "Unknown"


def _classify_text(text: str, current_priority: str, use_llm: bool = True) -> str:
    """Helper to classify a single source of text (Degree, Job, or Headline)."""
    if not text or not text.strip():
        return "Other"

    text_lower = text.lower()

    # Generic degree labels without specialization should not trigger LLM guesses.
    if current_priority == "degree" and text_lower.strip() in _GENERIC_DEGREE_MARKERS:
        return "Other"

    # 1. Deterministic UNT Major Match (Only relevant if current_priority is 'degree')
    if current_priority == 'degree':
        for disc_name, official_majors in UNT_CENG_MAJORS.items():
            for official in official_majors:
                official_clean = re.sub(r'[^\w\s]', '', official.lower())
                text_clean = re.sub(r'[^\w\s]', '', text_lower)
                if official_clean == text_clean or official_clean in text_clean:
                    return disc_name

    # 2. Keyword Anchor Overrides
    if any(kw in text_lower for kw in ["health informatics", "clinical informatics", "medical informatics", "drug safety", "pharmacovigilance"]):
        return "Biomedical Engineering"
    if any(kw in text_lower for kw in ["information", "analytics"]):
        if any(kw in text_lower for kw in ["health", "clinical", "medical", "bio", "pharma", "drug"]):
            return "Biomedical Engineering"
        return "Software, Data & AI Engineering"
    if "systems" in text_lower and any(kw in text_lower for kw in ["computer", "management", "information"]):
        return "Software, Data & AI Engineering"

    # 3. Non-engineering kill list (before LLM to avoid false positive guesses)
    for non_eng_kw in NON_ENGINEERING_DEGREE_KEYWORDS:
        if re.search(r'\b' + re.escape(non_eng_kw) + r'\b', text_lower):
            if non_eng_kw == "biology" and ("computational biology" in text_lower or "bioinformatics" in text_lower):
                continue
            return "Other"

    # 4. LLM Inference (only when the text actually contains engineering signals)
    if use_llm and _has_engineering_signal(text_lower):
        llm_result = _infer_discipline_with_llm(text)
        if llm_result and llm_result != "Unknown":
            return llm_result

    # Search for all approved disciplines
    matches = []
    for cat_index, (discipline_name, keywords) in enumerate(DISCIPLINES):
        # Safety Check: Business/Sculpture + Materials Science safeguard
        if current_priority == 'degree' and discipline_name == "Materials Science & Manufacturing":
            if any(kw in text_lower for kw in ["business", "sculpture"]) and "engineering" not in text_lower:
                continue

        for keyword in keywords:
            if len(keyword) <= 2:
                if re.search(r'\b' + re.escape(keyword) + r'\b', text_lower):
                    matches.append((len(keyword), cat_index, discipline_name))
            else:
                if keyword in text_lower:
                    matches.append((len(keyword), cat_index, discipline_name))

    if matches:
        matches.sort(key=lambda x: (-x[0], x[1]))
        return matches[0][2]

    return "Other"


def _is_unt_school_name(school_name: str) -> bool:
    """Return True when school string appears to be UNT."""
    text = (school_name or "").strip().lower()
    if not text:
        return False
    return ("university of north texas" in text) or bool(re.search(r"\bunt\b", text))


def _degree_rank(value: str) -> int:
    """
    Rank degree level for "highest UNT major" precedence.
    Higher number = higher degree.
    """
    text = (value or "").strip().lower()
    if not text:
        return 0
    if any(token in text for token in ("ph.d", "phd", "doctor", "doctorate")):
        return 4
    if any(token in text for token in ("master", "m.s", "ms", "mba")):
        return 3
    if any(token in text for token in ("bachelor", "b.s", "bs", "b.a", "ba")):
        return 2
    if "associate" in text:
        return 1
    return 0


def _combined_education_text(entry: dict) -> str:
    """Build a robust degree+major text blob for discipline classification."""
    degree_bits = [
        (entry.get("standardized_degree") or "").strip(),
        (entry.get("degree") or "").strip(),
    ]
    major_bits = [
        (entry.get("standardized_major") or "").strip(),
        (entry.get("major") or "").strip(),
    ]
    return " ".join(part for part in (*degree_bits, *major_bits) if part).strip()


def _has_major_text(entry: dict) -> bool:
    major = (entry.get("standardized_major") or "").strip() or (entry.get("major") or "").strip()
    return bool(major)


def infer_discipline(
    degree: str,
    job_title: str,
    headline: str,
    use_llm: bool = True,
    education_entries: Optional[List[dict]] = None,
    older_job_titles: Optional[List[str]] = None,
) -> str:
    """
    Infer engineering discipline using the requested priority:
      1) Highest UNT major
      2) Other UNT majors
      3) Non-UNT majors
      4) Current job title
      5) Older job titles
      6) Headline
    """
    prepared = []
    if education_entries:
        for idx, entry in enumerate(education_entries):
            if not isinstance(entry, dict):
                continue
            text = _combined_education_text(entry)
            if not text:
                continue
            degree_text = " ".join(
                part for part in (
                    (entry.get("standardized_degree") or "").strip(),
                    (entry.get("degree") or "").strip(),
                ) if part
            )
            prepared.append({
                "idx": idx,
                "text": text,
                "is_unt": _is_unt_school_name((entry.get("school") or "").strip()),
                "rank": _degree_rank(degree_text),
                "has_major": _has_major_text(entry),
            })

    # Backward-compatible fallback path for direct calls/tests that only pass degree text.
    if not prepared and degree and str(degree).strip():
        prepared.append({
            "idx": 0,
            "text": str(degree).strip(),
            "is_unt": False,
            "rank": _degree_rank(str(degree)),
            "has_major": True,
        })

    def _pick(entries: List[dict]) -> str:
        for e in entries:
            result = _classify_text(e["text"], "degree", use_llm)
            if result != "Other":
                return result
        return "Other"

    unt_major_entries = [e for e in prepared if e["is_unt"] and e["has_major"]]
    if unt_major_entries:
        highest_rank = max(e["rank"] for e in unt_major_entries)
        highest_unt = sorted(
            [e for e in unt_major_entries if e["rank"] == highest_rank],
            key=lambda x: x["idx"],
        )
        result = _pick(highest_unt)
        if result != "Other":
            return result

        remaining_unt = sorted(
            [e for e in unt_major_entries if e["rank"] != highest_rank],
            key=lambda x: (-x["rank"], x["idx"]),
        )
        result = _pick(remaining_unt)
        if result != "Other":
            return result

    non_unt_major_entries = sorted(
        [e for e in prepared if (not e["is_unt"]) and e["has_major"]],
        key=lambda x: (-x["rank"], x["idx"]),
    )
    result = _pick(non_unt_major_entries)
    if result != "Other":
        return result

    # 4) Current job title
    result = _classify_text(job_title, 'job', use_llm)
    if result != "Other":
        return result

    # 5) Older experience titles
    for older_title in (older_job_titles or []):
        result = _classify_text(older_title, "job", use_llm)
        if result != "Other":
            return result

    # 6) Headline
    return _classify_text(headline, 'headline', use_llm)
