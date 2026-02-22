import re
import os
import json
from typing import Optional, List, Tuple

# Approved engineering disciplines (only these will appear in the filter)
# Rule 2: "Unknown" is the catch-all.
APPROVED_ENGINEERING_DISCIPLINES = [
    'Software, Data & AI Engineering',
    'Embedded, Electrical & Hardware Engineering',
    'Mechanical & Energy Engineering',
    'Biomedical Engineering',
    'Materials Science & Manufacturing',
    'Construction & Engineering Management',
    'Unknown'
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


def _infer_discipline_with_llm(text: str, job_title_unused: str = "", headline_unused: str = "") -> str:
    """
    Uses Groq LLM to classify discipline for a specific piece of text.
    """
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return "Unknown"

    try:
        from groq import Groq
        client = Groq(api_key=api_key)

        prompt = f"""
        Classify this alumni into ONE of these EXACT engineering disciplines:
        {json.dumps(APPROVED_ENGINEERING_DISCIPLINES, indent=2)}
        
        REFERENCE: Use these official UNT CENG major mappings to guide your decision:
        {json.dumps(UNT_CENG_MAJORS, indent=2)}
        
        CRITICAL RULES:
        1. "Major-priority": Use the UNT CENG major mapping above as the PRIMARY source of truth. 
        2. If the info is blank or non-engineering (e.g., Business, Arts, Political Science, Biology, Chemistry), YOU MUST pick "Unknown".
        4. "Systems" rule: Only map "Systems" to "Software, Data & AI Engineering" if it is "Information Systems", "Computer Systems", or "Cybersecurity Systems". "Engineering Systems" usually maps to "Mechanical & Energy Engineering" if it relates to Manufacturing/Industrial.
        
        Current Data Piece to Classify:
        - Text: {text}
        
        Return ONLY JSON: {{ "discipline": "<Exact Name From List>" }}. No explanations.
        """
        
        completion = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are an automated engineering degree classifier. You must output strictly valid JSON."},
                {"role": "user", "content": prompt}
            ],
            model="llama-3.1-8b-instant",
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=64
        )
        
        result = json.loads(completion.choices[0].message.content)
        discipline = result.get("discipline", "Unknown")
        
        if discipline in APPROVED_ENGINEERING_DISCIPLINES:
            return discipline
        return "Unknown"

    except Exception as e:
        print(f"⚠️ LLM discipline inference failed: {e}")
        return "Unknown"


def _classify_text(text: str, current_priority: str, use_llm: bool = True) -> str:
    """Helper to classify a single source of text (Degree, Job, or Headline)."""
    if not text or not text.strip():
        return "Other"

    text_lower = text.lower()

    # 1. Deterministic UNT Major Match (Only relevant if current_priority is 'degree')
    if current_priority == 'degree':
        for disc_name, official_majors in UNT_CENG_MAJORS.items():
            for official in official_majors:
                official_clean = re.sub(r'[^\w\s]', '', official.lower())
                text_clean = re.sub(r'[^\w\s]', '', text_lower)
                if official_clean == text_clean or official_clean in text_clean:
                    return disc_name

    # 2. Keyword Anchor Overrides
    if any(kw in text_lower for kw in ["information", "analytics"]):
        return "Software, Data & AI Engineering"
    if "systems" in text_lower and any(kw in text_lower for kw in ["computer", "management", "information"]):
        return "Software, Data & AI Engineering"

    # 3. LLM Inference
    if use_llm:
        llm_result = _infer_discipline_with_llm(text)
        if llm_result and llm_result != "Unknown":
            return llm_result

    # 4. Regex Fallback
    # First check non-engineering kill list
    for non_eng_kw in NON_ENGINEERING_DEGREE_KEYWORDS:
        if re.search(r'\b' + re.escape(non_eng_kw) + r'\b', text_lower):
            if non_eng_kw == "biology" and ("computational biology" in text_lower or "bioinformatics" in text_lower):
                continue
            return "Other"

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


def infer_discipline(degree: str, job_title: str, headline: str, use_llm: bool = True) -> str:
    """
    Infer engineering discipline from degree, job title, and headline.
    Follows hierarchy: UNT Degree -> Job Title -> Headline.
    """
    # 1. Check Degree
    result = _classify_text(degree, 'degree', use_llm)
    if result != "Other":
        return result

    # 2. Check Job Title
    result = _classify_text(job_title, 'job', use_llm)
    if result != "Other":
        return result

    # 3. Check Headline
    result = _classify_text(headline, 'headline', use_llm)
    return result
