import re
import os
import json
from typing import Optional, List, Tuple

# Approved engineering disciplines (only these will appear in the filter)
# Rule 2: "Other" is now included in the approved disciplines.
APPROVED_ENGINEERING_DISCIPLINES = [
    'Software, Data & AI Engineering',
    'Embedded, Electrical & Hardware Engineering',
    'Mechanical & Energy Engineering',
    'Biomedical Engineering',
    'Materials Science & Manufacturing',
    'Construction & Engineering Management',
    'Other'
]

# =============================================================================
# DISCIPLINE CLASSIFICATION RULES
# Evaluated strictly for fallback keyword match: first match wins.
# Priority: Degree > Job Title > Headline (Wait: explicit check for non-engineering degrees first!)
# =============================================================================

# Keywords that heavily indicate a non-engineering degree ("Other")
NON_ENGINEERING_DEGREE_KEYWORDS = [
    "business", "arts", "fine arts", "history", "marketing", "sales", 
    "finance", "accounting", "nursing", "english", "literature", "music", 
    "theatre", "education", "sociology", "psychology", "political science", 
    "law", "communications", "journalism", "kinesiology", "biology", 
    "chemistry", "physics" # Unless explicitly tied to bio-engineering or chemical engineering
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
        "it", "infosec", "appsec", "security engineer", "computer applications"
    ]),
    # 2. EMBEDDED, ELECTRICAL & HARDWARE ENGINEERING
    ("Embedded, Electrical & Hardware Engineering", [
        "embedded systems engineer", "embedded", "embedded systems", "embedded engineer", "firmware",
        "firmware engineer", "rtos", "real-time systems", "electrical engineer",
        "electrical engineering", "computer engineering", "hardware",
        "hardware engineer", "electronics", "circuit design", "pcb",
        "schematic", "altium", "orcad", "cadence", "fpga", "verilog",
        "vhdl", "asic", "silicon", "semiconductor", "microcontroller",
        "arm", "stm32", "esp32", "bare metal", "low level", "device driver",
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
        "plant engineer",
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
        "six sigma", "lean manufacturing", "failure analysis", "corrosion",
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
    ]),
    # 7. CATCH-ALL ENGINEERING
    ("Other", [
        "engineer", "engineering"
    ]),
]


def _infer_discipline_with_llm(degree: str, job_title: str, headline: str) -> str:
    """
    Rule 3 & 4: Uses Groq LLM to classify discipline FIRST, prioritizing degree context.
    Rule 6: Restricts output safely exclusively to JSON format string value.
    """
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        print("⚠️ GROQ_API_KEY not set, skipping LLM discipline inference")
        return ""

    try:
        from groq import Groq
        client = Groq(api_key=api_key)

        prompt = f"""
        Classify this alumni into ONE of these EXACT engineering disciplines:
        {json.dumps(APPROVED_ENGINEERING_DISCIPLINES, indent=2)}
        
        CRITICAL RULES:
        1. "Degree-first priority": Usually check the degree first. 
        2. However, if the major is blank or non-engineering (e.g., Business, Arts, Political Science), you MUST check the job title. If the job title clearly indicates a tech/engineering role (e.g., "Software Engineer", "Senior UI Developer", "Data Scientist", "IT Manager"), classify them into the appropriate engineering discipline based on that job title rather than defaulting to 'Other'.
        3. If the job title also lacks clear engineering indicators or is missing, check the headline. If the headline clearly indicates a tech/engineering role, classify them into the appropriate engineering discipline based on the headline.
        4. ONLY if the degree, job title, and headline lack engineering/tech indicators, map to "Other". Do not falsely classify someone purely based on generalized non-tech roles (e.g., a "Sales Manager" selling "Software" should be "Other" if their degree is Business and job is Sales).
        
        Alumni Data:
        - Degree: {degree}
        - Job Title: {job_title}
        - Headline: {headline}
        
        Return JSON: {{ "discipline": "<Exact Name From List>" }}. No explanations. Never return free text!
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
        
        # Rule 6: Parse the JSON object properly
        result = json.loads(completion.choices[0].message.content)
        discipline = result.get("discipline", "Other")
        
        # Rule 3 & 4: Output strictly in approved array, else Other.
        if discipline in APPROVED_ENGINEERING_DISCIPLINES:
            return discipline
        return "Other"

    except Exception as e:
        print(f"⚠️ LLM discipline inference failed: {e}")

    # Explicit failure indicator so keyword fallback knows to run
    return ""


def infer_discipline(degree: str, job_title: str, headline: str, use_llm: bool = True) -> str:
    """
    Infer engineering discipline from degree, job title, and headline.
    
    Rule 1: Priority -> Degree > Job Title > Headline. (Checked in sequence).
    Rule 3: LLM goes first.
    Rule 5: Fallback regex keyword match.
    """
    # 1. Use the LLM (Groq API) as the first classification step
    if use_llm:
        llm_result = _infer_discipline_with_llm(degree, job_title, headline)
        if llm_result:
            return llm_result
    
    # 2. Keyword fallback - execute only if LLM was disabled or errored
    degree_lower = degree.lower() if degree else ""
    job_title_lower = job_title.lower() if job_title else ""
    headline_lower = headline.lower() if headline else ""

    # Rule 1 & Rule 2 explicit enforcement for regex:
    # If degree has obvious non-engineering terms, map to "Other" UNLESS 
    # the job title clearly matches an engineering field.
    if degree_lower:
        for non_eng_kw in NON_ENGINEERING_DEGREE_KEYWORDS:
            if re.search(r'\b' + re.escape(non_eng_kw) + r'\b', degree_lower):
                if non_eng_kw == "biology" and ("computational biology" in degree_lower or "bioinformatics" in degree_lower):
                    continue
                
                # Check if job title or headline has an engineering keyword to override the kill
                has_eng_override = False
                
                # Try job title first
                if job_title_lower:
                    for _, keywords in DISCIPLINES:
                        for kw in keywords:
                            if len(kw) <= 2:
                                if re.search(r'\b' + re.escape(kw) + r'\b', job_title_lower):
                                    has_eng_override = True; break
                            else:
                                if kw in job_title_lower:
                                    has_eng_override = True; break
                        if has_eng_override: break
                
                # Fall back to headline if job title didn't override
                if not has_eng_override and headline_lower:
                    for _, keywords in DISCIPLINES:
                        for kw in keywords:
                            if len(kw) <= 2:
                                if re.search(r'\b' + re.escape(kw) + r'\b', headline_lower):
                                    has_eng_override = True; break
                            else:
                                if kw in headline_lower:
                                    has_eng_override = True; break
                        if has_eng_override: break
                
                if not has_eng_override:
                    return "Other"

    # Enforce priority sequence: Degree -> Job -> Headline
    sources = [
        degree_lower,
        job_title_lower,
        headline_lower
    ]

    for text_lower in sources:
        if not text_lower:
            continue

        matches = []  # (keyword_length, category_index, discipline_name)
        for cat_index, (discipline_name, keywords) in enumerate(DISCIPLINES):
            for keyword in keywords:
                if len(keyword) <= 2:
                    if re.search(r'\b' + re.escape(keyword) + r'\b', text_lower):
                        matches.append((len(keyword), cat_index, discipline_name))
                else:
                    if keyword in text_lower:
                        matches.append((len(keyword), cat_index, discipline_name))

        if matches:
            # Longest keyword matched first, then category ordering (lower = higher priority)
            matches.sort(key=lambda x: (-x[0], x[1]))
            # Return strict approved match
            return matches[0][2]

    # Rule 4: Return one of the approved disciplines string
    return "Other"
