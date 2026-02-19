
import re
import os
import json
from typing import Optional, List, Tuple

# Approved engineering disciplines (only these will appear in the filter)
APPROVED_ENGINEERING_DISCIPLINES = [
    'Software, Data & AI Engineering',
    'Embedded, Electrical & Hardware Engineering',
    'Mechanical & Energy Engineering',
    'Biomedical Engineering',
    'Materials Science & Manufacturing',
    'Construction & Engineering Management',
]

# =============================================================================
# DISCIPLINE CLASSIFICATION RULES
# Ordered list: first match wins.  Priority per alumni: job_title > degree > headline
# =============================================================================
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
        "machine learning", "ml", "artificial intelligence", "ai",
        "deep learning", "cyber", "cybersecurity", "information technology",
        "it", "infosec", "appsec", "security engineer",
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
        "building systems", "facilities engineering", "osha",
    ]),
]

def infer_discipline(degree: str, job_title: str, headline: str, use_llm: bool = False) -> str:
    """
    Infer engineering discipline from degree, job title, and headline.
    Priority: Job Title > Degree > Headline
    
    Args:
        degree: The academic degree/major
        job_title: Current job title
        headline: LinkedIn headline
        use_llm: If True, use Groq API as fallback when keywords fail (slower)
        
    Returns:
        The best matching discipline or "Unknown" if no match.
    """
    sources = [job_title, degree, headline]

    for text in sources:
        if not text:
            continue
        text_lower = text.lower()

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
            # Longest keyword first, then by category order (lower = higher priority)
            matches.sort(key=lambda x: (-x[0], x[1]))
            return matches[0][2]

    # Keyword matching failed
    if use_llm:
        return _infer_discipline_with_llm(degree, job_title, headline)

    return "Other"

def _infer_discipline_with_llm(degree: str, job_title: str, headline: str) -> str:
    """Fallback: use Groq LLM to classify discipline."""
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        print("⚠️ GROQ_API_KEY not set, skipping LLM discipline inference")
        return "Other"

    try:
        from groq import Groq
        client = Groq(api_key=api_key)
        
        prompt = f"""
        Classify this alumni into ONE of these engineering disciplines:
        {json.dumps(APPROVED_ENGINEERING_DISCIPLINES, indent=2)}
        
        Alumni Data:
        - Job Title: {job_title}
        - Degree: {degree}
        - Headline: {headline}
        
        Return JSON: {{ "discipline": "Exact Name From List" }} or {{ "discipline": "Other" }} if none of the disciplines apply.
        """
        
        completion = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are a classifier. Output valid JSON only."},
                {"role": "user", "content": prompt}
            ],
            model="llama-3.1-8b-instant",
            response_format={"type": "json_object"},
            temperature=0
        )
        
        result = json.loads(completion.choices[0].message.content)
        discipline = result.get("discipline", "Other")
        
        if discipline in APPROVED_ENGINEERING_DISCIPLINES:
            return discipline
        # Groq returned something off-list — treat as Other
        return "Other"

    except Exception as e:
        print(f"⚠️ LLM discipline inference failed: {e}")

    return "Other"
