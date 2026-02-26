"""
Job Title Normalization Module

Maps semantically equivalent job titles to a single standardized category.
Preserves all original raw job title values — only the normalized mapping is stored.

Two strategies:
  1. Deterministic: cleanup + dictionary lookup (fast, offline, no API cost).
     Preferred for known, common titles to ensure 100% latency-free matching.
  2. Groq-based:    LLM classification against existing normalized titles.
     Used for "long-tail" titles to find semantic matches that the map might miss.

Usage:
    from job_title_normalization import get_or_create_normalized_title

    norm_id = get_or_create_normalized_title(raw_title)
    # norm_id is the PK in normalized_job_titles table (or None on failure)
"""

import os
import re
import json
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DETERMINISTIC TITLE MAP
# Keys are lowercase variations, values are the canonical normalized title.
# Add entries here to expand deterministic coverage.
# ---------------------------------------------------------------------------

TITLE_MAP = {
    # ── Software Engineering (all variants → "Software Engineer") ──
    "software developer": "Software Engineer",
    "software dev": "Software Engineer",
    "sde": "Software Engineer",
    "software engineer": "Software Engineer",
    "software engineer ii": "Software Engineer",
    "software engineer iii": "Software Engineer",
    "software engineer intern": "Software Engineer",
    "software engineering intern": "Software Engineer",
    "software developer intern": "Software Engineer",
    "software developer summer analyst": "Software Engineer",
    "software intern": "Software Engineer",
    "software development intern": "Software Engineer",
    "software development engineer 1": "Software Engineer",
    "software developer(ta)": "Software Engineer",
    "software application developer": "Software Engineer",
    "software associate": "Software Engineer",
    "associate software engineer": "Software Engineer",
    "associate software": "Software Engineer",
    "software systems engineer": "Systems Engineer",
    "sr. software engineer": "Software Engineer",
    "sr software engineer": "Software Engineer",
    "senior software engineer": "Software Engineer",
    "senior software developer": "Software Engineer",
    "sr. software developer": "Software Engineer",
    "sr software developer": "Software Engineer",
    "software": "Software Engineer",
    "software engineering student": "Software Engineer",
    "software development engineer": "Software Engineer",
    "software development engineer ii": "Software Engineer",
    "software development analyst": "Software Engineer",
    "software development analyst i": "Software Engineer",
    "software development analyst level 2": "Software Engineer",
    "software developer & consultant": "Software Engineer",
    "junior software engineer": "Software Engineer",
    "jr. software engineer": "Software Engineer",
    "jr software engineer": "Software Engineer",
    "junior software developer": "Software Engineer",
    "jr. software developer": "Software Engineer",
    "lead software engineer": "Software Engineer",
    "lead front end software engineer": "Software Engineer",
    "vice president lead software engineer": "Software Engineer",
    "principal software engineer": "Software Engineer",
    "staff software engineer": "Software Engineer",
    "sr. director of software engineering": "Director of Engineering",
    "vp of software engineering": "Engineering",
    "developer": "Software Engineer",
    "application developer": "Software Engineer",
    "application development analyst": "Software Engineer",
    "application engineer": "Software Engineer",
    "associate programmer, it aviator": "Software Engineer",
    "mainframe programmer": "Software Engineer",
    "programmer analyst trainee": "Software Engineer",
    "scientific programmer": "Software Engineer",

    # ── Full Stack ──
    "full stack developer": "Software Engineer",
    "full-stack developer": "Software Engineer",
    "fullstack developer": "Software Engineer",
    "full stack engineer": "Software Engineer",
    "full-stack engineer": "Software Engineer",
    "fullstack engineer": "Software Engineer",
    "full stack web developer": "Software Engineer",
    "full stack .net developer": "Software Engineer",
    "java full stack developer": "Software Engineer",
    "java fullstack developer": "Software Engineer",
    "sr. full stack java developer": "Software Engineer",
    "sr .net developer": "Software Engineer",

    # ── Frontend / Backend / Web ──
    "frontend developer": "Frontend Engineer",
    "front-end developer": "Frontend Engineer",
    "front end developer": "Frontend Engineer",
    "frontend engineer": "Frontend Engineer",
    "front-end engineer": "Frontend Engineer",
    "senior ui developer": "Frontend Engineer",
    "backend developer": "Backend Engineer",
    "back-end developer": "Backend Engineer",
    "back end developer": "Backend Engineer",
    "backend engineer": "Backend Engineer",
    "back-end engineer": "Backend Engineer",
    "back end engineer": "Backend Engineer",
    "web developer": "Software Engineer",
    "web dev": "Software Engineer",
    "web application developer": "Software Engineer",

    # ── Java / Python / ServiceNow specific ──
    "java developer": "Software Engineer",
    "python developer": "Software Engineer",
    "servicenow developer": "Software Engineer",

    # ── Data & AI ──
    "ai engineer": "AI/ML Engineer",
    "ai software engineer": "AI/ML Engineer",
    "ai/ml engineer": "AI/ML Engineer",
    "ml engineer": "AI/ML Engineer",
    "machine learning engineer": "AI/ML Engineer",
    "artificial intelligence engineer": "AI/ML Engineer",
    "software engineer - machine learning": "AI/ML Engineer",
    "data & ai engineer": "Data Engineer",
    "data scientist": "Data Scientist",
    "sr. data scientist": "Data Scientist",
    "sr data scientist": "Data Scientist",
    "senior data scientist": "Data Scientist",
    "senior data science consultant": "Data Scientist",
    "data analyst": "Data Analyst",
    "data engineer": "Data Engineer",
    "data engineer associate": "Data Engineer",
    "cloud data engineer": "Data Engineer",
    "databricks data engineer": "Data Engineer",
    "data architect": "Data Architect",
    "database administrator": "Database Administrator",
    "dba": "Database Administrator",
    "business analyst": "Business Analyst",
    "business intelligence analyst": "Business Intelligence Analyst",
    "bi analyst": "Business Intelligence Analyst",
    "bi developer": "Business Intelligence Developer",
    "operations & data analyst (recreational facilities)": "Data Analyst",
    "market research analyst": "Data Analyst",

    # ── DevOps & Cloud ──
    "devops engineer": "DevOps Engineer",
    "dev ops engineer": "DevOps Engineer",
    "aws devops engineer": "DevOps Engineer",
    "jr. devops engineer": "DevOps Engineer",
    "sr/sre/devops engineer": "DevOps Engineer",
    "site reliability engineer": "Site Reliability Engineer",
    "sre": "Site Reliability Engineer",
    "cloud engineer": "Cloud Engineer",
    "aws cloud practitioner- internship": "Cloud Engineer",
    "amazon web services (aws)": "Cloud Engineer",
    "cloud architect": "Cloud Architect",
    "solutions architect": "Solutions Architect",
    "jr. system architect": "Solutions Architect",
    "systems administrator": "Systems Administrator",
    "system administrator": "Systems Administrator",
    "sys admin": "Systems Administrator",
    "sysadmin": "Systems Administrator",
    "network engineer": "Network Engineer",
    "network administrator": "Network Administrator",

    # ── Systems Engineering ──
    "system engineer": "Systems Engineer",
    "systems engineer hil": "Systems Engineer",
    "technical systems engineer": "Systems Engineer",
    "assistant system design engineer": "Systems Engineer",

    # ── Security ──
    "cybersecurity analyst": "Cybersecurity Analyst",
    "cyber security analyst": "Cybersecurity Analyst",
    "information security analyst": "Cybersecurity Analyst",
    "cybersecurity analyst (graduate assistant)": "Cybersecurity Analyst",
    "security engineer": "Security Engineer",
    "cybersecurity engineer": "Security Engineer",
    "sr. cyber security engineer": "Security Engineer",
    "security analyst": "Security Analyst",

    # ── QA & Testing ──
    "qa engineer": "QA Engineer",
    "quality assurance engineer": "QA Engineer",
    "test engineer": "QA Engineer",
    "test analyst": "QA Engineer",
    "software tester": "QA Engineer",
    "sdet": "SDET",
    "software development engineer in test": "SDET",
    "senior test automation engineer": "QA Engineer",
    "quality control analyst": "Quality Engineer",
    "quality engineer": "Quality Engineer",
    "senior design quality engineer": "Quality Engineer",

    # ── Engineering (general, mechanical, civil, etc.) ──
    "engineer": "Engineer",
    "engineer i": "Engineer",
    "engineer ii": "Engineer",
    "associate engineer": "Engineer",
    "mechanical engineer": "Mechanical Engineer",
    "mechanical design engineer": "Mechanical Engineer",
    "mechanical engineering intern": "Mechanical Engineer",
    "development engineer - mechanical": "Mechanical Engineer",
    "civil engineer": "Civil Engineer",
    "civil enginnering co-op/intern": "Civil Engineering",
    "structural engineer": "Structural Engineer",
    "piping engineer": "Piping Engineer",
    "process engineer": "Process Engineer",
    "manufacturing engineer": "Manufacturing Engineer",
    "industrial engineer": "Industrial Engineer",
    "controls engineer": "Controls Engineer",
    "field engineer": "Field Engineer",
    "feo project engineer": "Project Engineer",
    "project engineer": "Project Engineer",
    "engineering technician": "Engineering Technician",
    "senior technical engineer": "Engineer",
    "senior materials engineer": "Engineer",
    "automation engineering lead": "Automation Engineer",

    # ── Management & Leadership ──
    "engineering manager": "Engineering Manager",
    "software engineering manager": "Engineering Manager",
    "technical lead": "Technical Lead",
    "tech lead": "Technical Lead",
    "team lead": "Team Lead",
    "project manager": "Project Manager",
    "project manager 2": "Project Manager",
    "senior project manager": "Project Manager",
    "commercial construction project manager": "Project Manager",
    "assistant project manager": "Project Manager",
    "project management coordinator": "Project Manager",
    "project management intern": "Project Manager",
    "program manager": "Program Manager",
    "product manager": "Product Manager",
    "scrum master": "Scrum Master",
    "vp of engineering": "Engineering",
    "vice president of engineering": "Engineering",
    "cto": "Technology Officer",
    "chief technology officer": "Technology Officer",
    "coo": "Operations Officer",
    "chief operations officer": "Operations Officer",
    "director of engineering": "Director of Engineering",
    "manager": "Manager",
    "senior manager": "Manager",
    "regional manager": "Manager",
    "senior manager of field marketing": "Marketing Manager",
    "senior marketing manager": "Marketing Manager",
    "operations manager": "Operations Manager",
    "operations supervisor": "Operations Manager",
    "production manager": "Operations Manager",
    "supply chain manager": "Operations Manager",
    "procurement manager": "Operations Manager",
    "executive vice president of operations": "Operations",
    "director of southwest region": "Director",
    "director of strategic initiatives": "Director",
    "director of engineering": "Director",
    "director of employee experience": "Director",
    "senior director, assurance & enterprise risk management": "Director",
    "partner and executive director": "Director",
    "founder and director": "Director",
    "co-owner & operations head": "Director",
    "owner": "Director",
    "vp of sales": "Sales",

    # ── Consulting & IT ──
    "consultant": "Consultant",
    "it consultant": "Consultant",
    "technology consultant": "Consultant",
    "dynamic 365 crm consultant": "Consultant",
    "healthcare information technology consultant": "Consultant",
    "graduate services consultant": "Consultant",
    "social media marketing consultant": "Marketing Consultant",
    "technical architect": "Solutions Architect",
    "technology analyst": "IT Analyst",
    "technology summer analyst": "IT Analyst",
    "senior analyst": "Analyst",
    "it systems analyst": "IT Analyst",
    "computing supervisor": "IT Support",
    "it support agent": "IT Support",
    "information technology support engineer": "IT Support",
    "information technology intern": "IT",
    "it specialist": "IT Specialist",
    "it support specialist": "IT Support",
    "technical support engineer": "IT Support",
    "help desk technician": "IT Support",
    "technical specialist": "Technical Specialist",
    "salesforce senior solution specialist": "Technical Specialist",
    "solutions representative - market analytics": "Analyst",
    "audio visual technician": "Technician",
    "field technician": "Technician",
    "system maintenance technician": "Technician",
    "tech i": "Technician",

    # ── Research & Academic ──
    "research assistant": "Graduate Assistant",
    "undergraduate research assistant": "Graduate Assistant",
    "undergraduate researcher": "Graduate Assistant",
    "research associate": "Research Associate",
    "research scientist": "Research Scientist",
    "research engineer": "Research Engineer",
    "researcher": "Graduate Assistant",
    "research technician": "Graduate Assistant",
    "research and development chemist": "Research Scientist",
    "postdoctoral researcher": "Graduate Assistant",
    "postdoc": "Graduate Assistant",
    "research assistant/ teaching fellow": "Graduate Assistant",

    # ── Teaching & Graduate Assistants ──
    "teaching assistant": "Graduate Assistant",
    "teaching assistant l2": "Graduate Assistant",
    "graduate teaching assistant": "Graduate Assistant",
    "graduate research assistant": "Graduate Assistant",
    "graduate research and teaching assistant": "Graduate Assistant",
    "graduate student assistant": "Graduate Assistant",
    "student assistant": "Graduate Assistant",
    "student assistant for ms analytics": "Graduate Assistant",
    "graduate student at university of north texas": "Graduate Student",
    "enterprise data warehousing teaching assistant": "Graduate Assistant",
    "instructional assistant": "Graduate Assistant",
    "supplemental instructor": "Graduate Assistant",
    "associate professor / director of ammpi": "Professor",
    "professor": "Professor",
    "assistant professor": "Professor",
    "associate professor": "Professor",
    "tutor": "Tutor",

    # ── Student Roles ──
    "student": "Student",
    "student ambassador": "Student Worker",
    "student laboratory technician": "Student Worker",
    "athletic and fitness center student worker": "Student Worker",
    "housing ambassador": "Student Worker",
    "lab assistant | computer engineering": "Student Worker",
    "ai4all college pathways participant": "Student",
    "team member - society of automotive engineers": "Student",
    "communications chair / founding officer": "Student Leader",

    # ── Internships (generic) ──
    "intern": "Intern",
    "summer internship": "Intern",
    "development intern": "Intern",
    "trainee": "Intern",
    "data science intern": "Data Science",
    "data analyst intern": "Data Analyst",
    "data engineer intern": "Data Engineer",
    "engineering intern": "Engineering",
    "it intern": "IT",
    "web development intern": "Web Developer",
    "web developer intern": "Web Developer",
    "graphic design intern": "Design",
    "architectural intern": "Architecture",
    "administrative assistant intern": "Administrative",
    "sales and marketing intern": "Marketing",

    # ── Design & Creative ──
    "ux designer": "UX Designer",
    "ui designer": "UI Designer",
    "ui/ux designer": "UI/UX Designer",
    "ux/ui designer": "UI/UX Designer",
    "product designer": "Product Designer",
    "graphic designer": "Graphic Designer",
    "creative & art design": "Graphic Designer",
    "3d asset and level designer": "Game Designer",
    "bim coordinator": "BIM Coordinator",
    "descriptive metadata writer": "Technical Writer",
    "technical writer": "Technical Writer",

    # ── Sales, Marketing & Finance ──
    "recruiter": "Recruiter",
    "latam techincal billingual recruiter": "Recruiter",
    "talent advisor": "Recruiter",
    "financial solutions advisor": "Financial Analyst",
    "senior accountant": "Accountant",
    "personal banker": "Financial Analyst",
    "sales associate": "Sales Associate",
    "social media and search engine evaluator": "Marketing Specialist",
    "client support associate": "Customer Support",
    "customer service representative": "Customer Support",

    # ── Construction & Field ──
    "assistant construction manager": "Construction Manager",
    "field project coordinator": "Project Coordinator",

    # ── Operations & Other ──
    "assembly specialist": "Technician",
    "assistant technician": "Technician",
    "retail manager": "Retail Manager",
    "real estate / retail manager": "Retail Manager",
    "retail specialist": "Retail Associate",
    "pharmacy technician": "Pharmacy Technician",
    "credentialing coordinator": "Coordinator",
    "office staff": "Administrative",
    "team member": "Team Member",
    "warehouse team member": "Team Member",
    "job coach": "Coach",
    "gymnastics coach": "Coach",
    "dishwasher": "Food Service",
    "remote": "Other",
    "jp morgan all star code": "Intern",
    "concentra": "Other",
    "crossvue": "Other",
}

# ---------------------------------------------------------------------------
# USER-REQUESTED TITLE COMPACTION RULES
# ---------------------------------------------------------------------------

_PREFERRED_TITLE_BUCKETS = [
    "Software Engineer",
    "Infrastructure / Cloud Engineer",
    "AI / ML Engineer",
    "Data Engineer",
    "Data Scientist",
    "Data Analyst",
    "Data Architect",
    "Operations Analyst",
    "Mechanical Engineer",
    "Manufacturing Engineer",
    "Industrial Engineer",
    "Electrical Engineer",
    "Design Engineer",
    "Field Engineer",
    "General Engineer",
    "Engineering Technician",
    "Construction / Architectural",
    "Business Analyst",
    "Financial Analyst",
    "IT Analyst",
    "Operations",
    "Consultant",
    "Marketing",
    "Human Resources",
    "Client / Customer Services",
    "Professional",
    "Project Manager",
    "Director",
    "Executive",
    "Intern",
    "Graduate Assistant",
    "Researcher",
    "Student Worker",
    "Student",
    "Retail Manager",
    "Associate",
    "Cashier",
    "Clerk",
    "Service Staff",
]

_COMPACT_TITLE_MAP = {
    "director of": "Director",
    "director of engineering": "Director",
    # Software
    "backend engineer": "Software Engineer",
    "frontend engineer": "Software Engineer",
    "senior backend engineer": "Software Engineer",
    "software": "Software Engineer",
    "software developer & consultant": "Software Engineer",
    "software development analyst i": "Software Engineer",
    "software development analyst level 2": "Software Engineer",
    "software development engineer": "Software Engineer",
    "software development engineer ii": "Software Engineer",
    "software engineer – revenue management systems engineering": "Software Engineer",
    "software engineer - revenue management systems engineering": "Software Engineer",
    "senior web developer": "Software Engineer",
    "mainframe developer": "Software Engineer",
    "syteline developer": "Software Engineer",
    "sdet/ automation tester": "Software Engineer",
    "qa engineer": "Software Engineer",

    # Cloud/infra
    "cloud engineer": "Infrastructure / Cloud Engineer",
    "devops engineer": "Infrastructure / Cloud Engineer",
    "cloud/sre/devops engineer": "Infrastructure / Cloud Engineer",
    "systems engineer": "Infrastructure / Cloud Engineer",
    "systems administrator": "Infrastructure / Cloud Engineer",
    "network engineer": "Infrastructure / Cloud Engineer",
    "security engineer": "Infrastructure / Cloud Engineer",

    # AI/ML
    "ai data engineer": "AI / ML Engineer",
    "ai/ml engineer": "AI / ML Engineer",
    "product analyst intern – ai / machine learning": "AI / ML Engineer",
    "product analyst intern - ai / machine learning": "AI / ML Engineer",
    "consultant, data & ai": "AI / ML Engineer",

    # Data
    "operations & data engineer": "Data Engineer",
    "operations analyst": "Operations Analyst",
    "financial analyst": "Financial Analyst",
    "it analyst": "IT Analyst",
    "laboratory safety manager": "Manager",
    "manager - innovation": "Manager",

    # Engineering
    "mechanical development engineer": "Mechanical Engineer",
    "advanced manufacturing engineer senior": "Manufacturing Engineer",
    "general manager / manufacturing engineer": "Manufacturing Engineer",
    "intermediate professional, electrical engineering": "Electrical Engineer",
    "application engineer ii": "Design Engineer",
    "engineer": "General Engineer",
    "engineering": "General Engineer",
    "assistant engineer": "General Engineer",
    "technician": "Engineering Technician",

    # Construction/architectural
    "bim coordinator": "Construction / Architectural",
    "architectural designer": "Construction / Architectural",
    "residential and commercial buildings": "Construction / Architectural",

    # Business/analyst
    "analyst": "Business Analyst",
    "accountant": "Financial Analyst",
    "itsm systems analyst": "IT Analyst",
    "operations officer": "Operations",
    "operations manager": "Operations",
    "senior integration consultant": "Consultant",
    "workday integrations consultant": "Consultant",
    "marketing consultant": "Marketing",
    "marketing manager": "Marketing",
    "marketing specialist": "Marketing",
    "human resources generalist": "Human Resources",
    "recruiter": "Human Resources",
    "technical bilingual recruiter": "Human Resources",
    "client service specialist - employee benefits": "Client / Customer Services",
    "customer support": "Client / Customer Services",
    "patient access manager": "Client / Customer Services",
    "pharmacy technician": "Client / Customer Services",

    # Management
    "project manager ii": "Project Manager",
    "project engineer": "Project Manager",
    "project coordinator": "Project Manager",
    "managing director": "Director",
    "team lead of consulting practices": "Director",
    "team leader of software development": "Director",
    "president": "Executive",
    "executive vice president of operations and human resources": "Executive",
    "technology officer": "Executive",
    "town manager": "Executive",
    "assistant vice president - financial solutions advisor": "Executive",
    "leader": "Director",

    # Student buckets
    "engineering co-op": "Intern",
    "summer": "Intern",
    "graduate graduate assistant-deep six lab": "Graduate Assistant",
    "graduate researcher": "Researcher",
    "lab assistant": "Researcher",
    "community assistant": "Student Worker",
    "front desk staff": "Student Worker",
    "supplementor instructor": "Student Worker",
    "tutor": "Student Worker",
    "graduate student": "Student",
    "student leader": "Student",
    "member": "Student",
    "women in cybersecurity (wicys)": "Student",
    "women in cybersecurity": "Student",
    "wicys": "Student",

    # Retail / service
    "general manager": "Retail Manager",
    "sales associate": "Associate",
    "seasonal sales associate": "Associate",
    "digital personal shopper": "Associate",
    "team member": "Associate",
    "title clerk": "Clerk",
    "server trainer": "Service Staff",

    # Misc guidance
    "coach": "Professional",
    "graphic designer": "Professional",
    "product designer": "Professional",
    "product specialist": "Professional",
    "solutions architect": "Professional",
    "solution advisor": "Professional",
    "technical writer": "Professional",
    "technical specialist": "Professional",
    "account executive": "Professional",
}


def _compact_normalized_title(candidate: str, raw_title: str = "") -> str:
    """Apply strict title buckets when known; otherwise preserve candidate."""
    source = (candidate or "").strip()
    raw = (raw_title or "").strip()
    if not source and not raw:
        return ""

    for text in (source, raw):
        t = (text or "").strip()
        if not t:
            continue
        low = t.lower()

        # Drop obvious non-title noise.
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}", low):
            return ""
        if low in {"yes", "no", "n/a", "na", "none", "null", "unknown"}:
            return ""

        mapped = _COMPACT_TITLE_MAP.get(low)
        if mapped:
            return mapped

    low = (source or raw).lower()

    # Regex-driven compaction for common families.
    if re.search(r"\b(software|frontend|front-end|backend|web developer|mainframe|syteline|sdet|qa)\b", low):
        return "Software Engineer"
    if re.search(r"\b(cloud|devops|sre|infrastructure|systems administrator|sysadmin|network engineer|security engineer)\b", low):
        return "Infrastructure / Cloud Engineer"
    if re.search(r"\b(ai|machine learning|ml)\b", low):
        return "AI / ML Engineer"
    if re.search(r"\bdata engineer\b", low):
        return "Data Engineer"
    if re.search(r"\bdata scientist\b", low):
        return "Data Scientist"
    if re.search(r"\bdata analyst\b", low):
        return "Data Analyst"
    if re.search(r"\bdata architect\b", low):
        return "Data Architect"
    if re.search(r"\bfinancial analyst\b", low):
        return "Financial Analyst"
    if re.search(r"\bit analyst\b", low):
        return "IT Analyst"
    if re.search(r"\bmechanical\b", low):
        return "Mechanical Engineer"
    if re.search(r"\bmanufacturing\b", low):
        return "Manufacturing Engineer"
    if re.search(r"\bindustrial\b", low):
        return "Industrial Engineer"
    if re.search(r"\belectrical\b", low):
        return "Electrical Engineer"
    if re.search(r"\b(field engineer)\b", low):
        return "Field Engineer"
    if re.search(r"\b(engineering technician|technician)\b", low):
        return "Engineering Technician"
    if re.search(r"\b(architectural|bim|construction)\b", low):
        return "Construction / Architectural"
    if re.search(r"\b(analyst)\b", low):
        return "Business Analyst"
    if re.search(r"\b(operations manager|operations officer)\b", low):
        return "Operations"
    if re.search(r"\b(consultant)\b", low):
        return "Consultant"
    if re.search(r"\b(marketing)\b", low):
        return "Marketing"
    if re.search(r"\b(recruiter|human resources)\b", low):
        return "Human Resources"
    if re.search(r"\b(project manager|project engineer|project coordinator)\b", low):
        return "Project Manager"
    if re.search(r"\b(intern|co-op)\b", low):
        return "Intern"
    if re.search(r"\b(graduate assistant)\b", low):
        return "Graduate Assistant"
    if re.search(r"\b(graduate researcher|lab assistant)\b", low):
        return "Researcher"
    if re.search(r"\b(student worker|community assistant|front desk|tutor)\b", low):
        return "Student Worker"
    if re.search(r"\b(student|graduate student)\b", low):
        return "Student"
    if re.search(r"\b(cashier)\b", low):
        return "Cashier"
    if re.search(r"^director\b", low):
        return "Director"
    if re.search(r"\blaboratory safety manager\b", low):
        return "Manager"
    if re.search(r"^manager\b", low):
        return "Manager"
    if re.search(r"\b(president|vice president|executive|officer)\b", low):
        return "Executive"

    return source or raw


# ---------------------------------------------------------------------------
# PATTERN-BASED NORMALIZATION
# Applied when exact TITLE_MAP match fails. Strips common modifiers and retries.
# ---------------------------------------------------------------------------

# Regex patterns to strip before re-matching
_LEVEL_SUFFIXES = re.compile(
    r'\s*[-–]\s*(?:level\s*)?\s*(?:i{1,3}|iv|v|[1-5])\s*$', re.IGNORECASE
)
_PAREN_QUALIFIER = re.compile(r'\s*\(.*?\)\s*$')
_SENIORITY_PREFIX = re.compile(
    r'^(?:senior|sr\.?|junior|jr\.?|lead|staff|principal|chief|associate|vice president|vp of|vp|assistant vice president|avp|assistant)\s+',
    re.IGNORECASE,
)
_SENIORITY_SUFFIX = re.compile(
    r'\s+(?:intern|trainee)$',
    re.IGNORECASE,
)

# Rule 1: Remove Locations and Regional Markers
_LOCATION_MARKERS = re.compile(
    r'\s*[-–]\s*(?:Austin|Dallas|Fort Worth|Houston|San Antonio|Denton|Plano|Frisco|Irving|Arlington|Garland|Mesquite|Grand Prairie|McKinney|Carrollton|Richardson|Lewisville|Allen|Flower Mound|Little Elm|The Colony|Southlake)\s*$', 
    re.IGNORECASE
)
_REGIONAL_MARKERS = re.compile(
    r'\b(?:LATAM|Southwest Region|North America|EMEA|APAC|US|USA|Global|Regional|Midwest|Northeast|Southeast|West Coast|Central)\b',
    re.IGNORECASE
)


def _looks_like_location_fragment(fragment: str) -> bool:
    """Heuristic for trailing location fragments like ', Hyderabad' or ' - Austin'."""
    if not fragment:
        return False
    frag = fragment.strip()
    if not frag:
        return False
    if re.search(r"\d", frag):
        return False

    low = frag.lower()
    if re.search(r"\b(engineer|manager|analyst|developer|consultant|director|intern|assistant|officer)\b", low):
        return False

    words = re.findall(r"[A-Za-z][A-Za-z'.&-]*", frag)
    return 1 <= len(words) <= 4


def _strip_trailing_location_fragment(text: str) -> str:
    """Strip location suffix when separated by comma/dash."""
    t = (text or "").strip()
    if not t:
        return ""
    for sep in [",", " - ", " – ", " — "]:
        if sep in t:
            head, tail = t.rsplit(sep, 1)
            if _looks_like_location_fragment(tail):
                return head.strip()
    return t


def _cleanup_title(raw: str) -> str:
    """
    Basic cleanup without mapping:
      - strip whitespace
      - collapse multiple spaces
      - remove trailing punctuation variants
      - remove locations and regional markers
    Returns cleaned string (preserves original case for display).
    """
    if not raw:
        return ""
    t = raw.strip()
    t = re.sub(r'\s+', ' ', t)
    t = _strip_trailing_location_fragment(t)
    
    # Remove locations (e.g., " - Austin")
    t = _LOCATION_MARKERS.sub('', t).strip()
    
    # Remove regional markers (e.g., "LATAM")
    t = _REGIONAL_MARKERS.sub('', t).strip()
    
    # Remove trailing punctuation variants
    t = re.sub(r'[.,\-]+$', '', t).strip()
    
    # More aggressive location removal (e.g., "Director Operations - Austin")
    t = re.sub(r'\s+[-–]\s+(?:Austin|Dallas|Fort Worth|Houston|San Antonio|Denton|Plano|Frisco|Irving|Arlington|Garland|Mesquite|Grand Prairie|McKinney|Carrollton|Richardson|Lewisville|Allen|Flower Mound|Little Elm|The Colony|Southlake)$', '', t, flags=re.IGNORECASE)
    
    return t.strip()


def normalize_title_deterministic(raw_title: str) -> str:
    """
    Deterministic normalization with multi-pass matching:
      1. Cleanup whitespace & punctuation
      2. Exact lookup in TITLE_MAP (case-insensitive)
      3. Strip level suffixes (I/II/III/1/2/3) and retry
      4. Strip parenthetical qualifiers and retry
      5. If not found, return the cleaned title as-is

    Returns the normalized title string.
    """
    cleaned = _cleanup_title(raw_title)
    if not cleaned:
        return ""
    key = cleaned.lower()

    candidate = None

    # Pass 1: exact match
    if key in TITLE_MAP:
        candidate = TITLE_MAP[key]

    # Special heuristic: force "soft" roles to Software Engineer
    if candidate is None and key.startswith('soft'):
        candidate = "Software Engineer"

    # Pass 2: strip level suffixes (e.g. "Engineer II" → "Engineer")
    if candidate is None:
        stripped = _LEVEL_SUFFIXES.sub('', key).strip()
        if stripped != key and stripped in TITLE_MAP:
            candidate = TITLE_MAP[stripped]

    # Pass 3: strip parenthetical qualifiers (e.g. "Analyst (Contract)" → "Analyst")
    if candidate is None:
        stripped = _PAREN_QUALIFIER.sub('', key).strip()
        if stripped != key and stripped in TITLE_MAP:
            candidate = TITLE_MAP[stripped]

    # Pass 4: strip both level suffixes and parens
    if candidate is None:
        stripped = _PAREN_QUALIFIER.sub('', _LEVEL_SUFFIXES.sub('', key)).strip()
        if stripped != key and stripped in TITLE_MAP:
            candidate = TITLE_MAP[stripped]

    # Pass 5: strip seniority (Senior, Intern, etc) on top of suffixes and parens
    if candidate is None:
        stripped_all = _SENIORITY_PREFIX.sub('', _PAREN_QUALIFIER.sub('', _LEVEL_SUFFIXES.sub('', key)))
        stripped_all = _SENIORITY_SUFFIX.sub('', stripped_all).strip()
        if stripped_all != key and stripped_all in TITLE_MAP:
            candidate = TITLE_MAP[stripped_all]

    if candidate is None:
        # No deterministic match — keep cleaned title as category with seniority stripped.
        final_clean = _SENIORITY_PREFIX.sub('', cleaned)
        final_clean = _SENIORITY_SUFFIX.sub('', final_clean).strip()
        if final_clean:
            final_clean = final_clean[0].upper() + final_clean[1:]
        candidate = final_clean or cleaned

    return _compact_normalized_title(candidate, raw_title=cleaned)


# ---------------------------------------------------------------------------
# GROQ-BASED NORMALIZATION (for future scraping)
# ---------------------------------------------------------------------------

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

_groq_client = None


def _get_groq_client():
    """Lazy-init Groq client."""
    global _groq_client
    if _groq_client is not None:
        return _groq_client
    if not GROQ_API_KEY:
        return None
    try:
        from groq import Groq
        _groq_client = Groq(api_key=GROQ_API_KEY)
        return _groq_client
    except ImportError:
        logger.warning("groq package not installed — Groq normalization disabled")
        return None
    except Exception as e:
        logger.error(f"Failed to init Groq client: {e}")
        return None


def _coerce_existing_title_choice(candidate: str, existing_titles: list[str]) -> str:
    """
    Normalize an LLM title output and restore exact existing title casing when possible.
    """
    if not candidate or not isinstance(candidate, str):
        return ""

    cleaned = re.sub(r"\s+", " ", candidate).strip().strip('"\'')
    cleaned = re.sub(r"\s*[|:;,.!?]+\s*$", "", cleaned).strip()

    if not cleaned:
        return ""

    existing_map = {}
    for title in existing_titles or []:
        if isinstance(title, str) and title.strip():
            existing_map[title.strip().casefold()] = title.strip()

    match = existing_map.get(cleaned.casefold())
    return match if match else cleaned


def normalize_title_with_groq(raw_title: str, existing_titles: list) -> str:
    """
    Use Groq LLM to classify a raw job title.

    Args:
        raw_title:       The raw scraped job title.
        existing_titles: List of already-known normalized titles in the DB.

    Returns:
        A normalized title string (either an existing one or a new suggestion).
        Falls back to deterministic normalization on any failure.
    """
    client = _get_groq_client()
    if not client:
        logger.info("Groq unavailable — falling back to deterministic normalization")
        return normalize_title_deterministic(raw_title)

    # Build prompt
    titles_list = "\n".join(f"- {t}" for t in existing_titles[:180])  # keep prompt lean
    raw_text = (raw_title or "").strip()[:180]

    preferred_list = "\n".join(f"- {t}" for t in _PREFERRED_TITLE_BUCKETS)

    prompt = f"""You are a job-title normalization engine.

Task:
Given a raw job title and existing normalized titles, produce one normalized title.

Rules:
1. If an existing title is semantically equivalent, return that EXACT existing string.
2. Prefer one of these canonical buckets when semantically equivalent:
{preferred_list}
3. Otherwise return a new concise base function title (1-4 words).
4. Remove seniority/level modifiers (Senior, Junior, II, III, Intern, Lead, etc.).
5. Remove org-specific fragments (department names, "at <university/company>").
6. Keep common technical acronyms when core to the role (QA, SRE, DevOps, UI/UX).
7. If raw title is empty/noise (N/A, unknown), return an empty string.
8. For memberships/officer roles in student clubs/orgs (e.g., WiCyS), normalize to "Student".

Existing normalized titles:
{titles_list}

Raw title: "{raw_text}"

Return JSON only:
{{"normalized_title":"<string>", "match_type":"existing|new|empty"}}"""

    try:
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "Output strictly valid JSON with normalized_title and match_type only."
                },
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=40
        )
        payload = json.loads(response.choices[0].message.content)
        result = _coerce_existing_title_choice(payload.get("normalized_title", ""), existing_titles)
        result = _compact_normalized_title(result, raw_title=raw_title)
        if result.casefold() in {"n/a", "na", "none", "null", "unknown", "other"}:
            logger.warning(f"Groq returned non-title value for {raw_title!r}: {result!r}")
            return normalize_title_deterministic(raw_title)
        if result and len(result) < 100:
            return result
        logger.warning(f"Groq returned suspicious title result: {result!r}")
        return normalize_title_deterministic(raw_title)
    except Exception as e:
        logger.error(f"Groq normalization failed: {e}")
        return normalize_title_deterministic(raw_title)


# ---------------------------------------------------------------------------
# DATABASE HELPERS
# ---------------------------------------------------------------------------

def get_all_normalized_titles(conn) -> list:
    """Fetch all existing normalized titles from the DB."""
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute("SELECT id, normalized_title FROM normalized_job_titles ORDER BY normalized_title")
            return cur.fetchall()
    except Exception as e:
        logger.error(f"Error fetching normalized titles: {e}")
        return []


def get_or_create_normalized_title(conn, raw_title: str, use_groq: bool = False) -> int | None:
    """
    Main entry point. Returns the normalized_job_title_id for a given raw title.

    1. Deterministic normalization to get a candidate string.
    2. If use_groq=True and deterministic produced a passthrough, try Groq.
    3. INSERT OR IGNORE into normalized_job_titles.
    4. Return the id.

    Args:
        conn:      Active DB connection (MySQL or SQLite-wrapped).
        raw_title: The raw job title string.
        use_groq:  Whether to attempt Groq classification.

    Returns:
        Integer ID from normalized_job_titles, or None on failure.
    """
    if not raw_title or not raw_title.strip():
        return None

    cleaned = _cleanup_title(raw_title)
    if not cleaned:
        return None

    # Step 1: deterministic
    norm = normalize_title_deterministic(raw_title)

    # Step 2: optionally consult Groq if deterministic was a passthrough
    if use_groq and norm == cleaned:
        existing = get_all_normalized_titles(conn)
        existing_titles = [r['normalized_title'] for r in existing]
        norm = normalize_title_with_groq(raw_title, existing_titles)

    # Step 3: upsert into normalized_job_titles
    try:
        with conn.cursor() as cur:
            # Try INSERT IGNORE / ON CONFLICT
            try:
                cur.execute(
                    "INSERT INTO normalized_job_titles (normalized_title) VALUES (%s) "
                    "ON DUPLICATE KEY UPDATE normalized_title = VALUES(normalized_title)",
                    (norm,)
                )
            except Exception:
                # SQLite fallback syntax
                cur.execute(
                    "INSERT OR IGNORE INTO normalized_job_titles (normalized_title) VALUES (?)",
                    (norm,)
                )

            # Fetch the id
            try:
                cur.execute(
                    "SELECT id FROM normalized_job_titles WHERE normalized_title = %s",
                    (norm,)
                )
            except Exception:
                cur.execute(
                    "SELECT id FROM normalized_job_titles WHERE normalized_title = ?",
                    (norm,)
                )

            row = cur.fetchone()
            if row:
                return row['id'] if isinstance(row, dict) else row[0]
            return None
    except Exception as e:
        logger.error(f"Error in get_or_create_normalized_title: {e}")
        return None
    if re.search(r"\bfinancial analyst\b", low):
        return "Financial Analyst"
    if re.search(r"\bit analyst\b", low):
        return "IT Analyst"
