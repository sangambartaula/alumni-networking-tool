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

from groq_client import apply_groq_retry_delay

apply_groq_retry_delay()

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
    "software development engine 1": "Software Engineer",
    "software developer(ta)": "Software Engineer",
    "software application developer": "Software Engineer",
    "software associate": "Software Engineer",
    "associate software engineer": "Software Engineer",
    "associate software": "Software Engineer",
    "software systems engineer": "Software Engineer",
    "member of technical staff": "Software Engineer",
    "mts": "Software Engineer",
    "smts": "Software Engineer",
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
    "lead validation engineer": "Software Engineer",
    "junior software engineer": "Software Engineer",
    "jr. software engineer": "Software Engineer",
    "vice president lead software engineer": "Software Engineer",
    "principal systems engineer": "Software Engineer",
    "lead systems engineer": "Software Engineer",
    "sr. director of software engineering": "Director",
    "vp of software engineering": "VP",
    "developer": "Software Engineer",
    "application developer": "Software Engineer",
    "application development analyst": "Software Engineer",
    "application engineer": "Application Engineer",
    "application engineer ii": "Application Engineer",
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
    "frontend developer": "Software Engineer",
    "front-end developer": "Software Engineer",
    "front end developer": "Software Engineer",
    "frontend engineer": "Software Engineer",
    "front-end engineer": "Software Engineer",
    "senior ui developer": "Software Engineer",
    "backend developer": "Software Engineer",
    "back-end developer": "Software Engineer",
    "back end developer": "Software Engineer",
    "backend engineer": "Software Engineer",
    "back-end engineer": "Software Engineer",
    "back end engineer": "Software Engineer",
    "web developer": "Software Engineer",
    "web dev": "Software Engineer",
    "web application developer": "Software Engineer",

    # ── Java / Python / ServiceNow specific ──
    "java developer": "Software Engineer",
    "python developer": "Software Engineer",
    "servicenow developer": "Software Engineer",

    # ── Data & AI ──
    "ai engineer": "AI / ML Engineer",
    "ai software engineer": "AI / ML Engineer",
    "ai/ml engineer": "AI / ML Engineer",
    "ml engineer": "AI / ML Engineer",
    "machine learning engineer": "AI / ML Engineer",
    "artificial intelligence engineer": "AI / ML Engineer",
    "software engineer - machine learning": "AI / ML Engineer",
    "principal ai engineer": "AI / ML Engineer",
    "senior ai data engineer": "AI / ML Engineer",
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
    "data architect": "Data Engineer",
    "data owner": "Data Analyst",
    "operations & data engineer": "Data Engineer",
    "database administrator": "Database Administrator",
    "dba": "Database Administrator",
    "business analyst": "Data Analyst",
    "product analyst": "Data Analyst",
    "business intelligence analyst": "Data Analyst",
    "bi analyst": "Data Analyst",
    "bi developer": "Data Analyst",
    "operations & data analyst (recreational facilities)": "Data Analyst",
    "market research analyst": "Data Analyst",
    "lead operations analyst": "Operations",
    "itsm systems analyst": "Data Analyst",
    "technology analyst": "Data Analyst",

    # ── DevOps & Cloud ──
    "devops engineer": "DevOps Engineer",
    "dev ops engineer": "DevOps Engineer",
    "aws devops engineer": "DevOps Engineer",
    "jr. devops engineer": "DevOps Engineer",
    "sr/sre/devops engineer": "DevOps Engineer",
    "cloud/sre/devops engineer": "DevOps Engineer",
    "sr cloud/sre/devops engineer": "DevOps Engineer",
    "site reliability engineer": "DevOps Engineer",
    "sre": "DevOps Engineer",
    "cloud engineer": "DevOps Engineer",
    "aws cloud practitioner- internship": "DevOps Engineer",
    "amazon web services (aws)": "DevOps Engineer",
    "cloud architect": "DevOps Engineer",
    "solutions architect": "Software Engineer",
    "jr. system architect": "Software Engineer",
    "systems administrator": "Systems Administrator",
    "system administrator": "Systems Administrator",
    "sys admin": "Systems Administrator",
    "sysadmin": "Systems Administrator",
    "network engineer": "Network Engineer",
    "network administrator": "Network Engineer",

    # ── Systems Engineering ──
    "system engineer": "Software Engineer",
    "systems engineer hil": "Software Engineer",
    "technical systems engineer": "Software Engineer",
    "assistant system design engineer": "Software Engineer",

    # ── Security ──
    "cybersecurity analyst": "Software Engineer",
    "cyber security analyst": "Software Engineer",
    "information security analyst": "Software Engineer",
    "cybersecurity analyst (graduate assistant)": "Software Engineer",
    "security engineer": "Software Engineer",
    "cybersecurity engineer": "Software Engineer",
    "sr. cyber security engineer": "Software Engineer",
    "security analyst": "Software Engineer",

    # ── QA & Testing ──
    "qa engineer": "Software Engineer",
    "quality assurance engineer": "Software Engineer",
    "test engineer": "Software Engineer",
    "test analyst": "Software Engineer",
    "software tester": "Software Engineer",
    "sdet": "Software Engineer",
    "sdet/ automation tester": "Software Engineer",
    "software development engineer in test": "Software Engineer",
    "senior test automation engineer": "Software Engineer",
    "quality control analyst": "Software Engineer",
    "quality engineer": "Software Engineer",
    "senior design quality engineer": "Software Engineer",

    # ── Engineering (general, mechanical, civil, etc.) ──
    "engineer": "Mechanical Engineer",
    "engineer i": "Mechanical Engineer",
    "engineer ii": "Mechanical Engineer",
    "associate engineer": "Mechanical Engineer",
    "mechanical engineer": "Mechanical Engineer",
    "mechanical design engineer": "Mechanical Engineer",
    "mechanical engineering intern": "Mechanical Engineer",
    "mechanical development engineer": "Mechanical Engineer",
    "development engineer - mechanical": "Mechanical Engineer",
    "civil engineer": "Civil Engineer",
    "civil enginnering co-op/intern": "Civil Engineer",
    "structural engineer": "Civil Engineer",
    "piping engineer": "Mechanical Engineer",
    "process engineer": "Mechanical Engineer",
    "manufacturing engineer": "Mechanical Engineer",
    "industrial engineer": "Mechanical Engineer",
    "controls engineer": "Mechanical Engineer",
    "field engineer": "Field Engineer",
    "feo project engineer": "Project Engineer",
    "project engineer": "Project Engineer",
    "engineering technician": "Mechanical Engineer",
    "senior technical engineer": "Mechanical Engineer",
    "senior materials engineer": "Mechanical Engineer",
    "automation engineering lead": "Mechanical Engineer",
    "design engineer": "Mechanical Engineer",
    "advanced manufacturing engineer senior": "Mechanical Engineer",
    "general manager / manufacturing engineer": "Mechanical Engineer",
    "intermediate professional, electrical engineering": "Mechanical Engineer",
    "assistant engineer": "Mechanical Engineer",
    "engineering co-op": "Mechanical Engineer",

    # ── Management & Leadership ──
    "engineering manager": "Manager",
    "software engineering manager": "Manager",
    "technical lead": "Manager",
    "tech lead": "Manager",
    "team lead": "Manager",
    "project manager": "Project Manager",
    "project manager 2": "Project Manager",
    "project manager ii": "Project Manager",
    "senior project manager": "Project Manager",
    "sr project manager": "Project Manager",
    "sr. project manager": "Project Manager",
    "project manager ii": "Project Manager",
    "assistant project manager": "Project Manager",
    "project management coordinator": "Project Manager",
    "project management intern": "Project Manager",
    "project coordinator": "Project Manager",
    "program manager": "Project Manager",
    "product manager": "Manager",
    "scrum master": "Manager",
    "vp of engineering": "VP",
    "vice president of engineering": "VP",
    "ceo": "CEO",
    "chief executive officer": "CEO",
    "cto": "CTO",
    "chief technology officer": "CTO",
    "coo": "COO",
    "chief operations officer": "COO",
    "cfo": "CFO",
    "chief financial officer": "CFO",
    "cmo": "CMO",
    "chief marketing officer": "CMO",
    "director of engineering": "Director",
    "manager": "Manager",
    "senior manager": "Manager",
    "regional manager": "Manager",
    "regional property manager": "Manager",
    "senior manager of field marketing": "Marketing",
    "senior marketing manager": "Marketing",
    "operations manager": "Manager",
    "operations supervisor": "Manager",
    "production manager": "Manager",
    "supply chain manager": "Manager",
    "procurement manager": "Manager",
    "executive vice president of operations": "VP",
    "executive vice president of operations and human resources": "VP",
    "director of southwest region": "Director",
    "director of strategic initiatives": "Director",
    "director of employee experience": "Director",
    "senior director, assurance & enterprise risk management": "Director",
    "partner and executive director": "Director",
    "founder and director": "Director",
    "co-owner & operations head": "Manager",
    "owner": "Manager",
    "managing director": "Manager",
    "general manager": "Manager",
    "laboratory safety manager": "Manager",
    "manager - innovation": "Manager",
    "leader": "Manager",
    "team lead of consulting practices": "Manager",
    "team leader of software development": "Manager",
    "town manager": "Manager",
    "computing supervisor": "Manager",
    "vp of sales": "Sales",
    "vp sales executive": "Sales",

    # ── Consulting & IT ──
    "consultant": "Consultant",
    "it consultant": "Consultant",
    "technology consultant": "Consultant",
    "dynamic 365 crm consultant": "Consultant",
    "healthcare information technology consultant": "Consultant",
    "graduate services consultant": "Consultant",
    "consultant, data & ai": "Consultant",
    "workday integrations consultant": "Consultant",
    "senior integration consultant": "Consultant",
    "social media marketing consultant": "Marketing",
    "technical architect": "Software Engineer",
    "technology summer analyst": "Operations",
    "senior analyst": "Data Analyst",
    "it systems analyst": "Data Analyst",
    "it support agent": "Software Engineer",
    "information technology support engineer": "Software Engineer",
    "information technology intern": "Student",
    "it specialist": "Software Engineer",
    "it support specialist": "Software Engineer",
    "technical support engineer": "Software Engineer",
    "help desk technician": "Software Engineer",
    "technical specialist": "Software Engineer",
    "salesforce senior solution specialist": "Software Engineer",
    "solutions representative - market analytics": "Data Analyst",

    # ── Research & Academic ──
    "research assistant": "Researcher",
    "undergraduate research assistant": "Researcher",
    "undergraduate researcher": "Researcher",
    "undergradute research assistant": "Researcher",
    "undergradute researcher": "Researcher",
    "research project assistant": "Researcher",
    "graduate research project assistant": "Graduate Assistant",
    "research associate": "Researcher",
    "research scientist": "Researcher",
    "research engineer": "Researcher",
    "researcher": "Researcher",
    "research technician": "Researcher",
    "research and development chemist": "Researcher",
    "postdoctoral researcher": "Postdoctoral Researcher",
    "postdoc": "Postdoctoral Researcher",
    "research assistant/ teaching fellow": "Graduate Assistant",
    "graduate researcher": "Researcher",
    "lab assistant": "Researcher",

    # ── Teaching & Graduate Assistants ──
    "teaching assistant": "Graduate Assistant",
    "teaching assistant l2": "Graduate Assistant",
    "graduate teaching assistant": "Graduate Assistant",
    "graduate research assistant": "Graduate Assistant",
    "graduate research and teaching assistant": "Graduate Assistant",
    "graduate student assistant": "Graduate Assistant",
    "graduate assistant/ teaching fellow": "Graduate Assistant",
    "graduate graduate assistant-deep six lab": "Graduate Assistant",
    "student assistant": "Student",
    "student assistant for ms analytics": "Student",
    "graduate student at university of north texas": "Student",
    "enterprise data warehousing teaching assistant": "Graduate Assistant",
    "instructional assistant": "Graduate Assistant",
    "supplemental instructor": "Graduate Assistant",
    "supplementor instructor": "Graduate Assistant",
    "associate professor / director of ammpi": "Professor",
    "professor": "Professor",
    "assistant professor": "Professor",
    "associate professor": "Professor",
    "tutor": "Student",

    # ── Student Roles ──
    "student": "Student",
    "student laboratory technician": "Student",
    "athletic and fitness center student worker": "Student",
    "lab assistant | computer engineering": "Student",
    "ai4all college pathways participant": "Student",
    "team member - society of automotive engineers": "Student",
    "communications chair / founding officer": "Student",
    "student leader": "Student",
    "student worker": "Student",
    "community assistant": "Student",
    "graduate student": "Student",
    "member": "Student",
    "women in cybersecurity (wicys)": "Student",
    "women in cybersecurity": "Student",
    "wicys": "Student",

    # ── Internships (generic) ──
    "intern": "Intern",
    "summer internship": "Intern",
    "summer intern": "Intern",
    "development intern": "Intern",
    "trainee": "Intern",
    "jp morgan all star code": "Student",
    "jp morgan all star code": "Student",

    # ── Design & Creative ──
    "ux designer": "Marketing",
    "ui designer": "Marketing",
    "ui/ux designer": "Marketing",
    "ux/ui designer": "Marketing",
    "product designer": "Marketing",
    "graphic designer": "Marketing",
    "creative & art design": "Marketing",
    "3d asset and level designer": "Marketing",
    "bim coordinator": "Project Manager",
    "descriptive metadata writer": "Marketing",
    "technical writer": "Marketing",
    "graphic design intern": "Marketing",
    "digital personal shopper": "Marketing",
    "development executive": "Marketing",

    # ── Sales ──
    "sales associate": "Sales",
    "seasonal sales associate": "Sales",
    "retail business development representative": "Sales",
    "account executive": "Sales",

    # ── Human Resources ──
    "recruiter": "Human Resources",
    "latam techincal billingual recruiter": "Human Resources",
    "talent advisor": "Human Resources",
    "technical bilingual recruiter": "Human Resources",
    "human resources generalist": "Human Resources",
    "head of human resources": "Human Resources",
    "hr manager": "Human Resources",

    # ── Finance / Accounting ──
    "accountant": "Accountant",
    "senior accountant": "Accountant",
    "sr accountant": "Accountant",
    "sr. accountant": "Accountant",
    "financial solutions advisor": "Finance / Accounting",
    "financial advisor": "Finance / Accounting",
    "financial reporting accountant": "Finance / Accounting",
    "personal banker": "Finance / Accounting",
    "title clerk": "Finance / Accounting",
    "due diligence associate": "Finance / Accounting",
    "assistant vice president - financial solutions advisor": "Finance / Accounting",

    # ── Customer Service ──
    "client support associate": "Customer Service",
    "customer service representative": "Customer Service",
    "client service specialist - employee benefits": "Customer Service",
    "customer support": "Customer Service",
    "front desk staff": "Customer Service",
    "front office supervisor": "Customer Service",
    "patient access manager": "Customer Service",
    "pharmacy technician": "Customer Service",
    "cashier": "Customer Service",
    "crew member": "Customer Service",
    "server trainer": "Customer Service",
    "dishwasher": "Customer Service",

    # ── Marketing ──
    "social media and search engine evaluator": "Marketing",
    "marketing specialist": "Marketing",
    "marketing consultant": "Marketing",
    "marketing manager": "Marketing",

    # ── Operations ──
    "operations analyst": "Operations",
    "operations officer": "Operations",
    "technology officer": "Operations",

    # ── Construction & Field ──
    "field project coordinator": "Project Manager",
    "residential and commercial buildings": "Mechanical Engineer",

    # ── Operations & Other ──
    "retail manager": "Manager",
    "real estate / retail manager": "Manager",
    "retail specialist": "Sales",
    "credentialing coordinator": "Manager",
    "office staff": "Manager",
    "team member": "Manager",
    "warehouse team member": "Manager",
    "job coach": "Manager",
    "gymnastics coach": "Manager",
    "remote": "Other",
    "concentra": "Other",
    "crossvue": "Other",
}

# ---------------------------------------------------------------------------
# CANONICAL TITLE BUCKETS (user-specified)
# These are the ONLY bucket names that should appear in normalized output.
# ---------------------------------------------------------------------------

_PREFERRED_TITLE_BUCKETS = [
    "Software Engineer",
    "AI / ML Engineer",
    "Data Engineer",
    "Data Scientist",
    "Data Analyst",
    "DevOps Engineer",
    "Mechanical Engineer",
    "Civil Engineer",
    "Field Engineer",
    "Project Engineer",
    "Researcher",
    "Graduate Assistant",
    "Student",
    "Project Manager",
    "Application Engineer",
    "Database Administrator",
    "Systems Administrator",
    "Network Engineer",
    "Accountant",
    "Manager",
    "Customer Service",
    "Sales",
    "Marketing",
    "Human Resources",
    "Finance / Accounting",
    "Consultant",
    "CEO",
    "CTO",
    "COO",
    "CFO",
    "CMO",
    "VP",
    "Director",
    "Professor",
    "Doctoral Candidate",
    "Postdoctoral Researcher",
    "Operations",
    "Ambassador",
    "Peer Mentor",
    "Technician",
    "Intern",
    "Associate",
]

# Quick set for O(1) membership checks
_PREFERRED_BUCKETS_SET = {b.casefold() for b in _PREFERRED_TITLE_BUCKETS}


def _compact_normalized_title(candidate: str, raw_title: str = "") -> str:
    """Regex-based fallback compaction for the DETERMINISTIC path only.

    This should NOT be called from the Groq path — Groq already receives
    the preferred bucket list in its prompt and is trusted to produce
    the correct bucket.

    If the candidate is already a preferred bucket, it is returned as-is.
    Otherwise, regex patterns attempt to classify it into a bucket.
    """
    source = (candidate or "").strip()
    raw = (raw_title or "").strip()
    if not source and not raw:
        return ""

    # Drop obvious non-title noise.
    text = source or raw
    low = text.lower()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}", low):
        return ""
    if low in {"yes", "no", "n/a", "na", "none", "null", "unknown"}:
        return ""

    # Reject obvious location-only artifacts.
    if _looks_like_location_only_title(text):
        return ""

    source = text
    low = source.lower()

    # Intern / Associate handling:
    # - Keep plain intern-only roles as "Intern"
    # - Drop Intern/Associate modifiers when the base role is present.
    if low in {"intern", "internship", "summer intern", "summer internship"}:
        return "Intern"
    if low == "associate":
        return "Associate"

    intern_base = re.sub(r"\s+intern(ship)?$", "", low).strip()
    if intern_base and intern_base != low:
        low = intern_base
        source = intern_base

    assoc_base = re.sub(r"^associate\s+", "", low).strip()
    if assoc_base and assoc_base != low:
        low = assoc_base
        source = assoc_base

    # If the candidate is already a preferred bucket, return it.
    if source.casefold() in _PREFERRED_BUCKETS_SET:
        return source

    # Try TITLE_MAP lookup on the candidate (handles when deterministic
    # returns a cleaned but unmapped title).
    mapped = TITLE_MAP.get(source.lower())
    if mapped:
        return mapped

    # If a likely company token prefixes "Engineer" (e.g., Apple Engineer),
    # normalize to Software Engineer rather than keeping brand text.
    company_prefixed_engineer = re.match(r"^([a-z0-9&.+-]+)\s+engineer$", low)
    if company_prefixed_engineer:
        prefix = company_prefixed_engineer.group(1)
        generic_engineering_prefixes = {
            "software", "data", "devops", "cloud", "network", "systems",
            "mechanical", "civil", "field", "project", "research", "application",
            "database", "quality", "security", "test", "manufacturing", "industrial",
            "electrical", "automation", "process",
        }
        if prefix not in generic_engineering_prefixes:
            return "Software Engineer"

    # Regex-driven compaction for common families.
    low = (source or raw).lower()

    # 1. Executive / C-Level — map to specific CxO buckets first
    if re.search(r"\b(ceo|chief executive officer)\b", low):
        return "CEO"
    if re.search(r"\b(cto|chief technology officer)\b", low):
        return "CTO"
    if re.search(r"\b(coo|chief operating officer|chief operations officer)\b", low):
        return "COO"
    if re.search(r"\b(cfo|chief financial officer)\b", low):
        return "CFO"
    if re.search(r"\b(cmo|chief marketing officer)\b", low):
        return "CMO"
    if re.search(r"\b(vp|vice president|president|cio|ciso|executive vice|evp|svp)\b", low):
        return "VP"

    # 2. Engineering & Tech Roles (Specific before generic)
    if re.search(r"\b(ai|artificial intelligence|machine learning|\bml\b)\b", low):
        return "AI / ML Engineer"
    if re.search(r"\b(cloud|devops|dev ops|sre|site reliability|infrastructure)\b", low):
        return "DevOps Engineer"

    # 2b. IT Infrastructure — distinct titles
    if re.search(r"\b(database administrator|\bdba\b)\b", low):
        return "Database Administrator"
    if re.search(r"\b(systems? administrator|sysadmin|sys admin)\b", low):
        return "Systems Administrator"
    if re.search(r"\b(network engineer|network administrator)\b", low):
        return "Network Engineer"

    if re.search(r"\b(software|frontend|front-end|backend|back-end|fullstack|full-stack|web developer|mainframe|syteline|sdet|qa\b|quality assurance|test engineer|systems? engineer|security engineer|cybersecurity|solutions architect)\b", low):
        return "Software Engineer"

    # 3. Data Roles
    if re.search(r"\bdata engineer\b", low):
        return "Data Engineer"
    if re.search(r"\bdata scientist\b", low):
        return "Data Scientist"
    if re.search(r"\b(data analyst|business analyst|product analyst|bi analyst|business intelligence|data owner)\b", low):
        return "Data Analyst"

    # 4. Directors / Managers / Project Managers
    if re.search(r"\b(director|managing director)\b", low):
        return "Director"
    if re.search(r"\b(project manager|project coordinator|program manager)\b", low):
        return "Project Manager"
    if re.search(r"\bproject engineer\b", low):
        return "Project Engineer"
    if re.search(r"\b(manager|team lead|supervisor|coordinator|office staff|head of|owner|founder)\b", low):
        return "Manager"

    # 5. Core Operational/Support
    if re.search(r"\b(recruiter|human resources|talent|hr\b)\b", low):
        return "Human Resources"
    if re.search(r"\b(accountant|financial|finance|banker|title clerk|due diligence)\b", low):
        return "Finance / Accounting"
    if re.search(r"\b(marketing|graphic design|ux design|ui design|product design|technical writer|digital personal|game design|designer)\b", low):
        return "Marketing"
    if re.search(r"\b(sales|account executive|retail.*development|business development)\b", low):
        return "Sales"
    if re.search(r"\b(operations|operations analyst|operations officer|technology officer|lead operations|business process|process analyst|scrum master)\b", low):
        return "Operations"

    # 6. Civil / Field Engineering — distinct from Mechanical
    if re.search(r"\b(civil|structural)\b", low):
        return "Civil Engineer"
    if re.search(r"\bfield engineer\b", low):
        return "Field Engineer"

    # 7. Mechanical / Traditional Engineering
    if re.search(r"\b(mechanical|manufacturing|industrial|piping|controls|design engineer|engineering technician|assembly|drafter|cad)\b", low):
        return "Mechanical Engineer"

    # 8. Customer Service (includes IT support/help desk)
    if re.search(r"\b(customer service|customer support|client service|front desk|cashier|crew member|server trainer|patient access|it support|help desk|technical support|desktop support)\b", low):
        return "Customer Service"

    # 9. Consulting
    if re.search(r"\b(consultant)\b", low):
        return "Consultant"

    # 10. Academic Hierarchy Structure
    if re.search(r"\bprofessor\b", low):
        return "Professor"
    if re.search(r"\b(ph\.?d\.?\s*candidate|doctoral candidate|doctoral scholar)\b", low):
        return "Doctoral Candidate"
    if re.search(r"\bpostdoc\b", low):
        return "Postdoctoral Researcher"
    if re.search(r"\b(researcher|research assistant|research technician|lab assistant|undergraduate research)\b", low):
        return "Researcher"
    if re.search(r"\b(graduate assistant|teaching assistant|instructional assistant|supplemental instructor)\b", low):
        return "Graduate Assistant"

    # 11. Student Catch-all
    if re.search(r"\bambassador\b", low):
        return "Ambassador"
    if re.search(r"\b(peer mentor|peer tutor)\b", low):
        return "Peer Mentor"
    if re.search(r"\b(technician|tech i|tech ii)\b", low):
        return "Technician"
    if re.search(r"\b(student|\bpeer\b|\bgrader\b)\b", low):
        return "Student"

    return source or raw


# ---------------------------------------------------------------------------
# PATTERN-BASED NORMALIZATION
# Applied when exact TITLE_MAP match fails. Strips common modifiers and retries.
# ---------------------------------------------------------------------------

# Regex patterns to strip before re-matching
_LEVEL_SUFFIXES = re.compile(
    r'\s*(?:[-–]\s*)?(?:level\s*)?\s*(?:i{1,5}|iv|v|[1-9])\s*$',
    re.IGNORECASE,
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


def _looks_like_location_only_title(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    low = t.lower()

    # Common location-only patterns that should never become normalized titles.
    if re.fullmatch(r"[a-z\s]+,\s*[a-z\s]+", low):
        if any(tok in low for tok in ("texas", "united states", "county", "metroplex", "area")):
            return True

    if any(low == x for x in {"denton", "denton, texas", "denton county, texas", "texas", "united states"}):
        return True

    # If no role-like terms are present but location words are, treat as location artifact.
    role_hint = re.search(r"\b(engineer|developer|analyst|manager|director|consultant|architect|scientist|assistant|officer|administrator|researcher|professor|technician|intern|student)\b", low)
    location_hint = re.search(r"\b(county|texas|united states|metroplex|city|dallas|houston|austin|denton|fort worth|plano|frisco|irving|arlington|carrollton|richardson)\b", low)
    return bool(location_hint and not role_hint)


def _strip_company_prefix_from_role(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""

    # One-token org prefix + one-token role (e.g., Workday Engineer).
    m_simple = re.match(
        r"^[A-Za-z0-9&.+'-]+\s+(Engineer|Developer|Analyst|Manager|Consultant|Architect|Scientist|Designer|Technician|Administrator|Specialist)$",
        t,
        re.IGNORECASE,
    )
    if m_simple:
        return m_simple.group(1)

    # One-token org prefix + two-word role family (e.g., Apple Software Engineer).
    m_two = re.match(
        r"^[A-Za-z0-9&.+'-]+\s+((?:Software|Data|DevOps|Cloud|Network|Systems|Mechanical|Civil|Project|Research|Security|Quality|Product|Application|Database)\s+(?:Engineer|Developer|Analyst|Manager|Consultant|Architect|Scientist|Administrator|Specialist))$",
        t,
        re.IGNORECASE,
    )
    if m_two:
        return m_two.group(1)

    return t


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

    # Explicit intern/associate fallbacks before broader stripping logic.
    low_clean = cleaned.casefold()
    if low_clean in {"intern", "internship", "summer intern", "summer internship"}:
        return "Intern"
    if low_clean == "associate":
        return "Associate"

    if _looks_like_location_only_title(cleaned):
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

    prompt = f"""You are a job-title normalization engine. Accuracy matters above all.

Task:
Given a raw job title and existing normalized titles, produce one standardized normalized title.

Rules:
1. If an existing title is semantically equivalent, return that EXACT existing string.
2. Prefer one of these canonical buckets when semantically equivalent:
{preferred_list}
3. Otherwise return a new concise base function title (1-4 words). Do NOT return "Other" unless there is absolutely no way to infer a role.
4. Remove seniority/level modifiers (Senior, Junior, II, III, Intern, Lead, etc.).
5. Remove org-specific fragments (department names, "at <university/company>").
6. Keep common technical acronyms when core to the role (QA, SRE, DevOps, UI/UX).
7. If raw title is empty/noise (N/A, unknown), return an empty string.
8. For memberships/officer roles in student clubs/orgs (e.g., WiCyS), normalize to "Student".
9. "Member of Technical Staff" is a software engineering role → "Software Engineer".
10. Executive titles: "CEO" → "CEO", "CTO" → "CTO", "COO" → "COO", "CFO" → "CFO", "CMO" → "CMO". VP/Vice President → "VP".
11. "Database Administrator", "Systems Administrator", "Network Engineer" should remain their own specific titles.
12. Civil Engineer, Field Engineer, Project Engineer remain distinct titles.
13. Graduate students in experience sections with no assistant title → "Student".
14. If the title is ambiguous, create a reasonable standardized title for a professional dashboard — do NOT default to "Other".
15. Never include company names in normalized titles (e.g., "Workday Engineer" -> "Software Engineer").
16. Never output location text as a normalized title (e.g., "Denton, Texas").
17. Intern handling:
    - Keep "Intern" only when role context is truly absent (e.g., "Summer Intern").
    - If role context exists (e.g., "Software Engineer Intern"), return the base role ("Software Engineer").
18. Associate handling:
    - Drop "Associate" when a concrete role exists (e.g., "Associate Engineer" -> "Engineer" family bucket).
    - Return "Associate" only when no role context exists.

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
        # Do NOT apply _compact_normalized_title here — Groq receives the
        # preferred bucket list in the prompt and is trusted to return the
        # correct bucket.  Compaction is a deterministic-only fallback.
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

    if not norm or not str(norm).strip():
        return None

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
