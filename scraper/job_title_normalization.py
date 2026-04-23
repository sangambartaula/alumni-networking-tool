"""
Job Title Normalization Module

Maps semantically equivalent job titles to a single standardized category.
Preserves all original raw job title values — only the normalized mapping is stored.

Architecture:
  1. Groq-first (when GROQ_API_KEY is set and the groq SDK is available): the model
     proposes a short bucket label with strict JSON + post-validation in code.
  2. Deterministic fallback: TITLE_MAP + regex compaction — used only when Groq is
     unavailable, fails validation, or callers pass use_groq=False.

Usage:
    from job_title_normalization import get_or_create_normalized_title

    norm_id = get_or_create_normalized_title(conn, raw_title, use_groq=True)
    # norm_id is the PK in normalized_job_titles table (or None on failure)
"""

import os
import re
import json
import logging
from pathlib import Path
from typing import Any
from dotenv import load_dotenv

load_dotenv()

try:
    from groq_client import apply_groq_retry_delay
except ImportError:
    from scraper.groq_client import apply_groq_retry_delay

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
    "associate software intern": "Software Engineer",
    "software systems engineer": "Systems Engineer",
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
    "principal systems engineer": "Systems Engineer",
    "lead systems engineer": "Systems Engineer",
    "sr. director of software engineering": "Director",
    "director of software engineering": "Director",
    "vp of software engineering": "VP",
    "developer": "Software Engineer",
    "application developer": "Software Engineer",
    "application development analyst": "Software Engineer",
    "application engineer": "Application Engineer",
    "application engineer ii": "Application Engineer",
    "associate programmer, it aviator": "Software Engineer",
    "mainframe programmer": "Software Engineer",
    "mainframe developer": "Software Engineer",
    "programmer analyst trainee": "Software Engineer",
    "scientific programmer": "Software Engineer",
    "syteline developer": "Software Engineer",

    # ── Full Stack ──
    "full stack developer": "Software Engineer",
    "full-stack developer": "Software Engineer",
    "fullstack developer": "Software Engineer",
    "full stack engineer": "Software Engineer",
    "full-stack engineer": "Software Engineer",
    "fullstack engineer": "Software Engineer",
    "full stack web developer": "Software Engineer",
    "full stack .net developer": "Software Engineer",
    "full stack java developer": "Software Engineer",
    "full stack python developer": "Software Engineer",
    "full stack python (aws) developer": "Software Engineer",
    "java full stack developer": "Software Engineer",
    "java fullstack developer": "Software Engineer",
    "sr. full stack java developer": "Software Engineer",
    "sr .net developer": "Software Engineer",

    # ── Frontend / Backend / Web ──
    "frontend developer": "Software Engineer",
    "front-end developer": "Software Engineer",
    "front end developer": "Software Engineer",
    "front end software engineer": "Software Engineer",
    "frontend software engineer": "Software Engineer",
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
    "financial analyst": "Finance / Accounting",
    "it analyst": "Data Analyst",
    "analyst": "Data Analyst",

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
    "infrastructure / cloud engineer": "DevOps Engineer",
    "infrastructure engineer": "DevOps Engineer",
    "solutions architect": "Solutions Architect",
    "solution advisor": "Consultant",
    "jr. system architect": "Solutions Architect",
    "systems administrator": "Systems Administrator",
    "system administrator": "Systems Administrator",
    "sys admin": "Systems Administrator",
    "sysadmin": "Systems Administrator",
    "network engineer": "Network Engineer",
    "network administrator": "Network Engineer",

    # ── Systems Engineering ──
    "system engineer": "Systems Engineer",
    "systems engineer": "Systems Engineer",
    "systems engineer hil": "Systems Engineer",
    "technical systems engineer": "Systems Engineer",
    "assistant system design engineer": "Systems Engineer",
    "assistant system engineer": "Systems Engineer",

    # ── Security ──
    "cybersecurity analyst": "Security Analyst",
    "cyber security analyst": "Security Analyst",
    "information security analyst": "Security Analyst",
    "cybersecurity analyst (graduate assistant)": "Security Analyst",
    "security engineer": "Security Engineer",
    "cybersecurity engineer": "Security Engineer",
    "sr. cyber security engineer": "Security Engineer",
    "security analyst": "Security Analyst",

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
    "quality supervisor": "Manager",

    # ── Engineering (general, mechanical, civil, etc.) ──
    "engineer": "Engineer",
    "engineer i": "Engineer",
    "engineer ii": "Engineer",
    "associate engineer": "Engineer",
    "mechanical engineer": "Mechanical Engineer",
    "mechanical design engineer": "Mechanical Engineer",
    "mechanical engineering intern": "Mechanical Engineer",
    "mechanical development engineer": "Mechanical Engineer",
    "development engineer - mechanical": "Mechanical Engineer",
    "civil engineer": "Civil Engineer",
    "civil enginnering co-op/intern": "Civil Engineer",
    "structural engineer": "Civil Engineer",
    "site engineer": "Site Engineer",
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
    "general manager / manufacturing engineer": "Manager",
    "intermediate professional, electrical engineering": "Mechanical Engineer",
    "assistant engineer": "Engineer",
    "engineering co-op": "Intern",
    "staff engineer": "Mechanical Engineer",
    "principal engineer": "Mechanical Engineer",
    "electrical engineer": "Mechanical Engineer",
    "automation engineer": "Mechanical Engineer",

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
    "managing director": "Director",
    "general manager": "Manager",
    "laboratory safety manager": "Manager",
    "manager - innovation": "Manager",
    "leader": "Manager",
    "team lead of consulting practices": "Manager",
    "team leader of software development": "Manager",
    "town manager": "Manager",
    "computing supervisor": "Manager",
    "retail manager": "Manager",
    "real estate / retail manager": "Manager",
    "patient access manager": "Manager",
    "vp of sales": "VP",
    "vp sales executive": "VP",
    "president": "President",
    "executive": "Executive",
    "property manager": "Manager",

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
    "marketing consultant": "Marketing",
    "solution advisor": "Consultant",
    "technical architect": "Software Engineer",
    "technology summer analyst": "Operations",
    "senior analyst": "Data Analyst",
    "it systems analyst": "Data Analyst",
    "it support agent": "Customer Service",
    "information technology support engineer": "Customer Service",
    "information technology intern": "Student",
    "it specialist": "Software Engineer",
    "it support specialist": "Customer Service",
    "it support": "Customer Service",
    "it": "Software Engineer",
    "technical support engineer": "Customer Service",
    "help desk technician": "Customer Service",
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
    "graduate researcher": "Graduate Assistant",
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
    "program assistant for youth programs": "Student",
    "graduate student": "Student",
    "member": "Student",
    "team member": "Student",
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

    # ── Design & Creative ──
    "ux designer": "UI/UX Designer",
    "ui designer": "UI/UX Designer",
    "ui/ux designer": "UI/UX Designer",
    "ux/ui designer": "UI/UX Designer",
    "product designer": "Designer",
    "graphic designer": "Designer",
    "creative & art design": "Designer",
    "3d asset and level designer": "Designer",
    "bim coordinator": "Coordinator",
    "descriptive metadata writer": "Writer",
    "technical writer": "Writer",
    "graphic design intern": "Designer",
    "digital personal shopper": "Designer",
    "development executive": "Designer",
    "special effects design": "Designer",
    "design": "Designer",
    "architectural designer": "Designer",

    # ── Sales ──
    "sales associate": "Sales",
    "seasonal sales associate": "Sales",
    "retail business development representative": "Sales",
    "account executive": "Sales",
    "sales executive": "Sales",
    "retail specialist": "Sales",

    "executive assistant": "Executive Assistant",
    "senior executive assistant": "Executive Assistant",

    # ── Human Resources ──
    "recruiter": "Human Resources",
    "latam techincal billingual recruiter": "Human Resources",
    "talent advisor": "Human Resources",
    "technical bilingual recruiter": "Human Resources",
    "human resources generalist": "Human Resources",
    "head of human resources": "Human Resources",
    "hr manager": "Human Resources",

    # ── Finance / Accounting ──
    "accountant": "Finance / Accounting",
    "senior accountant": "Finance / Accounting",
    "sr accountant": "Finance / Accounting",
    "sr. accountant": "Finance / Accounting",
    "financial solutions advisor": "Finance / Accounting",
    "financial advisor": "Finance / Accounting",
    "financial reporting accountant": "Finance / Accounting",
    "financial analyst": "Finance / Accounting",
    "personal banker": "Finance / Accounting",
    "title clerk": "Finance / Accounting",
    "due diligence associate": "Finance / Accounting",
    "assistant vice president - financial solutions advisor": "VP",
    "assistant vice president": "VP",

    # ── Customer Service ──
    "client support associate": "Customer Service",
    "customer service representative": "Customer Service",
    "client service specialist - employee benefits": "Customer Service",
    "customer support": "Customer Service",
    "front desk staff": "Customer Service",
    "front office supervisor": "Customer Service",
    "pharmacy technician": "Customer Service",
    "cashier": "Customer Service",
    "crew member": "Customer Service",
    "server trainer": "Customer Service",
    "dishwasher": "Customer Service",
    "flight attendant": "Customer Service",

    # ── Marketing ──
    "social media and search engine evaluator": "Marketing",
    "marketing specialist": "Marketing",
    "marketing consultant": "Marketing",
    "marketing manager": "Marketing",

    # ── Operations ──
    "operations analyst": "Operations",
    "operations officer": "Operations",
    "technology officer": "Operations",
    "drug safety associate": "Operations",
    "product specialist": "Operations",
    "administrative": "Operations",
    "administrative assistant": "Operations",
    "admin assistant": "Operations",
    "professional": "Operations",
    "intermediate professional": "Mechanical Engineer",

    # ── Construction & Field ──
    "field project coordinator": "Project Manager",

    # ── Operations & Other ──
    "retail manager": "Manager",
    "real estate / retail manager": "Manager",
    "credentialing coordinator": "Manager",
    "office staff": "Manager",
    "warehouse team member": "Manager",
    "job coach": "Manager",
    "gymnastics coach": "Manager",
    "coach": "Manager",
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
    "Engineer",
    "Mechanical Engineer",
    "Civil Engineer",
    "Field Engineer",
    "Project Engineer",
    "Systems Engineer",
    "Security Engineer",
    "Security Analyst",
    "Solutions Architect",
    "Researcher",
    "Graduate Assistant",
    "Student",
    "Project Manager",
    "Application Engineer",
    "Database Administrator",
    "Systems Administrator",
    "Network Engineer",
    "UI/UX Designer",
    "Designer",
    "Writer",
    "Coordinator",
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
    "President",
    "Executive",
    "Director",
    "Professor",
    "Doctoral Candidate",
    "Postdoctoral Researcher",
    "Operations",
    "Executive Assistant",
    "Ambassador",
    "Peer Mentor",
    "Technician",
    "Intern",
]

# Quick set for O(1) membership checks
_PREFERRED_BUCKETS_SET = {b.casefold() for b in _PREFERRED_TITLE_BUCKETS}

# Stale titles that may exist in the DB from before the consolidation.
# If Groq matches against one of these, remap to the correct bucket.
_STALE_TITLE_REMAP = {
    "accountant": "Finance / Accounting",
    "associate": "Operations",
    "software developer": "Software Engineer",
    "backend engineer": "Software Engineer",
    "frontend engineer": "Software Engineer",
    "systems engineer": "Systems Engineer",
    "security engineer": "Security Engineer",
    "cybersecurity analyst": "Security Analyst",
    "qa engineer": "Software Engineer",
    "quality engineer": "Software Engineer",
    "cloud engineer": "DevOps Engineer",
    "infrastructure / cloud engineer": "DevOps Engineer",
    "business analyst": "Data Analyst",
    "product analyst": "Data Analyst",
    "financial analyst": "Finance / Accounting",
    "financial advisor": "Finance / Accounting",
    "managing director": "Director",
    "operations manager": "Manager",
    "retail manager": "Manager",
    "sales associate": "Sales",
    "seasonal sales associate": "Sales",
    "account executive": "Sales",
    "recruiter": "Human Resources",
    "graphic designer": "Designer",
    "marketing specialist": "Marketing",
    "marketing consultant": "Marketing",
    "cashier": "Customer Service",
    "crew member": "Customer Service",
    "customer support": "Customer Service",
    "front desk staff": "Customer Service",
    "it support": "Customer Service",
    "graduate student": "Student",
    "student worker": "Student",
    "student leader": "Student",
    "member": "Student",
    "team member": "Student",
    "community assistant": "Student",
    "graduate researcher": "Graduate Assistant",
    "design engineer": "Mechanical Engineer",
    "manufacturing engineer": "Mechanical Engineer",
    "industrial engineer": "Mechanical Engineer",
    "electrical engineer": "Mechanical Engineer",
    "automation engineer": "Mechanical Engineer",
    "engineering technician": "Mechanical Engineer",
    "project coordinator": "Project Manager",
    "operations analyst": "Operations",
}


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
            "electrical", "automation", "process", "solutions", "enterprise",
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
    if re.search(r"\b(executive vice president|vice president|vp|ciso|cio|evp|svp)\b", low):
        return "VP"
    if re.search(r"\bpresident\b", low) and not re.search(r"\bvice\s+president\b", low):
        return "President"
    if low == "executive":
        return "Executive"

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

    if re.search(
        r"\b(solutions?\s+architect|enterprise\s+architect|technical\s+architect|software\s+architect|"
        r"application\s+architect|cloud\s+solutions?\s+architect)\b",
        low,
    ):
        return "Solutions Architect"
    if re.search(r"\b(security\s+engineer|cyber\s*security\s+engineer)\b", low):
        return "Security Engineer"
    if re.search(r"\b(systems?\s+engineer)\b", low):
        return "Systems Engineer"
    if re.search(r"\b(cyber\s*security|information\s+security)\b", low) and re.search(r"\b(analyst)\b", low):
        return "Security Analyst"

    if re.search(
        r"\b(software|frontend|front[\s-]?end|backend|back[\s-]?end|fullstack|full[\s-]?stack|"
        r"web developer|mainframe|syteline|sdet|qa\b|quality assurance|test engineer)\b",
        low,
    ):
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
    if re.search(r"\b(ui/ux|ux/ui|ux designer|ui designer)\b", low):
        return "UI/UX Designer"
    if re.search(r"\b(technical writer|metadata writer|copywriter)\b", low):
        return "Writer"
    if re.search(
        r"\b(graphic|product designer|ux\b|ui\b|\bdesigner\b|3d asset|level designer|creative\s*&\s*art|"
        r"special effects design|architectural designer)\b",
        low,
    ) and not re.search(r"\bengineer\b", low):
        return "Designer"
    if re.search(r"\b(marketing|digital personal|game design)\b", low):
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

    # 7b. Generic engineer with no stronger domain hint
    if re.search(r"\bengineer\b", low) and not re.search(
        r"\b(software|frontend|backend|full\s*stack|web|java|python|data|devops|sre|cloud|network|systems|"
        r"security|mechanical|civil|electrical|manufacturing|industrial|field|project|biomedical|piping|controls|"
        r"automation|design|solutions|enterprise|application|quality|test|ai|ml|database|infrastructure)\b",
        low,
    ):
        return "Engineer"

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
    r'\s+(?:intern|trainee|co-op)$',
    re.IGNORECASE,
)
# Well-known company/product names that should be stripped from titles
_COMPANY_PREFIX = re.compile(
    r'^(?:workday|salesforce|oracle|sap|aws|azure|google|apple|microsoft|ibm|cisco|vmware|dell|hp|hpe|adobe|servicenow|databricks|snowflake|terraform|kubernetes|docker|jenkins|jira|confluence|slack|zoom|stripe|twilio|shopify|hubspot|zendesk|pega|veeva|netsuite|epicor|syteline|infor|dynamics|marketo|eloqua|pardot|concur|ariba|coupa|anaplan|tableau|powerbi|looker|qlik|alteryx|informatica|talend|mulesoft|boomi|apigee|kong|nginx|redis|mongodb|elasticsearch|kafka|spark|hadoop|airflow|dbt|fivetran|segment|amplitude|mixpanel|heap|datadog|splunk|new relic|dynatrace|pagerduty|opsgenie|grafana|prometheus|nagios|zabbix)\s+',
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
    t = re.sub(r"\bdevelope\b", "developer", t, flags=re.I)
    t = re.sub(r"\bdevelopor\b", "developer", t, flags=re.I)
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
    # Guard against legitimate domain titles like "Data Scientist".
    m_simple = re.match(
        r"^([A-Za-z0-9&.+'-]+)\s+(Engineer|Developer|Analyst|Manager|Consultant|Architect|Scientist|Designer|Technician|Administrator|Specialist)$",
        t,
        re.IGNORECASE,
    )
    if m_simple:
        prefix = m_simple.group(1).casefold()
        non_company_prefixes = {
            "software", "data", "devops", "cloud", "network", "systems", "mechanical",
            "civil", "project", "research", "security", "quality", "product", "application",
            "database", "business", "financial", "operations", "marketing", "sales",
            "customer", "executive", "technical", "human", "information", "technology",
            "machine", "ai", "ml",
        }
        if prefix not in non_company_prefixes:
            return m_simple.group(2)

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
# GROQ-FIRST SESSION STATE (taxonomy review + metrics)
# ---------------------------------------------------------------------------

NEW_GROQ_TITLES: set[str] = set()
_TITLE_NORM_SESSION_STATS: dict[str, int] = {
    "groq_calls": 0,
    "groq_http_errors": 0,
    "groq_invalid_json": 0,
    "groq_invalid_output": 0,
    "groq_retries": 0,
    "deterministic_fallbacks": 0,
    "groq_accepted": 0,
    "new_titles_recorded": 0,
}


def reset_title_normalization_session_counters() -> None:
    """Clear per-scrape-run taxonomy review set and diagnostic counters."""
    global NEW_GROQ_TITLES, _TITLE_NORM_SESSION_STATS
    NEW_GROQ_TITLES.clear()
    for k in _TITLE_NORM_SESSION_STATS:
        _TITLE_NORM_SESSION_STATS[k] = 0


def get_title_normalization_session_stats() -> dict[str, int]:
    return dict(_TITLE_NORM_SESSION_STATS)


def _bump_stat(key: str, n: int = 1) -> None:
    _TITLE_NORM_SESSION_STATS[key] = _TITLE_NORM_SESSION_STATS.get(key, 0) + n


def export_new_groq_titles_session_summary(output_dir: Path | None = None) -> None:
    """
    Log and append NEW_GROQ_TITLES discovered this run (for manual taxonomy review).
    Safe to call even when the set is empty.
    """
    stats = get_title_normalization_session_stats()
    logger.info(
        "Job title normalization session stats: groq_calls=%s accepted=%s invalid_output=%s "
        "http_errors=%s bad_json=%s retries=%s deterministic_fallbacks=%s new_titles_recorded=%s",
        stats.get("groq_calls", 0),
        stats.get("groq_accepted", 0),
        stats.get("groq_invalid_output", 0),
        stats.get("groq_http_errors", 0),
        stats.get("groq_invalid_json", 0),
        stats.get("groq_retries", 0),
        stats.get("deterministic_fallbacks", 0),
        stats.get("new_titles_recorded", 0),
    )
    if not NEW_GROQ_TITLES:
        logger.info("NEW_GROQ_TITLES (this run): (none)")
        return
    lines = sorted(NEW_GROQ_TITLES)
    logger.info("NEW_GROQ_TITLES (this run, %s entries):\n%s", len(lines), "\n".join(f"  - {t}" for t in lines))
    base = output_dir or (Path(__file__).resolve().parent / "output")
    try:
        base.mkdir(parents=True, exist_ok=True)
        out_path = base / "new_groq_titles_last_run.txt"
        out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        logger.info("Wrote NEW_GROQ_TITLES list to %s", out_path)
    except OSError as exc:
        logger.warning("Could not write NEW_GROQ_TITLES file: %s", exc)


# ---------------------------------------------------------------------------
# GROQ CLIENT + PROMPTS
# ---------------------------------------------------------------------------

GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
_groq_client = None


def _groq_api_key() -> str:
    return (os.getenv("GROQ_API_KEY") or "").strip()


def _get_groq_client():
    """Lazy-init Groq client (re-reads API key from environment)."""
    global _groq_client
    if not _groq_api_key():
        return None
    if _groq_client is not None:
        return _groq_client
    try:
        from groq import Groq

        _groq_client = Groq(api_key=_groq_api_key())
        return _groq_client
    except ImportError:
        logger.warning("groq package not installed — Groq normalization disabled")
        return None
    except Exception as e:
        logger.error("Failed to init Groq client: %s", e)
        return None


def is_groq_configured_for_titles() -> bool:
    return bool(_groq_api_key())


_VAGUE_TITLE_FOLD = frozenset(
    {
        "other",
        "unknown",
        "n/a",
        "na",
        "none",
        "null",
        "misc",
        "miscellaneous",
        "various",
        "tbd",
        "unspecified",
        "general",
        "untitled",
        "title",
        "role",
        "position",
    }
)

_LEAKAGE_HINT = re.compile(
    r"(\bat\s+[A-Za-z][A-Za-z\s'.-]{2,40}\b|,\s*(texas|california|usa|u\.s\.a\.|united states|"
    r"india|canada|uk|china|mexico|florida|new york)\b|\b(inc\.?|llc|ltd\.?|corp\.?|corporation|company)\b|"
    r"\bremote\b|\bhybrid\b|\bon-?site\b)",
    re.IGNORECASE,
)


def _word_count_title(text: str) -> int:
    t = (text or "").strip()
    if not t:
        return 0
    return len(t.split())


def _maybe_compact_executive_title(text: str) -> str | None:
    """
    If Groq returned a long VP-style string, compact to the controlled VP bucket.
    """
    low = (text or "").casefold()
    if re.search(r"\bvice\s+president\b", low) or re.search(r"\bvp\b", low):
        return "VP"
    if re.search(r"\bpresident\b", low) and not re.search(r"\bvice\s+president\b", low):
        return "President"
    return None


def _truncate_to_max_words(text: str, max_words: int = 4) -> str:
    parts = (text or "").strip().split()
    if len(parts) <= max_words:
        return (text or "").strip()
    return " ".join(parts[:max_words]).strip()


def _is_trivial_raw_skip_groq(cleaned: str) -> bool:
    """Noise titles that should not consume a Groq call."""
    low = (cleaned or "").strip().casefold()
    if not low or len(low) <= 1:
        return True
    return low in _VAGUE_TITLE_FOLD or low in {"-", "--", "—", "…"}


def _deterministic_strong_match(raw_title: str) -> str:
    """
    Return a deterministic bucket for obvious/known titles (exact map hit).
    Used to avoid unnecessary Groq calls and terminal noise on low-ambiguity cases.
    """
    cleaned = _cleanup_title(raw_title or "")
    if not cleaned:
        return ""
    mapped = TITLE_MAP.get(cleaned.casefold())
    if not mapped:
        return ""
    return _compact_normalized_title(mapped, raw_title=cleaned).strip()


def _record_new_groq_title_if_needed(canonical: str) -> None:
    if not canonical:
        return
    if canonical.casefold() not in _PREFERRED_BUCKETS_SET:
        before = len(NEW_GROQ_TITLES)
        NEW_GROQ_TITLES.add(canonical)
        if len(NEW_GROQ_TITLES) > before:
            _bump_stat("new_titles_recorded")


def _coerce_existing_title_choice(candidate: str, existing_titles: list[str]) -> str:
    if not candidate or not isinstance(candidate, str):
        return ""
    cleaned = re.sub(r"\s+", " ", candidate).strip().strip('"\'')
    cleaned = re.sub(r"\s*[|:;,.!?]+\s*$", "", cleaned).strip()
    if not cleaned:
        return ""
    existing_map: dict[str, str] = {}
    for title in existing_titles or []:
        if isinstance(title, str) and title.strip():
            existing_map[title.strip().casefold()] = title.strip()
    match = existing_map.get(cleaned.casefold())
    return match if match else cleaned


def _merged_prompt_titles(existing_titles: list[str], limit: int = 200) -> list[str]:
    seen: set[str] = set()
    merged: list[str] = []
    for t in list(_PREFERRED_TITLE_BUCKETS) + list(existing_titles or []):
        if not isinstance(t, str):
            continue
        s = t.strip()
        if not s:
            continue
        k = s.casefold()
        if k in seen:
            continue
        seen.add(k)
        merged.append(s)
        if len(merged) >= limit:
            break
    return merged


def _is_vague_title_fold(s: str) -> bool:
    fold = (s or "").strip().casefold()
    return not fold or fold in _VAGUE_TITLE_FOLD


def _validate_groq_normalized_title(
    raw_title: str,
    title: str,
    match_type: str,
    existing_fold: set[str],
) -> tuple[bool, str, str]:
    """
    Enforce ontology + length + hygiene in code (not in the prompt alone).
    Returns (ok, reason, final_title).
    """
    mt = (match_type or "").strip().lower()
    if mt not in {"existing", "new", "empty"}:
        return False, f"bad_match_type:{match_type!r}", ""

    t = (title or "").strip()
    t = _cleanup_title(t)
    t = _strip_company_prefix_from_role(t)

    if mt == "empty":
        ok = not t
        return ok, "empty_ok" if ok else "empty_with_text", ""

    if not t:
        return False, "empty_title", ""

    if _is_vague_title_fold(t):
        return False, "vague", ""

    if "\n" in t or "\t" in t or len(t) > 96:
        return False, "format", ""

    if _looks_like_location_only_title(t) or _LEAKAGE_HINT.search(t):
        return False, "location_or_company_leakage", ""

    wc = _word_count_title(t)
    if wc > 4:
        compact = _maybe_compact_executive_title(t)
        if compact and _word_count_title(compact) <= 4:
            t = compact
            wc = _word_count_title(t)
        else:
            t2 = _truncate_to_max_words(t, 4)
            if _word_count_title(t2) <= 4 and t2 and not _is_vague_title_fold(t2):
                t = t2
                wc = _word_count_title(t)
        if wc > 4:
            return False, "too_many_words", ""

    # If Groq returns a near-miss label outside ontology (e.g., "Systems Analyst"),
    # attempt canonical compaction into a preferred bucket before rejecting.
    fold = t.casefold()
    in_preferred = fold in _PREFERRED_BUCKETS_SET
    in_existing = fold in existing_fold
    if not in_preferred and not in_existing:
        compact = _compact_normalized_title(t, raw_title=raw_title).strip()
        if compact:
            compact_fold = compact.casefold()
            compact_wc = _word_count_title(compact)
            if compact_wc <= 4 and (
                compact_fold in _PREFERRED_BUCKETS_SET or compact_fold in existing_fold
            ):
                t = compact
                wc = compact_wc
                fold = compact_fold
                in_preferred = fold in _PREFERRED_BUCKETS_SET
                in_existing = fold in existing_fold

    if not in_preferred and not in_existing:
        # Permit concise, clean novel titles even if the model labeled them
        # as "existing". This keeps taxonomy evolution possible while still
        # enforcing hard safety/format constraints in code.
        if wc > 4:
            return False, "new_title_too_long", ""
        return True, "accepted_new_title", t.strip()

    return True, "ok", t.strip()


def _apply_stale_remap(title: str) -> str:
    if not title:
        return title
    return _STALE_TITLE_REMAP.get(title.casefold(), title)


def _build_groq_user_prompt(raw_text: str, merged_titles: list[str], strict: bool) -> str:
    titles_list = "\n".join(f"- {t}" for t in merged_titles)
    raw_trim = (raw_text or "").strip()[:220]
    preferred_list = "\n".join(f"- {t}" for t in _PREFERRED_TITLE_BUCKETS)
    strict_extra = ""
    if strict:
        strict_extra = (
            "\nSTRICT MODE: Output must be max 4 words. Must be exactly one preferred bucket string "
            "when possible. No punctuation except slashes in known buckets (e.g. AI / ML Engineer). "
            "match_type must be 'existing' or 'empty' unless you are 100% sure a new 2-4 word function title is required.\n"
        )
    return f"""You are a job-title normalization engine.

Task: map the raw LinkedIn job title to ONE short standardized label.

Canonical buckets (prefer exact spelling when semantically equivalent):
{preferred_list}

Also consider these known normalized titles from the database (exact string match when equivalent):
{titles_list}

Rules (summary):
- Software-focused IC roles → Software Engineer when appropriate; keep Project Manager, Director, VP, CEO, CTO, etc. distinct.
- AI/ML-focused engineering → AI / ML Engineer; data modeling/research → Data Scientist; pipelines/warehousing → Data Engineer.
- Students / interns / graduate assistants / researchers → Student, Intern, Graduate Assistant, Researcher as appropriate.
- Never put company names, cities, regions, or departments in normalized_title.
- If the raw title is meaningless noise, return normalized_title "" and match_type "empty".
{strict_extra}
Raw title: "{raw_trim}"

Return JSON ONLY with keys normalized_title (string) and match_type (one of: existing, new, empty)."""


def _call_groq_title_json(prompt: str) -> dict[str, Any] | None:
    client = _get_groq_client()
    if not client:
        return None
    _bump_stat("groq_calls")
    try:
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": 'Reply with JSON only: {"normalized_title": string, "match_type": "existing"|"new"|"empty"}. No other keys.',
                },
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=96,
        )
        raw = response.choices[0].message.content or ""
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        _bump_stat("groq_invalid_json")
        logger.warning("Groq title JSON parse failed: %s", exc)
        return None
    except Exception as exc:
        _bump_stat("groq_http_errors")
        logger.error("Groq title normalization request failed: %s", exc)
        return None


def _groq_classify_once(raw_title: str, merged_titles: list[str], strict: bool) -> tuple[str, str] | None:
    prompt = _build_groq_user_prompt(raw_title, merged_titles, strict=strict)
    payload = _call_groq_title_json(prompt)
    if not isinstance(payload, dict):
        return None
    nt = payload.get("normalized_title", "")
    mt = payload.get("match_type", "")
    if not isinstance(nt, str):
        nt = str(nt) if nt is not None else ""
    if not isinstance(mt, str):
        mt = str(mt) if mt is not None else ""
    return nt, mt


def groq_normalize_title_validated(raw_title: str, existing_titles: list[str]) -> str | None:
    """
    Groq-first classification with code-side validation and a single strict retry.

    Returns:
        Accepted normalized title string, or None to signal deterministic fallback.
    """
    if not _groq_api_key() or _get_groq_client() is None:
        return None

    merged = _merged_prompt_titles(existing_titles)
    existing_fold = {str(x).strip().casefold() for x in merged if str(x).strip()}

    last_invalid: tuple[int, str, str, str] | None = None
    for attempt, strict in enumerate((False, True)):
        if attempt == 1:
            _bump_stat("groq_retries")
        parsed = _groq_classify_once(raw_title, merged, strict=strict)
        if not parsed:
            continue
        nt_raw, mt_raw = parsed
        coerced = _coerce_existing_title_choice(nt_raw, merged)
        coerced = _apply_stale_remap(coerced)
        ok, reason, final_t = _validate_groq_normalized_title(raw_title, coerced, mt_raw, existing_fold)
        if ok:
            final = _cleanup_title(final_t).strip()
            final = _strip_company_prefix_from_role(final)
            if mt_raw.strip().lower() == "empty" or not final:
                _bump_stat("groq_accepted")
                return ""
            _bump_stat("groq_accepted")
            if final.casefold() not in _PREFERRED_BUCKETS_SET:
                _record_new_groq_title_if_needed(final)
            return final
        _bump_stat("groq_invalid_output")
        last_invalid = (attempt + 1, coerced, mt_raw, reason)
        logger.debug(
            "Groq title candidate rejected (attempt %s/%s) for raw=%r title=%r match_type=%r reason=%s",
            attempt + 1,
            2,
            raw_title,
            coerced,
            mt_raw,
            reason,
        )
    if last_invalid:
        attempt_n, coerced, mt_raw, reason = last_invalid
        logger.warning(
            "Invalid Groq job title output after retry (last_attempt=%s/%s) for raw=%r title=%r match_type=%r reason=%s",
            attempt_n,
            2,
            raw_title,
            coerced,
            mt_raw,
            reason,
        )
    return None


def normalize_title_with_groq(raw_title: str, existing_titles: list) -> str:
    """
    Back-compat wrapper: Groq-first with deterministic fallback.
    """
    g = groq_normalize_title_validated(raw_title, list(existing_titles or []))
    if g is not None:
        return g
    _bump_stat("deterministic_fallbacks")
    return normalize_title_deterministic(raw_title)


def resolve_title_for_scrape(raw_title: str, extra_existing: list[str] | None = None) -> str:
    """
    Resolve a display / CSV normalized title during scraping (no DB connection).
    Uses Groq-first when configured; merges optional extra labels (e.g. JSON lookup values).
    """
    raw = (raw_title or "").strip()
    if not raw:
        return ""
    cleaned = _cleanup_title(raw)
    if not cleaned:
        return ""

    strong = _deterministic_strong_match(raw)
    if strong:
        return strong

    merged_extra = list(extra_existing or [])
    if _groq_api_key() and _get_groq_client() is not None and not _is_trivial_raw_skip_groq(cleaned):
        g = groq_normalize_title_validated(raw, merged_extra)
        if g is not None:
            if not g.strip():
                return ""
            return g.strip()
        _bump_stat("deterministic_fallbacks")
    else:
        if not _groq_api_key():
            logger.debug("GROQ_API_KEY unset — deterministic job title normalization for scrape path")
        _bump_stat("deterministic_fallbacks")
    out = normalize_title_deterministic(raw_title) or ""
    return out.strip()


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


def get_or_create_normalized_title(conn, raw_title: str, use_groq: bool = True) -> int | None:
    """
    Returns the normalized_job_title_id for a given raw title.

    When use_groq is True and Groq is configured, Groq is consulted first; deterministic
    normalization is only used if Groq is unavailable or fails validation (never overrides
    a validated Groq result).

    Args:
        conn:      Active DB connection (MySQL or SQLite-wrapped).
        raw_title: The raw job title string.
        use_groq:  When False, use deterministic normalization only (for migrations / tests).

    Returns:
        Integer ID from normalized_job_titles, or None on failure / empty title.
    """
    if not raw_title or not raw_title.strip():
        return None

    cleaned = _cleanup_title(raw_title)
    if not cleaned:
        return None

    norm: str | None = None
    strong = _deterministic_strong_match(raw_title)
    if strong:
        norm = strong
    use_llm = bool(use_groq) and bool(_groq_api_key()) and _get_groq_client() is not None

    if norm is None and use_llm and not _is_trivial_raw_skip_groq(cleaned):
        existing_rows = get_all_normalized_titles(conn)
        existing_titles = [r["normalized_title"] for r in existing_rows if r.get("normalized_title")]
        norm = groq_normalize_title_validated(raw_title, existing_titles)

    if norm is None:
        if use_groq:
            _bump_stat("deterministic_fallbacks")
        norm = normalize_title_deterministic(raw_title)
    elif norm == "":
        return None

    if not norm or not str(norm).strip():
        return None

    try:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    "INSERT INTO normalized_job_titles (normalized_title) VALUES (%s) "
                    "ON DUPLICATE KEY UPDATE normalized_title = VALUES(normalized_title)",
                    (norm,),
                )
            except Exception:
                cur.execute(
                    "INSERT OR IGNORE INTO normalized_job_titles (normalized_title) VALUES (?)",
                    (norm,),
                )

            try:
                cur.execute(
                    "SELECT id FROM normalized_job_titles WHERE normalized_title = %s",
                    (norm,),
                )
            except Exception:
                cur.execute(
                    "SELECT id FROM normalized_job_titles WHERE normalized_title = ?",
                    (norm,),
                )

            row = cur.fetchone()
            if row:
                return row["id"] if isinstance(row, dict) else row[0]
            return None
    except Exception as e:
        logger.error(f"Error in get_or_create_normalized_title: {e}")
        return None
