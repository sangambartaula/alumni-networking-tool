"""
Job Title Normalization Module

Maps semantically equivalent job titles to a single standardized category.
Preserves all original raw job title values — only the normalized mapping is stored.

Two strategies:
  1. Deterministic: cleanup + dictionary lookup (fast, offline, no API cost)
  2. Groq-based:    LLM classification against existing normalized titles (used during scraping)

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
    "software engineer": "Software Engineer",
    "software engineer ii": "Software Engineer",
    "software engineer iii": "Software Engineer",
    "software engineer intern": "Software Engineer Intern",
    "software engineering intern": "Software Engineer Intern",
    "software developer intern": "Software Engineer Intern",
    "software developer summer analyst": "Software Engineer Intern",
    "software intern": "Software Engineer Intern",
    "software development intern": "Software Engineer Intern",
    "software development engineer 1": "Software Engineer",
    "software developer(ta)": "Software Engineer",
    "software application developer": "Software Engineer",
    "software associate": "Software Engineer",
    "associate software engineer": "Software Engineer",
    "associate software": "Software Engineer",
    "software systems engineer": "Systems Engineer",
    "sr. software engineer": "Senior Software Engineer",
    "sr software engineer": "Senior Software Engineer",
    "senior software engineer": "Senior Software Engineer",
    "senior software developer": "Senior Software Engineer",
    "sr. software developer": "Senior Software Engineer",
    "sr software developer": "Senior Software Engineer",
    "junior software engineer": "Junior Software Engineer",
    "jr. software engineer": "Junior Software Engineer",
    "jr software engineer": "Junior Software Engineer",
    "junior software developer": "Junior Software Engineer",
    "jr. software developer": "Junior Software Engineer",
    "lead software engineer": "Lead Software Engineer",
    "lead front end software engineer": "Lead Software Engineer",
    "vice president lead software engineer": "Lead Software Engineer",
    "principal software engineer": "Principal Software Engineer",
    "staff software engineer": "Staff Software Engineer",
    "sr. director of software engineering": "Director of Engineering",
    "vp of software engineering": "VP of Engineering",
    "developer": "Software Engineer",
    "application developer": "Software Engineer",
    "application development analyst": "Software Engineer",
    "application engineer": "Software Engineer",
    "associate programmer, it aviator": "Software Engineer",
    "mainframe programmer": "Software Engineer",
    "programmer analyst trainee": "Software Engineer",
    "scientific programmer": "Software Engineer",

    # ── Full Stack ──
    "full stack developer": "Full Stack Engineer",
    "full-stack developer": "Full Stack Engineer",
    "fullstack developer": "Full Stack Engineer",
    "full stack engineer": "Full Stack Engineer",
    "full-stack engineer": "Full Stack Engineer",
    "fullstack engineer": "Full Stack Engineer",
    "full stack web developer": "Full Stack Engineer",
    "full stack .net developer": "Full Stack Engineer",
    "java full stack developer": "Full Stack Engineer",
    "java fullstack developer": "Full Stack Engineer",
    "sr. full stack java developer": "Senior Full Stack Engineer",
    "sr .net developer": "Senior Full Stack Engineer",

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
    "web developer": "Web Developer",
    "web dev": "Web Developer",
    "web application developer": "Web Developer",

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
    "sr. data scientist": "Senior Data Scientist",
    "sr data scientist": "Senior Data Scientist",
    "senior data scientist": "Senior Data Scientist",
    "senior data science consultant": "Senior Data Scientist",
    "data analyst": "Data Analyst",
    "data engineer": "Data Engineer",
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
    "aws cloud practitioner- internship": "Cloud Engineer Intern",
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
    "sr. cyber security engineer": "Senior Security Engineer",
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
    "mechanical engineering intern": "Mechanical Engineer Intern",
    "development engineer - mechanical": "Mechanical Engineer",
    "civil engineer": "Civil Engineer",
    "civil enginnering co-op/intern": "Civil Engineering Intern",
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
    "senior technical engineer": "Senior Engineer",
    "senior materials engineer": "Senior Engineer",
    "automation engineering lead": "Automation Engineer",

    # ── Management & Leadership ──
    "engineering manager": "Engineering Manager",
    "software engineering manager": "Engineering Manager",
    "technical lead": "Technical Lead",
    "tech lead": "Technical Lead",
    "team lead": "Team Lead",
    "project manager": "Project Manager",
    "project manager 2": "Project Manager",
    "senior project manager": "Senior Project Manager",
    "commercial construction project manager": "Project Manager",
    "assistant project manager": "Project Manager",
    "project management coordinator": "Project Manager",
    "project management intern": "Project Manager Intern",
    "program manager": "Program Manager",
    "product manager": "Product Manager",
    "scrum master": "Scrum Master",
    "vp of engineering": "VP of Engineering",
    "vice president of engineering": "VP of Engineering",
    "cto": "Chief Technology Officer",
    "chief technology officer": "Chief Technology Officer",
    "coo": "Chief Operations Officer",
    "chief operations officer": "Chief Operations Officer",
    "director of engineering": "Director of Engineering",
    "manager": "Manager",
    "senior manager": "Senior Manager",
    "regional manager": "Manager",
    "senior manager of field marketing": "Marketing Manager",
    "senior marketing manager": "Marketing Manager",
    "operations manager": "Operations Manager",
    "operations supervisor": "Operations Manager",
    "production manager": "Operations Manager",
    "supply chain manager": "Operations Manager",
    "procurement manager": "Operations Manager",
    "executive vice president of operations": "VP of Operations",
    "director of southwest region": "Director",
    "director of strategic initiatives": "Director",
    "senior director, assurance & enterprise risk management": "Director",
    "partner and executive director": "Director",
    "founder and director": "Director",
    "co-owner & operations head": "Director",
    "owner": "Director",
    "vp of sales": "VP of Sales",

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
    "technology summer analyst": "IT Analyst Intern",
    "senior analyst": "Analyst",
    "it systems analyst": "IT Analyst",
    "computing supervisor": "IT Support",
    "it support agent": "IT Support",
    "information technology support engineer": "IT Support",
    "information technology intern": "IT Intern",
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
    "research assistant": "Research Assistant",
    "undergraduate research assistant": "Research Assistant",
    "undergraduate researcher": "Research Assistant",
    "research associate": "Research Associate",
    "research scientist": "Research Scientist",
    "research engineer": "Research Engineer",
    "researcher": "Research Assistant",
    "research technician": "Research Assistant",
    "research and development chemist": "Research Scientist",
    "postdoctoral researcher": "Postdoctoral Researcher",
    "postdoc": "Postdoctoral Researcher",
    "research assistant/ teaching fellow": "Research Assistant",

    # ── Teaching & Graduate Assistants ──
    "teaching assistant": "Teaching Assistant",
    "teaching assistant l2": "Teaching Assistant",
    "graduate teaching assistant": "Teaching Assistant",
    "graduate research assistant": "Research Assistant",
    "graduate research and teaching assistant": "Teaching Assistant",
    "graduate student assistant": "Graduate Assistant",
    "graduate student at university of north texas": "Graduate Student",
    "enterprise data warehousing teaching assistant": "Teaching Assistant",
    "instructional assistant": "Teaching Assistant",
    "supplemental instructor": "Teaching Assistant",
    "associate professor / director of ammpi": "Professor",
    "professor": "Professor",
    "assistant professor": "Professor",
    "associate professor": "Professor",
    "tutor": "Tutor",

    # ── Student Roles ──
    "student": "Student",
    "student assistant": "Student Worker",
    "student assistant for ms analytics": "Student Worker",
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
    "data science intern": "Data Science Intern",
    "data analyst intern": "Data Analyst Intern",
    "data engineer intern": "Data Engineer Intern",
    "engineering intern": "Engineering Intern",
    "it intern": "IT Intern",
    "web development intern": "Web Developer Intern",
    "web developer intern": "Web Developer Intern",
    "graphic design intern": "Design Intern",
    "architectural intern": "Architecture Intern",
    "administrative assistant intern": "Administrative Intern",
    "sales and marketing intern": "Marketing Intern",

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
# PATTERN-BASED NORMALIZATION
# Applied when exact TITLE_MAP match fails. Strips common modifiers and retries.
# ---------------------------------------------------------------------------

# Regex patterns to strip before re-matching
_LEVEL_SUFFIXES = re.compile(
    r'\s*[-–]\s*(?:level\s*)?\s*(?:i{1,3}|iv|v|[1-5])\s*$', re.IGNORECASE
)
_PAREN_QUALIFIER = re.compile(r'\s*\(.*?\)\s*$')
_SENIORITY_PREFIX = re.compile(
    r'^(?:senior|sr\.?|junior|jr\.?|lead|staff|principal|chief|associate|vice president|vp of)\s+',
    re.IGNORECASE,
)


def _cleanup_title(raw: str) -> str:
    """
    Basic cleanup without mapping:
      - strip whitespace
      - collapse multiple spaces
      - remove trailing punctuation variants
    Returns cleaned string (preserves original case for display).
    """
    if not raw:
        return ""
    t = raw.strip()
    t = re.sub(r'\s+', ' ', t)
    # Remove trailing period, comma, dash
    t = re.sub(r'[.,\-]+$', '', t).strip()
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
    key = cleaned.lower()

    # Pass 1: exact match
    if key in TITLE_MAP:
        return TITLE_MAP[key]

    # Pass 2: strip level suffixes (e.g. "Engineer II" → "Engineer")
    stripped = _LEVEL_SUFFIXES.sub('', key).strip()
    if stripped != key and stripped in TITLE_MAP:
        return TITLE_MAP[stripped]

    # Pass 3: strip parenthetical qualifiers (e.g. "Analyst (Contract)" → "Analyst")
    stripped = _PAREN_QUALIFIER.sub('', key).strip()
    if stripped != key and stripped in TITLE_MAP:
        return TITLE_MAP[stripped]

    # Pass 4: strip both level suffixes and parens
    stripped = _PAREN_QUALIFIER.sub('', _LEVEL_SUFFIXES.sub('', key)).strip()
    if stripped != key and stripped in TITLE_MAP:
        return TITLE_MAP[stripped]

    # No match — return cleaned title as its own category
    return cleaned


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
    titles_list = "\n".join(f"- {t}" for t in existing_titles[:200])  # cap list size

    prompt = f"""You are a job-title normalization engine.

Given a raw job title and a list of existing normalized titles, do ONE of:
1. Return an EXACT match from the existing list if it is semantically equivalent.
2. If no match exists, return a NEW concise standardized title.

Rules:
- Preserve seniority levels (Senior, Junior, Lead, Staff, Principal).
- Use common industry terminology (e.g. "Software Engineer" not "Software Developer").
- Keep it concise: 1-4 words max.
- Return ONLY the normalized title string. No explanation.

Existing normalized titles:
{titles_list}

Raw title: {raw_title}

Normalized title:"""

    try:
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "You output exactly one job title string. No explanation, no quotes, no punctuation."
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=50
        )
        result = response.choices[0].message.content.strip()
        # Sanity: strip quotes
        result = result.strip('"\'')
        if result and len(result) < 100:
            return result
        else:
            logger.warning(f"Groq returned suspicious result: {result!r}")
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
