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
    # Software Engineering
    "software developer": "Software Engineer",
    "software dev": "Software Engineer",
    "sr. software engineer": "Senior Software Engineer",
    "sr software engineer": "Senior Software Engineer",
    "senior software developer": "Senior Software Engineer",
    "sr. software developer": "Senior Software Engineer",
    "sr software developer": "Senior Software Engineer",
    "junior software engineer": "Junior Software Engineer",
    "jr. software engineer": "Junior Software Engineer",
    "jr software engineer": "Junior Software Engineer",
    "junior software developer": "Junior Software Engineer",
    "jr. software developer": "Junior Software Engineer",
    "lead software engineer": "Lead Software Engineer",
    "principal software engineer": "Principal Software Engineer",
    "staff software engineer": "Staff Software Engineer",
    "full stack developer": "Full Stack Engineer",
    "full-stack developer": "Full Stack Engineer",
    "fullstack developer": "Full Stack Engineer",
    "full stack engineer": "Full Stack Engineer",
    "full-stack engineer": "Full Stack Engineer",
    "fullstack engineer": "Full Stack Engineer",
    "backend developer": "Backend Engineer",
    "back-end developer": "Backend Engineer",
    "back end developer": "Backend Engineer",
    "backend engineer": "Backend Engineer",
    "back-end engineer": "Backend Engineer",
    "frontend developer": "Frontend Engineer",
    "front-end developer": "Frontend Engineer",
    "front end developer": "Frontend Engineer",
    "frontend engineer": "Frontend Engineer",
    "front-end engineer": "Frontend Engineer",
    "web developer": "Web Developer",
    "web dev": "Web Developer",

    # Data & AI
    "ai engineer": "AI/ML Engineer",
    "ai software engineer": "AI/ML Engineer",
    "ai/ml engineer": "AI/ML Engineer",
    "ml engineer": "AI/ML Engineer",
    "machine learning engineer": "AI/ML Engineer",
    "artificial intelligence engineer": "AI/ML Engineer",
    "data scientist": "Data Scientist",
    "sr. data scientist": "Senior Data Scientist",
    "sr data scientist": "Senior Data Scientist",
    "senior data scientist": "Senior Data Scientist",
    "data analyst": "Data Analyst",
    "data engineer": "Data Engineer",
    "database administrator": "Database Administrator",
    "dba": "Database Administrator",
    "business analyst": "Business Analyst",
    "business intelligence analyst": "Business Intelligence Analyst",
    "bi analyst": "Business Intelligence Analyst",
    "bi developer": "Business Intelligence Developer",

    # DevOps & Cloud
    "devops engineer": "DevOps Engineer",
    "dev ops engineer": "DevOps Engineer",
    "site reliability engineer": "Site Reliability Engineer",
    "sre": "Site Reliability Engineer",
    "cloud engineer": "Cloud Engineer",
    "cloud architect": "Cloud Architect",
    "systems administrator": "Systems Administrator",
    "system administrator": "Systems Administrator",
    "sys admin": "Systems Administrator",
    "sysadmin": "Systems Administrator",
    "network engineer": "Network Engineer",
    "network administrator": "Network Administrator",

    # Security
    "cybersecurity analyst": "Cybersecurity Analyst",
    "cyber security analyst": "Cybersecurity Analyst",
    "information security analyst": "Cybersecurity Analyst",
    "security engineer": "Security Engineer",
    "cybersecurity engineer": "Security Engineer",
    "security analyst": "Security Analyst",

    # QA & Testing
    "qa engineer": "QA Engineer",
    "quality assurance engineer": "QA Engineer",
    "test engineer": "QA Engineer",
    "software tester": "QA Engineer",
    "sdet": "SDET",
    "software development engineer in test": "SDET",
    "automation engineer": "Automation Engineer",

    # Management & Leadership
    "engineering manager": "Engineering Manager",
    "software engineering manager": "Engineering Manager",
    "technical lead": "Technical Lead",
    "tech lead": "Technical Lead",
    "team lead": "Team Lead",
    "project manager": "Project Manager",
    "program manager": "Program Manager",
    "product manager": "Product Manager",
    "scrum master": "Scrum Master",
    "vp of engineering": "VP of Engineering",
    "vice president of engineering": "VP of Engineering",
    "cto": "Chief Technology Officer",
    "chief technology officer": "Chief Technology Officer",
    "director of engineering": "Director of Engineering",

    # Embedded & Hardware
    "embedded software engineer": "Embedded Software Engineer",
    "embedded systems engineer": "Embedded Systems Engineer",
    "firmware engineer": "Firmware Engineer",
    "hardware engineer": "Hardware Engineer",
    "electrical engineer": "Electrical Engineer",
    "electronics engineer": "Electronics Engineer",
    "pcb designer": "PCB Designer",

    # Mechanical & Civil
    "mechanical engineer": "Mechanical Engineer",
    "civil engineer": "Civil Engineer",
    "structural engineer": "Structural Engineer",
    "piping engineer": "Piping Engineer",
    "process engineer": "Process Engineer",
    "manufacturing engineer": "Manufacturing Engineer",
    "industrial engineer": "Industrial Engineer",
    "quality engineer": "Quality Engineer",

    # Design
    "ux designer": "UX Designer",
    "ui designer": "UI Designer",
    "ui/ux designer": "UI/UX Designer",
    "ux/ui designer": "UI/UX Designer",
    "product designer": "Product Designer",
    "graphic designer": "Graphic Designer",

    # Research & Academic
    "research assistant": "Research Assistant",
    "research associate": "Research Associate",
    "research scientist": "Research Scientist",
    "research engineer": "Research Engineer",
    "teaching assistant": "Teaching Assistant",
    "professor": "Professor",
    "assistant professor": "Assistant Professor",
    "associate professor": "Associate Professor",
    "graduate research assistant": "Graduate Research Assistant",
    "graduate teaching assistant": "Graduate Teaching Assistant",
    "postdoctoral researcher": "Postdoctoral Researcher",
    "postdoc": "Postdoctoral Researcher",

    # Consulting & Other
    "consultant": "Consultant",
    "it consultant": "IT Consultant",
    "technology consultant": "Technology Consultant",
    "solutions architect": "Solutions Architect",
    "technical architect": "Technical Architect",
    "systems engineer": "Systems Engineer",
    "it specialist": "IT Specialist",
    "it support specialist": "IT Support Specialist",
    "technical support engineer": "Technical Support Engineer",
    "help desk technician": "Help Desk Technician",
    "technical writer": "Technical Writer",

    # Intern variants
    "software engineer intern": "Software Engineer Intern",
    "software engineering intern": "Software Engineer Intern",
    "software developer intern": "Software Engineer Intern",
    "software intern": "Software Engineer Intern",
    "data science intern": "Data Science Intern",
    "data analyst intern": "Data Analyst Intern",
    "data engineer intern": "Data Engineer Intern",
    "engineering intern": "Engineering Intern",
    "it intern": "IT Intern",
    "web development intern": "Web Developer Intern",
    "web developer intern": "Web Developer Intern",
}


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
    Deterministic normalization:
      1. Cleanup whitespace & punctuation
      2. Lookup in TITLE_MAP (case-insensitive)
      3. If not found, return the cleaned title as-is (it becomes its own category)

    Returns the normalized title string.
    """
    cleaned = _cleanup_title(raw_title)
    if not cleaned:
        return ""
    key = cleaned.lower()
    return TITLE_MAP.get(key, cleaned)


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
