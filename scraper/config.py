import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

"""Handles all environment variables, constants, and logger setup."""

# ── Logging Setup ──────────────────────────────────────────
# Use rich for colored console output, plain for log files
try:
    from rich.logging import RichHandler
    from rich.console import Console
    _console = Console(stderr=True)
    _handler = RichHandler(
        console=_console,
        show_time=False,
        show_path=False,
        markup=True,
        rich_tracebacks=True,
        tracebacks_show_locals=False,
    )
    _handler.setLevel(logging.INFO)
    _handler.setFormatter(logging.Formatter("%(message)s"))
except ImportError:
    _handler = logging.StreamHandler()
    _handler.setLevel(logging.INFO)
    _handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    _console = None

logging.basicConfig(level=logging.DEBUG, handlers=[_handler])
logger = logging.getLogger("LinkedInScraper")
logger.setLevel(logging.DEBUG)  # allow DEBUG through; handler filters to INFO

# Suppress noisy HTTP loggers
for _noisy in ("httpx", "httpcore", "urllib3", "urllib3.connectionpool", "selenium", "filelock"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)


def print_profile_summary(data: dict, token_count: int = 0, status: str = "Saved"):
    """
    Print a clean, colored per-profile summary block.
    Uses rich if available, falls back to plain ANSI.
    """
    name = data.get("name", "Unknown")
    url = data.get("profile_url", "")
    location = data.get("location", "Not Found")
    separator = "━" * 50

    # Experience lines
    exp_raw_lines = []
    exp_std_lines = []
    for idx, suffix in enumerate(["", "2", "3"], start=1):
        title_key = f"exp{suffix}_title" if suffix else "job_title"
        comp_key = f"exp{suffix}_company" if suffix else "company"
        dates_key = f"exp{suffix}_dates" if suffix else None
        title = data.get(title_key, "")
        company = data.get(comp_key, "")
        if not title and not company:
            continue
        if dates_key:
            dates = data.get(dates_key, "")
        else:
            start = data.get("job_start_date", "")
            end = data.get("job_end_date", "")
            dates = f"{start}–{end}" if start or end else ""
        exp_raw_lines.append(f"  • {company} — {title} ({dates})" if dates else f"  • {company} — {title}")

        # Standardized title
        std_title_key = f"normalized_exp{suffix}_title" if suffix else "normalized_job_title"
        std_title = data.get(std_title_key, title)
        # Standardized company
        std_comp_key = f"normalized_exp{suffix}_company" if suffix else "normalized_company"
        std_comp = data.get(std_comp_key, company)
        exp_std_lines.append(f"  • {std_comp} — {std_title} ({dates})" if dates else f"  • {std_comp} — {std_title}")

    # Education lines
    edu_raw_lines = []
    edu_std_lines = []
    for suffix in ("", "2", "3"):
        sch_key = f"school{suffix}" if suffix else "school"
        deg_key = f"degree{suffix}" if suffix else "degree"
        maj_key = f"major{suffix}" if suffix else "major"
        std_deg_key = f"standardized_degree{suffix}" if suffix else "standardized_degree"
        std_maj_key = f"standardized_major{suffix}" if suffix else "standardized_major"
        school = data.get(sch_key, "")
        if not school:
            continue
        degree = data.get(deg_key, "")
        major = data.get(maj_key, "")
        std_deg = data.get(std_deg_key, degree)
        std_maj = data.get(std_maj_key, major)
        edu_raw_lines.append(f"  • {school} — {degree} / {major}" if major else f"  • {school} — {degree}")
        edu_std_lines.append(f"  • {school} — {std_deg} / {std_maj}" if std_maj else f"  • {school} — {std_deg}")

    discipline = data.get("discipline", "Other")
    wws = data.get("working_while_studying", "N/A")

    # Build output using rich if available
    if _console:
        from rich.text import Text
        out = Text()
        out.append(f"\n{separator}\n", style="blue bold")
        out.append(f"Profile: {name}\n", style="blue bold")
        out.append(f"URL: {url}\n", style="blue")
        out.append(f"{separator}\n\n", style="blue bold")

        out.append(f"Location: {location}\n\n", style="white")

        if exp_raw_lines:
            out.append(f"Experience (Raw • {len(exp_raw_lines)})\n", style="cyan bold")
            for line in exp_raw_lines:
                out.append(f"{line}\n", style="white")
            out.append(f"\nExperience (Standardized)\n", style="cyan bold")
            for line in exp_std_lines:
                out.append(f"{line}\n", style="white")
        else:
            out.append("Experience: None found\n", style="yellow")

        out.append("\n")

        if edu_raw_lines:
            out.append(f"Education (Raw • {len(edu_raw_lines)})\n", style="cyan bold")
            for line in edu_raw_lines:
                out.append(f"{line}\n", style="white")
            out.append(f"\nEducation (Standardized)\n", style="cyan bold")
            for line in edu_std_lines:
                out.append(f"{line}\n", style="white")
        else:
            out.append("Education: None found\n", style="yellow")

        out.append(f"\nDiscipline: {discipline}\n", style="cyan bold")
        out.append(f"Working While Studying: {wws.title()}\n\n", style="white")

        if token_count:
            out.append(f"Groq Tokens: {token_count:,}\n\n", style="white")

        out.append(f"✓ Completed — {status}\n", style="green bold")
        _console.print(out)
    else:
        # Plain fallback
        lines = [
            f"\n{separator}",
            f"Profile: {name}",
            f"URL: {url}",
            separator,
            f"",
            f"Location: {location}",
        ]
        if exp_raw_lines:
            lines.append(f"\nExperience (Raw • {len(exp_raw_lines)})")
            lines.extend(exp_raw_lines)
            lines.append(f"\nExperience (Standardized)")
            lines.extend(exp_std_lines)
        else:
            lines.append("\nExperience: None found")
        if edu_raw_lines:
            lines.append(f"\nEducation (Raw • {len(edu_raw_lines)})")
            lines.extend(edu_raw_lines)
            lines.append(f"\nEducation (Standardized)")
            lines.extend(edu_std_lines)
        else:
            lines.append("\nEducation: None found")
        lines.append(f"\nDiscipline: {discipline}")
        lines.append(f"Working While Studying: {wws.title()}")
        if token_count:
            lines.append(f"\nGroq Tokens: {token_count:,}")
        lines.append(f"\n✓ Completed — {status}")
        lines.append(separator)
        print("\n".join(lines))


# Load environment variables
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
logger.info(f"Loading .env from: {env_path}")

# --- Configuration Constants ---
LINKEDIN_EMAIL = os.getenv("LINKEDIN_EMAIL")
LINKEDIN_PASSWORD = os.getenv("LINKEDIN_PASSWORD")

# Validate credentials are set
if not LINKEDIN_EMAIL or not LINKEDIN_PASSWORD:
    logger.warning("⚠️ LINKEDIN_EMAIL or LINKEDIN_PASSWORD not set in environment!")

HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"
TESTING = os.getenv("TESTING", "false").lower() == "true"
USE_COOKIES = os.getenv("USE_COOKIES", "false").lower() == "true"
LINKEDIN_COOKIES_PATH = os.getenv("LINKEDIN_COOKIES_PATH", "linkedin_cookies.json")
SCRAPER_MODE = os.getenv("SCRAPER_MODE", "names").lower()
SCRAPE_RESUME_MAX_AGE_DAYS = int(os.getenv("SCRAPE_RESUME_MAX_AGE_DAYS", "7"))
OUTPUT_CSV_ENV = os.getenv("OUTPUT_CSV", "UNT_Alumni_Data.csv")
UPDATE_FREQUENCY = os.getenv("UPDATE_FREQUENCY", "6 months")
CONNECTIONS_CSV_PATH = os.getenv("CONNECTIONS_CSV", "connections.csv")

# Groq AI Configuration
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
USE_GROQ = os.getenv("USE_GROQ", "true").lower() == "true"
if USE_GROQ and not GROQ_API_KEY:
    logger.warning(
        "⚠️  Groq LLM extraction is DISABLED — no GROQ_API_KEY found.\n"
        "    To enable AI-powered job extraction:\n"
        "    1. Go to https://console.groq.com/keys and create a free API key\n"
        "    2. Add GROQ_API_KEY=gsk_... to your .env file\n"
        "    Falling back to CSS-only extraction."
    )

# Timeouts & Delays
PAGE_SETTLE_SECONDS = int(os.getenv("PAGE_SETTLE_SECONDS", "0"))
POST_SECTION_WAIT_SECONDS = float(os.getenv("POST_SECTION_WAIT_SECONDS", "0"))
EDU_READY_TIMEOUT_SECONDS = int(os.getenv("EDU_READY_TIMEOUT_SECONDS", "30"))

# Flagging Configuration
FLAG_MISSING_GRAD_YEAR = os.getenv("FLAG_MISSING_GRAD_YEAR", "false").lower() == "true"
FLAG_MISSING_DEGREE = os.getenv("FLAG_MISSING_DEGREE", "false").lower() == "true"
FLAG_MISSING_EXPERIENCE_DATA = os.getenv("FLAG_MISSING_EXPERIENCE_DATA", "true").lower() == "true"

if TESTING:
    MIN_DELAY = 15
    MAX_DELAY = 60
else:
    MIN_DELAY = 120
    MAX_DELAY = 600

# Paths
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
OUTPUT_CSV = OUTPUT_DIR / OUTPUT_CSV_ENV
COOKIES_FILE = OUTPUT_DIR / LINKEDIN_COOKIES_PATH
VISITED_HISTORY_FILE = OUTPUT_DIR / "visited_history.csv"
FLAGGED_PROFILES_FILE = OUTPUT_DIR / "flagged_for_review.txt"

# Columns
VISITED_HISTORY_COLUMNS = ['profile_url', 'saved', 'visited_at', 'update_needed', 'last_db_update']
CSV_COLUMNS = [
    'first', 'last', 'linkedin_url', 
    'school', 'degree', 'major', 'school_start', 'grad_year',
    'school2', 'degree2', 'major2',
    'school3', 'degree3', 'major3',
    'standardized_degree', 'standardized_major',
    'standardized_degree2', 'standardized_major2',
    'standardized_degree3', 'standardized_major3',
    'discipline',
    'location', 'working_while_studying', 
    'title', 'company', 'job_start', 'job_end', 
    'exp_2_title', 'exp_2_company', 'exp_2_dates', 
    'exp_3_title', 'exp_3_company', 'exp_3_dates',
    'scraped_at',
    'normalized_job_title', 'normalized_exp2_title', 'normalized_exp3_title'
]

# ── Blocked Profiles ─────────────────────────────────────────
# These LinkedIn slugs are fake / generic placeholder accounts.
# Any matching URL will be:
#   • skipped during scraping
#   • rejected by save_profile_to_csv
#   • auto-removed from the database on startup
BLOCKED_PROFILE_SLUGS = {
    "davidmartinez",
    "emilybrown",
    "jessicawilliams",
    "johnsmith",
    "lisaanderson",
    "moparthisaiashritha",
    "michaelchen",
    "roberttaylor",
    "sarahjohnson",
}


def is_blocked_url(url: str) -> bool:
    """Return True if the LinkedIn URL belongs to a blocked profile."""
    if not url:
        return False
    # Normalize: strip trailing slash, take last path segment
    slug = url.rstrip("/").split("/")[-1].split("#")[0].split("?")[0].lower()
    return slug in BLOCKED_PROFILE_SLUGS
