import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

"""Handles all environment variables, constants, and logger setup."""
# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("LinkedInScraper")

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
    'location', 'working_while_studying', 
    'title', 'company', 'job_start', 'job_end', 
    'exp_2_title', 'exp_2_company', 'exp_2_dates', 
    'exp_3_title', 'exp_3_company', 'exp_3_dates',
    'scraped_at'
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
