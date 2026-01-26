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

# Timeouts & Delays
PAGE_SETTLE_SECONDS = int(os.getenv("PAGE_SETTLE_SECONDS", "0"))
POST_SECTION_WAIT_SECONDS = float(os.getenv("POST_SECTION_WAIT_SECONDS", "0"))
EDU_READY_TIMEOUT_SECONDS = int(os.getenv("EDU_READY_TIMEOUT_SECONDS", "30"))

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

# Columns
VISITED_HISTORY_COLUMNS = ['profile_url', 'saved', 'visited_at', 'update_needed', 'last_db_update']
CSV_COLUMNS = [
    'name', 'headline', 'location',
    'job_title', 'company', 'job_start_date', 'job_end_date',
    'education', 'major', 'school_start_date', 'graduation_year',
    'working_while_studying',
    'profile_url', 'scraped_at'
]