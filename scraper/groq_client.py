"""
Shared Groq API client infrastructure.

Provides:
- Groq client initialization and availability checking
- Debug HTML saving (controlled by SCRAPER_DEBUG_HTML env var)
- JSON response parsing with fallbacks
- Common text utilities (doubled-text cleaning, date parsing)

Used by both groq_extractor_experience.py and groq_extractor_education.py.
"""

import os
import json
import re
from pathlib import Path
from datetime import datetime
from config import logger

# Try to import groq and BeautifulSoup
try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False
    logger.warning("⚠️ groq not installed. Run: pip install groq")

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

# Configuration
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_ENABLED = os.getenv("USE_GROQ", "true").lower() == "true"
# Model choice: "llama-3.1-8b-instant" (14.4K RPD) or "llama-3.3-70b-versatile" (1K RPD, better quality)
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

# Debug HTML saving — supports both new and legacy env var names
SCRAPER_DEBUG_HTML = (
    os.getenv("SCRAPER_DEBUG_HTML", "").lower() == "true"
    or os.getenv("DEBUG_SAVE_HTML", "false").lower() == "true"
)
DEBUG_HTML_DIR = Path(__file__).parent / "output" / "debug_html"

# Initialize the client once
_client = None


def _get_client():
    """Lazy initialization of Groq client."""
    global _client
    if _client is None and GROQ_AVAILABLE and GROQ_API_KEY:
        try:
            _client = Groq(api_key=GROQ_API_KEY)
            logger.info(f"✓ Groq API initialized (model: {GROQ_MODEL})")
        except Exception as e:
            logger.error(f"Failed to initialize Groq: {e}")
    return _client


def is_groq_available():
    """Check if Groq API is available and configured."""
    return GROQ_AVAILABLE and GROQ_ENABLED and bool(GROQ_API_KEY)


def save_debug_html(content: str, profile_name: str, section: str = "experience"):
    """
    Save debug content to file if SCRAPER_DEBUG_HTML is enabled.

    Args:
        content: The cleaned text or HTML to save.
        profile_name: Name of the profile (for filename).
        section: Section name, e.g. "experience" or "education".
    """
    if not SCRAPER_DEBUG_HTML:
        return
    try:
        DEBUG_HTML_DIR.mkdir(parents=True, exist_ok=True)
        safe_name = re.sub(r'[^\w\-]', '_', profile_name)[:50]
        timestamp = datetime.now().strftime("%H%M%S")
        ext = ".html" if "<" in content else ".txt"
        debug_file = DEBUG_HTML_DIR / f"{safe_name}_{section}_{timestamp}{ext}"
        debug_file.write_text(content, encoding='utf-8')
        logger.info(f"    📄 Saved debug {section} to: {debug_file.name}")
    except Exception as e:
        # Debug saving must never crash production
        logger.warning(f"    ⚠️ Failed to save debug {section}: {e}")


def parse_groq_json_response(result_text: str) -> dict | list | None:
    """
    Parse JSON from Groq response text with fallbacks.

    Returns:
        Parsed JSON (dict or list), or None on failure.
    """
    try:
        return json.loads(result_text)
    except json.JSONDecodeError:
        # Fallback: try to find a JSON array in the text
        match = re.search(r'\[.*\]', result_text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
        # Try to find a JSON object
        match = re.search(r'\{.*\}', result_text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
        logger.warning(f"⚠️ Groq returned invalid JSON: {result_text[:100]}...")
        return None


def _clean_doubled(text):
    """Clean up doubled text like 'EngineerEngineer' -> 'Engineer'."""
    if not text or len(text) < 4:
        return text
    # Exact duplication "WordWord"
    if len(text) % 2 == 0:
        mid = len(text) // 2
        if text[:mid] == text[mid:]:
            return text[:mid]
    # Duplication with space "Word Word Word Word" -> "Word Word"
    parts = text.split()
    if len(parts) >= 2 and len(parts) % 2 == 0:
        half = len(parts) // 2
        if parts[:half] == parts[half:]:
            return " ".join(parts[:half])
    return text


def parse_groq_date(date_str: str) -> dict:
    """
    Parse a date string like "Oct 2024" or "Present" into the format used by the scraper.

    Returns:
        dict with keys: year (int), month (int or None), is_present (bool)
    """
    if not date_str:
        return None

    date_str = date_str.strip()

    # Check for "Present"
    if date_str.lower() == "present":
        return {"year": 9999, "month": 12, "is_present": True}

    # Month mapping
    month_map = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
    }

    # Try "Mon YYYY" format
    match = re.match(r'([A-Za-z]{3})\s*(\d{4})', date_str)
    if match:
        month_str, year_str = match.groups()
        month = month_map.get(month_str.lower(), None)
        return {"year": int(year_str), "month": month, "is_present": False}

    # Try "YYYY" format
    match = re.match(r'^(\d{4})$', date_str)
    if match:
        return {"year": int(match.group(1)), "month": None, "is_present": False}

    return None


def strip_noise_elements(soup):
    """
    Remove common noise elements from a BeautifulSoup soup object.
    Shared between experience and education HTML cleaning.
    Modifies soup in place.
    """
    # Remove script/style/media
    for tag in soup.find_all(['script', 'style', 'svg', 'img', 'iframe', 'noscript']):
        tag.decompose()
    # Remove visually hidden and show-more elements
    for tag in soup.find_all(class_=lambda x: x and any(
        c in x for c in ['visually-hidden', 'inline-show-more-text', 'artdeco-button__icon']
    )):
        tag.decompose()
