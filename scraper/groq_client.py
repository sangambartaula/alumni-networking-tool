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

from settings import logger

# Configure Groq SDK HTTP retry backoff (429 / transient errors).
# This logic used to live in scraper/groq_retry_patch.py — inlined here so
# importing this module applies the configured retry delay automatically.
_applied = False


def apply_groq_retry_delay() -> None:
    """Apply environment-configured retry backoff to Groq's internal client.

    Env var: `GROQ_RETRY_DELAY_SECONDS` (default 5). Set to 0 to keep SDK defaults.
    """
    global _applied
    if _applied:
        return
    try:
        import groq._base_client as _bc
    except Exception:
        return
    try:
        sec = float(os.getenv("GROQ_RETRY_DELAY_SECONDS", "5"))
    except Exception:
        sec = 5.0
    if sec > 0:
        try:
            _bc.INITIAL_RETRY_DELAY = sec
            _bc.MAX_RETRY_DELAY = sec
        except Exception:
            # Best-effort: don't crash if attributes not present
            pass
    _applied = True


# Apply retry delay eagerly on import (preserves previous side-effect behavior).
apply_groq_retry_delay()

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

# Debug toggle: saves raw HTML/text sent to Groq. 
# This is crucial for prompt engineering and auditing why the LLM might have
# missed specific data points (e.g., if LinkedIn changed a CSS class).
SCRAPER_DEBUG_HTML = os.getenv("SCRAPER_DEBUG_HTML", "false").lower() == "true"
DEBUG_HTML_DIR = Path(__file__).parent / "output" / "debug_html"
GROQ_ACCURACY_AUDIT_FILE = Path(__file__).parent / "output" / "groq_accuracy_audit.jsonl"

# Initialize the client once
_client = None
_RUN_GROQ_ACCURACY_EVENTS = []

_MONTH_ALIASES = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_MONTH_NUM_TO_ABBR = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}


def _get_client():
    """Lazy initialization of Groq client."""
    global _client
    if _client is None and GROQ_AVAILABLE and GROQ_API_KEY:
        try:
            _client = Groq(api_key=GROQ_API_KEY)
            logger.debug(f"Groq API initialized (model: {GROQ_MODEL})")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Groq: {e}")
    return _client


def is_groq_available():
    """Check if Groq API is available and configured."""
    return GROQ_AVAILABLE and GROQ_ENABLED and bool(GROQ_API_KEY)


def save_debug_html(content: str, profile_name: str, section: str = "experience", force: bool = False):
    """
    Save debug content to file if SCRAPER_DEBUG_HTML is enabled.

    Args:
        content: The cleaned text or HTML to save.
        profile_name: Name of the profile (for filename).
        section: Section name, e.g. "experience" or "education".
    """
    if not force and not SCRAPER_DEBUG_HTML:
        return
    try:
        DEBUG_HTML_DIR.mkdir(parents=True, exist_ok=True)
        safe_name = re.sub(r'[^\w\-]', '_', profile_name)[:50]
        timestamp = datetime.now().strftime("%H%M%S")
        ext = ".html" if "<" in content else ".txt"
        debug_file = DEBUG_HTML_DIR / f"{safe_name}_{section}_{timestamp}{ext}"
        debug_file.write_text(content, encoding='utf-8')
        logger.debug(f"Saved debug {section} to: {debug_file.name}")
    except Exception as e:
        # Debug saving must never crash production
        logger.warning(f"    ⚠️ Failed to save debug {section}: {e}")


def reset_groq_accuracy_risk_events() -> None:
    """Reset per-run Groq accuracy risk tracking."""
    global _RUN_GROQ_ACCURACY_EVENTS
    _RUN_GROQ_ACCURACY_EVENTS = []


def get_groq_accuracy_risk_events() -> list[dict]:
    """Return a copy of the current run's Groq accuracy-risk events."""
    return list(_RUN_GROQ_ACCURACY_EVENTS)


def log_groq_accuracy_risk(
    section: str,
    reason: str,
    profile_name: str = "",
    profile_url: str = "",
    debug_payloads: dict | None = None,
) -> None:
    """
    Emit a high-visibility warning and append a persistent audit record when
    extraction skipped Groq or Groq returned no usable result.
    """
    global _RUN_GROQ_ACCURACY_EVENTS

    section_text = (section or "unknown").strip() or "unknown"
    reason_text = (reason or "Groq extraction was skipped or ineffective.").strip()
    clean_name = (profile_name or "unknown").strip() or "unknown"
    clean_url = (profile_url or "").strip()

    banner = "[bold white on red]" + ("!" * 92) + "[/bold white on red]"
    logger.warning(banner)
    logger.warning("[bold white on red]GROQ ACCURACY RISK[/bold white on red]")
    logger.warning(
        "[bold white on red]section:[/bold white on red] %s | [bold white on red]profile:[/bold white on red] %s",
        section_text,
        clean_name,
    )
    if clean_url:
        logger.warning("[bold white on red]url:[/bold white on red] %s", clean_url)
    logger.warning("[bold white on red]%s[/bold white on red]", reason_text)
    logger.warning(
        "[bold white on red]Results may be inaccurate because Groq extraction was skipped or ineffective.[/bold white on red]"
    )
    logger.warning(banner)

    event = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "run_uuid": os.getenv("SCRAPE_RUN_UUID", "").strip(),
        "scraper_email": os.getenv("LINKEDIN_EMAIL", "").strip().lower(),
        "section": section_text,
        "profile_name": clean_name,
        "profile_url": clean_url,
        "reason": reason_text,
    }
    _RUN_GROQ_ACCURACY_EVENTS.append(event)

    try:
        GROQ_ACCURACY_AUDIT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with GROQ_ACCURACY_AUDIT_FILE.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=False) + "\n")
    except Exception as exc:
        logger.warning(f"    ⚠️ Failed to append Groq accuracy audit log: {exc}")

    if debug_payloads:
        for suffix, content in debug_payloads.items():
            if not content:
                continue
            try:
                save_debug_html(
                    str(content),
                    clean_name,
                    f"{section_text}_{suffix}",
                    force=True,
                )
            except Exception as exc:
                logger.warning(f"    ⚠️ Failed to save forced debug payload {suffix}: {exc}")


def parse_groq_json_response(result_text: str) -> dict | list | None:
    """
    Parse JSON from Groq response text with fallbacks.

    Returns:
        Parsed JSON (dict or list), or None on failure.
    """
    try:
        return json.loads(result_text)
    except json.JSONDecodeError:
        # Fallback: try to find a JSON array/object in the text.
        # This handles cases where the LLM includes preamble/postamble text
        # despite being instructed to return ONLY JSON.
        # 1. Try standard cleaning (removes noise).
        # 2. Check if we lost the target university (UNT) keywords found in raw HTML.
        # 3. If lost, retry with relaxed cleaning.
        # This ensures we don't accidentally "clean away" the very data we need
        # due to overly aggressive noise removal heuristics.
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
        msg = result_text[:100] + "..." if len(result_text) > 100 else result_text
        logger.warning(f"⚠️ Groq returned invalid JSON: {msg}")
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
    Parse a date string like "Oct 2024", "Present", or "Expected 2026" into the
    format used by the scraper.

    Returns:
        dict with keys: year (int), month (int or None), is_present (bool),
                        is_expected (bool)
    """
    if not date_str:
        return None

    date_str = date_str.strip()
    normalized = re.sub(r"\s+", " ", date_str)
    normalized = normalized.replace("–", "-").replace("—", "-")

    # Check for "Present"
    if normalized.lower() == "present":
        return {
            "year": 9999,
            "month": 12,
            "is_present": True,
            "is_expected": False,
            "has_month": False,
            "raw": "Present",
        }

    # Check for "Expected YYYY" or "Expected: YYYY"
    expected_match = re.search(r"\bexpected[:\s]+(\d{4})\b", normalized, re.IGNORECASE)
    if expected_match:
        year = int(expected_match.group(1))
        return {
            "year": year,
            "month": None,
            "is_present": False,
            "is_expected": True,
            "has_month": False,
            "raw": str(year),
        }

    # Try explicit month + year ("Jan 2024", "January 2024", optional trailing period)
    month_pattern = (
        r"\b("
        r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|"
        r"Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|"
        r"Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?"
        r")\.?\s+(\d{4})\b"
    )
    match = re.search(month_pattern, normalized, re.IGNORECASE)
    if match:
        month_str, year_str = match.groups()
        month = _MONTH_ALIASES.get(month_str.lower().rstrip("."), None)
        year = int(year_str)
        if month:
            return {
                "year": year,
                "month": month,
                "is_present": False,
                "is_expected": False,
                "has_month": True,
                "raw": f"{_MONTH_NUM_TO_ABBR[month]} {year}",
            }

    # Try "YYYY" format
    match = re.fullmatch(r"\d{4}", normalized)
    if match:
        year = int(match.group(0))
        return {
            "year": year,
            "month": None,
            "is_present": False,
            "is_expected": False,
            "has_month": False,
            "raw": str(year),
        }

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


def verify_location(text: str) -> bool:
    """
    Use Groq to verify if a string is a valid geographic location.
    
    Returns:
        True if it's a location, False otherwise.
    """
    client = _get_client()
    if not client:
        return False
    
    prompt = f"""
    Determine if the following text is a valid geographic location (e.g., City, State, Country, or a Metro Area).
    
    Rules:
    - Company names, university names, or job titles are NOT locations.
    - If the text looks like a person's name or a fragment of a profile, it is NOT a location.
    - Locations usually have the format "City, State, Country" or "Region Area".
    
    Text to evaluate: "{text}"
    
    Respond ONLY with a JSON object:
    {{
        "is_location": true or false,
        "reason": "short explanation"
    }}
    """
    
    try:
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role": "system", "content": "You are a specialized geographic validation assistant."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.0
        )
        data = parse_groq_json_response(response.choices[0].message.content)
        if data and isinstance(data, dict):
            is_loc = data.get("is_location", False)
            reason = data.get("reason", "")
            if is_loc:
                logger.debug(f"✅ Groq verified location: {text}")
            else:
                logger.debug(f"❌ Groq rejected location: {text} ({reason})")
            return is_loc
        return False
    except Exception as e:
        logger.warning(f"⚠️ Groq location verification failed: {e}")
        return False
