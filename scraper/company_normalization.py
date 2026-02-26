"""
Company Name Normalization Module

Maps semantically equivalent company names to a single standardized form.
Preserves the original raw company value — only the normalized mapping is stored.

Priority flow:
  1. Deterministic: cleanup + suffix stripping + dictionary lookup (fast, offline)
  2. DB check:      case-insensitive match against existing normalized_companies table
  3. Groq-based:    LLM classification against existing normalized companies

Usage:
    from company_normalization import get_or_create_normalized_company

    norm_id = get_or_create_normalized_company(conn, raw_company)
"""

import os
import re
import json
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SUFFIX PATTERNS TO STRIP
# Common legal entity suffixes that are noise for grouping.
# ---------------------------------------------------------------------------

_SUFFIX_PATTERN = re.compile(
    r',?\s*\b('
    r'inc\.?|incorporated|'
    r'llc\.?|l\.l\.c\.?|'
    r'ltd\.?|limited|'
    r'corp\.?|corporation|'
    r'co\.?|company|'
    r'plc\.?|'
    r'pvt\.?\s*ltd\.?|private\s+limited|'
    r'gmbh|'
    r's\.?a\.?|'
    r'l\.?p\.?|'
    r'n\.?a\.?|'
    r'intl\.?|international'
    r')\s*\.?\s*$',
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# DETERMINISTIC COMPANY MAP
# Keys are lowercase variations, values are the canonical company name.
# ---------------------------------------------------------------------------

COMPANY_MAP = {
    # ── Big Tech ──
    "google": "Google",
    "google inc": "Google",
    "google llc": "Google",
    "google inc.": "Google",
    "alphabet": "Google",
    "alphabet inc": "Google",
    "alphabet inc.": "Google",
    "microsoft": "Microsoft",
    "microsoft corporation": "Microsoft",
    "microsoft corp": "Microsoft",
    "microsoft corp.": "Microsoft",
    "amazon": "Amazon",
    "amazon.com": "Amazon",
    "amazon web services": "Amazon",
    "aws": "Amazon",
    "amazon web services (aws)": "Amazon",
    "meta": "Meta",
    "meta platforms": "Meta",
    "meta platforms inc": "Meta",
    "facebook": "Meta",
    "apple": "Apple",
    "apple inc": "Apple",
    "apple inc.": "Apple",

    # ── Enterprise / Consulting ──
    "tata consultancy services": "Tata Consultancy Services",
    "tcs": "Tata Consultancy Services",
    "tata consultancy services limited": "Tata Consultancy Services",
    "tata consultancy services ltd": "Tata Consultancy Services",
    "infosys": "Infosys",
    "infosys limited": "Infosys",
    "infosys ltd": "Infosys",
    "infosys bpm": "Infosys",
    "wipro": "Wipro",
    "wipro limited": "Wipro",
    "wipro ltd": "Wipro",
    "cognizant": "Cognizant",
    "cognizant technology solutions": "Cognizant",
    "accenture": "Accenture",
    "accenture federal services": "Accenture",
    "deloitte": "Deloitte",
    "deloitte consulting": "Deloitte",
    "deloitte digital": "Deloitte",
    "ey": "Ernst & Young",
    "ernst & young": "Ernst & Young",
    "ernst and young": "Ernst & Young",
    "pwc": "PricewaterhouseCoopers",
    "pricewaterhousecoopers": "PricewaterhouseCoopers",
    "kpmg": "KPMG",
    "ibm": "IBM",
    "international business machines": "IBM",
    "ibm corporation": "IBM",
    "capgemini": "Capgemini",
    "capgemini engineering": "Capgemini",

    # ── Semiconductors / Hardware ──
    "intel": "Intel",
    "intel corporation": "Intel",
    "intel corp": "Intel",
    "texas instruments": "Texas Instruments",
    "ti": "Texas Instruments",
    "texas instruments incorporated": "Texas Instruments",
    "nvidia": "NVIDIA",
    "nvidia corporation": "NVIDIA",
    "qualcomm": "Qualcomm",
    "qualcomm incorporated": "Qualcomm",
    "amd": "AMD",
    "advanced micro devices": "AMD",

    # ── Software / SaaS ──
    "salesforce": "Salesforce",
    "salesforce.com": "Salesforce",
    "salesforce inc": "Salesforce",
    "oracle": "Oracle",
    "oracle corporation": "Oracle",
    "oracle corp": "Oracle",
    "sap": "SAP",
    "sap se": "SAP",
    "cisco": "Cisco",
    "cisco systems": "Cisco",
    "cisco systems inc": "Cisco",
    "vmware": "VMware",
    "vmware inc": "VMware",
    "adobe": "Adobe",
    "adobe inc": "Adobe",
    "adobe systems": "Adobe",
    "dell": "Dell Technologies",
    "dell technologies": "Dell Technologies",
    "dell inc": "Dell Technologies",

    # ── Finance ──
    "jpmorgan chase": "JPMorgan Chase",
    "jpmorgan chase & co": "JPMorgan Chase",
    "jpmorgan chase & co.": "JPMorgan Chase",
    "jpmorgan chase &": "JPMorgan Chase",
    "jpmorganchase": "JPMorgan Chase",
    "jp morgan chase": "JPMorgan Chase",
    "jpmorgan": "JPMorgan Chase",
    "jp morgan": "JPMorgan Chase",
    "chase": "JPMorgan Chase",
    "goldman sachs": "Goldman Sachs",
    "bank of america": "Bank of America",
    "wells fargo": "Wells Fargo",
    "citibank": "Citigroup",
    "citigroup": "Citigroup",
    "citi": "Citigroup",
    "capital one": "Capital One",
    "cvs": "CVS",
    "cvs health": "CVS",
    "cvs pharmacy": "CVS",
    "walmart": "Walmart",
    "walmart global tech": "Walmart",

    # ── Telecom ──
    "at&t": "AT&T",
    "att": "AT&T",
    "verizon": "Verizon",
    "verizon communications": "Verizon",
    "t-mobile": "T-Mobile",
    "t mobile": "T-Mobile",

    # ── Defense / Aerospace ──
    "lockheed martin": "Lockheed Martin",
    "lockheed martin corporation": "Lockheed Martin",
    "raytheon": "Raytheon",
    "raytheon technologies": "Raytheon",
    "boeing": "Boeing",
    "the boeing company": "Boeing",
    "northrop grumman": "Northrop Grumman",
    "northrop grumman corporation": "Northrop Grumman",
    "general dynamics": "General Dynamics",
    "l3harris": "L3Harris Technologies",
    "l3harris technologies": "L3Harris Technologies",

    # ── Energy / Oil ──
    "exxonmobil": "ExxonMobil",
    "exxon mobil": "ExxonMobil",
    "exxon": "ExxonMobil",
    "chevron": "Chevron",
    "chevron corporation": "Chevron",
    "shell": "Shell",
    "royal dutch shell": "Shell",
    "conocophillips": "ConocoPhillips",
    "halliburton": "Halliburton",

    # ── University of North Texas ──
    "university of north texas": "University of North Texas",
    "unt": "University of North Texas",
    "north texas": "University of North Texas",
}


# ---------------------------------------------------------------------------
# CLEANUP HELPERS
# ---------------------------------------------------------------------------

def _cleanup_company(raw: str) -> str:
    """
    Basic cleanup:
      - strip whitespace
      - collapse multiple spaces
      - remove trailing punctuation
      - strip common legal suffixes (Inc., LLC, Ltd., Corp., etc.)
    """
    if not raw:
        return ""
    t = raw.strip()
    t = re.sub(r'\s+', ' ', t)
    t = _strip_trailing_location_fragment(t)
    # Remove trailing period, comma, dash
    t = re.sub(r'[.,\-]+$', '', t).strip()
    # Strip legal suffixes
    t = _SUFFIX_PATTERN.sub('', t).strip()
    # Remove dangling ampersand left by patterns like "& Co."
    t = re.sub(r'\s*&\s*$', '', t).strip()
    return t


def _looks_like_location_fragment(fragment: str) -> bool:
    """Heuristic to detect trailing location chunks like ', Hyderabad' or ' - Austin'."""
    if not fragment:
        return False
    frag = fragment.strip()
    if not frag:
        return False

    low = frag.lower()
    # If this fragment looks like company descriptors, keep it.
    if re.search(
        r'\b(inc|llc|ltd|corp|company|co|technologies|technology|systems|solutions|'
        r'group|university|college|school|health|pharmacy|labs?|studio|restaurant)\b',
        low,
        re.I
    ):
        return False

    if re.search(r'\d', frag):
        return False

    words = re.findall(r"[A-Za-z][A-Za-z'.&-]*", frag)
    return 1 <= len(words) <= 4


def _strip_trailing_location_fragment(text: str) -> str:
    """Strip trailing location suffix when separated by comma/dash."""
    t = (text or "").strip()
    if not t:
        return ""

    for sep in [",", " - ", " – ", " — "]:
        if sep in t:
            head, tail = t.rsplit(sep, 1)
            if _looks_like_location_fragment(tail):
                return head.strip()
    return t


def normalize_company_deterministic(raw_company: str) -> str:
    """
    Deterministic normalization:
      1. Cleanup whitespace & suffixes
      2. Exact lookup in COMPANY_MAP (case-insensitive)
      3. Strip suffixes and retry
      4. If not found, return the cleaned name as-is

    Returns the normalized company name string.
    """
    cleaned = _cleanup_company(raw_company)
    if not cleaned:
        return ""
    key = cleaned.lower()

    # Pass 1: exact match
    if key in COMPANY_MAP:
        return COMPANY_MAP[key]

    # Pass 2: try with additional suffix stripping
    stripped = _SUFFIX_PATTERN.sub('', key).strip()
    if stripped != key and stripped in COMPANY_MAP:
        return COMPANY_MAP[stripped]

    # No match — return cleaned name (preserves original casing from cleanup)
    return cleaned


# ---------------------------------------------------------------------------
# GROQ-BASED NORMALIZATION
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
        logger.warning("groq package not installed — Groq company normalization disabled")
        return None
    except Exception as e:
        logger.error(f"Failed to init Groq client: {e}")
        return None


def _coerce_existing_company_choice(candidate: str, existing_companies: list[str]) -> str:
    """
    Normalize an LLM company output and restore exact existing company casing when possible.
    """
    if not candidate or not isinstance(candidate, str):
        return ""

    cleaned = re.sub(r"\s+", " ", candidate).strip().strip('"\'')
    cleaned = re.sub(r"\s*[|:;,.!?]+\s*$", "", cleaned).strip()

    if not cleaned:
        return ""

    existing_map = {}
    for company in existing_companies or []:
        if isinstance(company, str) and company.strip():
            existing_map[company.strip().casefold()] = company.strip()

    match = existing_map.get(cleaned.casefold())
    return match if match else cleaned


def normalize_company_with_groq(raw_company: str, existing_companies: list) -> str:
    """
    Use Groq LLM to classify a raw company name.

    Args:
        raw_company:        The raw scraped company name.
        existing_companies: List of already-known normalized company names in the DB.

    Returns:
        A normalized company name string (existing or new suggestion).
        Falls back to deterministic normalization on any failure.
    """
    client = _get_groq_client()
    if not client:
        logger.info("Groq unavailable — falling back to deterministic company normalization")
        return normalize_company_deterministic(raw_company)

    # Build prompt
    companies_list = "\n".join(f"- {c}" for c in existing_companies[:220])
    raw_text = (raw_company or "").strip()[:200]

    prompt = f"""You are a company-name normalization engine.

Task:
Given a raw company name and existing normalized company names, return one normalized company.

Rules:
1. If an existing company is the same entity, return that EXACT existing string.
2. Otherwise return a clean new company name.
3. Remove legal suffixes when they are not brand-essential (Inc, LLC, Ltd, Corp).
4. Collapse variants/abbreviations to common brand name when obvious.
5. Remove trailing location fragments from company names when present
   (e.g., "Avinash Enterprises, Hyderabad" -> "Avinash Enterprises").
6. For placeholders (self-employed, stealth startup, confidential), return a concise normalized placeholder.
7. If raw input is empty/noise, return an empty string.

Existing normalized companies:
{companies_list}

Raw company name: "{raw_text}"

Return JSON only:
{{"normalized_company":"<string>", "match_type":"existing|new|placeholder|empty"}}"""

    try:
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "Output strictly valid JSON with normalized_company and match_type only."
                },
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=48
        )
        payload = json.loads(response.choices[0].message.content)
        result = _coerce_existing_company_choice(payload.get("normalized_company", ""), existing_companies)
        result = _strip_trailing_location_fragment(result)
        if result.casefold() in {"n/a", "na", "none", "null", "unknown", "other"}:
            logger.warning(f"Groq returned non-company value for {raw_company!r}: {result!r}")
            return normalize_company_deterministic(raw_company)
        if result and len(result) < 150:
            return result
        logger.warning(f"Groq returned suspicious company result: {result!r}")
        return normalize_company_deterministic(raw_company)
    except Exception as e:
        logger.error(f"Groq company normalization failed: {e}")
        return normalize_company_deterministic(raw_company)


# ---------------------------------------------------------------------------
# DATABASE HELPERS
# ---------------------------------------------------------------------------

def get_all_normalized_companies(conn) -> list:
    """Fetch all existing normalized companies from the DB."""
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute("SELECT id, normalized_company FROM normalized_companies ORDER BY normalized_company")
            return cur.fetchall()
    except Exception as e:
        logger.error(f"Error fetching normalized companies: {e}")
        return []


def get_or_create_normalized_company(conn, raw_company: str, use_groq: bool = True) -> int | None:
    """
    Main entry point. Returns the normalized_company_id for a given raw company name.

    Priority flow:
      1. Deterministic map lookup (fast, offline)
      2. Case-insensitive check against existing normalized_companies table
      3. If use_groq=True and no match found, LLM classifies against existing list
      4. Upsert into normalized_companies, return the id.

    Args:
        conn:        Active DB connection (MySQL or SQLite-wrapped).
        raw_company: The raw company name string.
        use_groq:    Whether to attempt Groq classification.

    Returns:
        Integer ID from normalized_companies, or None on failure.
    """
    if not raw_company or not raw_company.strip():
        return None

    cleaned = _cleanup_company(raw_company)
    if not cleaned:
        return None

    # Step 1: deterministic map lookup
    norm = normalize_company_deterministic(raw_company)

    # Step 2: check if this normalized name already exists in DB
    existing = get_all_normalized_companies(conn)
    existing_names = [r['normalized_company'] for r in existing]
    existing_lower = {n.lower(): n for n in existing_names}

    # If deterministic result matches an existing entry, use it
    if norm.lower() in existing_lower:
        norm = existing_lower[norm.lower()]
    elif use_groq and norm == cleaned:
        # Step 3: deterministic was a passthrough (no map hit) — try Groq
        norm = normalize_company_with_groq(raw_company, existing_names)
        # Check again if Groq returned something in our list
        if norm.lower() in existing_lower:
            norm = existing_lower[norm.lower()]

    # Step 4: upsert into normalized_companies
    try:
        with conn.cursor() as cur:
            # Try INSERT IGNORE / ON CONFLICT
            try:
                cur.execute(
                    "INSERT INTO normalized_companies (normalized_company) VALUES (%s) "
                    "ON DUPLICATE KEY UPDATE normalized_company = VALUES(normalized_company)",
                    (norm,)
                )
            except Exception:
                # SQLite fallback syntax
                cur.execute(
                    "INSERT OR IGNORE INTO normalized_companies (normalized_company) VALUES (?)",
                    (norm,)
                )

            # Fetch the id
            try:
                cur.execute(
                    "SELECT id FROM normalized_companies WHERE normalized_company = %s",
                    (norm,)
                )
            except Exception:
                cur.execute(
                    "SELECT id FROM normalized_companies WHERE normalized_company = ?",
                    (norm,)
                )

            row = cur.fetchone()
            if row:
                return row['id'] if isinstance(row, dict) else row[0]
            return None
    except Exception as e:
        logger.error(f"Error in get_or_create_normalized_company: {e}")
        return None
