import mysql.connector
import pandas as pd
import os
import logging
import re
from contextlib import contextmanager
from dotenv import load_dotenv
from pathlib import Path


def is_sqlite_connection(connection):
    """Best-effort check for sqlite-backed connections/wrappers."""
    if connection is None:
        return False

    cls = connection.__class__
    name = getattr(cls, "__name__", "").lower()
    module = getattr(cls, "__module__", "").lower()
    if "sqlite" in name or "sqlite" in module:
        return True

    raw_conn = getattr(connection, "_conn", None)
    if raw_conn is not None:
        raw_cls = raw_conn.__class__
        raw_name = getattr(raw_cls, "__name__", "").lower()
        raw_module = getattr(raw_cls, "__module__", "").lower()
        if "sqlite" in raw_name or "sqlite" in raw_module:
            return True

    return False


def adapt_sql_parameter_style(query, use_sqlite):
    """Convert MySQL-style placeholders to sqlite style when needed."""
    if not query or not use_sqlite:
        return query
    return query.replace("%s", "?")


def execute_sql(cursor, query, params=None, connection=None, sqlite_query=None):
    """
    Execute SQL with optional sqlite override and placeholder adaptation.

    This allows callsites to keep MySQL-first SQL while remaining sqlite-safe.
    """
    use_sqlite = is_sqlite_connection(connection)
    sql = sqlite_query if (use_sqlite and sqlite_query) else query
    sql = adapt_sql_parameter_style(sql, use_sqlite and not sqlite_query)

    if params is None:
        return cursor.execute(sql)
    return cursor.execute(sql, params)


@contextmanager
def managed_db_cursor(get_connection_fn, dictionary=False, commit=False):
    """
    Provide a cursor with consistent commit/rollback/close handling.

    - On success: commits if commit=True.
    - On error: rolls back if commit=True and rollback exists.
    - Always closes cursor and connection.
    """
    conn = None
    cursor = None
    try:
        conn = get_connection_fn()
        if dictionary:
            try:
                cursor = conn.cursor(dictionary=True)
            except TypeError:
                cursor = conn.cursor()
        else:
            cursor = conn.cursor()

        yield conn, cursor

        if commit:
            conn.commit()
    except Exception:
        if commit and conn and hasattr(conn, "rollback"):
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if cursor and hasattr(cursor, "close"):
            try:
                cursor.close()
            except Exception:
                pass
        if conn and hasattr(conn, "close"):
            try:
                conn.close()
            except Exception:
                pass

_env_path = Path(__file__).resolve().parent.parent / '.env'
for _enc in ('utf-8', 'latin-1'):
    try:
        load_dotenv(_env_path, encoding=_enc)
        break
    except Exception:
        continue

APPROVED_ENGINEERING_DISCIPLINES = {
    "Software, Data, AI & Cybersecurity",
    "Embedded, Electrical & Hardware Engineering",
    "Mechanical Engineering & Manufacturing",
    "Biomedical Engineering",
    "Construction & Engineering Management",
}

UNT_ALLOWED_MAJORS = {
    "Artificial Intelligence",
    "Biomedical Engineering",
    "Computer Engineering",
    "Computer Science",
    "Construction Engineering Technology",
    "Construction Management",
    "Cybersecurity",
    "Data Engineering",
    "Electrical Engineering",
    "Engineering Management",
    "Geographic Information Systems + Computer Science",
    "Information Technology",
    "Materials Science and Engineering",
    "Mechanical and Energy Engineering",
    "Mechanical Engineering Technology",
    "Semiconductor Manufacturing Engineering",
    "Other",
}

FLAGGED_REVIEW_PATH = (
    Path(__file__).resolve().parent.parent / "scraper" / "output" / "flagged_for_review.txt"
)


def _clean_optional_text(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"nan", "none", "null", "na", "n/a"}:
        return None
    return text


def _truncate_optional_text(value, max_len=255):
    """Return cleaned optional text truncated to DB-safe length."""
    text = _clean_optional_text(value)
    if text is None:
        return None
    if len(text) <= max_len:
        return text
    return text[:max_len].rstrip()


def _csv_optional_str(row, *keys):
    """First non-empty CSV column among keys (supports new vs legacy column names)."""
    for k in keys:
        v = row.get(k)
        if pd.isna(v):
            continue
        text = str(v).strip()
        if text:
            return text
    return None


def _parse_float(value):
    """Parse a value to float, returning None on failure."""
    if value is None:
        return None
    if isinstance(value, float):
        return value if not pd.isna(value) else None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "na", "n/a", ""}:
        return None
    try:
        return float(text)
    except (ValueError, TypeError):
        return None


def _parse_bool(value):
    """Parse a value to bool, returning None on failure."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "1", "1.0", "yes"}:
        return True
    if text in {"false", "0", "0.0", "no"}:
        return False
    return None


def _parse_int(value):
    """Parse a value to int, returning None on failure."""
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if pd.isna(value):
            return None
        if value.is_integer():
            return int(value)
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "na", "n/a", ""}:
        return None
    try:
        f = float(text)
        if f.is_integer():
            return int(f)
    except (ValueError, TypeError):
        pass
    return None


_NAME_SUFFIX_TOKENS = {"ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x"}


def _normalize_person_name(raw_name):
    """Normalize person-name casing while preserving punctuation."""
    text = re.sub(r"\s+", " ", str(raw_name or "").strip())
    if not text:
        return ""
    text = re.sub(r"[,\s]+$", "", text).strip()

    def _cap_word(match):
        word = match.group(0)
        low = word.lower()
        if low in _NAME_SUFFIX_TOKENS:
            return low.upper()
        if len(word) == 1:
            return word.upper()
        return word[0].upper() + word[1:].lower()

    return re.sub(r"[A-Za-z]+", _cap_word, text)


def _sanitize_major_and_discipline(major, standardized_major, discipline):
    """
    Ensure major and discipline remain separate:
    - `major` must never contain a discipline label.
    - `discipline` must be one of the approved discipline labels (or empty).
    """
    normalized_major = _clean_optional_text(major)
    normalized_standardized_major = _clean_optional_text(standardized_major)
    normalized_discipline = _clean_optional_text(discipline) or ""
    review_reason = None
    major_was_discipline_label = False

    if normalized_discipline and normalized_discipline not in APPROVED_ENGINEERING_DISCIPLINES:
        normalized_discipline = ""

    if normalized_major in APPROVED_ENGINEERING_DISCIPLINES:
        major_was_discipline_label = True
        review_reason = "major_equals_discipline_label"
        if not normalized_discipline:
            normalized_discipline = normalized_major

        # When major is actually a discipline label, drop polluted major text.
        # Keep discipline separate and avoid forcing standardized values into raw major.
        normalized_major = None

    # Preserve raw major text when present, even if it is outside the engineering-only
    # approved list. Standardized major is stored separately for filtering.
    #
    # Only backfill major from standardized_major when raw major is truly missing.
    if (
        not normalized_major
        and not major_was_discipline_label
        and normalized_standardized_major
        and normalized_standardized_major in UNT_ALLOWED_MAJORS
    ):
        normalized_major = normalized_standardized_major

    return normalized_major, normalized_discipline, review_reason


def _append_flagged_review_urls(url_reason_map):
    if not url_reason_map:
        return

    try:
        FLAGGED_REVIEW_PATH.parent.mkdir(parents=True, exist_ok=True)
        existing_urls = set()
        if FLAGGED_REVIEW_PATH.exists():
            with open(FLAGGED_REVIEW_PATH, "r", encoding="utf-8") as handle:
                for line in handle:
                    existing_url = line.split("#")[0].strip().rstrip("/")
                    if existing_url:
                        existing_urls.add(existing_url)

        pending_lines = []
        for raw_url, reason in sorted(url_reason_map.items()):
            url = normalize_url(raw_url)
            if not url:
                continue
            if url in existing_urls:
                continue
            pending_lines.append(f"{url} # {reason}\n")
            existing_urls.add(url)

        if pending_lines:
            with open(FLAGGED_REVIEW_PATH, "a", encoding="utf-8") as handle:
                handle.writelines(pending_lines)
            logger.info(f"Flagged {len(pending_lines)} profile(s) for major/discipline review")
    except Exception as flag_err:
        logger.warning(f"Could not append major/discipline review flags: {flag_err}")

def _get_or_create_normalized_entity(cur, table, column, value):
    """
    Inline helper to insert normalized strings and return their DB ID.
    We normalize job titles and company names to a separate lookup table
    to ensure consistency and save disk space by avoiding redundant strings
    in the main 'alumni' table.
    """
    if not value or str(value).strip() == '': return None
    value = str(value).strip()
    try:
        cur.execute(f"INSERT INTO {table} ({column}) VALUES (%s) ON DUPLICATE KEY UPDATE {column}=VALUES({column})", (value,))
    except Exception:
        # SQLite fallback syntax
        cur.execute(f"INSERT OR IGNORE INTO {table} ({column}) VALUES (?)", (value,))
    
    try:
        cur.execute(f"SELECT id FROM {table} WHERE {column} = %s", (value,))
    except Exception:
        cur.execute(f"SELECT id FROM {table} WHERE {column} = ?", (value,))
    
    row = cur.fetchone()
    if not row: return None
    return row['id'] if isinstance(row, dict) else row[0]

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MySQL connection parameters (for direct access when needed)
MYSQL_HOST = os.getenv('MYSQLHOST')
MYSQL_USER = os.getenv('MYSQLUSER')
MYSQL_PASSWORD = os.getenv('MYSQLPASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
MYSQL_PORT = int(os.getenv('MYSQLPORT', 3306))

# Flag to control whether to use fallback system
USE_SQLITE_FALLBACK = os.getenv('USE_SQLITE_FALLBACK', '1') == '1'


def normalize_url(url):
    """Strip trailing slashes from URL."""
    if pd.isna(url) or url is None: return None
    s = str(url).strip()
    if not s or s.lower() == 'nan': return None
    return s.rstrip('/')


def _coerce_grad_year(value):
    """
    Convert a grad year value into an integer when possible.
    Returns None when parsing fails or the year is out of a reasonable range.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if pd.isna(value):
        return None

    def _in_range(year):
        return 1900 <= year <= 2100

    if isinstance(value, int):
        return value if _in_range(value) else None

    if isinstance(value, float):
        if value.is_integer():
            year = int(value)
            return year if _in_range(year) else None
        return None

    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"nan", "none", "null", "na", "n/a"}:
        return None

    try:
        numeric = float(text)
        if numeric.is_integer():
            year = int(numeric)
            return year if _in_range(year) else None
    except ValueError:
        pass

    matches = re.findall(r"(19\d{2}|20\d{2}|2100)", text)
    if not matches:
        return None

    year = int(matches[-1])
    return year if _in_range(year) else None


def _infer_grad_year_from_school_start_date(value):
    """
    Infer grad_year from school_start_date only when the value contains a single
    date/year signal (legacy data quirk where one date was put in start field).
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"nan", "none", "null", "na", "n/a"}:
        return None

    # Skip explicit ranges; this rule is only for single-date entries.
    if re.search(r"[-ΓÇôΓÇö]", text):
        return None

    years = re.findall(r"(19\d{2}|20\d{2}|2100)", text)
    if len(years) != 1:
        return None

    return _coerce_grad_year(years[0])


def _normalize_primary_education_dates(grad_year_value, school_start_value):
    """
    Normalize primary education date fields:
    - keep explicit grad_year when present
    - if grad_year missing and school_start has a single date/year, treat it as grad_year
      and clear school_start
    """
    grad_year = _coerce_grad_year(grad_year_value)

    school_start_text = None
    if school_start_value is not None and not pd.isna(school_start_value):
        school_start_text = str(school_start_value).strip() or None

    if grad_year is not None:
        return grad_year, school_start_text

    inferred_grad_year = _infer_grad_year_from_school_start_date(school_start_text)
    if inferred_grad_year is None:
        return None, school_start_text

    return inferred_grad_year, None



def get_connection():
    """
    Get a database connection.
    If USE_SQLITE_FALLBACK is enabled, routes to MySQL or SQLite based on availability.
    This "smart routing" allows the application to remain functional in local
    dev environments or during remote database outages by falling back to a 
    local .db file.
    """
    # Check if DB is explicitly disabled (dev mode)
    # in this mode, we force SQLite if fallback is enabled, regardless of cloud availability
    disable_db = os.getenv("DISABLE_DB", "0") == "1"
    
    if USE_SQLITE_FALLBACK:
        try:
            from sqlite_fallback import get_connection_manager, SQLiteConnectionWrapper
            manager = get_connection_manager()
            
            if disable_db:
                # Force offline/SQLite mode
                return SQLiteConnectionWrapper(manager.get_sqlite_connection(), manager)
            
            return manager.get_connection()
        except ImportError:
            logger.warning("sqlite_fallback module not found, falling back to direct MySQL")
    
    # MySQL connection
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        port=MYSQL_PORT
    )


def get_direct_mysql_connection():
    """Get a direct MySQL connection (bypasses fallback system)."""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        port=MYSQL_PORT
    )


