"""
Work While Studying Analysis Module — UNT-Aware (v2)

Determines if an alumnus was working while attending the University of North Texas
(UNT) by computing a proper attendance window and checking date-range overlap with
each experience record.

Tables used:
  education(
      alumni_id         INT,
      school_name       VARCHAR(255) NULL,   -- NEW
      school_start_date DATE NULL,           -- NEW
      school_start_year INT NULL,            -- NEW
      graduation_year   INT NULL,
      graduation_month  INT NULL,
      graduation_date   DATE NULL,
      is_expected       BOOLEAN
  )
  experience(
      alumni_id  INT,
      company    TEXT,
      title      TEXT,
      start_date DATE NULL,
      end_date   DATE NULL,
      is_current BOOLEAN
  )

Spec (per requirements):
  1. Use ONLY the education row whose school_name contains "University of North Texas"
     or "UNT" (case-insensitive). Ignore all other schools.
  2. Compute UNT attendance window:
       unt_start = school_start_date
                   else DATE(school_start_year, 8, 15)   [fall-semester fallback]
                   else None
       unt_end   = graduation_date
                   else DATE(graduation_year, 5, 15)     [spring graduation fallback]
                   else None
       if is_expected=True OR unt_end is None → unt_end = CURRENT_DATE
       if unt_start is None → worked_while_at_unt = False (cannot determine window)
  3. For each experience row:
       if start_date is None → skip
       job_end = end_date if not None else CURRENT_DATE
  4. Overlap: start_date <= unt_end AND job_end >= unt_start
  5. worked_while_at_unt = True if ANY job overlaps.
  6. Return evidence list of ONLY the overlapping jobs for debugging.
"""

from datetime import date
from typing import Any, Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Keywords used to identify UNT education rows (case-insensitive substring match)
_UNT_KEYWORDS = ("university of north texas", "unt")


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _is_unt_school(school_name: Optional[str]) -> bool:
    """Return True if school_name refers to the University of North Texas."""
    if not school_name:
        return False
    lower = school_name.lower().strip()
    return any(kw in lower for kw in _UNT_KEYWORDS)


def _resolve_date(exact: Optional[date], year: Optional[int], month: int, day: int) -> Optional[date]:
    """
    Return *exact* if set, otherwise try to build DATE(year, month, day).
    Returns None if neither is available or year is invalid.
    """
    if exact is not None:
        return exact
    if year is not None:
        try:
            return date(int(year), month, day)
        except (ValueError, TypeError):
            logger.warning(f"Invalid year value: {year!r}")
    return None


def _compute_unt_window(
    edu: Dict[str, Any],
    today: date,
) -> Tuple[Optional[date], date]:
    """
    Compute (unt_start, unt_end) from a UNT education record.

    Args:
        edu:   A dict with keys school_start_date, school_start_year,
               graduation_date, graduation_year, is_expected.
        today: The reference "current date" (passed in so callers can test it).

    Returns:
        (unt_start, unt_end)
        unt_start may be None (caller should treat as "unknown window start").
        unt_end is always a date (falls back to today when unknown/expected).
    """
    # --- UNT start ---
    unt_start = _resolve_date(
        edu.get("school_start_date"),
        edu.get("school_start_year"),
        month=8, day=15,   # August 15 — typical fall semester start
    )

    # --- UNT end ---
    unt_end = _resolve_date(
        edu.get("graduation_date"),
        edu.get("graduation_year"),
        month=5, day=15,   # May 15 — typical spring graduation
    )

    # Override with today if still-enrolled or unknown
    is_expected = edu.get("is_expected") or False
    if is_expected or unt_end is None:
        unt_end = today

    return unt_start, unt_end


# ---------------------------------------------------------------------------
# Legacy helpers (kept for backwards-compat with existing callers / tests)
# ---------------------------------------------------------------------------

def _get_graduation_date(
    graduation_date: Optional[date],
    graduation_year: Optional[int],
    graduation_month: Optional[int],
) -> Optional[date]:
    """
    Legacy helper.  Determine an effective graduation date.

    Priority:
      1. graduation_date (exact)
      2. DATE(graduation_year, 5, 15)    [May 15 fallback]
    graduation_month is accepted for API compat but not used in the fallback
    (the spec mandates May 15 regardless of reported month).
    """
    return _resolve_date(graduation_date, graduation_year, month=5, day=15)


def _get_graduated_status(
    graduation_year: Optional[int],
    graduation_date: Optional[date],
    is_expected: Optional[bool],
    current_year: Optional[int] = None,
) -> str:
    """
    Legacy helper. Classify graduation status.

    Returns: "graduated" | "not_yet_graduated" | "unknown"
    """
    if current_year is None:
        current_year = date.today().year

    if is_expected is True:
        return "not_yet_graduated"

    if graduation_year is not None and graduation_year > current_year:
        return "not_yet_graduated"

    if graduation_year is not None or graduation_date is not None:
        return "graduated"

    return "unknown"


# ---------------------------------------------------------------------------
# Main computation
# ---------------------------------------------------------------------------

def computeWorkWhileStudying(
    alumni_id: int,
    get_connection_func,
    today: Optional[date] = None,
) -> Optional[Dict[str, Any]]:
    """
    Determine whether an alumnus was working while attending UNT.

    Args:
        alumni_id:           The alumni ID to analyse.
        get_connection_func: Zero-arg callable that returns a DB connection.
        today:               Reference date (defaults to date.today()).
                             Pass explicitly in tests to get deterministic results.

    Returns:
        A dict:
        {
            "alumni_id":               int,
            "unt_start":               date | None,
            "unt_end":                 date | None,
            "worked_while_at_unt":     bool,
            # ---- backwards-compat aliases ----
            "graduation_year":         int | None,
            "graduation_date_used":    date | None,
            "graduated_status":        str,
            "is_working_while_studying": bool,   # == worked_while_at_unt
            "evidence_jobs": [
                {
                    "company":    str | None,
                    "title":      str | None,
                    "start_date": date | None,
                    "end_date":   date | None,
                }
            ]
        }

        Returns None on unrecoverable database error.
    """
    if today is None:
        today = date.today()

    conn = None
    try:
        conn = get_connection_func()

        # ------------------------------------------------------------------
        # 1. Fetch all education rows for this alumnus; pick the UNT one.
        # ------------------------------------------------------------------
        with conn.cursor(dictionary=True) as cur:
            cur.execute(
                """
                SELECT
                    school_name,
                    school_start_date,
                    school_start_year,
                    graduation_year,
                    graduation_month,
                    graduation_date,
                    is_expected
                FROM education
                WHERE alumni_id = %s
                """,
                (alumni_id,),
            )
            edu_rows = cur.fetchall() or []

        unt_edu = next((r for r in edu_rows if _is_unt_school(r.get("school_name"))), None)

        # Build a consistent "no UNT row" result
        _no_unt_result = {
            "alumni_id":               alumni_id,
            "unt_start":               None,
            "unt_end":                 None,
            "worked_while_at_unt":     False,
            # backwards-compat
            "graduation_year":         None,
            "graduation_date_used":    None,
            "graduated_status":        "unknown",
            "is_working_while_studying": False,
            "evidence_jobs":           [],
        }

        if unt_edu is None:
            logger.warning(f"No UNT education row found for alumni_id={alumni_id}")
            return _no_unt_result

        # ------------------------------------------------------------------
        # 2. Compute the UNT attendance window.
        # ------------------------------------------------------------------
        unt_start, unt_end = _compute_unt_window(unt_edu, today)

        # Backwards-compat fields
        graduation_year = unt_edu.get("graduation_year")
        graduation_date = unt_edu.get("graduation_date")
        graduation_month = unt_edu.get("graduation_month")
        is_expected = unt_edu.get("is_expected") or False

        graduation_date_used = _get_graduation_date(graduation_date, graduation_year, graduation_month)
        graduated_status = _get_graduated_status(graduation_year, graduation_date, is_expected)

        # If we cannot determine where UNT attendance started, we cannot compute overlap.
        if unt_start is None:
            logger.info(
                f"UNT start date unknown for alumni_id={alumni_id}; "
                "setting worked_while_at_unt=False"
            )
            return {
                **_no_unt_result,
                "unt_end":              unt_end,
                "graduation_year":      graduation_year,
                "graduation_date_used": graduation_date_used,
                "graduated_status":     graduated_status,
            }

        # ------------------------------------------------------------------
        # 3. Fetch all experience rows.
        # ------------------------------------------------------------------
        with conn.cursor(dictionary=True) as cur:
            cur.execute(
                """
                SELECT
                    company,
                    title,
                    start_date,
                    end_date,
                    is_current
                FROM experience
                WHERE alumni_id = %s
                ORDER BY start_date DESC
                """,
                (alumni_id,),
            )
            exp_rows = cur.fetchall() or []

        # ------------------------------------------------------------------
        # 4. Check each job for overlap with the UNT window.
        # ------------------------------------------------------------------
        evidence_jobs: List[Dict[str, Any]] = []
        worked_while_at_unt = False

        for exp in exp_rows:
            job_start = exp.get("start_date")

            # Rule: skip jobs with no start_date
            if job_start is None:
                logger.debug(
                    f"Skipping job for alumni_id={alumni_id}: "
                    f"company={exp.get('company')!r} (null start_date)"
                )
                continue

            # job_end: use end_date if available, otherwise today (covers is_current=True too)
            job_end = exp.get("end_date") or today

            # Overlap: the job interval [job_start, job_end] intersects [unt_start, unt_end]
            if job_start <= unt_end and job_end >= unt_start:
                evidence_jobs.append({
                    "company":    exp.get("company"),
                    "title":      exp.get("title"),
                    "start_date": job_start,
                    "end_date":   exp.get("end_date"),  # preserve None for current jobs
                })
                worked_while_at_unt = True

        return {
            "alumni_id":               alumni_id,
            "unt_start":               unt_start,
            "unt_end":                 unt_end,
            "worked_while_at_unt":     worked_while_at_unt,
            # backwards-compat
            "graduation_year":         graduation_year,
            "graduation_date_used":    graduation_date_used,
            "graduated_status":        graduated_status,
            "is_working_while_studying": worked_while_at_unt,
            "evidence_jobs":           evidence_jobs,
        }

    except Exception as exc:
        logger.error(f"Error computing work-while-studying for alumni_id={alumni_id}: {exc}")
        return None

    finally:
        if conn:
            try:
                conn.close()
            except Exception as exc:
                logger.error(f"Error closing connection: {exc}")


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def ensure_work_while_studying_schema(get_connection_func) -> bool:
    """
    Create / migrate the education and experience tables.

    The education table gains three new columns vs. the original schema:
      - school_name       VARCHAR(255)  — used to identify the UNT row
      - school_start_date DATE          — exact start date (if known)
      - school_start_year INT           — start year fallback (→ Aug 15)

    Safe to call on an already-existing database: ALTER TABLE statements are
    wrapped to ignore "duplicate column" errors.

    Returns True on success, False on error.
    """
    conn = None
    try:
        conn = get_connection_func()

        with conn.cursor() as cur:
            # ----------------------------------------------------------------
            # education table
            # ----------------------------------------------------------------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS education (
                    id                INT AUTO_INCREMENT PRIMARY KEY,
                    alumni_id         INT NOT NULL,
                    school_name       VARCHAR(255) DEFAULT NULL,
                    school_start_date DATE DEFAULT NULL,
                    school_start_year INT DEFAULT NULL,
                    graduation_year   INT DEFAULT NULL,
                    graduation_month  INT DEFAULT NULL,
                    graduation_date   DATE DEFAULT NULL,
                    is_expected       BOOLEAN DEFAULT FALSE,
                    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_alumni_id      (alumni_id),
                    INDEX idx_school_name    (school_name(100)),
                    INDEX idx_graduation_year (graduation_year)
                )
                """
            )
            logger.info("✅ education table created/verified")

            # Add new columns if they don't exist yet (idempotent migration)
            _add_column_if_missing(cur, "education", "school_name",       "VARCHAR(255) DEFAULT NULL")
            _add_column_if_missing(cur, "education", "school_start_date", "DATE DEFAULT NULL")
            _add_column_if_missing(cur, "education", "school_start_year", "INT DEFAULT NULL")

            # ----------------------------------------------------------------
            # experience table
            # ----------------------------------------------------------------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS experience (
                    id         INT AUTO_INCREMENT PRIMARY KEY,
                    alumni_id  INT NOT NULL,
                    company    TEXT DEFAULT NULL,
                    title      TEXT DEFAULT NULL,
                    start_date DATE DEFAULT NULL,
                    end_date   DATE DEFAULT NULL,
                    is_current BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_alumni_id  (alumni_id),
                    INDEX idx_start_date (start_date),
                    INDEX idx_is_current (is_current)
                )
                """
            )
            logger.info("✅ experience table created/verified")

        conn.commit()
        return True

    except Exception as exc:
        logger.error(f"Error ensuring work_while_studying schema: {exc}")
        return False

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _add_column_if_missing(cur, table: str, column: str, definition: str) -> None:
    """
    Attempt to ALTER TABLE ... ADD COLUMN.  Silently ignores 'duplicate column' errors.
    """
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        logger.info(f"  Added column {table}.{column}")
    except Exception as exc:
        msg = str(exc).lower()
        if "duplicate column" in msg or "already exists" in msg:
            logger.debug(f"  Column {table}.{column} already exists — skipped")
        else:
            raise
