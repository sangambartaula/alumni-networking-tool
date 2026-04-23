import importlib
import re
import sys

from flask import Blueprint, jsonify, request, send_from_directory

from geocoding import geocode_location
from middleware import login_required
from unt_alumni_status import compute_unt_alumni_status_from_row
from utils import rank_filter_option_counts


alumni_bp = Blueprint("alumni", __name__)


_SENIORITY_ALLOWED = ["Intern", "Mid", "Senior", "Manager", "Executive"]

_ROLE_HINT_RE = re.compile(
    r"\b(engineer|developer|analyst|manager|director|architect|administrator|scientist|consultant|"
    r"technician|specialist|officer|president|founder|partner|professor|researcher|student|intern|"
    r"assistant|coordinator|designer|writer)\b",
    re.I,
)

try:
    from scraper.job_title_normalization import normalize_title_deterministic as _normalize_job_title_deterministic
except Exception:
    _normalize_job_title_deterministic = None

_LEADERSHIP_TITLE_SIGNAL = re.compile(
    r"\b(ceo|cto|coo|cfo|cmo|chief|president|founder|co-founder|owner|chair|evp|svp)\b",
    re.I,
)

_TRUSTED_TITLE_WITHOUT_ROLE_TOKEN = frozenset(
    {
        "CEO",
        "CTO",
        "COO",
        "CFO",
        "CMO",
        "VP",
        "President",
        "Executive",
        "Director",
        "Professor",
        "Postdoctoral Researcher",
        "Doctoral Candidate",
        "Intern",
        "Student",
        "Graduate Assistant",
        "Executive Assistant",
    }
)


def _app_mod():
    return sys.modules.get("app") or sys.modules.get("__main__") or importlib.import_module("app")


def _validation_error(message, field):
    return jsonify({"error": {"field": field, "message": message}}), 400


def _parse_multi_value_param(name, split_commas=True):
    values = []
    for raw in request.args.getlist(name):
        if raw is None:
            continue
        if split_commas:
            parts = str(raw).split(",")
        else:
            parts = [str(raw)]
        for p in parts:
            v = p.strip()
            if v:
                values.append(v)
    return values


def _parse_optional_non_negative_int(param_name):
    raw_value = (request.args.get(param_name, "") or "").strip()
    if not raw_value:
        return None
    try:
        value = int(raw_value)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid integer value for {param_name}: {raw_value}")
    if value < 0:
        raise ValueError(f"{param_name} must be non-negative.")
    return value


def _validate_min_max(min_value, max_value, min_name, max_name):
    if min_value is None or max_value is None:
        return
    if min_value > max_value:
        raise ValueError(f"{min_name} cannot be greater than {max_name}.")


def _parse_unt_alumni_status_filter(value):
    text = (value or "").strip().lower()
    if not text:
        return None
    if text not in {"yes", "no", "unknown"}:
        raise ValueError("Invalid unt_alumni_status. Use yes, no, or unknown.")
    return text


def _normalize_working_while_studying_value(value):
    if value is True or value == 1:
        return "yes"
    if value is False or value == 0:
        return "no"
    text = (value or "").strip().lower()
    if text in {"yes", "true", "1", "currently"}:
        return "yes"
    if text in {"no", "false", "0"}:
        return "no"
    return "unknown"


def _normalize_degree_to_filter_label(value):
    text = (value or "").strip().lower()
    if not text:
        return ""
    if "phd" in text or "doctor" in text:
        return "PhD"
    if "master" in text or text in {"ms", "m.s", "graduate"}:
        return "Masters"
    if "bachelor" in text or text == "undergraduate":
        return "Bachelors"
    return ""


def classify_seniority_bucket(title, _stored):
    t = (title or "").lower()
    if not t:
        return "Others"
    if re.search(r"\bintern\b|\bco-?op\b", t):
        return "Intern"
    if re.search(r"\bvp\b|vice president|director|head of|chief|cto|ceo|cfo", t):
        return "Executive"
    if re.search(r"\bmanager\b|\bmgr\b|lead", t):
        return "Manager"
    if re.search(r"\bsenior\b|\bstaff\b|principal", t):
        return "Senior"
    if re.search(r"\bengineer\b|\banalyst\b|\bdeveloper\b|\bconsultant\b|\bassociate\b|\bjunior\b", t):
        return "Mid"
    return "Others"


def _normalize_requested_discipline(value):
    v = (value or "").strip().lower()
    if not v:
        return None
    if "software" in v or "ai" in v or "cyber" in v or "data" in v:
        return "Software, Data, AI & Cybersecurity"
    if "embedded" in v or "electrical" in v or "hardware" in v:
        return "Embedded, Electrical & Hardware Engineering"
    if "mechanical" in v or "manufacturing" in v:
        return "Mechanical Engineering & Manufacturing"
    if "biomedical" in v:
        return "Biomedical Engineering"
    if "construction" in v:
        return "Construction & Engineering Management"
    return value.strip()


def _raw_title_signals_professional_role(title: str) -> bool:
    t = (title or "").strip()
    if not t:
        return False
    without_level_suffix = re.sub(r"\s+(?:level\s*)?(?:i{1,5}|[1-9])$", "", t, flags=re.I).strip()
    without_seniority = re.sub(r"^(?:senior|sr\.?|junior|jr\.?|lead)\s+", "", without_level_suffix, flags=re.I).strip()
    canonical_title = without_seniority or without_level_suffix or t
    low = re.sub(r"\s+", " ", canonical_title.lower())
    flat = re.sub(r"[^a-z0-9]+", " ", low).strip()
    if _ROLE_HINT_RE.search(flat):
        return True
    if flat in {"principal", "member", "student"}:
        return True
    if _LEADERSHIP_TITLE_SIGNAL.search(low):
        return True
    return False


def _canonical_role_title_legacy(value):
    title = (value or "").strip()
    if not title:
        return ""

    without_level_suffix = re.sub(r"\s+(?:level\s*)?(?:i{1,5}|[1-9])$", "", title, flags=re.I).strip()
    without_seniority = re.sub(r"^(?:senior|sr\.?|junior|jr\.?|lead)\s+", "", without_level_suffix, flags=re.I).strip()
    canonical_title = without_seniority or without_level_suffix or title
    low = re.sub(r"\s+", " ", canonical_title.lower())
    flat = re.sub(r"[^a-z0-9]+", " ", low).strip()

    if "ai4all" not in flat:
        ai_signal = (
            bool(re.search(r"\b(ai|aiml|ml|llm)\b", flat))
            or "machine learning" in flat
            or "generative ai" in flat
        )
        ai_role_signal = bool(
            re.search(r"\b(engineer|developer|scientist|software|mlops|data|python|associate|project|projects)\b", flat)
        )
        if ai_signal and ai_role_signal:
            return "AI / ML Engineer"

    if low in {"director", "director of", "director of engineering"}:
        return "Director"
    if low in {
        "manager",
        "manager - innovation",
        "laboratory safety manager",
        "senior manager - innovation",
    }:
        return "Manager"
    if low in {
        "software engineer",
        "software developer",
        "software dev",
        "software development engineer",
        "full stack developer",
        "full-stack developer",
        "full stack engineer",
        "full-stack engineer",
    }:
        return "Software Engineer"
    if low == "project manager":
        return "Project Manager"
    if low in {"devops engineer", "jr. devops engineer"}:
        return "DevOps Engineer"
    if low in {"network engineer", "senior network engineer"}:
        return "Network Engineer"
    if low in {"quality engineer", "senior quality engineer"}:
        return "Quality Engineer"
    if low in {"data analyst", "junior data analyst", "senior data analyst", "bi data analyst"}:
        return "Data Analyst"
    if low == "data owner":
        return "Data Analyst"
    if not _ROLE_HINT_RE.search(flat) and flat not in {"principal", "member", "student"}:
        return ""
    return canonical_title


def _canonical_role_title(value):
    title = (value or "").strip()
    if not title:
        return ""

    flat = re.sub(r"[^a-z0-9]+", " ", re.sub(r"\s+", " ", title.lower()).strip()).strip()

    if "ai4all" not in flat:
        ai_signal = (
            bool(re.search(r"\b(ai|aiml|ml|llm)\b", flat))
            or "machine learning" in flat
            or "generative ai" in flat
        )
        ai_role_signal = bool(
            re.search(
                r"\b(engineer|developer|scientist|software|mlops|data|python|associate|project|projects)\b",
                flat,
            )
        )
        if ai_signal and ai_role_signal:
            return "AI / ML Engineer"

    if _normalize_job_title_deterministic is None:
        return _canonical_role_title_legacy(value)

    normed = (_normalize_job_title_deterministic(title) or "").strip()
    if not normed:
        return ""

    if not _raw_title_signals_professional_role(title) and normed not in _TRUSTED_TITLE_WITHOUT_ROLE_TOKEN:
        return ""

    return normed


def _canonical_company_name(value):
    name = (value or "").strip()
    if not name:
        return ""
    if "Dallas" in name:
        return name
    if (
        "University of North Texas" in name
        or name.startswith("UNT ")
        or name == "UNT"
        or " UNT " in name
        or name.endswith(" UNT")
    ):
        return "University of North Texas"
    return name


def _fetchone_dict(cur, key):
    if hasattr(cur, "fetchone"):
        row = cur.fetchone()
        if isinstance(row, dict):
            return int(row.get(key, 0) or 0)
        if isinstance(row, tuple) and row:
            return int(row[0] or 0)
    rows = cur.fetchall() or []
    if rows and isinstance(rows[0], dict):
        return int(rows[0].get(key, 0) or 0)
    if rows and isinstance(rows[0], tuple):
        return int(rows[0][0] or 0)
    return 0


def _map_alumni_item(row):
    seniority_level = row.get("seniority_level") or classify_seniority_bucket(row.get("current_job_title"), None)
    first = row.get("first_name") or ""
    last = row.get("last_name") or ""
    full_name = f"{first} {last}".strip()
    title = row.get("current_job_title") or row.get("headline") or ""
    normalized_from_db = (row.get("normalized_job_title") or "").strip()
    normalized_title = _canonical_role_title(normalized_from_db or title)
    standardized_major = (row.get("standardized_major") or "").strip()
    standardized_majors = [standardized_major] if standardized_major else []
    updated_at = row.get("updated_at") or row.get("created_at")
    if hasattr(updated_at, "isoformat"):
        updated_at = updated_at.isoformat()
    item = {
        "id": row.get("id"),
        "first": first,
        "last": last,
        "name": full_name,
        "linkedin_url": row.get("linkedin_url") or "",
        "school": row.get("school"),
        "school_start": row.get("school_start_date"),
        "degree_raw": row.get("degree"),
        "major_raw": row.get("major"),
        "major": (standardized_major or row.get("major") or "").strip(),
        "standardized_majors": standardized_majors,
        "discipline": (row.get("discipline") or "").strip(),
        "grad_year": row.get("grad_year"),
        "school2": row.get("school2"),
        "school3": row.get("school3"),
        "degree2": row.get("degree2"),
        "degree3": row.get("degree3"),
        "major2": row.get("major2"),
        "major3": row.get("major3"),
        "location": row.get("location"),
        "title": title,
        "role": normalized_title,
        "normalized_title": normalized_title,
        "normalized_job_title": normalized_from_db,
        "current_job_title": row.get("current_job_title") or "",
        "headline": row.get("headline") or "",
        "company": row.get("company"),
        "job_employment_type": row.get("job_employment_type"),
        "job_start": row.get("job_start_date"),
        "job_end": row.get("job_end_date"),
        "exp_2_title": row.get("exp2_title"),
        "exp_2_company": row.get("exp2_company"),
        "exp_2_dates": row.get("exp2_dates"),
        "exp_2_employment_type": row.get("exp2_employment_type"),
        "exp_3_title": row.get("exp3_title"),
        "exp_3_company": row.get("exp3_company"),
        "exp_3_dates": row.get("exp3_dates"),
        "exp_3_employment_type": row.get("exp3_employment_type"),
        "working_while_studying": row.get("working_while_studying"),
        "working_while_studying_status": row.get("working_while_studying_status"),
        "seniority_level": seniority_level,
        "updated_at": updated_at or "",
        "last_updated": updated_at or "",
        "unt_alumni_status": row.get("unt_alumni_status") or compute_unt_alumni_status_from_row(row),
    }
    return item


@alumni_bp.route("/alumni")
@login_required
def alumni_page():
    return send_from_directory("../frontend/public", "alumni.html")


@alumni_bp.route("/events")
@login_required
def events_page():
    return send_from_directory("../frontend/public", "events.html")


@alumni_bp.route("/events.js")
def serve_events_js():
    return send_from_directory("../frontend/public", "events.js")


@alumni_bp.route("/events_style.css")
def serve_events_css():
    return send_from_directory("../frontend/public", "events_style.css")


@alumni_bp.route("/api/alumni", methods=["GET"])
def api_get_alumni():
    try:
        limit = int(request.args.get("limit", 250))
    except Exception:
        limit = 250
    limit = max(1, min(limit, 500))

    try:
        offset = int(request.args.get("offset", 0))
    except Exception:
        offset = 0
    offset = max(0, offset)

    location_filters = _parse_multi_value_param("location", split_commas=False)
    role_filters = _parse_multi_value_param("role", split_commas=False)
    company_filters = _parse_multi_value_param("company", split_commas=False)
    major_filters_raw = _parse_multi_value_param("major", split_commas=False)
    standardized_major_filters = _parse_multi_value_param("standardized_major", split_commas=False)
    degree_filters = _parse_multi_value_param("degree", split_commas=False)
    seniority_filters = _parse_multi_value_param("seniority", split_commas=False)
    major_logic = (request.args.get("major_logic", "and") or "and").strip().lower()
    if major_logic not in {"and", "or"}:
        return jsonify({"error": "Invalid major_logic. Use 'and' or 'or'."}), 400

    query_text = (request.args.get("q", "") or "").strip().lower()
    wws_filter = (request.args.get("working_while_studying", "") or "").strip().lower()
    if wws_filter and wws_filter not in {"yes", "no"}:
        return jsonify({"error": "Invalid working_while_studying. Use yes or no."}), 400

    try:
        exp_min = _parse_optional_non_negative_int("exp_min")
        exp_max = _parse_optional_non_negative_int("exp_max")
        _validate_min_max(exp_min, exp_max, "exp_min", "exp_max")
    except ValueError as e:
        field = "exp_min" if "exp_min" in str(e) else "exp_max"
        return _validation_error(str(e), field)

    include_unknown_experience = (request.args.get("include_unknown_experience", "0") or "0").strip().lower() in {"1", "true", "yes"}

    try:
        unt_alumni_status_filter = _parse_unt_alumni_status_filter(request.args.get("unt_alumni_status", ""))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    for value in seniority_filters:
        if value not in _SENIORITY_ALLOWED:
            return jsonify({"error": "Invalid seniority. Use Intern, Mid, Senior, Manager, or Executive."}), 400

    try:
        grad_year_from = _parse_optional_non_negative_int("grad_year_from")
        grad_year_to = _parse_optional_non_negative_int("grad_year_to")
        _validate_min_max(grad_year_from, grad_year_to, "grad_year_from", "grad_year_to")
    except ValueError as e:
        field = "grad_year_from" if "grad_year_from" in str(e) else "grad_year_to"
        return _validation_error(str(e), field)

    bookmarked_only = (request.args.get("bookmarked_only", "0") or "0").strip().lower() in {"1", "true", "yes"}
    if bookmarked_only:
        uid = _app_mod().get_current_user_id()
        if not uid:
            return jsonify({"error": "User not found"}), 401

    sort_key = (request.args.get("sort", "name") or "name").strip().lower()
    direction = (request.args.get("direction", "asc") or "asc").strip().lower()
    direction_sql = "DESC" if direction == "desc" else "ASC"
    if sort_key == "year":
        order_clause = f"CASE WHEN a.grad_year IS NULL THEN 1 ELSE 0 END ASC, a.grad_year {direction_sql}, LOWER(a.first_name) {direction_sql}, LOWER(a.last_name) {direction_sql}"
    elif sort_key == "updated":
        order_clause = f"CASE WHEN a.updated_at IS NULL THEN 1 ELSE 0 END ASC, a.updated_at {direction_sql}, LOWER(a.first_name) {direction_sql}, LOWER(a.last_name) {direction_sql}"
    else:
        order_clause = f"LOWER(a.first_name) {direction_sql}, LOWER(a.last_name) {direction_sql}"

    where_clauses = []
    params = []

    raw_grad_year = (request.args.get("grad_year", "") or "").strip()
    if raw_grad_year:
        years = []
        for part in raw_grad_year.split(","):
            p = part.strip()
            if not p:
                continue
            if not p.isdigit() or len(p) != 4:
                return jsonify({"error": "Invalid grad_year. Use a 4-digit year."}), 400
            years.append(int(p))
        if years:
            if len(years) == 1:
                where_clauses.append("a.grad_year = %s")
                params.append(years[0])
            else:
                where_clauses.append("a.grad_year IN (" + ",".join(["%s"] * len(years)) + ")")
                params.extend(years)

    if grad_year_from is not None:
        where_clauses.append("a.grad_year >= %s")
        params.append(grad_year_from)
    if grad_year_to is not None:
        where_clauses.append("a.grad_year <= %s")
        params.append(grad_year_to)

    if location_filters:
        where_clauses.append("a.location IN (" + ",".join(["%s"] * len(location_filters)) + ")")
        params.extend(location_filters)

    normalized_disciplines = []
    for m in major_filters_raw:
        d = _normalize_requested_discipline(m)
        if d:
            normalized_disciplines.append(d)

    has_discipline_filters = bool(normalized_disciplines)
    has_standardized_major_filters = bool(standardized_major_filters)
    if has_discipline_filters and has_standardized_major_filters:
        if major_logic == "or":
            where_clauses.append(
                "(("
                + " OR ".join(["LOWER(a.discipline) = LOWER(%s)"] * len(normalized_disciplines))
                + ") OR ("
                + " OR ".join(["LOWER(a.standardized_major) = LOWER(%s)"] * len(standardized_major_filters))
                + "))"
            )
            params.extend(normalized_disciplines)
            params.extend(standardized_major_filters)
        else:
            where_clauses.append("(" + " OR ".join(["LOWER(a.discipline) = LOWER(%s)"] * len(normalized_disciplines)) + ")")
            params.extend(normalized_disciplines)
            where_clauses.append("(" + " OR ".join(["LOWER(a.standardized_major) = LOWER(%s)"] * len(standardized_major_filters)) + ")")
            params.extend(standardized_major_filters)
    elif has_discipline_filters:
        where_clauses.append("(" + " OR ".join(["LOWER(a.discipline) = LOWER(%s)"] * len(normalized_disciplines)) + ")")
        params.extend(normalized_disciplines)
    elif has_standardized_major_filters:
        where_clauses.append("(" + " OR ".join(["LOWER(a.standardized_major) = LOWER(%s)"] * len(standardized_major_filters)) + ")")
        params.extend(standardized_major_filters)

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    count_query = f"SELECT COUNT(*) AS total FROM alumni a{where_sql}"
    select_query = f"""
        SELECT a.id, a.first_name, a.last_name,
               a.linkedin_url, a.school, a.school_start_date,
               a.degree, a.major, a.discipline, a.standardized_major,
               a.grad_year, a.school2, a.school3, a.degree2, a.degree3, a.major2, a.major3,
               a.location, a.current_job_title, a.headline, a.company,
               a.job_employment_type, a.job_start_date, a.job_end_date,
               a.exp2_title, a.exp2_company, a.exp2_dates, a.exp2_employment_type,
               a.exp3_title, a.exp3_company, a.exp3_dates, a.exp3_employment_type,
               a.working_while_studying, a.working_while_studying_status,
               a.seniority_level, a.relevant_experience_months,
             a.latitude, a.longitude,
               a.created_at, a.updated_at,
               njt.normalized_title AS normalized_job_title
        FROM alumni a
        LEFT JOIN normalized_job_titles njt ON a.normalized_job_title_id = njt.id
        {where_sql}
        ORDER BY {order_clause}
        LIMIT %s OFFSET %s
    """

    conn = _app_mod().get_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute(count_query, params)
            db_total = _fetchone_dict(cur, "total")

            cur.execute(select_query, params + [limit, offset])
            rows = cur.fetchall() or []

            python_side_filters = bool(
                unt_alumni_status_filter
                or seniority_filters
                or role_filters
                or company_filters
                or query_text
                or degree_filters
                or wws_filter
                or exp_min is not None
                or exp_max is not None
            )
            if python_side_filters:
                # Preserve legacy behavior: apply these filters after row materialization,
                # then paginate in Python for deterministic behavior in tests.
                full_query = f"""
                    SELECT a.id, a.first_name, a.last_name,
                           a.linkedin_url, a.school, a.school_start_date,
                           a.degree, a.major, a.discipline, a.standardized_major,
                           a.grad_year, a.school2, a.school3, a.degree2, a.degree3, a.major2, a.major3,
                           a.location, a.current_job_title, a.headline, a.company,
                           a.job_employment_type, a.job_start_date, a.job_end_date,
                           a.exp2_title, a.exp2_company, a.exp2_dates, a.exp2_employment_type,
                           a.exp3_title, a.exp3_company, a.exp3_dates, a.exp3_employment_type,
                           a.working_while_studying, a.working_while_studying_status,
                           a.seniority_level, a.relevant_experience_months,
                           a.latitude, a.longitude,
                           a.created_at, a.updated_at,
                           njt.normalized_title AS normalized_job_title
                    FROM alumni a
                    LEFT JOIN normalized_job_titles njt ON a.normalized_job_title_id = njt.id
                    {where_sql}
                    ORDER BY {order_clause}
                """
                cur.execute(full_query, params)
                all_rows = cur.fetchall() or []

                requested_role_set = {_canonical_role_title(v) for v in role_filters if (v or "").strip()}
                requested_company_set = {_canonical_company_name(v) for v in company_filters if (v or "").strip()}
                requested_degree_set = {v for v in degree_filters if v in {"Bachelors", "Masters", "PhD"}}
                filtered = []
                for row in all_rows:
                    row_status = compute_unt_alumni_status_from_row(row)
                    row_bucket = row.get("seniority_level") or classify_seniority_bucket(row.get("current_job_title"), None)
                    row_role = _canonical_role_title(
                        row.get("normalized_job_title")
                        or row.get("current_job_title")
                        or row.get("headline")
                        or ""
                    )
                    row_company = _canonical_company_name(row.get("company") or "")
                    row_degree = _normalize_degree_to_filter_label(row.get("degree") or "")
                    row_wws = _normalize_working_while_studying_value(
                        row.get("working_while_studying_status")
                        or row.get("working_while_studying")
                    )
                    row_exp_months = row.get("relevant_experience_months")
                    if row_exp_months is not None:
                        try:
                            row_exp_months = int(row_exp_months)
                        except (TypeError, ValueError):
                            row_exp_months = None

                    if unt_alumni_status_filter and row_status != unt_alumni_status_filter:
                        continue
                    if seniority_filters and row_bucket not in seniority_filters:
                        continue
                    if requested_role_set and row_role not in requested_role_set:
                        continue
                    if requested_company_set and row_company not in requested_company_set:
                        continue
                    if requested_degree_set and row_degree not in requested_degree_set:
                        continue
                    if wws_filter and row_wws != wws_filter:
                        continue
                    if exp_min is not None or exp_max is not None:
                        if row_exp_months is None:
                            if not include_unknown_experience:
                                continue
                        else:
                            if exp_min is not None and row_exp_months < exp_min:
                                continue
                            if exp_max is not None and row_exp_months > exp_max:
                                continue
                    if query_text:
                        haystack = " ".join(
                            [
                                str(row.get("first_name") or ""),
                                str(row.get("last_name") or ""),
                                str(row.get("current_job_title") or ""),
                                str(row.get("headline") or ""),
                                str(row.get("company") or ""),
                                str(row.get("location") or ""),
                                str(row.get("discipline") or ""),
                                str(row.get("standardized_major") or ""),
                                str(row_role or ""),
                            ]
                        ).lower()
                        if query_text not in haystack:
                            continue
                    row["unt_alumni_status"] = row_status
                    row["seniority_level"] = row_bucket
                    filtered.append(row)

                total = len(filtered)
                rows = filtered[offset:offset + limit]
            else:
                total = db_total

        items = [_map_alumni_item(row) for row in rows]
        has_more = (offset + len(items)) < total
        return jsonify(
            {
                "success": True,
                "items": items,
                "alumni": items,
                "total": total,
                "count": len(items),
                "has_more": has_more,
                "limit": limit,
                "offset": offset,
            }
        ), 200
    except Exception as err:
        return jsonify({"error": f"Database error: {str(err)}"}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


@alumni_bp.route("/api/alumni/filter", methods=["GET"])
def api_alumni_filter_alias():
    try:
        unt_alumni_status_filter = _parse_unt_alumni_status_filter(request.args.get("unt_alumni_status", ""))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    grad_year_raw = (request.args.get("grad_year", "") or "").strip()
    location_filters = _parse_multi_value_param("location", split_commas=False)
    major_filters_raw = _parse_multi_value_param("major", split_commas=False)

    where_clauses = []
    params = []

    if grad_year_raw:
        if not grad_year_raw.isdigit() or len(grad_year_raw) != 4:
            return jsonify({"error": "Invalid grad_year. Use a 4-digit year."}), 400
        where_clauses.append("a.grad_year = %s")
        params.append(int(grad_year_raw))

    if location_filters:
        where_clauses.append("a.location IN (" + ",".join(["%s"] * len(location_filters)) + ")")
        params.extend(location_filters)

    normalized_disciplines = []
    for m in major_filters_raw:
        d = _normalize_requested_discipline(m)
        if d:
            normalized_disciplines.append(d)
    if normalized_disciplines:
        where_clauses.append("(" + " OR ".join(["LOWER(a.discipline) = LOWER(%s)"] * len(normalized_disciplines)) + ")")
        params.extend(normalized_disciplines)

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    query = f"""
        SELECT a.id, a.first_name, a.last_name,
               a.linkedin_url, a.school, a.school_start_date,
               a.degree, a.major, a.discipline, a.standardized_major,
               a.grad_year, a.school2, a.school3, a.degree2, a.degree3, a.major2, a.major3,
               a.location, a.current_job_title, a.headline, a.company,
               a.job_employment_type, a.job_start_date, a.job_end_date,
               a.exp2_title, a.exp2_company, a.exp2_dates, a.exp2_employment_type,
               a.exp3_title, a.exp3_company, a.exp3_dates, a.exp3_employment_type,
               a.working_while_studying, a.working_while_studying_status,
               a.seniority_level, a.relevant_experience_months,
             a.latitude, a.longitude,
               a.created_at, a.updated_at,
               njt.normalized_title AS normalized_job_title
        FROM alumni a
        LEFT JOIN normalized_job_titles njt ON a.normalized_job_title_id = njt.id
        {where_sql}
        ORDER BY LOWER(a.first_name) ASC, LOWER(a.last_name) ASC
    """

    conn = _app_mod().get_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute(query, params)
            rows = cur.fetchall() or []

        items = []
        for row in rows:
            row_status = compute_unt_alumni_status_from_row(row)
            if unt_alumni_status_filter and row_status != unt_alumni_status_filter:
                continue
            row["unt_alumni_status"] = row_status
            items.append(_map_alumni_item(row))

        return jsonify({"success": True, "alumni": items, "items": items, "count": len(items), "total": len(items)}), 200
    except Exception as err:
        return jsonify({"error": f"Database error: {str(err)}"}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


@alumni_bp.route("/api/alumni/<int:alumni_id>", methods=["GET"])
def api_get_alumni_detail(alumni_id):
    conn = _app_mod().get_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute("SELECT * FROM alumni WHERE id = %s", (alumni_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Alumni not found"}), 404
            row["unt_alumni_status"] = row.get("unt_alumni_status") or compute_unt_alumni_status_from_row(row)
            return jsonify({"success": True, "alumni": row}), 200
    except Exception as err:
        return jsonify({"error": f"Database error: {str(err)}"}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


@alumni_bp.route("/api/alumni/<int:alumni_id>", methods=["PUT"])
def api_update_alumni(alumni_id):
    data = request.get_json(silent=True) or {}
    updates = {}
    for key in ["first_name", "last_name", "company", "current_job_title", "headline", "location", "linkedin_url"]:
        if key in data:
            updates[key] = data.get(key)

    if not updates:
        return jsonify({"error": "No updatable fields provided."}), 400

    if updates.get("location"):
        try:
            coords = geocode_location(updates.get("location"))
            if coords:
                updates["latitude"] = coords[0]
                updates["longitude"] = coords[1]
        except Exception:
            pass

    conn = _app_mod().get_connection()
    try:
        cols = list(updates.keys())
        vals = [updates[c] for c in cols]
        set_sql = ", ".join([f"{c} = %s" for c in cols])
        with conn.cursor() as cur:
            cur.execute(f"UPDATE alumni SET {set_sql} WHERE id = %s", vals + [alumni_id])
            conn.commit()
        return jsonify({"success": True, "message": "Alumni updated."}), 200
    except Exception as err:
        return jsonify({"error": f"Database error: {str(err)}"}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


@alumni_bp.route("/api/alumni/majors", methods=["GET"])
def api_get_majors():
    conn = _app_mod().get_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute(
                """
                SELECT DISTINCT standardized_major
                FROM alumni
                WHERE standardized_major IS NOT NULL AND standardized_major <> ''
                ORDER BY standardized_major ASC
                """
            )
            rows = cur.fetchall() or []
            majors = [r.get("standardized_major") for r in rows if r.get("standardized_major")]
            return jsonify({"success": True, "majors": majors}), 200
    except Exception as err:
        return jsonify({"error": f"Database error: {str(err)}"}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


@alumni_bp.route("/api/alumni/filter-options", methods=["GET"])
def api_filter_options():
    if _app_mod().app.config.get("DISABLE_DB") and not _app_mod().app.config.get("USE_SQLITE_FALLBACK"):
        return jsonify({"success": True, "field": "location", "query": "", "options": [], "count": 0}), 200

    field = (request.args.get("field", "location") or "location").strip().lower()
    q = (request.args.get("q", "") or "").strip()
    try:
        limit = int(request.args.get("limit", 15))
    except Exception:
        limit = 15
    limit = max(1, min(100, limit))

    field_map = {"location": "location", "company": "company", "role": "normalized_job_title"}
    column = field_map.get(field)
    if not column:
        return jsonify({"success": False, "error": "Invalid field. Use 'location', 'company', or 'role'."}), 400

    excludes = set()
    for raw in request.args.getlist("exclude"):
        for part in (raw or "").split(","):
            cleaned = part.strip()
            if cleaned:
                excludes.add(cleaned.lower())

    conn = _app_mod().get_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
            if field == "role":
                sql = """
                    SELECT njt.normalized_title AS option_value, COUNT(*) AS option_count
                    FROM alumni a
                    JOIN normalized_job_titles njt ON a.normalized_job_title_id = njt.id
                    WHERE njt.normalized_title IS NOT NULL
                      AND TRIM(njt.normalized_title) <> ''
                """
                params = []
                if q:
                    sql += " AND LOWER(njt.normalized_title) LIKE %s"
                    params.append(f"%{q.lower()}%")
                sql += " GROUP BY njt.normalized_title"
                cur.execute(sql, tuple(params))
                rows = cur.fetchall() or []
                counts = {}
                for row in rows:
                    value = (row.get("option_value") or "").strip()
                    if not value or value.lower() in excludes:
                        continue
                    counts[value] = int(row.get("option_count") or 0)
            else:
                sql = f"""
                    SELECT a.{column} AS option_value
                    FROM alumni a
                    WHERE a.{column} IS NOT NULL
                      AND TRIM(a.{column}) <> ''
                """
                params = []
                if q:
                    sql += f" AND LOWER(a.{column}) LIKE %s"
                    params.append(f"%{q.lower()}%")

                cur.execute(sql, tuple(params))
                rows = cur.fetchall() or []

                counts = {}
                for row in rows:
                    value = (row.get("option_value") or "").strip()
                    if not value:
                        continue
                    if value.lower() in excludes:
                        continue
                    counts[value] = counts.get(value, 0) + 1

        options = rank_filter_option_counts(counts, query=q, limit=limit)
        return jsonify({"success": True, "field": field, "query": q, "options": options, "count": len(options)}), 200
    except Exception as err:
        return jsonify({"success": False, "error": f"Database error: {str(err)}"}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass
