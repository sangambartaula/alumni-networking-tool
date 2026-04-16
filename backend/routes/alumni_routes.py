import importlib
import re

from flask import Blueprint, jsonify, request, send_from_directory

from geocoding import geocode_location
from middleware import login_required
from unt_alumni_status import compute_unt_alumni_status_from_row
from utils import rank_filter_option_counts


alumni_bp = Blueprint("alumni", __name__)


_SENIORITY_ALLOWED = ["Intern", "Mid", "Senior", "Manager", "Executive"]


def _app_mod():
    return importlib.import_module("app")


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


def _canonical_role_title(value):
    title = (value or "").strip()
    if not title:
        return ""

    without_level_suffix = re.sub(r"\s+(?:level\s*)?(?:i{1,5}|[1-9])$", "", title, flags=re.I).strip()
    without_seniority = re.sub(r"^(?:senior|sr\.?)\s+", "", without_level_suffix, flags=re.I).strip()
    canonical_title = without_seniority or without_level_suffix or title
    low = re.sub(r"\s+", " ", canonical_title.lower())

    if low in {"director", "director of", "director of engineering"}:
        return "Director"
    if low in {
        "manager",
        "manager - innovation",
        "laboratory safety manager",
        "senior manager - innovation",
    }:
        return "Manager"
    if low == "data owner":
        return "Data Analyst"
    if low in {"software developer", "software dev"}:
        return "Software Engineer"
    return canonical_title


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
    item = {
        "id": row.get("id"),
        "first": row.get("first_name") or "",
        "last": row.get("last_name") or "",
        "linkedin_url": row.get("linkedin_url") or "",
        "school": row.get("school"),
        "school_start": row.get("school_start_date"),
        "degree_raw": row.get("degree"),
        "major_raw": row.get("major"),
        "major": (row.get("standardized_major") or row.get("major") or "").strip(),
        "discipline": (row.get("discipline") or "").strip(),
        "grad_year": row.get("grad_year"),
        "school2": row.get("school2"),
        "school3": row.get("school3"),
        "degree2": row.get("degree2"),
        "degree3": row.get("degree3"),
        "major2": row.get("major2"),
        "major3": row.get("major3"),
        "location": row.get("location"),
        "title": row.get("current_job_title") or row.get("headline") or "",
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
    seniority_filters = _parse_multi_value_param("seniority", split_commas=False)

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
    if normalized_disciplines:
        where_clauses.append("(" + " OR ".join(["LOWER(a.discipline) = LOWER(%s)"] * len(normalized_disciplines)) + ")")
        params.extend(normalized_disciplines)

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
               a.created_at, a.updated_at
        FROM alumni a
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
                           a.created_at, a.updated_at
                    FROM alumni a
                    {where_sql}
                    ORDER BY {order_clause}
                """
                cur.execute(full_query, params)
                all_rows = cur.fetchall() or []

                requested_role_set = {_canonical_role_title(v) for v in role_filters if (v or "").strip()}
                requested_company_set = {_canonical_company_name(v) for v in company_filters if (v or "").strip()}
                filtered = []
                for row in all_rows:
                    row_status = compute_unt_alumni_status_from_row(row)
                    row_bucket = row.get("seniority_level") or classify_seniority_bucket(row.get("current_job_title"), None)
                    row_role = _canonical_role_title(row.get("current_job_title") or row.get("headline") or "")
                    row_company = _canonical_company_name(row.get("company") or "")
                    if unt_alumni_status_filter and row_status != unt_alumni_status_filter:
                        continue
                    if seniority_filters and row_bucket not in seniority_filters:
                        continue
                    if requested_role_set and row_role not in requested_role_set:
                        continue
                    if requested_company_set and row_company not in requested_company_set:
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
               a.created_at, a.updated_at
        FROM alumni a
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

    field_map = {"location": "location", "company": "company"}
    column = field_map.get(field)
    if not column:
        return jsonify({"success": False, "error": "Invalid field. Use 'location' or 'company'."}), 400

    excludes = set()
    for raw in request.args.getlist("exclude"):
        for part in (raw or "").split(","):
            cleaned = part.strip()
            if cleaned:
                excludes.add(cleaned.lower())

    conn = _app_mod().get_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
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
