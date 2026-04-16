from flask import Blueprint, current_app, jsonify, request, send_from_directory

from database import get_connection
from geocoding import geocode_location
from middleware import login_required
from unt_alumni_status import compute_unt_alumni_status_from_row


alumni_bp = Blueprint("alumni", __name__)


@alumni_bp.route("/alumni")
@login_required
def alumni_page():
    return send_from_directory("../frontend/public", "alumni.html")


@alumni_bp.route("/heatmap")
@login_required
def heatmap_page():
    return send_from_directory("../frontend/public", "heatmap.html")


@alumni_bp.route("/analytics")
@login_required
def analytics_page():
    return send_from_directory("../frontend/public", "analytics.html")


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


@alumni_bp.route("/heatmap.js")
def serve_heatmap_js():
    return send_from_directory("../frontend/public", "heatmap.js")


@alumni_bp.route("/heatmap_style.css")
def serve_heatmap_css():
    return send_from_directory("../frontend/public", "heatmap_style.css")


def _fetch_rows_as_dict(cur):
    rows = cur.fetchall() or []
    if rows and isinstance(rows[0], dict):
        return rows
    if not rows:
        return []
    columns = [c[0] for c in getattr(cur, "description", [])]
    if not columns:
        return []
    return [dict(zip(columns, r)) for r in rows]


@alumni_bp.route("/api/alumni", methods=["GET"])
def api_get_alumni():
    if current_app.config.get("DISABLE_DB") and not current_app.config.get("USE_SQLITE_FALLBACK"):
        return jsonify(
            {
                "success": True,
                "items": [],
                "alumni": [],
                "total": 0,
                "has_more": False,
                "limit": 0,
                "offset": 0,
            }
        ), 200

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

    search_term = (request.args.get("q", "") or "").strip().lower()

    conn = get_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
            where = []
            params = []
            if search_term:
                like = f"%{search_term}%"
                where.append(
                    "(LOWER(name) LIKE %s OR LOWER(current_role) LIKE %s OR LOWER(company) LIKE %s OR LOWER(location) LIKE %s)"
                )
                params.extend([like, like, like, like])

            where_sql = f"WHERE {' AND '.join(where)}" if where else ""

            count_sql = f"SELECT COUNT(*) AS total FROM alumni {where_sql}"
            cur.execute(count_sql, tuple(params))
            count_row = cur.fetchone() or {}
            total = int(count_row.get("total", 0) or 0)

            data_sql = (
                "SELECT * FROM alumni "
                f"{where_sql} "
                "ORDER BY name ASC "
                "LIMIT %s OFFSET %s"
            )
            cur.execute(data_sql, tuple(params + [limit, offset]))
            items = cur.fetchall() or []
    except Exception:
        # SQLite fallback query style.
        try:
            cur = conn.cursor()
            where = []
            params = []
            if search_term:
                like = f"%{search_term}%"
                where.append(
                    "(LOWER(name) LIKE ? OR LOWER(current_role) LIKE ? OR LOWER(company) LIKE ? OR LOWER(location) LIKE ?)"
                )
                params.extend([like, like, like, like])

            where_sql = f"WHERE {' AND '.join(where)}" if where else ""
            cur.execute(f"SELECT COUNT(*) AS total FROM alumni {where_sql}", tuple(params))
            row = cur.fetchone()
            if isinstance(row, dict):
                total = int(row.get("total", 0) or 0)
            elif isinstance(row, tuple):
                total = int(row[0] or 0)
            else:
                total = 0

            cur.execute(
                f"SELECT * FROM alumni {where_sql} ORDER BY name ASC LIMIT ? OFFSET ?",
                tuple(params + [limit, offset]),
            )
            items = _fetch_rows_as_dict(cur)
        except Exception as err:
            return jsonify({"error": f"Database error: {str(err)}"}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass

    for row in items:
        if "unt_alumni_status" not in row or not row.get("unt_alumni_status"):
            row["unt_alumni_status"] = compute_unt_alumni_status_from_row(row)

    has_more = (offset + len(items)) < total
    return jsonify(
        {
            "success": True,
            "items": items,
            "alumni": items,
            "total": total,
            "has_more": has_more,
            "limit": limit,
            "offset": offset,
        }
    ), 200


@alumni_bp.route("/api/alumni/<int:alumni_id>", methods=["GET"])
def api_get_alumni_detail(alumni_id):
    conn = get_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute("SELECT * FROM alumni WHERE id = %s", (alumni_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Alumni not found"}), 404
            row["unt_alumni_status"] = row.get("unt_alumni_status") or compute_unt_alumni_status_from_row(row)
            return jsonify({"success": True, "alumni": row}), 200
    except Exception:
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM alumni WHERE id = ?", (alumni_id,))
            rows = _fetch_rows_as_dict(cur)
            if not rows:
                return jsonify({"error": "Alumni not found"}), 404
            row = rows[0]
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
    allowed = {
        "name",
        "company",
        "current_role",
        "location",
        "headline",
        "linkedin_url",
    }
    updates = {k: v for k, v in data.items() if k in allowed}
    if not updates:
        return jsonify({"error": "No updatable fields provided."}), 400

    if "location" in updates and updates.get("location"):
        try:
            coords = geocode_location(updates["location"])
            if coords:
                updates["latitude"] = coords[0]
                updates["longitude"] = coords[1]
        except Exception:
            pass

    conn = get_connection()
    try:
        cols = list(updates.keys())
        vals = [updates[c] for c in cols]
        set_sql = ", ".join([f"{c} = %s" for c in cols])
        with conn.cursor() as cur:
            cur.execute(f"UPDATE alumni SET {set_sql} WHERE id = %s", tuple(vals + [alumni_id]))
            conn.commit()
        return jsonify({"success": True, "message": "Alumni updated."}), 200
    except Exception:
        try:
            set_sql = ", ".join([f"{c} = ?" for c in updates.keys()])
            vals = list(updates.values())
            cur = conn.cursor()
            cur.execute(f"UPDATE alumni SET {set_sql} WHERE id = ?", tuple(vals + [alumni_id]))
            conn.commit()
            return jsonify({"success": True, "message": "Alumni updated."}), 200
        except Exception as err:
            try:
                conn.rollback()
            except Exception:
                pass
            return jsonify({"error": f"Database error: {str(err)}"}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


@alumni_bp.route("/api/alumni/majors", methods=["GET"])
def api_get_majors():
    conn = get_connection()
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
    conn = get_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute("SELECT DISTINCT location FROM alumni WHERE location IS NOT NULL AND location <> '' ORDER BY location ASC")
            locations = [r.get("location") for r in (cur.fetchall() or []) if r.get("location")]

            cur.execute("SELECT DISTINCT company FROM alumni WHERE company IS NOT NULL AND company <> '' ORDER BY company ASC")
            companies = [r.get("company") for r in (cur.fetchall() or []) if r.get("company")]

            cur.execute("SELECT DISTINCT current_role FROM alumni WHERE current_role IS NOT NULL AND current_role <> '' ORDER BY current_role ASC")
            roles = [r.get("current_role") for r in (cur.fetchall() or []) if r.get("current_role")]

            cur.execute("SELECT DISTINCT standardized_major FROM alumni WHERE standardized_major IS NOT NULL AND standardized_major <> '' ORDER BY standardized_major ASC")
            majors = [r.get("standardized_major") for r in (cur.fetchall() or []) if r.get("standardized_major")]

        return jsonify(
            {
                "success": True,
                "locations": locations,
                "companies": companies,
                "roles": roles,
                "majors": majors,
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
    return api_get_alumni()
