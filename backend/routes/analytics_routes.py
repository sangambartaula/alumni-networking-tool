import importlib
import time as _time
import sys

import mysql.connector
from flask import Blueprint, jsonify, request, send_from_directory
from geocoding import search_location_candidates

from routes.alumni_routes import (
    _parse_multi_value_param,
    _parse_optional_non_negative_int,
    _parse_unt_alumni_status_filter,
    _validate_min_max,
    classify_seniority_bucket,
)
from middleware import api_login_required, login_required
from unt_alumni_status import compute_unt_alumni_status_from_row
from db_core_common import UNT_ALLOWED_MAJORS


analytics_bp = Blueprint("analytics", __name__)

_heatmap_cache = {}
_HEATMAP_CACHE_TTL = 60


def _app_mod():
    return sys.modules.get("app") or sys.modules.get("__main__") or importlib.import_module("app")


def _is_meaningful_location_label(value):
    text = (value or "").strip()
    if not text:
        return False
    return text.lower() not in {"not found", "unknown", "n/a"}


def classify_degree(degree, _headline):
    t = (degree or "").lower()
    if "phd" in t or "doctor" in t:
        return "PhD"
    if "master" in t or "m.s" in t or "ms" in t:
        return "Graduate"
    return "Undergraduate"


def _normalize_degree_to_filter_label(value):
    normalized = (value or "").strip().lower()
    if not normalized:
        return ""
    if normalized in {"undergraduate", "bachelors"}:
        return "Bachelors"
    if normalized in {"graduate", "masters"}:
        return "Masters"
    if normalized == "phd":
        return "PhD"
    return ""


def get_continent(lat, lon):
    if lat >= 5 and lat <= 83 and lon >= -170 and lon <= -52:
        return "North America"
    if lat >= -56 and lat <= 13 and lon >= -81 and lon <= -34:
        return "South America"
    if lat >= 35 and lat <= 71 and lon >= -25 and lon <= 45:
        return "Europe"
    if lat >= 1 and lat <= 77 and lon >= 26 and lon <= 180:
        return "Asia"
    if lat >= -35 and lat <= 38 and lon >= -20 and lon <= 52:
        return "Africa"
    if lat >= -50 and lat <= 0 and lon >= 110 and lon <= 180:
        return "Oceania"
    if lat <= -60:
        return "Antarctica"
    return "Unknown"


@analytics_bp.route("/heatmap")
@login_required
def heatmap_page():
    return send_from_directory("../frontend/public", "heatmap.html")


@analytics_bp.route("/analytics")
@login_required
def analytics_page():
    return send_from_directory("../frontend/public", "analytics.html")


@analytics_bp.route("/heatmap.js")
def serve_heatmap_js():
    return send_from_directory("../frontend/public", "heatmap.js")


@analytics_bp.route("/heatmap_style.css")
def serve_heatmap_css():
    return send_from_directory("../frontend/public", "heatmap_style.css")


@analytics_bp.route("/api/heatmap", methods=["GET"])
def get_heatmap_data():
    continent_filter = request.args.get("continent") or None
    try:
        unt_alumni_status_filter = _parse_unt_alumni_status_filter(request.args.get("unt_alumni_status", ""))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    try:
        grad_year_from = _parse_optional_non_negative_int("grad_year_from")
        grad_year_to = _parse_optional_non_negative_int("grad_year_to")
        _validate_min_max(grad_year_from, grad_year_to, "grad_year_from", "grad_year_to")
    except ValueError as e:
        field = "grad_year_from" if "grad_year_from" in str(e) else "grad_year_to"
        return jsonify({"error": {"field": field, "message": str(e)}}), 400

    heatmap_seniority_filters = _parse_multi_value_param("seniority")
    for value in heatmap_seniority_filters:
        if value not in {"Intern", "Mid", "Senior", "Manager", "Executive"}:
            return jsonify({"error": "Invalid seniority. Use Intern, Mid, Senior, Manager, or Executive."}), 400
    heatmap_seniority_filter_set = set(heatmap_seniority_filters)

    heatmap_major_filters = _parse_multi_value_param("standardized_major")
    approved_major_values = {m for m in UNT_ALLOWED_MAJORS if m and m != "Other"}
    for value in heatmap_major_filters:
        if value not in approved_major_values:
            return jsonify({"error": "Invalid standardized_major value."}), 400
    heatmap_major_filter_set = set(heatmap_major_filters)

    heatmap_degree_filters = _parse_multi_value_param("degree")
    for value in heatmap_degree_filters:
        if value not in {"Bachelors", "Masters", "PhD"}:
            return jsonify({"error": "Invalid degree. Use Bachelors, Masters, or PhD."}), 400
    heatmap_degree_filter_set = set(heatmap_degree_filters)

    if (
        _app_mod().app.config.get("DISABLE_DB")
        and not _app_mod().app.config.get("USE_SQLITE_FALLBACK")
    ):
        return jsonify({"success": True, "locations": [], "total_alumni": 0, "max_count": 0}), 200

    use_cache = (
        grad_year_from is None
        and grad_year_to is None
        and not heatmap_seniority_filter_set
        and not heatmap_major_filter_set
        and not heatmap_degree_filter_set
    )
    cache_key = f"{continent_filter or '__all__'}|{unt_alumni_status_filter or '__all__'}"
    if use_cache:
        cached = _heatmap_cache.get(cache_key)
        if cached and (_time.time() - cached["ts"]) < _HEATMAP_CACHE_TTL:
            return jsonify(cached["data"]), 200

    try:
        conn = _app_mod().get_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute(
                    """
                    SELECT id, first_name, last_name, location,
                           latitude, longitude, current_job_title, headline,
                           company, linkedin_url, created_at, grad_year,
                           school, school2, school3, degree, degree2, degree3,
                           major, major2, major3,
                           standardized_major, standardized_major_alt,
                           standardized_major2, standardized_major3,
                           seniority_level
                    FROM alumni
                    ORDER BY location ASC
                    """
                )
                rows = cur.fetchall() or []

            location_clusters = {}
            location_details = {}
            total_mapped_alumni = 0
            total_filtered_alumni = 0
            missing_location_count = 0
            ungeocoded_with_location_count = 0

            for row in rows:
                if grad_year_from is not None and row.get("grad_year") is not None and row.get("grad_year") < grad_year_from:
                    continue
                if grad_year_to is not None and row.get("grad_year") is not None and row.get("grad_year") > grad_year_to:
                    continue

                unt_alumni_status = compute_unt_alumni_status_from_row(row)
                if unt_alumni_status_filter and unt_alumni_status != unt_alumni_status_filter:
                    continue

                seniority_bucket = row.get("seniority_level") or classify_seniority_bucket(row.get("current_job_title"), None)
                if heatmap_seniority_filter_set and seniority_bucket not in heatmap_seniority_filter_set:
                    continue

                if heatmap_major_filter_set:
                    row_majors = {
                        (row.get("standardized_major") or "").strip(),
                        (row.get("standardized_major_alt") or "").strip(),
                        (row.get("standardized_major2") or "").strip(),
                        (row.get("standardized_major3") or "").strip(),
                    }
                    row_majors.discard("")
                    if not any(m in heatmap_major_filter_set for m in row_majors):
                        continue

                if heatmap_degree_filter_set:
                    degree_label = _normalize_degree_to_filter_label(classify_degree(row.get("degree"), row.get("headline", "")))
                    if degree_label not in heatmap_degree_filter_set:
                        continue

                total_filtered_alumni += 1

                lat = row.get("latitude")
                lon = row.get("longitude")
                if lat is None or lon is None:
                    if (row.get("location") or "").strip().lower() in {"", "not found", "unknown", "n/a"}:
                        missing_location_count += 1
                    else:
                        ungeocoded_with_location_count += 1
                    continue

                continent = get_continent(lat, lon)
                if continent_filter and continent != continent_filter:
                    continue

                total_mapped_alumni += 1
                cluster_key = (round(lat, 3), round(lon, 3))
                if cluster_key not in location_clusters:
                    location_clusters[cluster_key] = 0
                    location_details[cluster_key] = {
                        "location": row.get("location"),
                        "location_counts": {},
                        "latitude": lat,
                        "longitude": lon,
                        "continent": continent,
                        "sample_alumni": [],
                    }

                location_clusters[cluster_key] += 1
                row_location = (row.get("location") or "").strip()
                if _is_meaningful_location_label(row_location):
                    counts = location_details[cluster_key]["location_counts"]
                    counts[row_location] = counts.get(row_location, 0) + 1
                location_details[cluster_key]["sample_alumni"].append(
                    {
                        "id": row.get("id"),
                        "name": f"{row.get('first_name', '')} {row.get('last_name', '')}".strip(),
                        "role": row.get("current_job_title") or row.get("headline") or "Alumni",
                        "company": row.get("company"),
                        "location": row.get("location"),
                        "linkedin": row.get("linkedin_url"),
                        "created_at": row.get("created_at").isoformat()
                        if hasattr(row.get("created_at"), "isoformat")
                        else row.get("created_at"),
                        "unt_alumni_status": unt_alumni_status,
                        "degree": classify_degree(row.get("degree"), row.get("headline", "")),
                        "seniority_level": seniority_bucket,
                        "seniority_bucket": seniority_bucket,
                        "standardized_major": (row.get("standardized_major") or "").strip(),
                        "standardized_major_alt": (row.get("standardized_major_alt") or "").strip(),
                    }
                )

            locations = []
            max_count = 0
            for cluster_key, count in location_clusters.items():
                details = location_details[cluster_key]
                max_count = max(max_count, count)
                location_counts = details.get("location_counts") or {}
                resolved_location = details.get("location") or ""
                if location_counts:
                    resolved_location = sorted(
                        location_counts.items(),
                        key=lambda item: (-item[1], item[0].lower()),
                    )[0][0]
                locations.append(
                    {
                        "latitude": details["latitude"],
                        "longitude": details["longitude"],
                        "location": resolved_location,
                        "continent": details["continent"],
                        "count": count,
                        "sample_alumni": details["sample_alumni"],
                    }
                )

            response_data = {
                "success": True,
                "locations": locations,
                "total_alumni": total_mapped_alumni,
                "total_filtered_alumni": total_filtered_alumni,
                "missing_location_count": missing_location_count,
                "ungeocoded_with_location_count": ungeocoded_with_location_count,
                "max_count": max_count,
            }

            if use_cache:
                _heatmap_cache[cache_key] = {"data": response_data, "ts": _time.time()}

            return jsonify(response_data), 200
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except mysql.connector.Error as err:
        return jsonify({"error": f"Database error: {str(err)}"}), 500
    except Exception as e:
        return jsonify({"error": f"Server error: {str(e)}"}), 500


@analytics_bp.route("/api/geocode")
@api_login_required
def api_geocode():
    query = (request.args.get("q") or "").strip()

    if not query:
        return jsonify({"success": False, "results": []}), 400

    # Keep compatibility with tests that monkeypatch app.search_location_candidates.
    resolver = getattr(_app_mod(), "search_location_candidates", search_location_candidates)
    results = resolver(query)
    return jsonify({"success": True, "count": len(results), "results": results}), 200
