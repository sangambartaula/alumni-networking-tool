"""Facade for split database core implementation."""

try:
    from .db_core_common import *
    from .db_core_common import (
        _clean_optional_text,
        _truncate_optional_text,
        _csv_optional_str,
        _parse_float,
        _parse_bool,
        _parse_int,
        _normalize_person_name,
        _sanitize_major_and_discipline,
        _append_flagged_review_urls,
        _get_or_create_normalized_entity,
        _coerce_grad_year,
        _infer_grad_year_from_school_start_date,
        _normalize_primary_education_dates,
    )
    from .db_core_schema import *
    from .db_core_profiles import *
    from .db_core_scrape import *
    from .db_core_scrape import _build_alumni_upsert_payload, _upsert_alumni_payload
    from .db_core_maintenance import *
except ImportError:
    from db_core_common import *
    from db_core_common import (
        _clean_optional_text,
        _truncate_optional_text,
        _csv_optional_str,
        _parse_float,
        _parse_bool,
        _parse_int,
        _normalize_person_name,
        _sanitize_major_and_discipline,
        _append_flagged_review_urls,
        _get_or_create_normalized_entity,
        _coerce_grad_year,
        _infer_grad_year_from_school_start_date,
        _normalize_primary_education_dates,
    )
    from db_core_schema import *
    from db_core_profiles import *
    from db_core_scrape import *
    from db_core_scrape import _build_alumni_upsert_payload, _upsert_alumni_payload
    from db_core_maintenance import *
