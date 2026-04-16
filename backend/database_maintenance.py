try:
    from .database_core import (
        seed_alumni_data,
        has_alumni_records,
        truncate_dot_fields,
        cleanup_trailing_slashes,
        normalize_existing_grad_years,
        normalize_single_date_education_semantics,
    )
except ImportError:
    from database_core import (
        seed_alumni_data,
        has_alumni_records,
        truncate_dot_fields,
        cleanup_trailing_slashes,
        normalize_existing_grad_years,
        normalize_single_date_education_semantics,
    )


__all__ = [
    "seed_alumni_data",
    "has_alumni_records",
    "truncate_dot_fields",
    "cleanup_trailing_slashes",
    "normalize_existing_grad_years",
    "normalize_single_date_education_semantics",
]
