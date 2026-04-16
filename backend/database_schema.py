try:
    from .database_core import (
        init_db,
        ensure_normalized_job_title_column,
        ensure_normalized_degree_column,
        ensure_normalized_company_column,
        ensure_alumni_timestamp_columns,
        ensure_alumni_work_school_date_columns,
        ensure_alumni_major_column,
        ensure_education_columns,
        ensure_experience_analysis_columns,
        ensure_scrape_run_tracking_schema,
        ensure_all_alumni_schema_migrations,
    )
except ImportError:
    from database_core import (
        init_db,
        ensure_normalized_job_title_column,
        ensure_normalized_degree_column,
        ensure_normalized_company_column,
        ensure_alumni_timestamp_columns,
        ensure_alumni_work_school_date_columns,
        ensure_alumni_major_column,
        ensure_education_columns,
        ensure_experience_analysis_columns,
        ensure_scrape_run_tracking_schema,
        ensure_all_alumni_schema_migrations,
    )


__all__ = [
    "init_db",
    "ensure_normalized_job_title_column",
    "ensure_normalized_degree_column",
    "ensure_normalized_company_column",
    "ensure_alumni_timestamp_columns",
    "ensure_alumni_work_school_date_columns",
    "ensure_alumni_major_column",
    "ensure_education_columns",
    "ensure_experience_analysis_columns",
    "ensure_scrape_run_tracking_schema",
    "ensure_all_alumni_schema_migrations",
]
