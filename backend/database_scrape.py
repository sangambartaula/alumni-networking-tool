try:
    from .database_core import (
        increment_scraper_activity,
        get_scraper_activity,
        create_scrape_run,
        finalize_scrape_run,
        increment_scrape_run_profiles,
        record_scrape_run_flag,
        _build_alumni_upsert_payload,
        _upsert_alumni_payload,
        upsert_scraped_profile,
    )
except ImportError:
    from database_core import (
        increment_scraper_activity,
        get_scraper_activity,
        create_scrape_run,
        finalize_scrape_run,
        increment_scrape_run_profiles,
        record_scrape_run_flag,
        _build_alumni_upsert_payload,
        _upsert_alumni_payload,
        upsert_scraped_profile,
    )


__all__ = [
    "increment_scraper_activity",
    "get_scraper_activity",
    "create_scrape_run",
    "finalize_scrape_run",
    "increment_scrape_run_profiles",
    "record_scrape_run_flag",
    "_build_alumni_upsert_payload",
    "_upsert_alumni_payload",
    "upsert_scraped_profile",
]
