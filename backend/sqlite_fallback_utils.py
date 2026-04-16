try:
    from .sqlite_fallback_core import (
        logger,
        SQLITE_DB_PATH,
        CLOUD_RETRY_INTERVAL,
        SQLITE_TIMEOUT,
        SQLITE_RETRY_COUNT,
        SQLITE_RETRY_DELAY,
        MYSQL_HOST,
        MYSQL_USER,
        MYSQL_PASSWORD,
        MYSQL_DATABASE,
        MYSQL_PORT,
        TABLE_CONFIG,
        utc_now,
        parse_timestamp,
    )
except ImportError:
    from sqlite_fallback_core import (
        logger,
        SQLITE_DB_PATH,
        CLOUD_RETRY_INTERVAL,
        SQLITE_TIMEOUT,
        SQLITE_RETRY_COUNT,
        SQLITE_RETRY_DELAY,
        MYSQL_HOST,
        MYSQL_USER,
        MYSQL_PASSWORD,
        MYSQL_DATABASE,
        MYSQL_PORT,
        TABLE_CONFIG,
        utc_now,
        parse_timestamp,
    )


__all__ = [
    "logger",
    "SQLITE_DB_PATH",
    "CLOUD_RETRY_INTERVAL",
    "SQLITE_TIMEOUT",
    "SQLITE_RETRY_COUNT",
    "SQLITE_RETRY_DELAY",
    "MYSQL_HOST",
    "MYSQL_USER",
    "MYSQL_PASSWORD",
    "MYSQL_DATABASE",
    "MYSQL_PORT",
    "TABLE_CONFIG",
    "utc_now",
    "parse_timestamp",
]
