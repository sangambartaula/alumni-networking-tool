"""SQLite fallback public API exports grouped in one module.

This consolidates re-exports from sqlite_fallback_core into one medium-sized
module instead of multiple tiny shim files.
"""

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
        ConnectionManager,
        get_connection_manager,
        get_fallback_status,
        SQLiteConnectionWrapper,
        SQLiteCursorWrapper,
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
        ConnectionManager,
        get_connection_manager,
        get_fallback_status,
        SQLiteConnectionWrapper,
        SQLiteCursorWrapper,
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
    "ConnectionManager",
    "get_connection_manager",
    "get_fallback_status",
    "SQLiteConnectionWrapper",
    "SQLiteCursorWrapper",
]
