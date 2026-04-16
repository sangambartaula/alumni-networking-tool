"""Compatibility facade for sqlite fallback APIs.

This module keeps historical imports stable while implementation is split.
"""

try:
    from . import sqlite_fallback_core as _core
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
    import sqlite_fallback_core as _core
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


def __getattr__(name):
    return getattr(_core, name)


def __dir__():
    return sorted(set(globals().keys()) | set(dir(_core)))


if __name__ == "__main__":
    import os
    import runpy

    runpy.run_path(
        os.path.join(os.path.dirname(__file__), "sqlite_fallback_core.py"),
        run_name="__main__",
    )
