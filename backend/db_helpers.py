"""Compatibility shim for DB helper utilities.

Canonical implementations now live in db_core_common.
"""

try:
    from .db_core_common import (
        is_sqlite_connection,
        adapt_sql_parameter_style,
        execute_sql,
        managed_db_cursor,
    )
except ImportError:
    from db_core_common import (
        is_sqlite_connection,
        adapt_sql_parameter_style,
        execute_sql,
        managed_db_cursor,
    )


__all__ = [
    "is_sqlite_connection",
    "adapt_sql_parameter_style",
    "execute_sql",
    "managed_db_cursor",
]