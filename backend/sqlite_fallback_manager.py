try:
    from .sqlite_fallback_core import (
        ConnectionManager,
        get_connection_manager,
        get_fallback_status,
    )
except ImportError:
    from sqlite_fallback_core import (
        ConnectionManager,
        get_connection_manager,
        get_fallback_status,
    )


__all__ = [
    "ConnectionManager",
    "get_connection_manager",
    "get_fallback_status",
]
