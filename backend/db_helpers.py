from contextlib import contextmanager


def is_sqlite_connection(connection):
    """Best-effort check for sqlite-backed connections/wrappers."""
    if connection is None:
        return False

    cls = connection.__class__
    name = getattr(cls, "__name__", "").lower()
    module = getattr(cls, "__module__", "").lower()
    if "sqlite" in name or "sqlite" in module:
        return True

    raw_conn = getattr(connection, "_conn", None)
    if raw_conn is not None:
        raw_cls = raw_conn.__class__
        raw_name = getattr(raw_cls, "__name__", "").lower()
        raw_module = getattr(raw_cls, "__module__", "").lower()
        if "sqlite" in raw_name or "sqlite" in raw_module:
            return True

    return False


def adapt_sql_parameter_style(query, use_sqlite):
    """Convert MySQL-style placeholders to sqlite style when needed."""
    if not query or not use_sqlite:
        return query
    return query.replace("%s", "?")


def execute_sql(cursor, query, params=None, connection=None, sqlite_query=None):
    """
    Execute SQL with optional sqlite override and placeholder adaptation.

    This allows callsites to keep MySQL-first SQL while remaining sqlite-safe.
    """
    use_sqlite = is_sqlite_connection(connection)
    sql = sqlite_query if (use_sqlite and sqlite_query) else query
    sql = adapt_sql_parameter_style(sql, use_sqlite and not sqlite_query)

    if params is None:
        return cursor.execute(sql)
    return cursor.execute(sql, params)


@contextmanager
def managed_db_cursor(get_connection_fn, dictionary=False, commit=False):
    """
    Provide a cursor with consistent commit/rollback/close handling.

    - On success: commits if commit=True.
    - On error: rolls back if commit=True and rollback exists.
    - Always closes cursor and connection.
    """
    conn = None
    cursor = None
    try:
        conn = get_connection_fn()
        if dictionary:
            try:
                cursor = conn.cursor(dictionary=True)
            except TypeError:
                cursor = conn.cursor()
        else:
            cursor = conn.cursor()

        yield conn, cursor

        if commit:
            conn.commit()
    except Exception:
        if commit and conn and hasattr(conn, "rollback"):
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if cursor and hasattr(cursor, "close"):
            try:
                cursor.close()
            except Exception:
                pass
        if conn and hasattr(conn, "close"):
            try:
                conn.close()
            except Exception:
                pass