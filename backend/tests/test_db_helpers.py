from db_helpers import managed_db_cursor, execute_sql


class _RecordingCursor:
    def __init__(self):
        self.calls = []
        self.closed = False

    def execute(self, query, params=None):
        self.calls.append((query, params))
        return None

    def close(self):
        self.closed = True


class _RecordingConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self, dictionary=False):
        return self._cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class _SQLiteConnectionWrapper(_RecordingConnection):
    pass


def test_managed_db_cursor_commits_and_closes_on_success():
    cursor = _RecordingCursor()
    conn = _RecordingConnection(cursor)

    with managed_db_cursor(lambda: conn, commit=True) as (_conn, cur):
        cur.execute("SELECT 1")

    assert conn.committed is True
    assert conn.rolled_back is False
    assert conn.closed is True
    assert cursor.closed is True


def test_managed_db_cursor_rolls_back_on_exception_and_closes():
    cursor = _RecordingCursor()
    conn = _RecordingConnection(cursor)

    try:
        with managed_db_cursor(lambda: conn, commit=True):
            raise RuntimeError("boom")
    except RuntimeError:
        pass

    assert conn.committed is False
    assert conn.rolled_back is True
    assert conn.closed is True
    assert cursor.closed is True


def test_execute_sql_adapts_mysql_placeholders_for_sqlite():
    cursor = _RecordingCursor()
    conn = _SQLiteConnectionWrapper(cursor)

    execute_sql(
        cursor,
        "UPDATE alumni SET latitude = %s, longitude = %s WHERE id = %s",
        (1.0, 2.0, 5),
        connection=conn,
    )

    assert cursor.calls
    query, params = cursor.calls[-1]
    assert query == "UPDATE alumni SET latitude = ?, longitude = ? WHERE id = ?"
    assert params == (1.0, 2.0, 5)


def test_execute_sql_uses_sqlite_override_when_provided():
    cursor = _RecordingCursor()
    conn = _SQLiteConnectionWrapper(cursor)

    execute_sql(
        cursor,
        "DELETE FROM authorized_emails WHERE email = %s",
        ("a@b.com",),
        connection=conn,
        sqlite_query="DELETE FROM authorized_emails WHERE email = ?",
    )

    query, params = cursor.calls[-1]
    assert query == "DELETE FROM authorized_emails WHERE email = ?"
    assert params == ("a@b.com",)
