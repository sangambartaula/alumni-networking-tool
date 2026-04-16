import geocoding


class _FakeCursor:
    def __init__(self, records=None, distinct_locations=None, update_counts=None):
        self.records = records or []
        self.distinct_locations = distinct_locations or []
        self.update_counts = update_counts or {}
        self._result = []
        self.rowcount = 0
        self.closed = False

    def execute(self, query, params=None):
        q = " ".join(str(query).split()).lower()
        if "select id, location from alumni" in q:
            self._result = list(self.records)
            self.rowcount = len(self._result)
            return
        if "select distinct location from alumni" in q:
            self._result = [{"location": loc} for loc in self.distinct_locations]
            self.rowcount = len(self._result)
            return
        if "update alumni" in q:
            location = params[2] if params and len(params) >= 3 else None
            self.rowcount = int(self.update_counts.get(location, 1))
            return

        self._result = []
        self.rowcount = 0

    def fetchall(self):
        return list(self._result)

    def close(self):
        self.closed = True


class _FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def cursor(self, dictionary=False):
        return self._cursor

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1

    def close(self):
        self.closed = True


def test_populate_missing_coordinates_updates_only_geocoded_records(monkeypatch):
    cursor = _FakeCursor(
        records=[
            {"id": 1, "location": "Denton, Texas"},
            {"id": 2, "location": "Unknown Place"},
        ]
    )
    conn = _FakeConnection(cursor)

    monkeypatch.setattr(geocoding, "get_connection", lambda: conn)
    monkeypatch.setattr(
        geocoding,
        "geocode_location",
        lambda location: (33.2, -97.1) if location == "Denton, Texas" else None,
    )

    result = geocoding.populate_missing_coordinates()

    assert result == 1
    assert conn.commit_count == 1


def test_verify_and_update_all_coordinates_accumulates_rowcount(monkeypatch):
    cursor = _FakeCursor(
        distinct_locations=["Denton, Texas", "Austin, Texas"],
        update_counts={"Denton, Texas": 2, "Austin, Texas": 0},
    )
    conn = _FakeConnection(cursor)

    monkeypatch.setattr(geocoding, "get_connection", lambda: conn)
    monkeypatch.setattr(
        geocoding,
        "geocode_location",
        lambda _location: (32.0, -96.0),
    )

    result = geocoding.verify_and_update_all_coordinates()

    assert result == 2
    assert conn.commit_count == 1
