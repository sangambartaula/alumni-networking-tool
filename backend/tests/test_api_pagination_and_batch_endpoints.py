import app as backend_app


def _alumni_row(
    alumni_id,
    first_name,
    last_name,
    grad_year=2020,
    school="University of North Texas",
    job_title="Software Engineer",
    relevant_experience_months=None,
):
    return {
        "id": alumni_id,
        "first_name": first_name,
        "last_name": last_name,
        "grad_year": grad_year,
        "degree": "Bachelor of Science",
        "major": "Software, Data, AI & Cybersecurity",
        "discipline": "Software, Data, AI & Cybersecurity",
        "standardized_major": "Computer Science",
        "linkedin_url": f"https://www.linkedin.com/in/{first_name.lower()}-{alumni_id}",
        "current_job_title": job_title,
        "company": "Acme Corp",
        "location": "Austin, TX",
        "headline": "Software Engineer",
        "updated_at": None,
        "normalized_title": "Software Engineer",
        "normalized_company": "Acme Corp",
        "working_while_studying": None,
        "working_while_studying_status": None,
        "school": school,
        "school2": None,
        "school3": None,
        "degree2": None,
        "degree3": None,
        "major2": None,
        "major3": None,
        "seniority_level": None,
        "relevant_experience_months": relevant_experience_months,
    }


class _AlumniCursor:
    def __init__(self, rows, query_log):
        self._rows = rows
        self._query_log = query_log
        self._mode = None
        self._selected_rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        params_tuple = tuple(params or ())
        self._query_log.append((query, params_tuple))
        if "COUNT(*) AS total" in query:
            self._mode = "count"
            return

        self._mode = "rows"
        if "LIMIT %s OFFSET %s" in query and params_tuple:
            limit = int(params_tuple[-2])
            offset = int(params_tuple[-1])
            self._selected_rows = self._rows[offset:offset + limit]
        else:
            self._selected_rows = list(self._rows)

    def fetchone(self):
        if self._mode == "count":
            return {"total": len(self._rows)}
        return None

    def fetchall(self):
        if self._mode == "count":
            return [{"total": len(self._rows)}]
        return self._selected_rows


class _AlumniCursorNoFetchone:
    def __init__(self, rows, query_log):
        self._rows = rows
        self._query_log = query_log
        self._mode = None
        self._selected_rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        params_tuple = tuple(params or ())
        self._query_log.append((query, params_tuple))
        if "COUNT(*) AS total" in query:
            self._mode = "count"
            return

        self._mode = "rows"
        if "LIMIT %s OFFSET %s" in query and params_tuple:
            limit = int(params_tuple[-2])
            offset = int(params_tuple[-1])
            self._selected_rows = self._rows[offset:offset + limit]
        else:
            self._selected_rows = list(self._rows)

    def fetchall(self):
        if self._mode == "count":
            return [{"total": len(self._rows)}]
        return self._selected_rows


class _InteractionsCursor:
    def __init__(self, interactions, bookmarked_total, query_log):
        self._interactions = interactions
        self._bookmarked_total = bookmarked_total
        self._query_log = query_log
        self._mode = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self._query_log.append((query, tuple(params or ())))
        if "COUNT(*) AS bookmarked_total" in query:
            self._mode = "count"
        else:
            self._mode = "rows"

    def fetchone(self):
        if self._mode == "count":
            return {"bookmarked_total": self._bookmarked_total}
        return None

    def fetchall(self):
        if self._mode == "count":
            return [{"bookmarked_total": self._bookmarked_total}]
        return self._interactions


class _InteractionsCursorNoFetchone:
    def __init__(self, interactions, bookmarked_total, query_log):
        self._interactions = interactions
        self._bookmarked_total = bookmarked_total
        self._query_log = query_log
        self._mode = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self._query_log.append((query, tuple(params or ())))
        if "COUNT(*) AS bookmarked_total" in query:
            self._mode = "count"
        else:
            self._mode = "rows"

    def fetchall(self):
        if self._mode == "count":
            return [{"bookmarked_total": self._bookmarked_total}]
        return self._interactions


class _NotesSummaryCursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, _query, _params=None):
        return None

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, cursor_factory):
        self._cursor_factory = cursor_factory

    def cursor(self, dictionary=False):
        return self._cursor_factory()

    def close(self):
        return None


def _latest_select_query(query_log):
    select_queries = [q for q, _ in query_log if "SELECT a.id, a.first_name, a.last_name" in q]
    return select_queries[-1] if select_queries else ""


def test_api_alumni_returns_pagination_fields(client, monkeypatch):
    rows = [
        _alumni_row(1, "Alice", "Anderson"),
        _alumni_row(2, "Bob", "Baker"),
        _alumni_row(3, "Cara", "Clark"),
    ]
    query_log = []
    monkeypatch.setattr(
        backend_app,
        "get_connection",
        lambda: _FakeConn(lambda: _AlumniCursor(rows, query_log)),
    )

    resp = client.get("/api/alumni?limit=2&offset=1")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["success"] is True
    assert payload["limit"] == 2
    assert payload["offset"] == 1
    assert payload["total"] == 3
    assert payload["has_more"] is False
    assert len(payload["items"]) == 2
    assert len(payload["alumni"]) == 2
    # Verify count and paged select queries were both executed.
    assert any("COUNT(*) AS total" in q for q, _ in query_log)
    assert any("LIMIT %s OFFSET %s" in q for q, _ in query_log)


def test_api_alumni_uses_default_page_size_when_limit_missing(client, monkeypatch):
    rows = [_alumni_row(i, f"First{i}", f"Last{i}") for i in range(1, 301)]
    monkeypatch.setattr(
        backend_app,
        "get_connection",
        lambda: _FakeConn(lambda: _AlumniCursor(rows, [])),
    )

    resp = client.get("/api/alumni")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["limit"] == 250
    assert len(payload["items"]) == 250
    assert payload["has_more"] is True
    assert payload["total"] == 300


def test_api_alumni_caps_limit_to_500(client, monkeypatch):
    rows = [_alumni_row(i, f"First{i}", f"Last{i}") for i in range(1, 601)]
    monkeypatch.setattr(
        backend_app,
        "get_connection",
        lambda: _FakeConn(lambda: _AlumniCursor(rows, [])),
    )

    resp = client.get("/api/alumni?limit=999&offset=0")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["limit"] == 500
    assert len(payload["items"]) == 500
    assert payload["has_more"] is True
    assert payload["total"] == 600


def test_api_alumni_classifies_seniority_buckets_from_title(client, monkeypatch):
    rows = [
        _alumni_row(1, "Intern", "Case", job_title="Software Intern"),
        _alumni_row(2, "Junior", "Case", job_title="Junior Engineer"),
        _alumni_row(3, "Senior", "Case", job_title="Senior Staff Specialist"),
        _alumni_row(4, "Director", "Case", job_title="Director of Engineering"),
        _alumni_row(6, "Manager", "Case", job_title="Engineering Manager"),
        _alumni_row(5, "Unknown", "Case", job_title=None),
    ]
    monkeypatch.setattr(
        backend_app,
        "get_connection",
        lambda: _FakeConn(lambda: _AlumniCursor(rows, [])),
    )

    resp = client.get("/api/alumni?limit=10&offset=0")
    payload = resp.get_json()

    assert resp.status_code == 200
    items_by_id = {item["id"]: item for item in payload["items"]}
    assert items_by_id[1]["seniority_level"] == "Intern"
    assert items_by_id[2]["seniority_level"] == "Mid"
    assert items_by_id[3]["seniority_level"] == "Senior"
    assert items_by_id[4]["seniority_level"] == "Executive"
    assert items_by_id[6]["seniority_level"] == "Executive"
    assert items_by_id[5]["seniority_level"] == "Others"


def test_api_alumni_filters_by_seniority_bucket(client, monkeypatch):
    rows = [
        _alumni_row(1, "Intern", "Case", job_title="Software Intern"),
        _alumni_row(2, "Mid", "Case", job_title="Junior Engineer"),
        _alumni_row(3, "Senior", "Case", job_title="Senior Developer"),
        _alumni_row(4, "Executive", "Case", job_title="VP Engineering"),
        _alumni_row(5, "Other", "Case", job_title=None),
    ]
    monkeypatch.setattr(
        backend_app,
        "get_connection",
        lambda: _FakeConn(lambda: _AlumniCursor(rows, [])),
    )

    resp = client.get("/api/alumni?seniority=Mid&seniority=Executive&limit=10&offset=0")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["success"] is True
    assert payload["total"] == 2
    ids = {item["id"] for item in payload["items"]}
    assert ids == {2, 4}


def test_api_alumni_rejects_invalid_seniority_filter(client):
    resp = client.get("/api/alumni?seniority=Others")
    payload = resp.get_json()

    assert resp.status_code == 400
    assert payload["error"] == "Invalid seniority. Use Intern, Mid, Senior, or Executive."


def test_api_alumni_name_sort_uses_first_name_then_last_name(client, monkeypatch):
    rows = [_alumni_row(1, "Alice", "Zulu"), _alumni_row(2, "Bob", "Alpha")]
    query_log = []
    monkeypatch.setattr(
        backend_app,
        "get_connection",
        lambda: _FakeConn(lambda: _AlumniCursor(rows, query_log)),
    )

    resp = client.get("/api/alumni?sort=name&direction=asc&limit=2&offset=0")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["success"] is True
    select_query = _latest_select_query(query_log)
    assert "ORDER BY LOWER(a.first_name) ASC, LOWER(a.last_name) ASC" in select_query


def test_api_alumni_year_sort_puts_missing_grad_year_last(client, monkeypatch):
    rows = [
        _alumni_row(1, "Alice", "A", grad_year=2020),
        _alumni_row(2, "Bob", "B", grad_year=None),
        _alumni_row(3, "Cara", "C", grad_year=2024),
    ]
    query_log = []
    monkeypatch.setattr(
        backend_app,
        "get_connection",
        lambda: _FakeConn(lambda: _AlumniCursor(rows, query_log)),
    )

    resp = client.get("/api/alumni?sort=year&direction=desc&limit=3&offset=0")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["success"] is True
    select_query = _latest_select_query(query_log)
    assert "CASE WHEN a.grad_year IS NULL THEN 1 ELSE 0 END ASC" in select_query
    assert "a.grad_year DESC" in select_query


def test_api_alumni_applies_inclusive_grad_year_range(client, monkeypatch):
    rows = [
        _alumni_row(1, "Past", "Case", grad_year=2022),
        _alumni_row(2, "Inside", "Case", grad_year=2024),
        _alumni_row(3, "Future", "Case", grad_year=2026),
    ]
    query_log = []
    monkeypatch.setattr(
        backend_app,
        "get_connection",
        lambda: _FakeConn(lambda: _AlumniCursor(rows, query_log)),
    )

    resp = client.get("/api/alumni?grad_year_from=2023&grad_year_to=2025&limit=10&offset=0")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["success"] is True
    count_query = next((q for q, _ in query_log if "COUNT(*) AS total" in q), "")
    assert "a.grad_year >= %s" in count_query
    assert "a.grad_year <= %s" in count_query

    count_params = next((params for q, params in query_log if "COUNT(*) AS total" in q), ())
    assert count_params == (2023, 2025)


def test_api_alumni_rejects_grad_year_range_when_min_greater_than_max(client):
    resp = client.get("/api/alumni?grad_year_from=2025&grad_year_to=2023")
    payload = resp.get_json()

    assert resp.status_code == 400
    assert payload["error"]["field"] == "grad_year_from"
    assert payload["error"]["message"] == "grad_year_from cannot be greater than grad_year_to."


def test_api_alumni_unt_status_filter_uses_python_side_pagination(client, monkeypatch):
    rows = [
        _alumni_row(1, "Alice", "Past", grad_year=2020),
        _alumni_row(2, "Bob", "Future", grad_year=2030),
        _alumni_row(3, "Cara", "Future2", grad_year=2031),
    ]
    monkeypatch.setattr(
        backend_app,
        "get_connection",
        lambda: _FakeConn(lambda: _AlumniCursor(rows, [])),
    )

    resp = client.get("/api/alumni?unt_alumni_status=no&limit=1&offset=0")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["success"] is True
    assert payload["total"] == 2
    assert len(payload["items"]) == 1
    assert payload["has_more"] is True
    assert payload["items"][0]["unt_alumni_status"] == "no"


def test_api_alumni_handles_cursor_without_fetchone(client, monkeypatch):
    rows = [_alumni_row(1, "Alice", "Anderson")]
    monkeypatch.setattr(
        backend_app,
        "get_connection",
        lambda: _FakeConn(lambda: _AlumniCursorNoFetchone(rows, [])),
    )

    resp = client.get("/api/alumni?limit=1&offset=0")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["success"] is True
    assert payload["total"] == 1
    assert len(payload["items"]) == 1


def test_api_alumni_bookmarked_only_requires_user_context(client):
    resp = client.get("/api/alumni?bookmarked_only=1")
    payload = resp.get_json()

    assert resp.status_code == 401
    assert payload["error"] == "User not found"


def test_api_user_interactions_supports_alumni_id_filter(client, monkeypatch):
    with client.session_transaction() as sess:
        sess["linkedin_token"] = "fake_token"

    monkeypatch.setattr(backend_app, "get_current_user_id", lambda: 42)
    query_log = []
    interactions = [
        {"id": 1, "alumni_id": 11, "interaction_type": "bookmarked", "notes": "", "created_at": None, "updated_at": None},
        {"id": 2, "alumni_id": 12, "interaction_type": "connected", "notes": "", "created_at": None, "updated_at": None},
    ]
    monkeypatch.setattr(
        backend_app,
        "get_connection",
        lambda: _FakeConn(lambda: _InteractionsCursor(interactions, 7, query_log)),
    )

    resp = client.get("/api/user-interactions?alumni_ids=11,12")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["success"] is True
    assert payload["bookmarked_total"] == 7
    assert payload["count"] == 2
    assert len(payload["interactions"]) == 2
    assert any("alumni_id IN" in q for q, _ in query_log)


def test_api_user_interactions_requires_auth(client):
    resp = client.get("/api/user-interactions")
    payload = resp.get_json()

    assert resp.status_code == 401
    assert payload["error"] == "Not authenticated"


def test_api_user_interactions_handles_cursor_without_fetchone(client, monkeypatch):
    with client.session_transaction() as sess:
        sess["linkedin_token"] = "fake_token"

    monkeypatch.setattr(backend_app, "get_current_user_id", lambda: 42)
    interactions = [{"id": 1, "alumni_id": 11, "interaction_type": "bookmarked", "notes": "", "created_at": None, "updated_at": None}]
    monkeypatch.setattr(
        backend_app,
        "get_connection",
        lambda: _FakeConn(lambda: _InteractionsCursorNoFetchone(interactions, 3, [])),
    )

    resp = client.get("/api/user-interactions")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["success"] is True
    assert payload["bookmarked_total"] == 3
    assert payload["count"] == 1


def test_api_notes_summary_returns_boolean_map(client, monkeypatch):
    with client.session_transaction() as sess:
        sess["linkedin_token"] = "fake_token"

    monkeypatch.setattr(backend_app, "get_current_user_id", lambda: 42)
    monkeypatch.setattr(
        backend_app,
        "get_connection",
        lambda: _FakeConn(lambda: _NotesSummaryCursor([{"alumni_id": 11}, {"alumni_id": 13}])),
    )

    resp = client.get("/api/notes/summary?ids=11,12,13")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["success"] is True
    assert payload["count"] == 2
    assert payload["summary"]["11"] is True
    assert payload["summary"]["12"] is False
    assert payload["summary"]["13"] is True


def test_api_notes_summary_requires_auth(client):
    resp = client.get("/api/notes/summary?ids=11,12")
    payload = resp.get_json()

    assert resp.status_code == 401
    assert payload["error"] == "Not authenticated"


def test_api_notes_summary_short_circuits_when_no_ids(client, monkeypatch):
    with client.session_transaction() as sess:
        sess["linkedin_token"] = "fake_token"

    monkeypatch.setattr(backend_app, "get_current_user_id", lambda: 42)

    def _should_not_run():
        raise AssertionError("get_connection should not be called when ids are missing")

    monkeypatch.setattr(backend_app, "get_connection", _should_not_run)

    resp = client.get("/api/notes/summary")
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["success"] is True
    assert payload["summary"] == {}
    assert payload["count"] == 0
