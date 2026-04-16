def _get_or_create_normalized_entity_id(cur, table, column, value):
    value = (value or "").strip()
    if not value:
        return None

    try:
        cur.execute(
            f"INSERT INTO {table} ({column}) VALUES (%s) ON DUPLICATE KEY UPDATE {column}=VALUES({column})",
            (value,),
        )
    except Exception:
        cur.execute(f"INSERT OR IGNORE INTO {table} ({column}) VALUES (?)", (value,))

    try:
        cur.execute(f"SELECT id FROM {table} WHERE {column} = %s", (value,))
    except Exception:
        cur.execute(f"SELECT id FROM {table} WHERE {column} = ?", (value,))

    row = cur.fetchone()
    if not row:
        return None
    return row["id"] if isinstance(row, dict) else row[0]


def parse_int_list_param(request, param_name, strict=False):
    values = []
    for raw in request.args.getlist(param_name):
        if raw is None:
            continue
        pieces = str(raw).split(",")
        for piece in pieces:
            cleaned = piece.strip()
            if not cleaned:
                continue
            try:
                values.append(int(cleaned))
            except Exception:
                if strict:
                    raise ValueError(f"Invalid integer value for {param_name}: {cleaned}")
                continue
    return values


def rank_filter_option_counts(counts, query="", limit=15):
    """
    Rank filter options similar to analytics autocomplete behavior.
    - Empty query: popular entries first (count desc), then alphabetical.
    - With query: exact match, then starts-with, then contains; ties by popularity.
    """
    q = (query or "").strip().lower()
    entries = [(value, count) for value, count in (counts or {}).items() if value]

    if q:
        entries = [item for item in entries if q in item[0].lower()]

        def sort_key(item):
            value, count = item
            value_lower = value.lower()
            if value_lower == q:
                rank = 0
            elif value_lower.startswith(q):
                rank = 1
            else:
                rank = 2
            return (rank, -count, value_lower)

        entries.sort(key=sort_key)
    else:
        entries.sort(key=lambda item: (-item[1], item[0].lower()))

    return [{"value": value, "count": count} for value, count in entries[:limit]]
