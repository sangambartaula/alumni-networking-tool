from app import _rank_filter_option_counts


def test_rank_filter_option_counts_popular_first_without_query():
    counts = {
        "Austin": 12,
        "Dallas": 20,
        "Seattle": 12,
        "Boston": 5
    }

    ranked = _rank_filter_option_counts(counts, query="", limit=10)
    values = [item["value"] for item in ranked]

    # Popularity first; ties are alphabetical.
    assert values == ["Dallas", "Austin", "Seattle", "Boston"]


def test_rank_filter_option_counts_relevance_then_popularity_with_query():
    counts = {
        "DataDog": 8,          # starts-with "data"
        "Data Bricks": 3,      # starts-with "data"
        "Metadata Inc": 20,    # contains "data"
        "BigData Labs": 5,     # contains "data"
        "Data": 1,             # exact match
        "Not Related": 100
    }

    ranked = _rank_filter_option_counts(counts, query="data", limit=10)
    values = [item["value"] for item in ranked]

    assert values == [
        "Data",           # exact match wins even with lower count
        "DataDog",        # starts-with, then popularity desc
        "Data Bricks",    # starts-with
        "Metadata Inc",   # contains, then popularity desc
        "BigData Labs"    # contains
    ]
