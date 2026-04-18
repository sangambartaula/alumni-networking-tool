"""
Normalization Report Generator

Shows every normalized bucket and the raw values that map into it,
sorted by count (most → least) for each section.

Usage:
    python scripts/normalization_report.py
    python scripts/normalization_report.py --output report.txt
"""

import os
import sys
import argparse
from collections import defaultdict

import pandas as pd

CSV_PATH = os.path.join(
    os.path.dirname(__file__), "..", "scraper", "output", "UNT_Alumni_Data.csv"
)

# ── Column pairs: (raw_column, normalized_column) ──────────────────────────
# Some sections pull from multiple column sets (exp1/2/3, edu1/2/3).
SECTIONS = {
    "Job Titles": [
        ("title",       "normalized_job_title"),
        ("exp_2_title", "normalized_exp2_title"),
        ("exp_3_title", "normalized_exp3_title"),
    ],
    "Degrees": [
        ("degree",  "standardized_degree"),
        ("degree2", "standardized_degree2"),
        ("degree3", "standardized_degree3"),
    ],
    "Majors": [
        ("major",  "standardized_major"),
        ("major2", "standardized_major2"),
        ("major3", "standardized_major3"),
    ],
}


def build_mapping(df: pd.DataFrame, col_pairs: list) -> dict:
    """
    Build { normalized_value: { raw_value: count } } across all column pairs.
    """
    mapping = defaultdict(lambda: defaultdict(int))

    for raw_col, norm_col in col_pairs:
        if raw_col not in df.columns or norm_col not in df.columns:
            continue
        for _, row in df[[raw_col, norm_col]].dropna().iterrows():
            raw = str(row[raw_col]).strip()
            norm = str(row[norm_col]).strip()
            if raw and norm:
                mapping[norm][raw] += 1

    return mapping


def print_section(title: str, mapping: dict, file=None):
    """Print a formatted section of the report."""
    out = file or sys.stdout

    # Sort normalized buckets by total count (most → least)
    sorted_buckets = sorted(
        mapping.items(),
        key=lambda x: sum(x[1].values()),
        reverse=True,
    )

    total_buckets = len(sorted_buckets)
    total_entries = sum(sum(v.values()) for v in mapping.values())

    print(f"\n{'=' * 80}", file=out)
    print(f"  {title}  ({total_buckets} buckets, {total_entries} entries)", file=out)
    print(f"{'=' * 80}\n", file=out)

    for norm_value, raw_counts in sorted_buckets:
        # Sort raw values by count desc, then alpha
        sorted_raws = sorted(
            raw_counts.items(),
            key=lambda x: (-x[1], x[0].lower()),
        )
        total = sum(raw_counts.values())
        raw_str = ", ".join(f"{raw} ({cnt})" for raw, cnt in sorted_raws)
        print(f"  {norm_value} [{total}]", file=out)
        print(f"    -> {raw_str}", file=out)
        print(file=out)


def main():
    parser = argparse.ArgumentParser(description="Normalization mapping report")
    parser.add_argument(
        "--output", "-o",
        help="Write report to file instead of stdout",
        default=None,
    )
    args = parser.parse_args()

    if not os.path.exists(CSV_PATH):
        print(f"Error: CSV not found at {CSV_PATH}")
        sys.exit(1)

    df = pd.read_csv(CSV_PATH)
    print(f"Loaded {len(df)} rows from CSV")

    file = None
    if args.output:
        file = open(args.output, "w", encoding="utf-8")

    for section_title, col_pairs in SECTIONS.items():
        mapping = build_mapping(df, col_pairs)
        if mapping:
            print_section(section_title, mapping, file=file)

    if file:
        file.close()
        print(f"\nReport saved to: {args.output}")


if __name__ == "__main__":
    main()
