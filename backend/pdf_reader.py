#!/usr/bin/env python3
"""
PDF Engineering Student Extractor

- Scans PDFs in ./data_pdfs
- Extracts student names + engineering majors from tables or free text
- Saves CSV of matches and a Google search query list for LinkedIn lookups

Usage:
  python pdf_reader.py
"""

import os
import re
import glob
import pdfplumber
import pandas as pd
from rapidfuzz import fuzz, process

# -----------------------------
# CONFIG
# -----------------------------
PDF_DIR = "data_pdfs"
OUT_CSV = "engineering_students.csv"
OUT_QUERIES = "search_queries.txt"

# Engineering majors to look for (these are the specific majors we want to capture)
ENGINEERING_MAJORS = {
    "Computer Science": ["computer science", "cs", "computing", "computer sciences"],
    "Computer Engineering": ["computer engineering"],
    "Mechanical Engineering": ["mechanical engineering"],
    "Electrical Engineering": ["electrical engineering", "ee"],
    "Civil Engineering": ["civil engineering"],
    "Industrial Engineering": ["industrial engineering"],
    "Materials Science Engineering": ["materials science", "materials engineering"],
    "Software Engineering": ["software engineering"],
    "Aerospace Engineering": ["aerospace engineering"],
    "Biomedical Engineering": ["biomedical engineering"],
}

# Columns to try when a table is detected
POSSIBLE_NAME_COLS = ["Name", "Student Name", "Full Name", "Student", "Last, First", "First Name", "Last Name"]
POSSIBLE_MAJOR_COLS = ["Major", "Program", "Department", "College", "Field", "Degree"]
POSSIBLE_YEAR_COLS = ["Graduation Year", "Year", "Class", "Grad Year", "Graduation"]

# Name pattern (First Last)
name_regex = re.compile(r"\b([A-Z][a-zA-Z'`-]+)\s+([A-Z][a-zA-Z'`-]+)\b")

# -----------------------------
# HELPERS
# -----------------------------
def get_engineering_major(text: str) -> tuple[bool, str]:
    """
    Returns (is_engineering, specific_major_string).
    If generic engineering terms appear but not a specific match, returns "Engineering (General)".
    """
    if not text:
        return False, ""
    t = text.lower()

    # Check against our known engineering majors first
    for major_name, keywords in ENGINEERING_MAJORS.items():
        if any(kw in t for kw in keywords):
            return True, major_name

    # Generic fallback
    if "engineering" in t or "engineer" in t:
        return True, "Engineering (General)"

    return False, ""


def normalize_name(full_name: str):
    """
    Accepts "Last, First" or "First Last" and returns (first, last, full)
    """
    s = (full_name or "").strip()
    if not s:
        return "", "", ""

    if "," in s:
        last, first = [x.strip() for x in s.split(",", 1)]
        return first, last, f"{first} {last}"

    m = name_regex.search(s)
    if m:
        first, last = m.group(1), m.group(2)
        return first, last, f"{first} {last}"

    parts = s.split()
    if len(parts) >= 2:
        return parts[0], parts[-1], " ".join(parts)

    # Single token fallback
    return s, "", s


def get_graduation_year_from_pdf(pdf_path: str) -> str:
    """
    Extract graduation term/year from PDF filename if present.
    Returns strings like "Fall 2024", "Spring 2025", or "" if not found.
    """
    filename = os.path.basename(pdf_path).lower()

    # term-year patterns like fall-2024, spring_2025, summer 2026, winter2027
    term_match = re.search(r"(spring|summer|fall|winter)[-_ ]?20(\d{2})", filename)
    if term_match:
        term = term_match.group(1).capitalize()
        year = f"20{term_match.group(2)}"
        return f"{term} {year}"

    # plain year e.g., "...2024.pdf"
    year_match = re.search(r"\b(20\d{2})\b", filename)
    if year_match:
        return year_match.group(1)

    return ""


def extract_text_lines(pdf_path):
    """
    Yields a list of tuples:
      ("__TABLE__", table_matrix)  for each extracted table
      ("__TEXT__", line)           for each text line
    """
    lines = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # Tables
            tables = page.extract_tables() or []
            for tbl in tables:
                if not tbl or len(tbl) < 1:
                    continue
                lines.append(("__TABLE__", tbl))

            # Raw text
            txt = page.extract_text() or ""
            for ln in txt.splitlines():
                lines.append(("__TEXT__", ln))
    return lines


def parse_table_to_rows(table, pdf_path):
    """
    Parse a single table into rows of dicts (name, major, etc.).
    Uses fuzzy header mapping + heuristics fallback.
    """
    rows = []
    header = table[0]
    df = pd.DataFrame(table[1:], columns=[f"col{i}" for i in range(len(header))])

    # Attempt to rename columns using fuzzy matches
    rename_map = {}
    for i, h in enumerate(header):
        h = (h or "").strip()
        if not h:
            continue

        # Fuzzy match to candidate headers
        match_name = process.extractOne(h, POSSIBLE_NAME_COLS, scorer=fuzz.token_sort_ratio)
        match_major = process.extractOne(h, POSSIBLE_MAJOR_COLS, scorer=fuzz.token_sort_ratio)
        match_year = process.extractOne(h, POSSIBLE_YEAR_COLS, scorer=fuzz.token_sort_ratio)

        score_name = match_name[1] if match_name else 0
        score_major = match_major[1] if match_major else 0
        score_year = match_year[1] if match_year else 0

        if score_name >= 75:
            rename_map[f"col{i}"] = "Name"
        elif score_major >= 75:
            rename_map[f"col{i}"] = "Major"
        elif score_year >= 75:
            rename_map[f"col{i}"] = "GradYear"

    df = df.rename(columns=rename_map)

    # Heuristic fallback if not found
    if "Name" not in df.columns and df.shape[1] >= 1:
        df = df.rename(columns={df.columns[0]: "Name"})
    if "Major" not in df.columns and df.shape[1] >= 2:
        df = df.rename(columns={df.columns[1]: "Major"})
    if "GradYear" not in df.columns and df.shape[1] >= 3:
        df = df.rename(columns={df.columns[2]: "GradYear"})

    for _, r in df.iterrows():
        name_raw = str(r.get("Name", "") or "").strip()
        major_raw = str(r.get("Major", "") or "").strip()
        grad_year_cell = str(r.get("GradYear", "") or "").strip()

        if not name_raw:
            continue

        is_eng, specific_major = get_engineering_major(major_raw)
        if not is_eng:
            # skip non-engineering rows
            continue

        first, last, full = normalize_name(name_raw)
        grad_year = grad_year_cell or get_graduation_year_from_pdf(pdf_path)

        rows.append({
            "first_name": first,
            "last_name": last,
            "full_name": full,
            "major": specific_major,
            "department": "Engineering",
            "graduation_year": grad_year,
            
        })

    return rows


def parse_text_line_to_row(line, pdf_path):
    """
    For non-table PDFs: look for a name on a line + an engineering keyword nearby.
    """
    line_s = str(line or "")
    m = name_regex.search(line_s)
    if not m:
        return None

    is_eng, specific_major = get_engineering_major(line_s)
    if not is_eng:
        return None

    first, last, full = normalize_name(f"{m.group(1)} {m.group(2)}")
    grad_year = get_graduation_year_from_pdf(pdf_path)

    return {
        "first_name": first,
        "last_name": last,
        "full_name": full,
        "major": specific_major,
        "department": "Engineering",
        "graduation_year": grad_year,
        "source_file": os.path.basename(pdf_path),
    }


# -----------------------------
# MAIN PIPELINE
# -----------------------------
def main():
    all_rows = []

    for pdf_path in glob.glob(os.path.join(PDF_DIR, "*.pdf")):
        lines = extract_text_lines(pdf_path)
        for kind, payload in lines:
            if kind == "__TABLE__":
                try:
                    rows = parse_table_to_rows(payload, pdf_path)
                    all_rows.extend(rows)
                except Exception:
                    # malformed table; skip safely
                    pass
            else:  # "__TEXT__"
                hit = parse_text_line_to_row(payload, pdf_path)
                if hit:
                    all_rows.append(hit)

    if not all_rows:
        print("No engineering students found. Check PDF content and parsing rules.")
        return

    df = pd.DataFrame(all_rows)

    # Deduplicate by full name (keep the first occurrence)
    df = df.drop_duplicates(subset=["full_name"], keep="first")

    # Sort nicely
    if {"last_name", "first_name"}.issubset(df.columns):
        df = df.sort_values(by=["last_name", "first_name"])
    else:
        df = df.sort_values(by=["full_name"])

    # Reorder columns to focus on the requested information
    columns = [
        "full_name",
        "department",
        "major",
        "graduation_year",
        "first_name",
        "last_name",
        
    ]
    # Keep only existing columns in that order
    columns = [c for c in columns if c in df.columns]
    df = df[columns]

    # Save CSV
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f" Saved {len(df)} students to {OUT_CSV}")

    # Print a small sample
    show_cols = [c for c in ["full_name", "department", "major", "graduation_year"] if c in df.columns]
    print("\nSample of extracted data:")
    print(df[show_cols].head())

    # Build Google search lines for scraping (adjust school as needed)
    school = "University of North Texas"
    lines = [
        f'site:linkedin.com/in "{row.full_name}" "{school}"'
        for _, row in df.iterrows()
        if isinstance(row.full_name, str) and row.full_name.strip()
    ]
    with open(OUT_QUERIES, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"Wrote search queries to {OUT_QUERIES}")


if __name__ == "__main__":
    main()
