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

# Keywords that imply Engineering (edit to your school’s catalog/majors)
ENGINEERING_KEYWORDS = [
    "engineering", "engineer", "engineers",
    "computer engineering", "electrical engineering", "mechanical engineering",
    "civil engineering", "materials science", "industrial engineering",
    "software engineering", "aerospace engineering", "biomedical engineering",
    "cs", "computer science"  # keep if CS is in College of Engineering
]

# Columns to try when a table is detected (change to match your PDFs if they have headers)
POSSIBLE_NAME_COLS = ["Name", "Student Name", "Full Name", "Student", "Last, First", "First Name", "Last Name"]
POSSIBLE_MAJOR_COLS = ["Major", "Program", "Department", "College", "Field"]

# How fuzzy the department match should be (0-100)
FUZZY_THRESHOLD = 70

# -----------------------------
# HELPERS
# -----------------------------

name_regex = re.compile(
    r"\b([A-Z][a-zA-Z'`-]+)\s+([A-Z][a-zA-Z'`-]+)\b"  # First Last
)

def is_engineering_text(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    # exact-ish
    if any(kw in t for kw in ENGINEERING_KEYWORDS):
        return True
    # fuzzy backup (handles "enginering"/"eng.")
    choice, score, _ = process.extractOne(t, ENGINEERING_KEYWORDS, scorer=fuzz.partial_ratio)
    return (score or 0) >= FUZZY_THRESHOLD

def normalize_name(full_name: str):
    # Accepts "Last, First" or "First Last" -> returns (first, last, full)
    s = full_name.strip()
    if "," in s:
        last, first = [x.strip() for x in s.split(",", 1)]
        return first, last, f"{first} {last}"
    # Try regex First Last
    m = name_regex.search(s)
    if m:
        first, last = m.group(1), m.group(2)
        return first, last, f"{first} {last}"
    # Fallback: single token or more than two -> keep full as last col
    parts = s.split()
    if len(parts) >= 2:
        return parts[0], parts[-1], " ".join(parts)
    return s, "", s

def extract_text_lines(pdf_path):
    lines = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # Try table extraction first
            tables = page.extract_tables() or []
            for tbl in tables:
                # skip tiny tables
                if not tbl or len(tbl) < 1:
                    continue
                lines.append(("__TABLE__", tbl))

            # Also grab raw text lines to catch non-tabular PDFs
            txt = page.extract_text() or ""
            for ln in txt.splitlines():
                lines.append(("__TEXT__", ln))
    return lines

def parse_table_to_rows(table, pdf_path):
    rows = []
    # Treat first row as header if it looks like headers
    header = table[0]
    # Build a mapping if header row has any known columns
    df = pd.DataFrame(table[1:], columns=[f"col{i}" for i in range(len(header))])
    # Attempt to rename columns to something meaningful using fuzzy
    rename_map = {}
    for i, h in enumerate(header):
        h = (h or "").strip()
        if not h:
            continue
        # Try name-like
        match_name, score_name, _ = process.extractOne(h, POSSIBLE_NAME_COLS, scorer=fuzz.token_sort_ratio)
        match_major, score_major, _ = process.extractOne(h, POSSIBLE_MAJOR_COLS, scorer=fuzz.token_sort_ratio)
        if (score_name or 0) >= 75:
            rename_map[f"col{i}"] = "Name"
        elif (score_major or 0) >= 75:
            rename_map[f"col{i}"] = "Major"
        else:
            # keep as-is
            pass
    df = df.rename(columns=rename_map)

    # If we didn’t detect headers, try heuristics: assume first column is name, second is major
    if "Name" not in df.columns and df.shape[1] >= 1:
        df = df.rename(columns={df.columns[0]: "Name"})
    if "Major" not in df.columns and df.shape[1] >= 2:
        df = df.rename(columns={df.columns[1]: "Major"})

    for _, r in df.iterrows():
        name_raw = str(r.get("Name", "") or "").strip()
        major_raw = str(r.get("Major", "") or "").strip()
        if not name_raw:
            continue
        # Filter by engineering
        if is_engineering_text(major_raw):
            first, last, full = normalize_name(name_raw)
            rows.append({
                "first_name": first,
                "last_name": last,
                "full_name": full,
                "major": major_raw,
                "department": "Engineering",
                "source_file": os.path.basename(pdf_path)
            })
    return rows

def parse_text_line_to_row(line, pdf_path):
    # For non-table PDFs: we look for a name on a line, and a nearby major keyword
    line_s = str(line)
    maybe_name = None
    # Find a name (First Last) on the line
    m = name_regex.search(line_s)
    if m:
        maybe_name = f"{m.group(1)} {m.group(2)}"
    # If line itself mentions engineering, assume this line is a student hit
    if maybe_name and is_engineering_text(line_s):
        first, last, full = normalize_name(maybe_name)
        return {
            "first_name": first,
            "last_name": last,
            "full_name": full,
            "major": "Engineering (text-hit)",
            "department": "Engineering",
            "source_file": os.path.basename(pdf_path)
        }
    return None

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
                    # table may be malformed; skip safely
                    pass
            else:  # "__TEXT__"
                hit = parse_text_line_to_row(payload, pdf_path)
                if hit:
                    all_rows.append(hit)

    if not all_rows:
        print("No engineering students found. Adjust ENGINEERING_KEYWORDS or parsing rules.")
        return

    df = pd.DataFrame(all_rows)

    # Deduplicate by full_name + source
    df = df.drop_duplicates(subset=["full_name"])

    # Sort nicely
    df = df.sort_values(by=["last_name", "first_name"])

    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"✅ Saved {len(df)} students to {OUT_CSV}")

    # Build Google search lines for scraping
    # Adjust school strings to your needs
    school = 'University of North Texas'
    college = 'College of Engineering'
    lines = [
        f'site:linkedin.com/in "{row.full_name}" "{school}"'
        for _, row in df.iterrows()
    ]
    with open(OUT_QUERIES, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"🧭 Wrote search queries to {OUT_QUERIES}")

if __name__ == "__main__":
    main()
