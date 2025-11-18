#!/usr/bin/env python3
"""
Extract all names from the College of Engineering sections in a UNT commencement PDF.
"""

import re
import sys
import argparse
from pathlib import Path
import pandas as pd

def extract_text_from_pdf(pdf_path: Path) -> list[str]:
    """Extract text from PDF using pdfplumber with PyPDF2 fallback."""
    text_lines = []
    
    # Try pdfplumber first
    try:
        import pdfplumber
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                text_lines.extend(text.splitlines())
    except Exception as e:
        # Fallback to PyPDF2
        try:
            import PyPDF2
            with open(pdf_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                for page in reader.pages:
                    text = page.extract_text() or ""
                    text_lines.extend(text.splitlines())
        except Exception as e2:
            print(f"Failed to extract text: pdfplumber error: {e}, PyPDF2 error: {e2}", 
                  file=sys.stderr)
            sys.exit(1)
    
    # Clean lines
    return [re.sub(r'\s+', ' ', line).strip() for line in text_lines if line.strip()]

def find_engineering_section(lines: list[str]) -> list[tuple[int, int]]:
    """Find all College of Engineering sections including all degree levels."""
    sections = []
    start_idx = -1
    
    # Engineering-related keywords
    eng_keywords = [
        "COLLEGE OF ENGINEERING",
        "ENGINEERING AND COMPUTER SCIENCE",
        "BACHELOR OF SCIENCE",
        "MASTER OF SCIENCE",
        "DOCTOR OF PHILOSOPHY",
        "BIOMEDICAL ENGINEERING",
        "COMPUTER SCIENCE",
        "ELECTRICAL ENGINEERING",
        "MECHANICAL ENGINEERING",
        "MATERIALS SCIENCE",
        "ENGINEERING MANAGEMENT",
        "DATA ENGINEERING",
        "ENGINEERING TECHNOLOGY",
        "COMPUTER ENGINEERING",
        "ARTIFICIAL INTELLIGENCE",
        "CYBERSECURITY"
    ]
    
    for i, line in enumerate(lines):
        line_upper = line.upper()
        
        # Check for start of engineering sections
        if any(keyword in line_upper for keyword in eng_keywords) or \
           (line_upper.startswith("COLLEGE OF") and "ENGINEERING" in line_upper):
            if start_idx == -1:  # Only set start if we haven't started a section
                start_idx = i
            continue
            
        # Look for section endings
        if start_idx != -1:
            # End section if we find another college or major section break
            if ((line.startswith("College of") and "Engineering" not in line) or
                "COLLEGE OF" in line_upper and "ENGINEERING" not in line_upper or
                i == len(lines) - 1):
                sections.append((start_idx, i if i < len(lines) - 1 else i + 1))
                start_idx = -1

    if not sections:  # Fallback for different format
        for i, line in enumerate(lines):
            if any(dept in line.upper() for dept in [
                "COMPUTER SCIENCE", "ELECTRICAL ENGINEERING", 
                "MECHANICAL ENGINEERING", "BIOMEDICAL ENGINEERING",
                "ENGINEERING TECHNOLOGY", "MATERIALS SCIENCE"
            ]):
                section_end = i + 1
                while section_end < len(lines):
                    if lines[section_end].strip() == "" or any(x in lines[section_end].upper() for x in ["COLLEGE OF", "DEPARTMENT OF"]):
                        break
                    section_end += 1
                sections.append((i, section_end))
                
    return sections

def is_valid_name(name: str) -> bool:
    """Validate if a string is likely a person's name."""
    if not name or len(name) < 3:
        return False
        
    # Skip lines containing header words but allow them in names
    skip_words = {
        "college of", "dean of", "department of", "school of",
        "office of", "division of", "program in", "studies in",
        "major in", "minor in", "requirements for", "catalog",
        "fall", "spring", "summer", "semester", "term",
        "page", "continued", "continued from", "see page",
        "effective", "student", "faculty", "staff", "and"
    }
    
    # Additional checks to prevent combined names
    if " and " in name.lower() or "&" in name:
        return False
    
    name_lower = name.lower()
    if any(word in name_lower for word in skip_words):
        return False
        
    # Check if it has valid name parts
    parts = name.split()
    if len(parts) < 2 or len(parts) > 5:
        return False
        
    # Validate each part
    for part in parts:
        if not re.match(r'^[A-Za-z\'-]+$', part):
            return False
        if len(part) < 2 or len(part) > 20:
            return False
            
    return True

def extract_engineering_names(pdf_path: Path, year: int) -> pd.DataFrame:
    """Extract all names from College of Engineering sections.
    
    Returns DataFrame with columns: Name, Degree, SemYr, Honors
    """
    from datetime import datetime
    
    # Read and clean PDF text
    lines = extract_text_from_pdf(pdf_path)
    
    # Find engineering sections
    sections = find_engineering_section(lines)
    if not sections:
        print("No College of Engineering section found!", file=sys.stderr)
        return pd.DataFrame(columns=["Name", "Degree", "SemYr", "Honors"])
    
    # Detect semester from current date
    month = datetime.now().month
    if month <= 5:
        semester = "Spring"
    elif month <= 7:
        semester = "Summer"
    else:
        semester = "Fall"
    semyr = f"{semester} {year}"
    
    # Honors patterns to detect
    honors_patterns = [
        "Summa Cum Laude",
        "Magna Cum Laude",
        "Cum Laude",
        "Honors College",
        "With Highest Honors",
        "With High Honors",
        "With Honors",
    ]
    
    graduates = []
    
    for start, end in sections:
        current_degree_full = None
        
        for line in lines[start:end]:
            raw = line.strip()
            if not raw:
                continue
            
            # Check if this is a degree header line
            up = raw.upper()
            if "BACHELOR" in up or "MASTER" in up or "DOCTOR" in up:
                # Normalize: Title case, keep 'in' lowercase
                d = re.sub(r"\s+", " ", raw).strip()
                d = re.sub(r"\sIN\s", " in ", d, flags=re.IGNORECASE)
                current_degree_full = d.title().replace(" In ", " in ")
                continue
            
            # Extract honors and clean name
            honors_found = []
            for pat in honors_patterns:
                if pat in raw:
                    honors_found.append(pat)
                    raw = raw.replace(pat, "").strip()
            
            # Check if valid name
            if is_valid_name(raw):
                name = re.sub(r"\s+", " ", raw).strip()
                
                # Use degree from header, or fallback to "Bachelor of Science"
                degree_str = current_degree_full or "Bachelor of Science"
                
                graduates.append({
                    "Name": name,
                    "Degree": degree_str,
                    "SemYr": semyr,
                    "Honors": ", ".join(honors_found) if honors_found else "",
                })
    
    # Create DataFrame with correct columns
    df = pd.DataFrame(graduates, columns=["Name", "Degree", "SemYr", "Honors"])
    
    if not df.empty:
        df = df.drop_duplicates(subset=["Name"], keep="first")
        df = df.sort_values(["Name"])
    
    return df

def main():
    parser = argparse.ArgumentParser(
        description="Extract names from UNT College of Engineering commencement PDF"
    )
    parser.add_argument("pdf", help="Path to the commencement PDF file")
    parser.add_argument(
        "-o", "--output",
        default="engineering_graduate.csv",
        help="Output CSV file path (default: engineering_graduate.csv)"
    )
    parser.add_argument(
        "-y", "--year",
        type=int,
        default=2024,
        help="Graduation year (default: 2024)"
    )
    
    args = parser.parse_args()
    
    # Validate PDF exists
    pdf_path = Path(args.pdf)
    if not pdf_path.exists():
        print(f"Error: PDF file not found: {pdf_path}", file=sys.stderr)
        sys.exit(1)
        
    # Extract names
    df = extract_engineering_names(pdf_path, args.year)
    
    if df.empty:
        print("No names found in engineering sections!", file=sys.stderr)
        sys.exit(1)
        
    # Save results to CSV
    output_path = Path(args.output)
    
    print(f"\nExtracted {len(df)} graduates from engineering sections")
    print("\nFirst 5 rows:")
    print(df.head().to_string(index=False))
    
    # Save full DataFrame with all columns (Name, Degree, SemYr, Honors)
    df.to_csv(output_path, index=False)
    print(f"\nResults saved to: {output_path}")

if __name__ == "__main__":
    main()