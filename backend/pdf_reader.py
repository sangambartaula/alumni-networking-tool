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
    """Extract all names from College of Engineering sections."""
    # Read and clean PDF text
    lines = extract_text_from_pdf(pdf_path)
    
    # Find engineering sections
    sections = find_engineering_section(lines)
    if not sections:
        print("No College of Engineering section found!", file=sys.stderr)
        return pd.DataFrame()
        
    # Department patterns for major detection
    department_patterns = {
        "Computer Science": [
            r"computer\s+science",
            r"computing",
            r"artificial intelligence",
            r"machine learning",
            r"data science"
        ],
        "Electrical Engineering": [
            r"electrical\s+engineering",
            r"computer\s+engineering",
            r"electronics"
        ],
        "Mechanical Engineering": [
            r"mechanical\s+engineering",
            r"aerospace",
            r"robotics"
        ],
        "Biomedical Engineering": [
            r"biomedical\s+engineering",
            r"bioengineering"
        ],
        "Materials Science": [
            r"materials\s+science",
            r"materials\s+engineering"
        ],
        "Engineering Technology": [
            r"engineering\s+technology",
            r"industrial\s+technology"
        ]
    }
    
    # Extract names
    names = []
    
    for start, end in sections:
        current_degree = None
        current_major = None
        
        # Get section text for context
        section_text = " ".join(lines[start:end]).lower()
        
        # Determine major from section context
        for major, patterns in department_patterns.items():
            if any(re.search(pattern, section_text) for pattern in patterns):
                current_major = major
                break
                
        for i in range(start, end):
            line = lines[i].strip()
            line_lower = line.lower()
            
            # Enhanced degree level detection
            if any(x in line_lower for x in [
                "bachelor of", "b.s.", "bs in", "b.s in", "undergraduate",
                "bachelor's", "bachelors", "bs degree", "b.s. degree",
                "bachelor of science", "b.sc.", "b.s"
            ]):
                current_degree = "BS"
                
            # Check for master's degree indicators
            elif any(x in line_lower for x in [
                "master of", "m.s.", "ms in", "m.s in", "master's",
                "masters", "ms degree", "m.s. degree", "master of science",
                "m.sc.", "m.s", "graduate degree"
            ]):
                current_degree = "MS"
                
            # Check for doctoral degree indicators
            elif any(x in line_lower for x in [
                "doctor", "ph.d", "ph.d.", "philosophy", "dissertation",
                "doctoral", "doctorate", "doctor of philosophy",
                "d.phil.", "phd candidate", "doctoral candidate"
            ]):
                current_degree = "PhD"
                
            # Look for degree hints in surrounding context if not found
            if not current_degree:
                context_start = max(start, i - 3)
                context_end = min(end, i + 3)
                context = " ".join(lines[context_start:context_end]).lower()
                
                if any(x in context for x in ["bachelor", "b.s.", "undergraduate"]):
                    current_degree = "BS"
                elif any(x in context for x in ["master", "m.s.", "graduate"]):
                    current_degree = "MS"
                elif any(x in context for x in ["doctor", "ph.d", "dissertation"]):
                    current_degree = "PhD"
                
            # Update major if found in line
            for major, patterns in department_patterns.items():
                if any(re.search(pattern, line_lower) for pattern in patterns):
                    current_major = major
                    break
                
            # Process potential name
            if is_valid_name(line):
                parts = line.split()
                
                # Handle titles
                titles = {"Dr.", "Mr.", "Mrs.", "Ms.", "Prof.", "Professor", "Sir", "Lady"}
                if parts[0] in titles:
                    parts = parts[1:]
                
                # Handle suffixes and credentials
                suffixes = {
                    "Jr.", "Sr.", "II", "III", "IV", "V",
                    "Jr", "Sr", "I", "PhD", "Ph.D.", "M.D.",
                    "D.D.S.", "Esq.", "P.E."
                }
                
                suffix = None
                if parts[-1] in suffixes:
                    suffix = parts[-1]
                    parts = parts[:-1]
                
                if len(parts) >= 2:
                    first_name = parts[0]
                    last_name = " ".join(parts[1:])
                    if suffix:
                        last_name = f"{last_name} {suffix}"
                        
                    names.append({
                        "first_name": first_name,
                        "last_name": last_name,
                        "major": current_major or "Unknown",
                        "degree": current_degree or "Unknown",
                        "graduation_year": year
                    })
    
    # Create DataFrame
    df = pd.DataFrame(names)
    
    if not df.empty:
        # Remove duplicates
        df = df.drop_duplicates(subset=["first_name", "last_name"], keep="first")
        # Sort by name
        df = df.sort_values(["last_name", "first_name"])
        
    return df
    
    # Extract names
    names = []
    
    for start, end in sections:
        current_degree = None
        current_major = None
        
        # Get section text for context
        section_text = " ".join(lines[start:end]).lower()
        
        # Determine major from section context
        for major, patterns in department_patterns.items():
            if any(re.search(pattern, section_text) for pattern in patterns):
                current_major = major
                break
        
        for i in range(start, end):
            line = lines[i].strip()
            line_lower = line.lower()
            
            # Enhanced degree level detection
            line_lower = line.lower().strip()
            
            # Check for bachelor's degree indicators
            if any(x in line_lower for x in [
                "bachelor of", "b.s.", "bs in", "b.s in", "undergraduate",
                "bachelor's", "bachelors", "bs degree", "b.s. degree",
                "bachelor of science", "b.sc.", "b.s"
            ]):
                current_degree = "BS"
                
            # Check for master's degree indicators
            elif any(x in line_lower for x in [
                "master of", "m.s.", "ms in", "m.s in", "master's",
                "masters", "ms degree", "m.s. degree", "master of science",
                "m.sc.", "m.s", "graduate degree"
            ]):
                current_degree = "MS"
                
            # Check for doctoral degree indicators
            elif any(x in line_lower for x in [
                "doctor", "ph.d", "ph.d.", "philosophy", "dissertation",
                "doctoral", "doctorate", "doctor of philosophy",
                "d.phil.", "phd candidate", "doctoral candidate"
            ]):
                current_degree = "PhD"
                
            # Look for degree hints in surrounding context
            context_start = max(start, i - 3)
            context_end = min(end, i + 3)
            context = " ".join(lines[context_start:context_end]).lower()
            
            if not current_degree:
                if any(x in context for x in ["bachelor of", "b.s.", "bs in", "b.s in", "undergraduate", "bachelor's"]):
                    current_degree = "BS"
                elif any(x in context for x in ["master of", "m.s.", "ms in", "m.s in", "master's"]):
                    current_degree = "MS"
                elif any(x in context for x in ["doctor", "ph.d", "ph.d.", "philosophy", "dissertation"]):
                    current_degree = "PhD"
                
            # Update major if found in line
            for major, patterns in department_patterns.items():
                if any(re.search(pattern, line_lower) for pattern in patterns):
                    current_major = major
                    break
                
            # Process potential name
            if is_valid_name(line):
                parts = line.split()
                
                # Handle titles
                titles = {"Dr.", "Mr.", "Mrs.", "Ms.", "Prof."}
                if parts[0] in titles:
                    parts = parts[1:]
                
                # Handle suffixes
                suffixes = {"Jr.", "Sr.", "II", "III", "IV"}
                suffix = None
                if parts[-1] in suffixes:
                    suffix = parts[-1]
                    parts = parts[:-1]
                
                if len(parts) >= 2:
                    first_name = parts[0]
                    last_name = " ".join(parts[1:])
                    if suffix:
                        last_name = f"{last_name} {suffix}"
                        
                    names.append({
                        "first_name": first_name,
                        "last_name": last_name,
                        "major": current_major or "Computer Science",
                        "degree": current_degree or "MS",
                        "graduation_year": year
                    })
    
    # Create DataFrame
    df = pd.DataFrame(names)
    
    if not df.empty:
        # Remove duplicates
        df = df.drop_duplicates(subset=["first_name", "last_name"], keep="first")
        # Sort by name
        df = df.sort_values(["last_name", "first_name"])
        
    return df

def main():
    parser = argparse.ArgumentParser(
        description="Extract names from UNT College of Engineering commencement PDF"
    )
    parser.add_argument("pdf", help="Path to the commencement PDF file")
    parser.add_argument(
        "-o", "--output",
        default="engineering_graduates.csv",
        help="Output CSV file path (default: engineering_graduates.csv)"
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
        
    # Save results with only name and year columns
    output_path = Path(args.output)
    
    # Print detailed summary before filtering columns
    print(f"\nExtracted {len(df)} names from engineering sections:")
    print("\nDegree distribution:")
    print(df["degree"].value_counts())
    print("\nMajor distribution:")
    print(df["major"].value_counts())
    
    # Now save only names and year to CSV
    df = df[["first_name", "last_name", "graduation_year"]]  # Only keep name and year
    df.to_csv(output_path, index=False)
    print(f"\nResults saved to: {output_path} (names and graduation year only)")

if __name__ == "__main__":
    main()