"""
CSV Data Cleanup Script

This script fixes issues in the UNT_Alumni_Data.csv:
1. Fixes swapped job_title and company columns
2. Removes location data that was incorrectly placed in company field
3. Normalizes text (removes newlines, special characters)

Run from project root: python scraper/fix_csv_data.py
"""

import sys
import re
from pathlib import Path

# Add scraper to path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
from entity_classifier import classify_entity, is_location, is_university, get_classifier
from database_handler import normalize_text
from config import logger, OUTPUT_CSV


def fix_swapped_entries(df):
    """
    Fix rows where job_title and company appear to be swapped.
    Uses the entity classifier to detect and correct swaps.
    """
    classifier = get_classifier()
    fixes_made = 0
    
    for idx, row in df.iterrows():
        job_title = str(row.get('job_title', '')).strip()
        company = str(row.get('company', '')).strip()
        
        # Skip if both are empty
        if not job_title and not company:
            continue
        
        # Normalize text first
        job_title = normalize_text(job_title)
        company = normalize_text(company)
        
        needs_swap = False
        new_job_title = job_title
        new_company = company
        
        # Check if company field contains a location (should be removed/moved)
        if company and is_location(company):
            logger.info(f"[{row.get('name', 'Unknown')}] Removing location from company: '{company}'")
            new_company = ""
            fixes_made += 1
        
        # Check if job_title looks like a company
        if job_title:
            jt_type, jt_conf = classify_entity(job_title)
            if jt_type == "company" and jt_conf >= 0.8:
                needs_swap = True
        
        # Check if company looks like a job title
        if company and not is_location(company):
            co_type, co_conf = classify_entity(company)
            if co_type == "job_title" and co_conf >= 0.5:
                needs_swap = True
        
        # Perform swap if needed
        if needs_swap:
            logger.info(f"[{row.get('name', 'Unknown')}] Swapping: job_title='{job_title}' <-> company='{company}'")
            new_job_title = company if not is_location(company) else ""
            new_company = job_title
            fixes_made += 1
        
        # Update the row
        df.at[idx, 'job_title'] = new_job_title
        df.at[idx, 'company'] = new_company
    
    return df, fixes_made


def normalize_all_text_fields(df):
    """Normalize all text fields to remove newlines and special characters."""
    text_fields = ['name', 'headline', 'location', 'job_title', 'company', 'major',
                   'exp2_title', 'exp2_company', 'exp3_title', 'exp3_company']
    
    fixes_made = 0
    for field in text_fields:
        if field in df.columns:
            for idx, value in df[field].items():
                if pd.notna(value) and isinstance(value, str):
                    original = value
                    normalized = normalize_text(value)
                    if original != normalized:
                        df.at[idx, field] = normalized
                        if '\n' in original or '\r' in original:
                            logger.info(f"Fixed newline in {field} for row {idx}")
                            fixes_made += 1
    
    return df, fixes_made


def fix_specific_known_issues(df):
    """Fix specific known issues based on profile URLs."""
    fixes = {
        # profile_url: {field: correct_value}
        "https://www.linkedin.com/in/claire-asonganyi-b3481b235": {
            "job_title": "Student",
            "company": "University of North Texas"
        },
        "https://www.linkedin.com/in/bryan-tang-932a511ba/": {
            "job_title": "Software Engineer",
            "company": "Ethos Group"
        },
        "https://www.linkedin.com/in/jvguzman21/": {
            "job_title": "Software Engineer II",
            "company": "State Farm"
        },
        "https://www.linkedin.com/in/sandilya-pemmaraju-5877b532/": {
            "job_title": "Senior Technical Engineer",
            "company": "ServiceNow"
        },
        "https://www.linkedin.com/in/vaishnavi-sabna-b55b7020b": {
            "exp2_title": "Software Engineer trainee",
            "exp2_company": "Cognizant"
        },
        "https://www.linkedin.com/in/davidarendon": {
            "job_title": "Engineer",
            "company": "Linbeck Group, LLC"
        }
    }
    
    fixes_made = 0
    for idx, row in df.iterrows():
        url = str(row.get('profile_url', '')).strip()
        if url in fixes:
            for field, value in fixes[url].items():
                if df.at[idx, field] != value:
                    logger.info(f"[{row.get('name', 'Unknown')}] Setting {field} = '{value}'")
                    df.at[idx, field] = value
                    fixes_made += 1
    
    return df, fixes_made


def main():
    """Main cleanup function."""
    print("=" * 60)
    print("CSV DATA CLEANUP SCRIPT")
    print("=" * 60)
    
    # Read the CSV
    if not OUTPUT_CSV.exists():
        print(f"ERROR: CSV file not found: {OUTPUT_CSV}")
        return 1
    
    print(f"\nReading: {OUTPUT_CSV}")
    df = pd.read_csv(OUTPUT_CSV, encoding='utf-8')
    print(f"Loaded {len(df)} rows")
    
    # Create backup
    backup_path = OUTPUT_CSV.with_suffix('.csv.backup')
    df.to_csv(backup_path, index=False, encoding='utf-8')
    print(f"Backup created: {backup_path}")
    
    total_fixes = 0
    
    # Step 1: Normalize all text fields (fix newlines)
    print("\n[Step 1] Normalizing text fields...")
    df, fixes = normalize_all_text_fields(df)
    total_fixes += fixes
    print(f"  Text normalization fixes: {fixes}")
    
    # Step 2: Auto-detect and fix swapped entries
    print("\n[Step 2] Detecting and fixing swapped job_title/company...")
    df, fixes = fix_swapped_entries(df)
    total_fixes += fixes
    print(f"  Swap fixes: {fixes}")
    
    # Step 3: Apply known fixes LAST (overrides any auto-detection errors)
    print("\n[Step 3] Applying known issue fixes...")
    df, fixes = fix_specific_known_issues(df)
    total_fixes += fixes
    print(f"  Known issue fixes: {fixes}")
    
    # Save the fixed CSV
    print(f"\nSaving fixed CSV to: {OUTPUT_CSV}")
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
    
    print("\n" + "=" * 60)
    print(f"COMPLETE: {total_fixes} total fixes applied")
    print("=" * 60)
    
    # Show the fixed data summary
    print("\nFixed data summary (first 5 rows):")
    print(df[['name', 'job_title', 'company']].head().to_string())
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
