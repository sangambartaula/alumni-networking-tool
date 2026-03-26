#!/usr/bin/env python3
"""
Test the Relevance Engine on existing CSV entries.

Reads UNT_Alumni_Data.csv, scores up to 20 profiles, and outputs
structured JSON results to scraper/output/relevance_test_results.json.

Usage:
    python3 scripts/test_relevance_on_existing.py
"""

import sys
import json
from pathlib import Path

# Setup paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'scraper'))
sys.path.insert(0, str(PROJECT_ROOT / 'backend'))

import pandas as pd
from config import logger

# Import relevance scorer
from relevance_scorer import (
    get_relevance_json,
    analyze_profile_relevance,
    is_groq_available,
    RELEVANCE_THRESHOLD_RELEVANT,
)

# Config
MAX_PROFILES = 20
CSV_PATH = PROJECT_ROOT / 'scraper' / 'output' / 'UNT_Alumni_Data.csv'
OUTPUT_PATH = PROJECT_ROOT / 'scraper' / 'output' / 'relevance_test_results.json'


def _extract_major_from_field(raw_major):
    """
    The old CSV schema stores degree + major together (e.g. "Masters, Computer Science").
    Extract just the major part.
    """
    if not raw_major or str(raw_major).strip().lower() in ('nan', 'none', ''):
        return ''
    text = str(raw_major).strip()
    # If it contains a comma, take the part after the comma (the major)
    if ',' in text:
        parts = text.split(',', 1)
        return parts[1].strip()
    # If it looks like a date range (e.g. "2024 - 2026"), it's not a major
    if ' - ' in text and text.replace(' - ', '').replace(' ', '').isdigit():
        return ''
    return text


def main():
    if not is_groq_available():
        print("❌ Groq API not available. Set GROQ_API_KEY in .env")
        sys.exit(1)

    print(f"📊 Relevance Engine Test")
    print(f"   Threshold: {RELEVANCE_THRESHOLD_RELEVANT}")
    print(f"   CSV: {CSV_PATH}")
    print(f"   Output: {OUTPUT_PATH}")
    print()

    if not CSV_PATH.exists():
        print(f"❌ CSV not found: {CSV_PATH}")
        sys.exit(1)

    df = pd.read_csv(CSV_PATH)
    print(f"📂 Loaded {len(df)} profiles from CSV")
    print()

    # Map old CSV columns to the format analyze_profile_relevance expects
    results = []
    profiles_processed = 0

    for idx, row in df.iterrows():
        if profiles_processed >= MAX_PROFILES:
            break

        name = str(row.get('name', 'Unknown')).strip()
        
        # Build profile_data dict mapping old columns to expected keys
        major = _extract_major_from_field(row.get('major', ''))
        
        profile_data = {
            'title': str(row.get('job_title', '')).strip() if pd.notna(row.get('job_title')) else '',
            'company': str(row.get('company', '')).strip() if pd.notna(row.get('company')) else '',
            'job_start_date': str(row.get('job_start_date', '')).strip() if pd.notna(row.get('job_start_date')) else '',
            'job_end_date': str(row.get('job_end_date', '')).strip() if pd.notna(row.get('job_end_date')) else '',
            'exp2_title': str(row.get('exp2_title', '')).strip() if pd.notna(row.get('exp2_title')) else '',
            'exp2_company': str(row.get('exp2_company', '')).strip() if pd.notna(row.get('exp2_company')) else '',
            'exp2_dates': str(row.get('exp2_dates', '')).strip() if pd.notna(row.get('exp2_dates')) else '',
            'exp3_title': str(row.get('exp3_title', '')).strip() if pd.notna(row.get('exp3_title')) else '',
            'exp3_company': str(row.get('exp3_company', '')).strip() if pd.notna(row.get('exp3_company')) else '',
            'exp3_dates': str(row.get('exp3_dates', '')).strip() if pd.notna(row.get('exp3_dates')) else '',
            'standardized_major': major,
            'major': major,
        }

        print(f"{'─' * 60}")
        print(f"👤 [{profiles_processed + 1}/{min(len(df), MAX_PROFILES)}] {name}")
        print(f"   Major: {major or '(none)'}")

        if not major:
            print(f"   ⏩ Skipped (no major)")
            results.append({
                'name': name,
                'major': '',
                'jobs': [],
                'flat_scores': {},
                'skipped_reason': 'no_major',
            })
            profiles_processed += 1
            continue

        # Get structured JSON output
        json_output = get_relevance_json(profile_data)
        
        # Get flat dict output
        flat_output = analyze_profile_relevance(profile_data)

        # Print results
        if not json_output:
            print(f"   ⏩ No jobs found")
        for job in json_output:
            score_str = f"{job['score']:.2f}" if job['score'] is not None else "N/A"
            relevant_str = "✅" if job['is_relevant'] else "❌"
            print(f"   {relevant_str} {job['title']} @ {job['company']} → {score_str}")

        if flat_output.get('relevant_experience_months') is not None:
            print(f"   📅 Relevant experience: {flat_output['relevant_experience_months']} months")

        results.append({
            'name': name,
            'major': major,
            'jobs': json_output,
            'flat_scores': {k: v for k, v in flat_output.items()},
        })
        profiles_processed += 1

    print(f"\n{'─' * 60}")
    print(f"✅ Processed {profiles_processed} profiles")

    # Write results to JSON
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, default=str)

    print(f"📄 Results saved to: {OUTPUT_PATH}")


if __name__ == '__main__':
    main()
