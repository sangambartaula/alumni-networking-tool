"""Re-normalize all degrees in the CSV using the updated normalization logic."""
import sys, os
import pandas as pd

# Fix path to include scraper module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scraper'))
os.chdir(os.path.join(os.path.dirname(__file__), '..'))

from degree_normalization import standardize_degree

CSV_PATH = 'scraper/output/UNT_Alumni_Data.csv'

if not os.path.exists(CSV_PATH):
    print(f"Error: Could not find {CSV_PATH}")
    sys.exit(1)

df = pd.read_csv(CSV_PATH)

degree_cols = {
    'standardized_degree': 'degree',
    'standardized_degree2': 'degree2',
    'standardized_degree3': 'degree3',
}

changes = 0
for norm_col, raw_col in degree_cols.items():
    if norm_col not in df.columns or raw_col not in df.columns: continue
    
    for idx in df.index:
        raw = df.at[idx, raw_col]
        old_norm = df.at[idx, norm_col]
        
        if pd.isna(raw) or not str(raw).strip():
            continue
            
        new_norm = standardize_degree(str(raw))
        if new_norm and str(old_norm) != new_norm:
            df.at[idx, norm_col] = new_norm
            changes += 1

print(f"Updated {changes} standardized degree entries")

# Verify new unique counts
for col in ['standardized_degree', 'standardized_degree2', 'standardized_degree3']:
    if col in df.columns:
        unique_counts = df[col].dropna().value_counts()
        print(f"\n  {col}:")
        for val, cnt in unique_counts.items():
            print(f"    {val}: {cnt}")

# Save
df.to_csv(CSV_PATH, index=False)
print(f"\nSaved to {CSV_PATH}")
