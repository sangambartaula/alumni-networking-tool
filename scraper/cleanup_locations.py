import pandas as pd
from pathlib import Path
import sys
import os

# Add parent to path for imports
sys.path.append(str(Path(__file__).parent))

from entity_classifier import get_classifier
from config import OUTPUT_CSV, logger

def cleanup_locations():
    if not OUTPUT_CSV.exists():
        logger.error(f"File not found: {OUTPUT_CSV}")
        return

    logger.info(f"🧹 Starting location cleanup on {OUTPUT_CSV.name}...")
    df = pd.read_csv(OUTPUT_CSV)
    classifier = get_classifier()
    
    total_fixed = 0
    
    # Specific known bad mappings
    HARDCODED_FIXES = {
        "(Sowmya Maddukuri)": "Denton, Texas, United States",
        "Vignan's Foundation for Science, Technology & Research": "Lewisville, Texas, United States",
        "Vignan's Foundation": "Lewisville, Texas, United States",
        "Denton, Texas, United States. Vignan's Foundation.. whatever": "Lewisville, Texas, United States"
    }

    def fix_row(row):
        nonlocal total_fixed
        loc = str(row['location']) if pd.notna(row['location']) else ""
        
        # 1. Check hardcoded fixes
        for bad, good in HARDCODED_FIXES.items():
            if bad in loc:
                row['location'] = good
                total_fixed += 1
                logger.info(f"  ✅ Fixed hardcoded: '{loc}' -> '{good}'")
                return row
        
        # 2. Check heuristics
        if loc and not classifier.is_location(loc):
            # If it's not a valid location, clear it (or set to Not Found)
            # For these specific users, the user provided the correct locations:
            # Sowmya Maddukuri -> Denton, Texas, United States
            # Vignan's -> Lewisville, Texas, United States
            
            # Check if this row belongs to Sowmya (by name or URL)
            name = f"{row['first']} {row['last']}".lower()
            if "sowmya" in name:
                row['location'] = "Denton, Texas, United States"
                total_fixed += 1
                logger.info(f"  ✅ Fixed Sowmya's location: '{loc}' -> 'Denton, Texas, United States'")
            elif "gopi chand" in name or "uthanda lohitha" in name: # These had Vignan's
                row['location'] = "Lewisville, Texas, United States"
                total_fixed += 1
                logger.info(f"  ✅ Fixed Vignan's related location: '{loc}' -> 'Lewisville, Texas, United States'")
            else:
                # Generic invalid location - clear it
                row['location'] = ""
                total_fixed += 1
                logger.info(f"  🗑️ Cleared invalid location: '{loc}'")
        
        return row

    df = df.apply(fix_row, axis=1)
    
    if total_fixed > 0:
        df.to_csv(OUTPUT_CSV, index=False)
        logger.info(f"✨ Successfully fixed {total_fixed} location entries!")
    else:
        logger.info("✅ No invalid locations found to fix.")

if __name__ == "__main__":
    cleanup_locations()
