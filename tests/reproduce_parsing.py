
import sys
import os
from pathlib import Path
import logging

# Add scraper to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scraper"))

# Setup logging to console
logging.basicConfig(level=logging.INFO)

from entity_classifier import classify_entity

def test_parsing():
    test_cases = [
        "Undergraduate Researcher",
        "Sr. Director of Software Engineering",
        "Southlake, Texas", 
        "West Coast Dental Services",
        "UNT Electrical Engineering Department"
    ]

    with open("parsing_results.txt", "w") as f:
        f.write(f"{'Text':<40} | {'Prediction':<15} | {'Conf':<5}\n")
        f.write("-" * 70 + "\n")
        
        for text in test_cases:
            cls, conf = classify_entity(text)
            f.write(f"{text:<40} | {cls:<15} | {conf:<5}\n")
            
    print("Done. Wrote to parsing_results.txt")

if __name__ == "__main__":
    test_parsing()
