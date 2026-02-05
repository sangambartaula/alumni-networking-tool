"""
Backfill engineering disciplines for existing alumni based on degree, job title, and headline.

Priority: Degree > Job Title > Headline

Run: python backend/backfill_disciplines.py
"""
import os
import sys
import re
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

from database import get_connection
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# UNT College of Engineering discipline categories with keyword lists
# Format: "Display Name": [list of keywords to match]
DISCIPLINES = {
    "Computer Science & Engineering": [
        # CS core
        "computer science", "cs ", " cs", "software", "developer", "programmer",
        "full stack", "fullstack", "frontend", "front-end", "backend", "back-end",
        "web developer", "mobile developer", "ios", "android", "devops",
        # IT
        "information technology", "it engineer", "it specialist", "systems admin",
        "network admin", "helpdesk", "tech support",
        # Cybersecurity  
        "cybersecurity", "cyber security", "security engineer", "security analyst",
        "penetration", "infosec", "information security", "soc analyst",
        # Data/AI
        "data engineer", "data scientist", "machine learning", "ml engineer",
        "artificial intelligence", " ai ", "deep learning", "neural network",
        "nlp", "computer vision", "big data", "data analytics"
    ],
    
    "Electrical & Computer Engineering": [
        "electrical engineer", "electronics", "circuit", "power engineer",
        "embedded", "hardware engineer", "fpga", "vlsi", "semiconductor",
        "rf engineer", "signal processing", "control systems", "automation",
        "computer engineer", "microcontroller", "firmware", "pcb",
        "telecommunications", "wireless", "antenna"
    ],
    
    "Biomedical Engineering": [
        "biomedical", "biomed", "medical device", "healthcare technology",
        "clinical engineer", "bioinformatics", "biotechnology", "biotech",
        "medical imaging", "prosthetics", "biomechanics", "health informatics"
    ],
    
    "Mechanical & Energy Engineering": [
        "mechanical engineer", "hvac", "manufacturing", "cad engineer",
        "solidworks", "thermal", "fluid dynamics", "automotive", "aerospace",
        "robotics", "mechatronics", "design engineer", "product engineer",
        "energy engineer", "renewable energy", "solar", "wind energy",
        "petroleum", "oil and gas", "drilling"
    ],
    
    "Materials Science & Engineering": [
        "materials science", "materials engineer", "polymer", "plastics",
        "metallurgy", "ceramics", "composites", "nanotechnology", "nano",
        "corrosion", "coatings", "thin film"
    ],
    
    "Construction & Engineering Management": [
        "construction manager", "construction management", "project manager",
        "engineering manager", "technical lead", "tech lead", "vp engineering",
        "director of engineering", "cto", "chief technology", "head of engineering",
        "site manager", "field engineer", "estimator", "superintendent"
    ],
    
    "Engineering Technology": [
        "engineering technology", "engineering tech", "technician",
        "manufacturing tech", "quality technician", "lab technician",
        "maintenance engineer", "process technician"
    ]
}


def infer_discipline(degree, job_title, headline):
    """
    Infer engineering discipline from degree, job title, and headline.
    Priority: Degree > Job Title > Headline
    Returns the best matching discipline or None if no match.
    """
    # Check each source in priority order
    sources = [
        (degree, "degree"),
        (job_title, "job"),
        (headline, "headline")
    ]
    
    for text, source_name in sources:
        if not text:
            continue
            
        text_lower = text.lower()
        
        # Check each discipline's keywords
        for discipline, keywords in DISCIPLINES.items():
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    return discipline
    
    return None


def backfill_disciplines():
    """Backfill major field for all alumni without one."""
    logger.info("🔧 Backfilling engineering disciplines...")
    logger.info("   Priority: Degree > Job Title > Headline")
    conn = get_connection()
    
    try:
        cursor = conn.cursor()
        
        # Get all alumni (we might override weak matches with better ones)
        cursor.execute("""
            SELECT id, degree, current_job_title, headline, major
            FROM alumni 
        """)
        
        rows = cursor.fetchall()
        logger.info(f"Found {len(rows)} total alumni to analyze")
        
        updated = 0
        skipped = 0
        discipline_counts = {}
        
        for row in rows:
            alumni_id, degree, job_title, headline, current_major = row
            
            # Infer discipline
            discipline = infer_discipline(degree, job_title, headline)
            
            if discipline:
                # Only update if no major set or if we found a better match
                if not current_major or current_major == '':
                    cursor.execute(
                        "UPDATE alumni SET major = %s WHERE id = %s",
                        (discipline, alumni_id)
                    )
                    updated += 1
                    discipline_counts[discipline] = discipline_counts.get(discipline, 0) + 1
                else:
                    skipped += 1
            
        conn.commit()
        
        logger.info(f"\n✨ Results:")
        logger.info(f"   Updated: {updated} alumni")
        logger.info(f"   Skipped (already has major): {skipped}")
        logger.info(f"\n📊 Discipline breakdown:")
        for disc, count in sorted(discipline_counts.items(), key=lambda x: -x[1]):
            logger.info(f"   - {disc}: {count}")
        
    except Exception as e:
        logger.error(f"Error backfilling disciplines: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def show_sample_assignments():
    """Show sample assignments for verification."""
    logger.info("\n📋 Sample assignments (first 10 with disciplines):")
    conn = get_connection()
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT CONCAT(first_name, ' ', last_name) as full_name, 
                   degree, current_job_title, major
            FROM alumni
            WHERE major IS NOT NULL AND major != ''
            LIMIT 10
        """)
        
        for row in cursor.fetchall():
            full_name, degree, job, disc = row
            logger.info(f"   {full_name}")
            logger.info(f"      Discipline: {disc}")
            logger.info(f"      Degree: {(degree or 'N/A')[:40]}")
            logger.info(f"      Job: {(job or 'N/A')[:40]}")
            logger.info("")
            
    except Exception as e:
        logger.error(f"Error showing samples: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("ENGINEERING DISCIPLINE BACKFILL")
    logger.info("=" * 60)
    
    backfill_disciplines()
    show_sample_assignments()
    
    logger.info("=" * 60)
    logger.info("🎉 Backfill complete!")
