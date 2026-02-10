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

# =============================================================================
# DISCIPLINE CLASSIFICATION SPEC (ORDERED)
# CHECK ORDER MATTERS - DO NOT CHANGE ORDER
# =============================================================================

# Ordered list of (discipline_name, keywords) tuples
# Order matters: first match wins
DISCIPLINES = [
    # 1. SOFTWARE, DATA & AI ENGINEERING (largest group, checked first)
    ("Software, Data & AI Engineering", [
        "cs", "computer science", "computing", "software", "software engineer",
        "software developer", "software architect", "backend engineer",
        "frontend engineer", "full stack", "full-stack", "web developer",
        "application developer", "platform engineer", "systems engineer",
        "systems administrator", "system administrator", "sysadmin", "sys admin",
        "devops", "site reliability engineer", "sre", "cloud engineer",
        "aws", "azure", "gcp", "api", "microservices", "programmer",
        "coding", "programming", "python", "java", "javascript", "typescript",
        "c++", "go", "rust", "data engineer", "data scientist", "data science",
        "data analyst", "data analytics", "data architect", "data warehousing",
        "business analytics", "business intelligence",
        "machine learning", "ml", "artificial intelligence", "ai",
        "deep learning", "cyber", "cybersecurity", "information technology",
        "information science", "information system", "information systems", "information studies",
        "informatics", "health informatics",
        "it", "infosec", "appsec", "security engineer",
        "network engineer", "network administrator", "network engineering",
        "ui developer", "ux designer", "ux engineer",
        "visual analytics", "human-computer interaction",
        "mainframe", "mainframe developer",
        "solutions architect", "solution architect",
        "salesforce", "servicenow", "dynamics 365", "crm consultant",
        "database administrator", "dba",
        "computer and information"
    ]),
    
    # 2. EMBEDDED, ELECTRICAL & HARDWARE ENGINEERING
    ("Embedded, Electrical & Hardware Engineering", [
        "embedded systems engineer", "embedded", "embedded systems", "embedded engineer", "firmware",
        "firmware engineer", "rtos", "real-time systems", "electrical engineer",
        "electrical engineering", "computer engineering", "hardware",
        "hardware engineer", "electronics", "circuit design", "pcb",
        "schematic", "altium", "orcad", "cadence", "fpga", "verilog",
        "vhdl", "asic", "silicon", "semiconductor", "microcontroller",
        "arm", "stm32", "esp32", "bare metal", "low level", "device driver",
        "linux kernel", "robotics", "controls engineer", "mechatronics",
        "signal processing", "digital design", "analog design", "power systems"
    ]),
    
    # 3. MECHANICAL & ENERGY ENGINEERING
    ("Mechanical & Energy Engineering", [
        "mechanical engineering", "mechanical engineer", "mech engineer",
        "mechanical", "aeronautical", "aeronautical engineer",
        "mechanics", "machine design", "cad", "solidworks", "catia",
        "autocad", "ansys", "manufacturing engineer", "manufacturing engineering",
        "hvac", "thermal engineering", "thermodynamics", "heat transfer",
        "fluids", "fluid mechanics", "energy engineering", "energy systems",
        "renewable energy", "power generation", "turbines", "combustion",
        "automotive engineering", "aerospace engineering", "structural analysis",
        "stress analysis", "finite element", "fea", "industrial engineering",
        "industrial engineer", "industrial management", "engineering/industrial",
        "plant engineer"
    ]),
    
    # 4. BIOMEDICAL ENGINEERING
    ("Biomedical Engineering", [
        "biomedical", "biomedical engineering", "biomedical engineer",
        "bioengineering", "medical engineering", "medical devices",
        "medical device engineer", "clinical engineer", "clinical engineering",
        "healthcare engineering", "biotech", "biotechnology", "bioinformatics",
        "computational biology", "medical imaging", "imaging engineer",
        "mri", "ct scan", "ultrasound", "x-ray", "biosensors", "prosthetics",
        "implants", "rehabilitation engineering", "neural engineering",
        "tissue engineering", "biomaterials", "physiological systems",
        "regulatory affairs", "fda"
    ]),
    
    # 5. MATERIALS SCIENCE & MANUFACTURING
    # NOTE: "sem" and "tem" removed — they matched "system"/"systems" as false positives
    ("Materials Science & Manufacturing", [
        "materials science", "materials engineering", "materials engineer",
        "materials engineer", "senior materials engineer",
        "metallurgy", "metallurgical engineering", "polymers", "polymer science",
        "ceramics", "composites", "nanomaterials", "nanotechnology",
        "thin films", "surface science", "crystallography", "solid state materials",
        "semiconductor materials", "process engineering", "process engineer",
        "manufacturing science", "manufacturing process", "quality engineering",
        "six sigma", "lean manufacturing", "failure analysis", "corrosion",
        "heat treatment", "additive manufacturing", "3d printing",
        "powder metallurgy", "materials characterization", "material characterization",
        "analytical chemistry", "polymer chemist", "chemical formulation",
        "xrd", "scanning electron", "transmission electron",
        "supply chain", "logistics", "production manager"
    ]),
    
    # 6. CONSTRUCTION & ENGINEERING MANAGEMENT
    # Note: "project manager" can be IT, so this is intentionally construction-heavy
    ("Construction & Engineering Management", [
        "construction management", "construction manager", "construction engineer",
        "construction", "construction project",
        "civil engineering", "civil engineer", "structural engineering",
        "structural engineer", "project manager construction",
        "engineering manager construction", "site engineer", "field engineer",
        "general contractor", "subcontractor", "estimating", "cost estimation",
        "quantity surveying", "scheduler", "scheduling", "primavera", "p6",
        "ms project", "bim", "revit", "autodesk construction", "project controls",
        "capital projects", "infrastructure projects", "transportation engineering",
        "geotechnical engineering", "surveying", "land development",
        "building systems", "facilities engineering", "osha"
    ]),
]


def infer_discipline(degree, job_title, headline):
    """
    Infer engineering discipline from degree, job title, and headline.
    Priority: Job Title > Degree > Headline
    Returns the best matching discipline or "Unknown" if no match.
    
    Matching algorithm:
    1. For each source text (job_title, degree, headline in priority order)
    2. Find ALL matching keywords across ALL categories
    3. Pick the LONGEST matching keyword (more specific = better)
    4. If tie on length, use category order (Software first per spec)
    """
    # Check each source in priority order
    sources = [
        (job_title, "job"),
        (degree, "degree"),
        (headline, "headline")
    ]
    
    for text, source_name in sources:
        if not text:
            continue
            
        text_lower = text.lower()
        
        # Find all matching keywords with their discipline and priority
        matches = []  # [(keyword_length, category_index, discipline_name)]
        
        for cat_index, (discipline_name, keywords) in enumerate(DISCIPLINES):
            for keyword in keywords:
                # Check if keyword matches
                # Short keywords (<=3 chars) use word boundary to avoid
                # false positives (e.g. "arm" in "pharmacy", "cad" in "decade")
                if len(keyword) <= 3:
                    pattern = r'\b' + re.escape(keyword) + r'\b'
                    if re.search(pattern, text_lower):
                        matches.append((len(keyword), cat_index, discipline_name))
                else:
                    if keyword.lower() in text_lower:
                        matches.append((len(keyword), cat_index, discipline_name))
        
        if matches:
            # Sort by: longest keyword first, then by category order (lower index = higher priority)
            matches.sort(key=lambda x: (-x[0], x[1]))
            return matches[0][2]  # Return the discipline name
    
    return "Unknown"


def backfill_disciplines():
    """Backfill major field for all alumni - reset and re-classify all."""
    logger.info("🔧 Backfilling engineering disciplines...")
    logger.info("   Priority: Job Title > Degree > Headline")
    logger.info("   ⚠️ Resetting ALL disciplines for re-classification")
    conn = get_connection()
    
    try:
        cursor = conn.cursor()
        
        # First, reset all majors to NULL for re-classification
        cursor.execute("UPDATE alumni SET major = NULL")
        reset_count = cursor.rowcount
        logger.info(f"   Reset {reset_count} alumni majors to NULL")
        conn.commit()
        
        # Get all alumni 
        cursor.execute("""
            SELECT id, degree, current_job_title, headline
            FROM alumni 
        """)
        
        rows = cursor.fetchall()
        logger.info(f"Found {len(rows)} total alumni to analyze")
        
        updated = 0
        discipline_counts = {}
        
        for row in rows:
            alumni_id, degree, job_title, headline = row
            
            # Infer discipline
            discipline = infer_discipline(degree, job_title, headline)
            
            # Update the record
            cursor.execute(
                "UPDATE alumni SET major = %s WHERE id = %s",
                (discipline, alumni_id)
            )
            updated += 1
            discipline_counts[discipline] = discipline_counts.get(discipline, 0) + 1
            
        conn.commit()
        
        logger.info(f"\n✨ Results:")
        logger.info(f"   Updated: {updated} alumni")
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
