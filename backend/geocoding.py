"""
Geocoding service to convert location strings to latitude/longitude coordinates.
Uses Nominatim (OpenStreetMap) for free geocoding without API keys.
"""

import requests
import time
import logging
import re
from typing import Optional, Tuple, List, Dict, Any
import mysql.connector
from database import get_connection


logger = logging.getLogger(__name__)

# Nominatim API endpoint (free, no API key needed)
NOMINATIM_BASE_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {
    "User-Agent": "alumni-networking-tool/1.0"
}

# Rate limiting: Nominatim requires at least 1 second between requests
REQUEST_INTERVAL = 1.5

# Default Coordinates for Regex Matches
DFW_COORDS = (32.85, -96.85)


# ---------------------------------------------------------
# PRIMARY FUNCTION: Used by the background updater
# Returns exactly one (lat, lon) tuple for database storage
# ---------------------------------------------------------
def geocode_location(location_string: str) -> Optional[Tuple[float, float]]:
    """
    Convert a location string (e.g., 'Denton, Texas, United States') to lat/lon.
    """
    if not location_string or location_string.strip() == '' or location_string.strip().lower() == 'not found':
        return None
    
    # --- CHECK FOR DALLAS-FORT WORTH METROPLEX ---
    dfw_pattern = r"(?i)(dallas.*(fort|ft).*worth|dfw.*metroplex)"
    if re.search(dfw_pattern, location_string):
        logger.info(f"✓ Matched DFW Regex for '{location_string}' → Using default {DFW_COORDS}")
        return DFW_COORDS
    # ---------------------------------------------

    try:
        params = {
            "q": location_string,
            "format": "json",
            "limit": 1
        }
        
        response = requests.get(
            NOMINATIM_BASE_URL,
            params=params,
            headers=NOMINATIM_HEADERS,
            timeout=10
        )
        response.raise_for_status()
        
        results = response.json()
        
        if results and len(results) > 0:
            result = results[0]
            lat = float(result.get('lat'))
            lon = float(result.get('lon'))
            logger.info(f"✓ Geocoded '{location_string}' → ({lat}, {lon})")
            return (lat, lon)
        else:
            logger.warning(f"✗ No geocoding results for '{location_string}'")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Geocoding API error for '{location_string}': {e}")
        return None
    except (ValueError, KeyError) as e:
        logger.error(f"✗ Error parsing geocoding response for '{location_string}': {e}")
        return None


# ---------------------------------------------------------
# SECONDARY FUNCTION: Returns full list of details
# Useful for frontend search bars or manual selection
# ---------------------------------------------------------
def search_location_candidates(query: str) -> List[Dict[str, Any]]:
    """
    Returns list of {display_name, lat, lon} for a search query.
    Used for UI/Frontend search, not for background processing.
    """
    params = {
        "q": query,
        "format": "json",
        "limit": 5
    }

    try:
        r = requests.get(NOMINATIM_BASE_URL, params=params, headers=NOMINATIM_HEADERS, timeout=5)
        r.raise_for_status()
        data = r.json()

        results = []
        for item in data:
            results.append({
                "display_name": item.get("display_name"),
                "lat": item.get("lat"),
                "lon": item.get("lon"),
            })

        return results

    except Exception as e:
        logger.error(f"Geocoding search error: {e}")
        return []


def populate_missing_coordinates(limit: Optional[int] = None) -> int:
    """
    Find alumni records with missing latitude/longitude and geocode them.
    """
    conn = None
    try:
        conn = get_connection()
        geocoded_count = 0
        
        with conn.cursor(dictionary=True) as cur:
            # Find alumni with location but no coordinates
            if limit:
                cur.execute("""
                    SELECT id, location FROM alumni
                    WHERE location IS NOT NULL 
                    AND location != ''
                    AND (latitude IS NULL OR longitude IS NULL)
                    LIMIT %s
                """, (limit,))
            else:
                cur.execute("""
                    SELECT id, location FROM alumni
                    WHERE location IS NOT NULL 
                    AND location != ''
                    AND (latitude IS NULL OR longitude IS NULL)
                """)
            
            records = cur.fetchall()
            total = len(records)
            logger.info(f"Found {total} alumni records needing geocoding")
            
            for idx, record in enumerate(records, 1):
                alumni_id = record['id']
                location = record['location']
                
                logger.info(f"[{idx}/{total}] Geocoding alumni ID {alumni_id}: '{location}'")
                
                # Using the primary function (returns tuple)
                coords = geocode_location(location)
                
                if coords:
                    lat, lon = coords
                    cur.execute("""
                        UPDATE alumni
                        SET latitude = %s, longitude = %s
                        WHERE id = %s
                    """, (lat, lon, alumni_id))
                    conn.commit()
                    geocoded_count += 1
                
                if idx < total:
                    time.sleep(REQUEST_INTERVAL)
        
        logger.info(f"✓ Successfully geocoded {geocoded_count}/{total} records")
        return geocoded_count
        
    except mysql.connector.Error as e:
        logger.error(f"Database error during geocoding: {e}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        return 0
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info("Starting geocoding process...")
    geocoded = populate_missing_coordinates()
    logger.info(f"Geocoding complete! {geocoded} records updated.")