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

# In-memory cache to store geocoded results during this session
# Format: { "Location String": (lat, lon) }
_LOCATION_CACHE: Dict[str, Tuple[float, float]] = {}


# ---------------------------------------------------------
# PRIMARY FUNCTION: Used by the background updater
# Returns exactly one (lat, lon) tuple for database storage
# ---------------------------------------------------------
def geocode_location(location_string: str) -> Optional[Tuple[float, float]]:
    """
    Convert a location string (e.g., 'Denton, Texas, United States') to lat/lon.
    Checks in-memory cache first to avoid redundant API calls.
    """
    if not location_string or location_string.strip() == '' or location_string.strip().lower() == 'not found':
        return None
    
    location_string = location_string.strip()

    # 1. CHECK CACHE
    if location_string in _LOCATION_CACHE:
        # logger.debug(f"✓ Cache hit for '{location_string}'") 
        return _LOCATION_CACHE[location_string]
    
    # 2. CHECK FOR DALLAS-FORT WORTH METROPLEX REGEX
    dfw_pattern = r"(?i)(dallas.*(fort|ft).*worth|dfw.*metroplex)"
    if re.search(dfw_pattern, location_string):
        logger.info(f"✓ Matched DFW Regex for '{location_string}' → Using default {DFW_COORDS}")
        _LOCATION_CACHE[location_string] = DFW_COORDS
        return DFW_COORDS

    # 3. CALL API (If not in cache)
    try:
        # Respect rate limiting before making the call
        time.sleep(REQUEST_INTERVAL)

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
            
            # Store in cache
            _LOCATION_CACHE[location_string] = (lat, lon)
            
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
                
                # Note: time.sleep is now handled inside geocode_location if API is called
        
        logger.info(f"✓ Successfully geocoded {geocoded_count}/{total} records")
        return geocoded_count
        
    except Exception as e:
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


def verify_and_update_all_coordinates() -> int:
    """
    Iterates through ALL unique locations in the database.
    Geocodes them (using cache).
    Updates ANY record where the coordinates do not match the location string.
    
    This fixes issues where a user's location string changed (e.g. moved cities)
    but their coordinates remained set to the old location.
    """
    conn = None
    try:
        conn = get_connection()
        updated_total = 0
        
        with conn.cursor(dictionary=True) as cur:
            logger.info("Fetching all unique locations from database...")
            cur.execute("SELECT DISTINCT location FROM alumni WHERE location IS NOT NULL AND location != ''")
            unique_locations = [row['location'] for row in cur.fetchall()]
            
        total_locs = len(unique_locations)
        logger.info(f"Found {total_locs} unique locations to verify.")
        
        for idx, loc_str in enumerate(unique_locations, 1):
            # Geocode (will use cache if we've seen this string before)
            coords = geocode_location(loc_str)
            
            if coords:
                lat, lon = coords
                
                # Update ALL records with this location string if coords are missing OR different
                # We use a small epsilon (0.001) for float comparison to avoid unnecessary updates
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE alumni 
                        SET latitude = %s, longitude = %s 
                        WHERE location = %s 
                        AND (
                            latitude IS NULL 
                            OR longitude IS NULL 
                            OR ABS(latitude - %s) > 0.001 
                            OR ABS(longitude - %s) > 0.001
                        )
                    """, (lat, lon, loc_str, lat, lon))
                    
                    if cur.rowcount > 0:
                        updated_total += cur.rowcount
                        conn.commit()
                        logger.info(f"[{idx}/{total_locs}] Fixed {cur.rowcount} records for '{loc_str}'")
            
            # Progress log every 10 items if using cache (fast)
            if idx % 10 == 0:
                logger.info(f"Processed {idx}/{total_locs} locations...")

        logger.info(f"✓ Verification complete. Updated {updated_total} records total.")
        return updated_total

    except Exception as e:
        logger.error(f"Error during verification: {e}")
        return 0
    finally:
        if conn: conn.close()


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
    
    # Ask user what mode to run
    print("\nSelect Mode:")
    print("1. Populate missing coordinates only (Fast)")
    print("2. Verify and update ALL coordinates (Slow - checks for mismatches)")
    choice = input("Enter 1 or 2: ").strip()
    
    if choice == '2':
        logger.info("Running full verification...")
        verify_and_update_all_coordinates()
    else:
        logger.info("Populating missing coordinates...")
        populate_missing_coordinates()
        
    logger.info("Done.")