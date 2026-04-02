import sys, os
backend_dir = os.path.join(os.path.dirname(__file__), "backend")
sys.path.insert(0, backend_dir)
from database import get_connection

try:
    conn = get_connection()
    print("Connection:", conn)
    print("Is connected:", conn.is_connected())
except Exception as e:
    print("DB Exception:", type(e).__name__, e)

from geocoding import geocode_location_with_status
try:
    coords, status = geocode_location_with_status("Denton, Texas")
    print("Geocode:", coords, status)
except Exception as e:
    print("Geo Exception:", type(e).__name__, e)
