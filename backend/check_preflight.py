import sys, os
from geocoding import geocode_location_with_status
from database import get_connection

print("DB STARTING")
try:
    conn = get_connection()
    print("Has attribute:", hasattr(conn, 'is_connected'))
    if hasattr(conn, 'is_connected'):
        print("is_connected result:", conn.is_connected())
except Exception as e:
    print("DB Exception:", e)

print("GEO STARTING")
try:
    coords, status = geocode_location_with_status("Denton, Texas")
    print("Geo result:", coords, status)
except Exception as e:
    print("Geo Exception:", e)
