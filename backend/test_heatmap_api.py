#!/usr/bin/env python
"""Test script to verify heatmap API without authentication"""

import json
from database import get_connection
import mysql.connector

# Direct database query to test heatmap data
conn = get_connection()
try:
    with conn.cursor(dictionary=True) as cur:
        # Get all alumni with valid coordinates
        cur.execute("""
            SELECT id, first_name, last_name, location, latitude, longitude, 
                   current_job_title, headline, company
            FROM alumni
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY location ASC
        """)
        rows = cur.fetchall()
    
    # Aggregate locations
    location_clusters = {}
    location_details = {}
    
    for row in rows:
        lat = row['latitude']
        lon = row['longitude']
        cluster_key = (round(lat, 2), round(lon, 2))
        
        if cluster_key not in location_clusters:
            location_clusters[cluster_key] = 0
            location_details[cluster_key] = {
                "location": row['location'],
                "latitude": lat,
                "longitude": lon,
                "sample_alumni": []
            }
        
        location_clusters[cluster_key] += 1
        
        if len(location_details[cluster_key]["sample_alumni"]) < 3:
            location_details[cluster_key]["sample_alumni"].append({
                "id": row['id'],
                "name": f"{row['first_name']} {row['last_name']}".strip(),
                "role": row['current_job_title'] or row['headline'] or 'Alumni',
                "company": row['company']
            })
    
    # Build final response
    locations = []
    for cluster_key, count in location_clusters.items():
        details = location_details[cluster_key]
        locations.append({
            "latitude": details["latitude"],
            "longitude": details["longitude"],
            "location": details["location"],
            "count": count,
            "sample_alumni": details["sample_alumni"]
        })
    
    response = {
        "success": True,
        "locations": locations,
        "total_alumni": len(rows)
    }
    
    print("✅ Heatmap API Response Test:\n")
    print(json.dumps(response, indent=2))
    
finally:
    conn.close()
