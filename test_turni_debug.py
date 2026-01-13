#!/usr/bin/env python
"""Test diretto dell'API turni simulando una sessione autenticata"""
import mysql.connector
import json
import sys
sys.path.insert(0, 'E:/Progetti/JOBLogApp')

# Simula quello che fa l'API
cfg = json.load(open('config.json'))
db_cfg = cfg.get('database', {})

conn = mysql.connector.connect(
    host=db_cfg.get('host', 'localhost'),
    user=db_cfg.get('user', 'root'),
    password=db_cfg.get('password', ''),
    database=db_cfg.get('name', 'joblog'),
    port=db_cfg.get('port', 3306)
)
cursor = conn.cursor(dictionary=True)

username = 'giannipi'

# Check user
cursor.execute('SELECT rentman_crew_id FROM app_users WHERE username = %s', (username,))
user_row = cursor.fetchone()
crew_id = user_row['rentman_crew_id'] if user_row else None
print(f"User: {username}, crew_id: {crew_id}")

# Get company settings
cursor.execute('SELECT custom_settings FROM company_settings WHERE id = 1')
row = cursor.fetchone()
custom = json.loads(row['custom_settings']) if row else {}
timbratura_config = custom.get('timbratura', {})
gps_locations = timbratura_config.get('gps_locations', [])
print(f"\nGPS Locations ({len(gps_locations)}):")
for loc in gps_locations:
    print(f"  - {loc.get('name')}: lat={loc.get('latitude')}, lon={loc.get('longitude')}")

# Se crew_id è None, usa employee_shifts
if not crew_id:
    print("\n=== Usando employee_shifts (no crew_id) ===")
    from datetime import datetime
    today = datetime.now()
    day_of_week = today.weekday()
    
    cursor.execute('''
        SELECT start_time, end_time, break_start, break_end, location_name
        FROM employee_shifts
        WHERE username = %s AND day_of_week = %s AND is_active = 1
    ''', (username, day_of_week))
    
    shift = cursor.fetchone()
    if shift:
        location_name = shift.get('location_name')
        print(f"Shift trovato: location_name={location_name}")
        
        # Cerca coordinate
        timbratura_lat = None
        timbratura_lon = None
        if location_name:
            for loc in gps_locations:
                if loc.get('name') == location_name:
                    timbratura_lat = loc.get('latitude')
                    timbratura_lon = loc.get('longitude')
                    print(f"MATCH trovato! lat={timbratura_lat}, lon={timbratura_lon}")
                    break
            if not timbratura_lat:
                print(f"NESSUN MATCH per '{location_name}' nelle gps_locations!")
        
        print(f"\nRisultato turno:")
        print(f"  location_name: {location_name}")
        print(f"  timbratura_location: {location_name}")
        print(f"  timbratura_lat: {timbratura_lat}")
        print(f"  timbratura_lon: {timbratura_lon}")
    else:
        print(f"Nessun shift per giorno {day_of_week}")
else:
    print(f"\n=== Usando rentman_plannings (crew_id={crew_id}) ===")

conn.close()
