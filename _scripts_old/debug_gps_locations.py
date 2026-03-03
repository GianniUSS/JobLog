#!/usr/bin/env python
"""Debug GPS locations for shifts"""
import mysql.connector
import json

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

# GPS Locations in DB
cursor.execute('SELECT custom_settings FROM company_settings WHERE id=1')
row = cursor.fetchone()
custom = json.loads(row['custom_settings']) if row else {}
timbratura = custom.get('timbratura', {})
locs = timbratura.get('gps_locations', [])
print(f'=== GPS Locations nel DB ({len(locs)}) ===')
for l in locs:
    print(f"  - {l.get('name')}: lat={l.get('latitude')}, lon={l.get('longitude')}")

# Trova utenti attivi
print('\n=== Utenti con employee_shifts ===')
cursor.execute('SELECT DISTINCT username FROM employee_shifts WHERE is_active = 1')
usernames = [r['username'] for r in cursor.fetchall()]
print(f'  Utenti con turni attivi: {usernames}')

# Per ogni utente, mostra turni
for uname in usernames[:3]:  # Max 3
    print(f'\n=== Turni per {uname} ===')
    cursor.execute('SELECT * FROM employee_shifts WHERE username = %s AND is_active = 1', (uname,))
    for s in cursor.fetchall():
        print(f"  day={s['day_of_week']}, location={s.get('location_name')}, start={s.get('start_time')}, end={s.get('end_time')}")

conn.close()
