#!/usr/bin/env python3
"""Quick update of extra_minutes for existing Extra Turno requests"""
import mysql.connector
import json

with open('config.json') as f:
    cfg = json.load(f)

db_cfg = cfg['database']
conn = mysql.connector.connect(
    host=db_cfg['host'],
    port=db_cfg['port'],
    user=db_cfg['user'],
    password=db_cfg['password'],
    database=db_cfg['name'],
    charset='utf8mb4'
)
cursor = conn.cursor()

# Fix ID 65: rounded_start 09:15, turno 10:00 -> extra = 45 min
cursor.execute("""
    UPDATE user_requests 
    SET extra_data = JSON_SET(extra_data, '$.extra_minutes', 45, '$.extra_minutes_before', 45) 
    WHERE id = 65
""")
print(f"ID 65: {cursor.rowcount} righe aggiornate")

# Fix ID 63: rounded_start 09:00, turno 10:00 -> extra = 60 min  
cursor.execute("""
    UPDATE user_requests 
    SET extra_data = JSON_SET(extra_data, '$.extra_minutes', 60, '$.extra_minutes_before', 60) 
    WHERE id = 63
""")
print(f"ID 63: {cursor.rowcount} righe aggiornate")

conn.commit()
conn.close()
print("Fatto!")
