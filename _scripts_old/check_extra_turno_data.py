#!/usr/bin/env python3
"""Check extra_data for Extra Turno requests"""
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
cursor = conn.cursor(dictionary=True)

# Query Extra Turno requests
cursor.execute('''
    SELECT id, username, extra_data 
    FROM user_requests 
    WHERE request_type_id = 9 
    ORDER BY created_ts DESC 
    LIMIT 3
''')
rows = cursor.fetchall()
conn.close()

print("RICHIESTE EXTRA TURNO:\n")
for r in rows:
    print(f"ID: {r['id']}, User: {r['username']}")
    if r['extra_data']:
        try:
            ed = json.loads(r['extra_data']) if isinstance(r['extra_data'], str) else r['extra_data']
            print(f"  extra_data: {json.dumps(ed, indent=4)}")
        except:
            print(f"  extra_data (raw): {r['extra_data']}")
    print()
