import mysql.connector
import json

with open('config.json') as f:
    config = json.load(f)

conn = mysql.connector.connect(
    host=config['database']['host'],
    user=config['database']['user'],
    password=config['database']['password'],
    database=config['database']['name']
)
cursor = conn.cursor(dictionary=True)

# Ultime richieste per giannipi
cursor.execute("""
    SELECT id, request_type_id, notes, status, extra_data, created_ts
    FROM user_requests 
    WHERE user_id = 'giannipi' 
    ORDER BY created_ts DESC 
    LIMIT 5
""")
for r in cursor.fetchall():
    print(f"ID: {r['id']}, Tipo: {r['request_type_id']}, Status: {r['status']}")
    print(f"  notes: {r['notes']}")
    print(f"  extra_data: {r['extra_data']}")
    print(f"  created_ts: {r['created_ts']}")
    print()

cursor.close()
conn.close()
