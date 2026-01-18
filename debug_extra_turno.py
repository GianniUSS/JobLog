import mysql.connector
import json
import os

os.chdir(r'E:\Progetti\JOBLogApp')
config = json.load(open('config.json'))
db_config = config.get('mysql') or config.get('database')
# Rimuovi 'vendor' se presente
if 'vendor' in db_config:
    db_config = {k:v for k,v in db_config.items() if k != 'vendor'}
# Rinomina 'name' in 'database' se necessario
if 'name' in db_config:
    db_config['database'] = db_config.pop('name')
    
db = mysql.connector.connect(**db_config)
cursor = db.cursor(dictionary=True)

# Cerca le ultime richieste Extra Turno
print("=== ULTIME RICHIESTE EXTRA TURNO ===\n")
cursor.execute('''
    SELECT r.id, r.username, r.status, r.extra_data, rt.name as type_name
    FROM user_requests r
    JOIN request_types rt ON r.request_type = rt.id
    WHERE rt.name = 'Extra Turno'
    ORDER BY r.id DESC
    LIMIT 5
''')
for row in cursor.fetchall():
    print(f"ID: {row['id']}")
    print(f"User: {row['username']}")
    print(f"Status: {row['status']}")
    print(f"Extra Data: {row['extra_data']}")
    if row['extra_data']:
        try:
            ed = json.loads(row['extra_data'])
            print(f"  -> extra_type: {ed.get('extra_type')}")
            print(f"  -> planned_start: {ed.get('planned_start')}")
            print(f"  -> planned_end: {ed.get('planned_end')}")
        except:
            pass
    print("-" * 50)

# Controlla le timbrature associate
print("\n=== TIMBRATURE CEDOLINO ASSOCIATE ===\n")
cursor.execute('''
    SELECT id, username, data_riferimento, ora_originale, ora_modificata, 
           overtime_request_id, synced_ts
    FROM cedolino_timbrature
    WHERE overtime_request_id IS NOT NULL
    ORDER BY id DESC
    LIMIT 10
''')
for row in cursor.fetchall():
    print(f"ID: {row['id']}, User: {row['username']}")
    print(f"  Data: {row['data_riferimento']}")
    print(f"  Ora orig: {row['ora_originale']}, Ora mod: {row['ora_modificata']}")
    print(f"  Request ID: {row['overtime_request_id']}, Synced: {row['synced_ts']}")
    print()

db.close()
