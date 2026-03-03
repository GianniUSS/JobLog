import mysql.connector
import json

config = json.load(open('config.json'))
db = mysql.connector.connect(**config['mysql'])
cursor = db.cursor(dictionary=True)

# Cerca le ultime richieste
print("=== ULTIME 10 RICHIESTE ===")
cursor.execute('''
    SELECT r.id, r.username, r.status, rt.name as type_name, rt.value_type
    FROM user_requests r
    JOIN request_types rt ON r.request_type = rt.id
    ORDER BY r.id DESC
    LIMIT 10
''')
for row in cursor.fetchall():
    print(f"ID: {row['id']}, User: {row['username']}, Type: {row['type_name']}, value_type: {row['value_type']}, Status: {row['status']}")

# Controlla il tipo "Extra Turno"
print("\n=== TIPO EXTRA TURNO ===")
cursor.execute("SELECT * FROM request_types WHERE name LIKE '%Extra%'")
for row in cursor.fetchall():
    print(row)

db.close()
