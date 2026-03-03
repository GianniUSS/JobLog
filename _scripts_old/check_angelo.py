import json
import mysql.connector

conn = mysql.connector.connect(host='localhost', port=3306, user='tim_root', password='gianni225524', database='joblog')
cur = conn.cursor()

print('=== OPERATORE Angelo ===')
cur.execute('SELECT id, name, timbratura_override FROM crew_members WHERE name LIKE %s', ('%Angelo%',))
for r in cur.fetchall():
    print(f'ID: {r[0]}, Nome: "{r[1]}", Override: {r[2]}')

print()
print('=== UTENTE angelo ===')
cur.execute('SELECT username, display_name, full_name FROM app_users WHERE username = %s', ('angelo',))
for r in cur.fetchall():
    print(f'Username: "{r[0]}", Display: "{r[1]}", Full: "{r[2]}"')

print()
print('=== CONFIG AZIENDALE (company_settings) ===')
cur.execute('SELECT custom_settings FROM company_settings LIMIT 1')
row = cur.fetchone()
if row and row[0]:
    settings = json.loads(row[0]) if isinstance(row[0], str) else row[0]
    timb = settings.get('timbratura', {})
    print(f'QR: {timb.get("qr_enabled")}, GPS: {timb.get("gps_enabled")}')
else:
    print('Nessuna config in DB')

conn.close()
