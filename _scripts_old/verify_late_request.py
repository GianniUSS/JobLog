import mysql.connector
from datetime import datetime

conn = mysql.connector.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog'
)
cursor = conn.cursor()

# Verifica richiesta creata
print('=== RICHIESTA RITARDO CREATA (ID: 233) ===')
cursor.execute('''
    SELECT r.id, rt.name, r.date_from, r.value_amount, r.status, r.notes, r.created_ts
    FROM user_requests r
    JOIN request_types rt ON r.request_type_id = rt.id
    WHERE r.id = 233
''')
req = cursor.fetchone()
if req:
    print(f'ID: {req[0]}')
    print(f'Tipo: {req[1]}')
    print(f'Data: {req[2]}')
    print(f'Minuti ritardo: {req[3]}')
    print(f'Status: {req[4]}')
    print(f'Note: {req[5]}')
    print(f'Created: {datetime.fromtimestamp(req[6]/1000)}')
else:
    print('Richiesta non trovata')

conn.close()
