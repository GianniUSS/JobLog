import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog'
)
cursor = conn.cursor()

# Verifica richiesta 233
print('=== CERCA RICHIESTA 233 ===')
cursor.execute('SELECT COUNT(*) FROM user_requests WHERE id = 233')
count = cursor.fetchone()[0]
print(f'Esiste: {count > 0}')

if count > 0:
    cursor.execute('SELECT id, username, request_type_id, notes, extra_data FROM user_requests WHERE id = 233')
    r = cursor.fetchone()
    print(f'\nID: {r[0]}')
    print(f'Username: {r[1]}')
    print(f'Type ID: {r[2]}')
    print(f'Notes: {r[3]}')
    print(f'Extra data: {r[4]}')

print('\n=== ULTIME 3 RICHIESTE DONATO ===')
cursor.execute('SELECT id, request_type_id, notes, extra_data FROM user_requests WHERE username = %s ORDER BY id DESC LIMIT 3', ('donato',))
for r in cursor.fetchall():
    print(f'\nID: {r[0]}, Type: {r[1]}')
    print(f'  Notes: {r[2][:80] if r[2] else "None"}')
    print(f'  Extra: {str(r[3])[:100] if r[3] else "None"}')
        
conn.close()
