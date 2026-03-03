import mysql.connector
from datetime import datetime

conn = mysql.connector.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog'
)
cursor = conn.cursor()

# Verifica timbratura di donato oggi
print('=== ULTIMA TIMBRATURA DONATO ===')
cursor.execute('''
    SELECT id, tipo, data, ora, ora_mod, created_ts
    FROM timbrature 
    WHERE username = 'donato' AND data = '2026-02-13'
    ORDER BY created_ts DESC
    LIMIT 1
''')
timb = cursor.fetchone()
if timb:
    print(f'ID: {timb[0]}')
    print(f'Tipo: {timb[1]}')
    print(f'Ora: {timb[3]}')
    print(f'Ora mod: {timb[4]}')
    print(f'Created: {datetime.fromtimestamp(timb[5]/1000)}')
    
    # Cerca richieste create dopo questa timbratura
    print(f'\n=== RICHIESTE DOPO TIMBRATURA (created_ts > {timb[5]}) ===')
    cursor.execute('''
        SELECT r.id, rt.name, r.date_from, r.status, r.created_ts, r.notes
        FROM user_requests r
        JOIN request_types rt ON r.request_type_id = rt.id
        WHERE r.username = 'donato' AND r.created_ts >= %s
        ORDER BY r.created_ts
    ''', (timb[5],))
    requests = cursor.fetchall()
    if requests:
        for req in requests:
            print(f'  ID: {req[0]}, Tipo: {req[1]}, Status: {req[3]}')
            print(f'  Created: {datetime.fromtimestamp(req[4]/1000)}')
            if req[5]:
                print(f'  Note: {req[5]}')
    else:
        print('  Nessuna richiesta creata dopo la timbratura')

# Verifica tipo richiesta "Giustificazione Ritardo"
print('\n=== TIPO RICHIESTA RITARDO ===')
cursor.execute('SELECT id, name, description FROM request_types WHERE name LIKE "%itardo%"')
types = cursor.fetchall()
if types:
    for t in types:
        print(f'  ID: {t[0]}, Nome: {t[1]}, Desc: {t[2]}')
else:
    print('  TIPO NON TROVATO!')

conn.close()
