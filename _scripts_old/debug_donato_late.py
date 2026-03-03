import mysql.connector
from datetime import datetime, date

conn = mysql.connector.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog'
)
cursor = conn.cursor()

# Trova utente donato
cursor.execute('SELECT username, full_name, external_id, external_group_id FROM app_users WHERE username LIKE "%donato%" OR full_name LIKE "%donato%"')
users = cursor.fetchall()
print('=== UTENTI DONATO ===')
for u in users:
    print(f'Username: {u[0]}, Nome: {u[1]}, External ID: {u[2]}, External Group ID: {u[3]}')

if users:
    user = users[0]
    username = user[0]
    user_id = user[2]  # external_id
    gruppo_id = user[3]  # external_group_id
    
    print(f'\n=== CONFIGURAZIONE GRUPPO {gruppo_id} ===')
    
    # Get column names
    cursor.execute('SHOW COLUMNS FROM group_timbratura_rules')
    cols = [c[0] for c in cursor.fetchall()]
    print('Colonne disponibili:', ', '.join(cols))
    
    cursor.execute('SELECT * FROM group_timbratura_rules WHERE group_id = %s', (gruppo_id,))
    row = cursor.fetchone()
    if row:
        rules = dict(zip(cols, row))
        print(f"\nRegole gruppo {gruppo_id}:")
        for key, val in rules.items():
            print(f'  {key}: {val}')
    else:
        print(f'NESSUNA REGOLA trovata per gruppo_id={gruppo_id}')
    
    # Verifica turni oggi
    today = date.today().isoformat()
    print(f'\n=== TURNI OGGI ({today}) per {username} ===')
    cursor.execute('''
        SELECT id, project_name, shift_start, shift_end, location_name, status, crew_id
        FROM rentman_plannings 
        WHERE crew_id = %s AND DATE(shift_start) = %s
        ORDER BY shift_start
    ''', (user_id, today))
    shifts = cursor.fetchall()
    if shifts:
        for s in shifts:
            print(f'  Turno ID: {s[0]}')
            print(f'    Progetto: {s[1]}')
            print(f'    Inizio: {s[2]}')
            print(f'    Fine: {s[3]}')
            print(f'    Location: {s[4]}')
            print(f'    Status: {s[5]}')
            print(f'    Crew ID: {s[6]}')
            print()
    else:
        print('  Nessun turno trovato per oggi')
    
    # Verifica timbrature oggi
    print(f'\n=== TIMBRATURE OGGI ({today}) per {username} ===')
    cursor.execute('''
        SELECT id, tipo, data, ora, created_ts
        FROM timbrature 
        WHERE username = %s AND data = %s
        ORDER BY id DESC
        LIMIT 5
    ''', (username, today))
    timbrature = cursor.fetchall()
    if timbrature:
        for t in timbrature:
            print(f'  Timbratura ID: {t[0]}, Tipo: {t[1]}, Data: {t[2]}, Ora: {t[3]}, Created: {t[4]}')
    else:
        print('  Nessuna timbratura trovata per oggi')
    
    # Verifica richieste pending
    print(f'\n=== RICHIESTE PENDING per {username} ===')
    cursor.execute('''
        SELECT r.id, rt.name, r.status, r.created_at
        FROM user_requests r
        JOIN request_types rt ON r.request_type_id = rt.id
        WHERE r.username = %s AND r.status = 'pending'
        ORDER BY r.created_at DESC
        LIMIT 5
    ''', (username,))
    requests = cursor.fetchall()
    if requests:
        for req in requests:
            print(f'  Request ID: {req[0]}, Tipo: {req[1]}, Status: {req[2]}, Created: {req[3]}')
    else:
        print('  Nessuna richiesta pending')

conn.close()
