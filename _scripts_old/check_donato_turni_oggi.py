import mysql.connector
from datetime import date

conn = mysql.connector.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog'
)
cursor = conn.cursor()

today = date.today().isoformat()
print(f'=== ANALISI DONATO - {today} ===\n')

# Turni oggi
print('TURNI OGGI:')
cursor.execute('''
    SELECT id, project_name, plan_start, plan_end, location_name, is_obsolete
    FROM rentman_plannings 
    WHERE crew_id = 1923 AND DATE(plan_start) = %s
    ORDER BY plan_start
''', (today,))
shifts = cursor.fetchall()
if shifts:
    for s in shifts:
        print(f'  ID: {s[0]}')
        print(f'  Progetto: {s[1]}')
        print(f'  Inizio: {s[2]}')
        print(f'  Fine: {s[3]}')
        print(f'  Location: {s[4]}')
        print(f'  Obsoleto: {s[5]}')
        print()
else:
    print('  Nessun turno trovato\n')

# Timbrature oggi
print('TIMBRATURE OGGI:')
cursor.execute('''
    SELECT id, tipo, data, ora, created_ts
    FROM timbrature 
    WHERE username = 'donato' AND data = %s
    ORDER BY created_ts
''', (today,))
timbrature = cursor.fetchall()
if timbrature:
    for t in timbrature:
        print(f'  ID: {t[0]}, Tipo: {t[1]}, Ora: {t[3]}, Created: {t[4]}')
else:
    print('  Nessuna timbratura oggi\n')

# Richieste pending
print('\nRICHIESTE PENDING:')
cursor.execute('''
    SELECT r.id, rt.name, r.date_from, r.status, r.created_ts, r.notes
    FROM user_requests r
    JOIN request_types rt ON r.request_type_id = rt.id
    WHERE r.username = 'donato'
    ORDER BY r.created_ts DESC
    LIMIT 5
''')
requests = cursor.fetchall()
if requests:
    for req in requests:
        print(f'  ID: {req[0]}, Tipo: {req[1]}, Data: {req[2]}, Status: {req[3]}')
        if req[5]:
            print(f'    Note: {req[5][:100]}')
else:
    print('  Nessuna richiesta')

conn.close()
