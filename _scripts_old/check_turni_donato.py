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

# Utente Donato
cursor.execute('SELECT username, rentman_crew_id FROM users WHERE username = %s', ('donato',))
user = cursor.fetchone()
crew = user['rentman_crew_id']
print(f'Utente donato: crew_id={crew}')

# Turni recenti
cursor.execute('SELECT DISTINCT crew_id, crew_name, DATE(shift_start) as data FROM rentman_plannings WHERE DATE(shift_start) >= CURDATE() - INTERVAL 2 DAY ORDER BY shift_start')
print('\nTurni recenti nel DB:')
for r in cursor.fetchall():
    print(f"  crew_id={r['crew_id']}, {r['crew_name']}, data={r['data']}")

# Turni Donato per nome
cursor.execute('SELECT id, rentman_id, crew_id, crew_name, project_name, shift_start, status FROM rentman_plannings WHERE crew_name LIKE %s ORDER BY shift_start DESC LIMIT 10', ('%Donato%',))
rows = cursor.fetchall()
print(f'\nTurni con nome Donato ({len(rows)}):')
for r in rows:
    print(f"  id={r['id']}, crew_id={r['crew_id']}, {r['shift_start']} [{r['status']}]")

# Turni per crew_id=1923
cursor.execute('SELECT id, rentman_id, crew_id, crew_name, project_name, shift_start, status FROM rentman_plannings WHERE crew_id = %s ORDER BY shift_start DESC LIMIT 10', (crew,))
rows = cursor.fetchall()
print(f'\nTurni per crew_id={crew} ({len(rows)}):')
for r in rows:
    print(f"  id={r['id']}, {r['shift_start']} [{r['status']}]")

conn.close()
