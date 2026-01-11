import mysql.connector, json

cfg = json.load(open('config.json'))['database']
conn = mysql.connector.connect(host=cfg['host'], user=cfg['user'], password=cfg['password'], database=cfg['name'])
cur = conn.cursor()

# Crew ID di Donato nell'app
cur.execute("SELECT username, rentman_crew_id FROM app_users WHERE username LIKE '%donato%'")
print('Utenti Donato in app_users:')
for r in cur.fetchall():
    print(f'  {r[0]}: crew_id={r[1]}')

# Turni salvati con nome Donato
cur.execute("SELECT id, crew_id, crew_name, DATE(shift_start) as d, sent_to_webservice FROM rentman_plannings WHERE crew_name LIKE '%Donato%' ORDER BY shift_start DESC LIMIT 5")
print('\nTurni con nome Donato in rentman_plannings:')
for r in cur.fetchall():
    print(f'  id={r[0]}, crew_id={r[1]}, {r[2]}, data={r[3]}, sent={r[4]}')

# Tutti i turni per oggi
cur.execute("SELECT crew_id, crew_name, sent_to_webservice FROM rentman_plannings WHERE DATE(shift_start) = '2026-01-11'")
print('\nTutti i turni per 11/01/2026:')
for r in cur.fetchall():
    print(f'  crew_id={r[0]}, {r[1]}, sent={r[2]}')

conn.close()
