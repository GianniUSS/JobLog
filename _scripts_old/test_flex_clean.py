import pymysql, json
with open('config.json') as f:
    cfg = json.load(f)
db = cfg['database']
conn = pymysql.connect(host=db['host'], user=db['user'], password=db['password'], database=db['name'], cursorclass=pymysql.cursors.DictCursor)
cur = conn.cursor()

# Aggiorna richiesta #127 per simulare che verrà creata una nuova
print("=== SCENARIO TEST ===")
print("Cancello la richiesta #127 obsoleta e le timbrature associate")

# Cancella richiesta
cur.execute('DELETE FROM user_requests WHERE id = 127')

# Cancella timbrature cedolino associate alla richiesta 127 (se presenti)
cur.execute('DELETE FROM cedolino_timbrature WHERE overtime_request_id = 127')

# Cancella le ultime 3 timbrature di giannipi per il 16/01/2026
cur.execute('SELECT id FROM cedolino_timbrature WHERE username = "giannipi" AND data_riferimento = "2026-01-16" ORDER BY id DESC LIMIT 3')
ids = [row['id'] for row in cur.fetchall()]
for id in ids:
    cur.execute('DELETE FROM cedolino_timbrature WHERE id = %s', (id,))

conn.commit()
print(f"Cancellate richieste: 1, timbrature: {len(ids)}")
print("\nPuoi adesso timbrate di nuovo per generare una nuova richiesta Fuori Flessibilità")
print("La timbratura NON verrà sincronizzata a CedolinoWeb finché non approvi la richiesta")

cur.close()
conn.close()
