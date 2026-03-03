import pymysql, json
with open('config.json') as f:
    cfg = json.load(f)
db = cfg['database']
conn = pymysql.connect(host=db['host'], user=db['user'], password=db['password'], database=db['name'], cursorclass=pymysql.cursors.DictCursor)
cur = conn.cursor()

# Ultima richiesta Fuori Flessibilità
cur.execute('SELECT id, status, username, date_from, extra_data FROM user_requests WHERE request_type_id = 17 ORDER BY id DESC LIMIT 1')
r = cur.fetchone()
if r:
    print('=== RICHIESTA FUORI FLESSIBILITA ===')
    print(f"ID: {r['id']}, Status: {r['status']}, User: {r['username']}")
    print(f"Data: {r['date_from']}")
    extra = json.loads(r['extra_data']) if r['extra_data'] else {}
    print(f"Extra data: {json.dumps(extra, indent=2)}")
    
    # Timbratura cedolino
    print('\n=== TIMBRATURE CEDOLINO ===')
    cur.execute('SELECT id, ora_originale, ora_modificata, synced_ts FROM cedolino_timbrature WHERE username = %s AND data_riferimento = %s ORDER BY id DESC', (r['username'], r['date_from']))
    for t in cur.fetchall():
        synced = 'SÌ' if t['synced_ts'] else 'NO'
        print(f"ID {t['id']}: orig={t['ora_originale']}, mod={t['ora_modificata']}, synced={synced}")
else:
    print('Nessuna richiesta Fuori Flessibilità trovata')

conn.close()
