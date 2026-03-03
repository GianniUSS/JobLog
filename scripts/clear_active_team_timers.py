#!/usr/bin/env python3
import json
import os
import sys

try:
    import pymysql
except Exception as e:
    print("Errore: serve il pacchetto 'pymysql' nel virtualenv. Installa con: pip install pymysql")
    raise

ROOT = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(ROOT, 'config.json')
if not os.path.exists(CONFIG_PATH):
    print(f"Impossibile trovare {CONFIG_PATH}")
    sys.exit(1)

with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    cfg = json.load(f)

db = cfg.get('database', {})
host = db.get('host', 'localhost')
port = int(db.get('port', 3306) or 3306)
user = db.get('user')
password = db.get('password')
dbname = db.get('name')

if not all([user, dbname]):
    print('config.json mancante di parametri database obbligatori (user/name)')
    sys.exit(1)

sql_count = "SELECT COUNT(*) AS c FROM warehouse_active_timers WHERE running=1 AND LOWER(source) LIKE %s"
sql_delete = "DELETE FROM warehouse_active_timers WHERE running=1 AND LOWER(source) LIKE %s"
param = ('%squadra%',)

conn = None
try:
    conn = pymysql.connect(host=host, user=user, password=password, db=dbname, port=port, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    with conn.cursor() as cur:
        cur.execute(sql_count, param)
        r = cur.fetchone()
        c = r['c'] if r else 0
        print(f"Trovati {c} timer attivi (running=1) con source LIKE '%squadra%'.")
        if c == 0:
            print("Nessuna azione necessaria.")
        else:
            cur.execute(sql_delete, param)
            deleted = cur.rowcount
            conn.commit()
            print(f"Eliminati {deleted} record da 'warehouse_active_timers'.")

except Exception as e:
    print('Errore durante connessione/operazione DB:', e)
    raise
finally:
    if conn:
        conn.close()

print('Operazione completata.')
