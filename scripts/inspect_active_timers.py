#!/usr/bin/env python3
import json, os, sys
try:
    import pymysql
except Exception:
    print("Installare pymysql: pip install pymysql")
    raise
ROOT = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(ROOT, 'config.json')
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    cfg = json.load(f)
db = cfg.get('database', {})
conn = pymysql.connect(host=db.get('host','localhost'), user=db.get('user'), password=db.get('password'), db=db.get('name'), port=int(db.get('port',3306) or 3306), charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
with conn:
    with conn.cursor() as cur:
        cur.execute("SELECT id, username, project_code, project_name, activity_label, start_ts, running, paused FROM warehouse_active_timers WHERE running=1 ORDER BY start_ts DESC LIMIT 50")
        rows = cur.fetchall()
        if not rows:
            print('Nessun timer attivo trovato (running=1).')
        else:
            for r in rows:
                print(r)
