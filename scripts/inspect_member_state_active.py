#!/usr/bin/env python3
import json, os
try:
    import pymysql
except Exception:
    print('Installare pymysql: pip install pymysql')
    raise
ROOT = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(ROOT, 'config.json')
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    cfg = json.load(f)
db = cfg.get('database', {})
conn = pymysql.connect(host=db.get('host','localhost'), user=db.get('user'), password=db.get('password'), db=db.get('name'), port=int(db.get('port',3306) or 3306), charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
with conn:
    with conn.cursor() as cur:
        cur.execute("SELECT member_key, member_name, project_code, activity_id, running, start_ts, elapsed_cached, entered_ts FROM member_state WHERE activity_id IS NOT NULL ORDER BY project_code, member_name")
        rows = cur.fetchall()
        if not rows:
            print('Nessuna entry in member_state con activity_id IS NOT NULL.')
        else:
            print(f"Trovate {len(rows)} righe in member_state con activity_id non NULL:\n")
            for r in rows:
                print(r)
