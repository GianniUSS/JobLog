#!/usr/bin/env python3
"""
Scollega tutte le attività memorizzate in `member_state` (activity_id IS NOT NULL).
Imposta activity_id=NULL, running=0, start_ts=NULL, elapsed_cached=0, pause_start=NULL, entered_ts=NULL.
Uso: python clear_all_member_state_active.py [--yes]
"""
import json, os, sys
try:
    import pymysql
except Exception:
    print('Installare pymysql: pip install pymysql')
    raise

force = False
args = sys.argv[1:]
if '--yes' in args or '-y' in args or 'yes' in [a.lower() for a in args]:
    force = True

ROOT = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(ROOT, 'config.json')
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    cfg = json.load(f)

db = cfg.get('database', {})
conn = pymysql.connect(host=db.get('host','localhost'), user=db.get('user'), password=db.get('password'), db=db.get('name'), port=int(db.get('port',3306) or 3306), charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
with conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM member_state WHERE activity_id IS NOT NULL")
        r = cur.fetchone()
        total = r['c'] if r else 0
        print(f"Trovate {total} righe con activity_id IS NOT NULL in member_state.")
        if total == 0:
            print('Nessuna azione necessaria.')
            sys.exit(0)
        if not force:
            confirm = input('Procedo ad azzerare tutte queste sessioni? (s/N): ').strip().lower()
            if confirm != 's':
                print('Annullato dall\'utente.')
                sys.exit(0)
        cur.execute("UPDATE member_state SET activity_id = NULL, running = 0, start_ts = NULL, elapsed_cached = 0, pause_start = NULL, entered_ts = NULL WHERE activity_id IS NOT NULL")
        conn.commit()
        print(f"Aggiornate {cur.rowcount} righe in member_state.")
print('Operazione completata.')
