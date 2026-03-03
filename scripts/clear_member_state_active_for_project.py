#!/usr/bin/env python3
"""
Usage: python clear_member_state_active_for_project.py 4589 [4589,3498]

This script sets activity_id=NULL and running=0 for rows in `member_state`
where running=1 and project_code is in the provided list.
"""
import json, os, sys
try:
    import pymysql
except Exception:
    print('Installare pymysql: pip install pymysql')
    raise

if len(sys.argv) < 2:
    print('Usage: python clear_member_state_active_for_project.py <project_code> [<project_code> ...] [--yes|force]')
    sys.exit(1)

# Support passing one or more project codes and an optional force flag
args = sys.argv[1:]
force = False
projects = []
for a in args:
    if a.lower() in ('-y', '--yes', 'yes', 'force', 's'):
        force = True
    else:
        projects.append(a)
ROOT = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(ROOT, 'config.json')
with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    cfg = json.load(f)
db = cfg.get('database', {})
conn = pymysql.connect(host=db.get('host','localhost'), user=db.get('user'), password=db.get('password'), db=db.get('name'), port=int(db.get('port',3306) or 3306), charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
with conn:
    with conn.cursor() as cur:
        placeholder = ','.join(['%s'] * len(projects))
        sql_select = f"SELECT member_key, member_name, project_code, activity_id FROM member_state WHERE running=1 AND project_code IN ({placeholder})"
        cur.execute(sql_select, tuple(projects))
        rows = cur.fetchall()
        print(f"Trovati {len(rows)} righe con running=1 per project_code in {projects}")
        if rows:
            for r in rows:
                print(r)
            if not force:
                confirm = input('Procedo ad azzerare queste sessioni? (s/N): ').strip().lower()
                if confirm != 's':
                    print('Annullato dall\'utente.')
                    sys.exit(0)
            sql_update = f"UPDATE member_state SET activity_id = NULL, running = 0, start_ts = NULL, elapsed_cached = 0, pause_start = NULL, entered_ts = NULL WHERE running=1 AND project_code IN ({placeholder})"
            cur.execute(sql_update, tuple(projects))
            conn.commit()
            print(f"Aggiornate {cur.rowcount} righe in member_state.")
        else:
            print('Nessuna riga da aggiornare.')
