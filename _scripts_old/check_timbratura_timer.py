import mysql.connector
from datetime import datetime

conn = mysql.connector.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
cur = conn.cursor(dictionary=True)

# Ultime timbrature
cur.execute("SELECT id, data, ora, ora_mod, created_ts, tipo FROM timbrature WHERE username='donato' AND data='2026-02-03' ORDER BY id DESC LIMIT 5")
rows = cur.fetchall()
print('Ultime timbrature donato:')
for r in rows:
    created_dt = datetime.fromtimestamp(r['created_ts'] / 1000) if r['created_ts'] else None
    print(f"  ID {r['id']}: tipo={r['tipo']} data={r['data']} ora={r['ora']} ora_mod={r['ora_mod']} created_ts={created_dt}")

# Timer attivo
cur.execute("SELECT * FROM warehouse_active_timers WHERE username='donato'")
timer = cur.fetchone()
print('\nTimer attivo:')
if timer:
    start_dt = datetime.fromtimestamp(timer['start_ts'] / 1000)
    print(f"  start_ts: {timer['start_ts']} = {start_dt.strftime('%H:%M:%S')}")
    print(f"  elapsed_ms: {timer['elapsed_ms']}")
