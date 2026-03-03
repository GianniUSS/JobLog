import mysql.connector
from datetime import datetime, date

conn = mysql.connector.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
cur = conn.cursor(dictionary=True)

today = date.today().isoformat()
print(f"=== Debug per donato - {today} ===\n")

# 1. Timbrature di oggi
print("1. TIMBRATURE DI OGGI:")
cur.execute("SELECT id, tipo, data, ora, ora_mod, created_ts FROM timbrature WHERE username='donato' AND data=%s ORDER BY id", (today,))
for r in cur.fetchall():
    print(f"   ID {r['id']}: {r['tipo']} ora={r['ora']} ora_mod={r['ora_mod']}")

# 2. Richieste overtime/flex pending
print("\n2. RICHIESTE PENDING:")
cur.execute("SELECT id, status FROM overtime_requests WHERE username='donato' ORDER BY id DESC LIMIT 5")
for r in cur.fetchall():
    print(f"   ID {r['id']}: status={r['status']}")

# 3. Timer attivo
print("\n3. TIMER ATTIVO:")
cur.execute("SELECT * FROM warehouse_active_timers WHERE username='donato'")
timer = cur.fetchone()
if timer:
    for k, v in timer.items():
        print(f"   {k}: {v}")
else:
    print("   Nessun timer attivo")

# 4. Sessioni di oggi
print("\n4. SESSIONI WAREHOUSE OGGI:")
cur.execute("SELECT id, activity_label, start_ts, end_ts, duration_ms FROM warehouse_sessions WHERE username='donato' AND DATE(FROM_UNIXTIME(start_ts/1000))=%s", (today,))
for r in cur.fetchall():
    start = datetime.fromtimestamp(r['start_ts']/1000).strftime('%H:%M:%S') if r['start_ts'] else 'N/A'
    end = datetime.fromtimestamp(r['end_ts']/1000).strftime('%H:%M:%S') if r['end_ts'] else 'N/A'
    dur = r['duration_ms']/1000/60 if r['duration_ms'] else 0
    print(f"   ID {r['id']}: {r['activity_label']} {start}-{end} ({dur:.1f} min)")
