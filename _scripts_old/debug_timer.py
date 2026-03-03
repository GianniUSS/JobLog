import mysql.connector
from datetime import datetime

conn = mysql.connector.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
cur = conn.cursor(dictionary=True)

# Timer attivo
cur.execute("SELECT * FROM warehouse_active_timers WHERE username='donato'")
t = cur.fetchone()
print("Timer attivo:")
if t:
    print(f"  ID: {t['id']}")
    print(f"  running={t['running']} paused={t['paused']}")
    print(f"  start_ts: {datetime.fromtimestamp(t['start_ts']/1000)}")
    print(f"  updated_ts: {datetime.fromtimestamp(t['updated_ts']/1000)}")
else:
    print("  Nessun timer attivo")

# Sessioni salvate
print("\nSessioni salvate (ultime 3):")
cur.execute("SELECT id, start_ts, end_ts, elapsed_ms FROM warehouse_sessions WHERE username='donato' ORDER BY id DESC LIMIT 3")
for r in cur.fetchall():
    start = datetime.fromtimestamp(r['start_ts']/1000) if r['start_ts'] else 'N/A'
    end = datetime.fromtimestamp(r['end_ts']/1000) if r['end_ts'] else 'N/A'
    print(f"  ID {r['id']}: {start} - {end} ({r['elapsed_ms']/60000:.1f} min)")
