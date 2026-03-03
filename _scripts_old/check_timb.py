import mysql.connector
conn = mysql.connector.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
cur = conn.cursor(dictionary=True)

print("Timbrature donato 3/2/2026:")
cur.execute("SELECT id, tipo, data, ora FROM timbrature WHERE username='donato' AND data='2026-02-03' ORDER BY id")
for r in cur.fetchall():
    print(f"  {r['id']}: {r['tipo']} - {r['ora']}")

print("\nTimer attivo:")
cur.execute("SELECT * FROM warehouse_active_timers WHERE username='donato'")
t = cur.fetchone()
if t:
    print(f"  running={t['running']} paused={t['paused']} project={t['project_code']}")
else:
    print("  Nessuno")
    
print("\nSessioni warehouse oggi:")
cur.execute("SELECT * FROM warehouse_sessions WHERE username='donato' ORDER BY id DESC LIMIT 3")
for r in cur.fetchall():
    print(f"  {r}")
