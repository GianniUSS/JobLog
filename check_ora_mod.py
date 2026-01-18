import mysql.connector

conn = mysql.connector.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
cur = conn.cursor(dictionary=True)
cur.execute("SELECT id, tipo, data, ora, ora_mod FROM timbrature WHERE username='giannipi' AND data='2026-01-01' ORDER BY ora")
rows = cur.fetchall()
print('Timbrature 1 gennaio:')
for r in rows:
    print(f"  {r['tipo']}: ora={r['ora']} ora_mod={r['ora_mod']}")
conn.close()
