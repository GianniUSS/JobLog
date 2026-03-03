import pymysql

conn = pymysql.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
cur = conn.cursor()

print("=== Prime 20 timbrature di giannipi (Gennaio 2026) ===")
cur.execute("""
    SELECT data, tipo, ora 
    FROM timbrature 
    WHERE username='giannipi' AND data >= '2026-01-01' 
    ORDER BY data, ora 
    LIMIT 20
""")
for r in cur.fetchall():
    print(f"{r[0]} | {str(r[1]):20} | {r[2]}")

print("\n=== Conteggio tipi ===")
cur.execute("""
    SELECT tipo, COUNT(*) as cnt
    FROM timbrature 
    WHERE username='giannipi' AND data >= '2026-01-01' 
    GROUP BY tipo
""")
for r in cur.fetchall():
    print(f"{str(r[0]):20} | {r[1]}")

conn.close()
