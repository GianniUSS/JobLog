import mysql.connector

conn = mysql.connector.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
cur = conn.cursor()

print("=== Utenti con timbrature ===")
cur.execute("SELECT DISTINCT username FROM timbrature")
for row in cur.fetchall():
    print(f"  {row[0]}")

print("\n=== Timbrature Gennaio 2026 per utente ===")
cur.execute("""
    SELECT username, COUNT(*) as cnt 
    FROM timbrature 
    WHERE data >= '2026-01-01' AND data < '2026-02-01'
    GROUP BY username
""")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]} timbrature")

print("\n=== Tutti gli utenti nel sistema ===")
cur.execute("SELECT username, display_name FROM users")
for row in cur.fetchall():
    print(f"  {row[0]} - {row[1]}")

conn.close()
