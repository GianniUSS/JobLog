import mysql.connector

conn = mysql.connector.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
c = conn.cursor(dictionary=True)
c.execute("SHOW COLUMNS FROM rentman_plannings LIKE 'gestione_squadra'")
rows = c.fetchall()

if not rows:
    print("Colonna non presente, eseguo migrazione...")
    c.execute("ALTER TABLE rentman_plannings ADD COLUMN gestione_squadra TINYINT(1) DEFAULT 0")
    conn.commit()
    print("Migrazione completata!")
else:
    print("Colonna già presente:", rows)

c.execute("SELECT project_code, gestione_squadra FROM rentman_plannings WHERE planning_date = CURDATE() LIMIT 10")
for r in c.fetchall():
    print(f"  project={r['project_code']}, gestione_squadra={r['gestione_squadra']}")

conn.close()
