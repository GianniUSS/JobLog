import mysql.connector

conn = mysql.connector.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
cur = conn.cursor(dictionary=True)

# Aggiorno alcune timbrature per simulare la normalizzazione
# Per esempio: ora originale 9:23 normalizzata a 9:30 (+7 min)
updates = [
    # 1 gennaio - simulo che l'utente è arrivato alle 9:23 e normalizzato a 9:30
    ("UPDATE timbrature SET ora='9:23:00', ora_mod='09:30:00' WHERE username='giannipi' AND data='2026-01-01' AND tipo='inizio_giornata'", "1 gen: inizio 9:23->9:30"),
    # 1 gennaio - simulo uscita alle 18:07 normalizzata a 18:00
    ("UPDATE timbrature SET ora='18:07:00', ora_mod='18:00:00' WHERE username='giannipi' AND data='2026-01-01' AND tipo='fine_giornata'", "1 gen: fine 18:07->18:00"),
    
    # 2 gennaio - inizio alle 8:12 normalizzato a 8:15
    ("UPDATE timbrature SET ora='8:12:00', ora_mod='08:15:00' WHERE username='giannipi' AND data='2026-01-02' AND tipo='inizio_giornata'", "2 gen: inizio 8:12->8:15"),
    # 2 gennaio - fine alle 19:33 normalizzata a 19:30
    ("UPDATE timbrature SET ora='19:33:00', ora_mod='19:30:00' WHERE username='giannipi' AND data='2026-01-02' AND tipo='fine_giornata'", "2 gen: fine 19:33->19:30"),
    
    # 5 gennaio - inizio alle 7:42 normalizzato a 7:45
    ("UPDATE timbrature SET ora='7:42:00', ora_mod='07:45:00' WHERE username='giannipi' AND data='2026-01-05' AND tipo='inizio_giornata'", "5 gen: inizio 7:42->7:45"),
]

for sql, desc in updates:
    cur.execute(sql)
    print(f"✓ {desc} ({cur.rowcount} righe)")

conn.commit()
print("\n✓ Modifiche salvate!")

# Verifica
print("\nVerifica timbrature aggiornate:")
cur.execute("""
    SELECT data, tipo, ora, ora_mod 
    FROM timbrature 
    WHERE username='giannipi' 
    AND data IN ('2026-01-01', '2026-01-02', '2026-01-05')
    AND ora_mod IS NOT NULL
    ORDER BY data, ora
""")
for r in cur.fetchall():
    print(f"  {r['data']} {r['tipo']}: {r['ora']} -> {r['ora_mod']}")

conn.close()
