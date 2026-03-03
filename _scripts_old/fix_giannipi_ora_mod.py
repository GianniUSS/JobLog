"""Fix corrupted ora_mod for giannipi timbratura id=1137 and related cedolino records."""
import mysql.connector
import json

with open('config.json') as f:
    cfg = json.load(f)['database']

conn = mysql.connector.connect(
    host=cfg['host'], port=cfg['port'],
    user=cfg['user'], password=cfg['password'],
    database=cfg['name']
)
cur = conn.cursor()

# 1. Fix timbratura 1137: ora_mod should be '10:15' (matching ora 10:15:43)
cur.execute("UPDATE timbrature SET ora_mod = '10:15' WHERE id = 1137")
print(f"Timbrature updated: {cur.rowcount}")

# 2. Fix cedolino_timbrature: reset ora_modificata = ora_originale
#    (9 corrupted records for giannipi 2026-02-16 timeframe_id=1, all had 20:15:00)
cur.execute("""UPDATE cedolino_timbrature 
               SET ora_modificata = ora_originale 
               WHERE username = 'giannipi' 
               AND data_riferimento = '2026-02-16' 
               AND timeframe_id = 1""")
print(f"Cedolino updated: {cur.rowcount}")

conn.commit()
print("DB fix committed successfully")

# Verify
cur2 = conn.cursor(dictionary=True)
cur2.execute("SELECT id, ora, ora_mod FROM timbrature WHERE id = 1137")
print(f"Verify timbratura: {cur2.fetchone()}")

cur2.execute("""SELECT id, ora_originale, ora_modificata FROM cedolino_timbrature 
               WHERE username = 'giannipi' AND data_riferimento = '2026-02-16' 
               AND timeframe_id = 1 ORDER BY id LIMIT 3""")
for r in cur2.fetchall():
    print(f"Verify cedolino: {r}")

conn.close()
