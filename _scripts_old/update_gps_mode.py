import json
import mysql.connector

# Leggi config
with open('config.json') as f:
    cfg = json.load(f)

db_cfg = cfg['database']
db = mysql.connector.connect(
    host=db_cfg['host'],
    user=db_cfg['user'],
    password=db_cfg['password'],
    database=db_cfg['name']
)

cursor = db.cursor()

# Aggiorna i turni che hanno NULL timbratura_gps_mode
cursor.execute("""
    UPDATE rentman_plannings 
    SET timbratura_gps_mode = 'group'
    WHERE timbratura_gps_mode IS NULL
""")

updated = cursor.rowcount
db.commit()

print(f"✓ Aggiornati {updated} turni con timbratura_gps_mode = 'group'")

# Mostra quanti turni hanno adesso un valore
cursor.execute("SELECT COUNT(*) FROM rentman_plannings WHERE timbratura_gps_mode IS NOT NULL")
total = cursor.fetchone()[0]
print(f"✓ Turni con timbratura_gps_mode definito: {total}")
