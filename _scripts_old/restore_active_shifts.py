#!/usr/bin/env python3
import json
import mysql.connector

# Carica config
with open('config.json', 'r') as f:
    config = json.load(f)

db_config = config['database']

# Connetti a MySQL
conn = mysql.connector.connect(
    host=db_config['host'],
    user=db_config['user'],
    password=db_config['password'],
    database=db_config['name']
)

cursor = conn.cursor()

print("ANNULLO il flag obsolete per i turni che dovrebbero essere attivi...")

# Annulla is_obsolete per turni inviati che sono nella settimana corrente (5-11 gen)
cursor.execute('''
    UPDATE rentman_plannings
    SET is_obsolete = 0
    WHERE sent_to_webservice = 1
    AND planning_date BETWEEN '2026-01-05' AND '2026-01-11'
''')

updated = cursor.rowcount
conn.commit()

print(f"✅ Marcati {updated} turni come ATTIVI (is_obsolete = 0)")

# Mostra il nuovo stato del 8 gennaio
cursor.execute('''
    SELECT COUNT(*), 
           SUM(CASE WHEN is_obsolete = 0 THEN 1 ELSE 0 END) as attivi,
           SUM(CASE WHEN is_obsolete = 1 THEN 1 ELSE 0 END) as obsoleti
    FROM rentman_plannings
    WHERE planning_date = '2026-01-08'
''')

row = cursor.fetchone()
total, active, obsolete = row

print(f"\n📊 Stato 8 gennaio:")
print(f"   Totale: {total}")
print(f"   Attivi: {active}")
print(f"   Obsoleti: {obsolete}")

cursor.close()
conn.close()
