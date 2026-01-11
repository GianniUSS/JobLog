#!/usr/bin/env python3
import json
import mysql.connector
from datetime import datetime

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

# Seleziona turni del 8 gennaio (data di test)
cursor.execute('''
    SELECT id, rentman_id, crew_name, project_code, plan_start, plan_end, 
           sent_to_webservice, is_obsolete, created_ts, updated_ts
    FROM rentman_plannings
    WHERE planning_date = '2026-01-08'
    ORDER BY rentman_id, plan_start
''')

rows = cursor.fetchall()

print("=" * 120)
print(f"TURNI DEL 8 GENNAIO 2026 (totale: {len(rows)})")
print("=" * 120)
print(f"{'ID':<5} {'rentman_id':<12} {'crew_name':<20} {'project':<6} {'ora_inizio':<20} "
      f"{'sent':<5} {'obsolete':<10} {'created':<12}")
print("-" * 120)

for row in rows:
    db_id, rentman_id, crew, proj, start, end, sent, obsolete, created, updated = row
    start_str = str(start)[11:16] if start else "?"
    sent_str = "YES" if sent else "NO"
    obsolete_str = "YES" if obsolete else "NO"
    created_str = datetime.fromtimestamp(created/1000).strftime("%d/%m %H:%M") if created else "?"
    
    print(f"{db_id:<5} {rentman_id:<12} {crew[:19]:<20} {str(proj):<6} {start_str:<20} "
          f"{sent_str:<5} {obsolete_str:<10} {created_str:<12}")

print("=" * 120)
print("\nRiepilogo:")
total = len(rows)
sent_count = sum(1 for r in rows if r[6] == 1)
obsolete_count = sum(1 for r in rows if r[7] == 1)
active = total - obsolete_count

print(f"  Totale turni: {total}")
print(f"  Inviati (sent_to_webservice=1): {sent_count}")
print(f"  Marcati obsoleti (is_obsolete=1): {obsolete_count}")
print(f"  Attivi da mostrare (not obsolete): {active}")

cursor.close()
conn.close()
