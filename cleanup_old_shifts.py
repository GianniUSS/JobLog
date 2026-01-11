#!/usr/bin/env python3
import json
import mysql.connector
from datetime import datetime, timedelta

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

# Prendi lunedì della settimana corrente
today = datetime.now().date()
monday = today - timedelta(days=today.weekday())
sunday = monday + timedelta(days=6)

print(f"Settimana corrente: {monday} a {sunday}")
print(f"Marcando come obsoleti tutti i turni inviati FUORI da questa settimana...\n")

# Marca come obsoleti i turni inviati FUORI dalla settimana corrente
cursor.execute('''
    UPDATE rentman_plannings
    SET is_obsolete = 1, updated_ts = %s
    WHERE sent_to_webservice = 1
    AND is_obsolete = 0
    AND (planning_date < %s OR planning_date > %s)
''', (int(datetime.now().timestamp() * 1000), monday, sunday))

updated = cursor.rowcount
conn.commit()

# Mostra i turni che rimangono attivi
cursor.execute('''
    SELECT planning_date, crew_name, project_code, plan_start, plan_end, is_obsolete
    FROM rentman_plannings
    WHERE sent_to_webservice = 1
    AND planning_date >= %s
    AND planning_date <= %s
    ORDER BY planning_date, plan_start
''', (monday, sunday))

active = cursor.fetchall()

print(f"✅ Marcati {updated} turni come obsoleti (fuori dalla settimana {monday} - {sunday})")
print(f"\n📋 Turni ATTIVI rimasti ({len(active)}):")
for row in active:
    date, crew, proj, start, end, obsolete = row
    print(f"   {date} | {crew} | {proj} | {start}-{end} | obsolete={obsolete}")

print(f"\nDatabase: {db_config['name']} @ {db_config['host']}")

cursor.close()
conn.close()
