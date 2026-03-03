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

# Prendi la data di oggi
today = datetime.now().date()

# Marca come obsoleti i turni inviati con una data passata (più di 7 giorni fa)
cutoff_date = today - timedelta(days=7)

cursor.execute('''
    UPDATE rentman_plannings
    SET is_obsolete = 1
    WHERE sent_to_webservice = 1
    AND is_obsolete = 0
    AND planning_date < %s
''', (cutoff_date,))

updated = cursor.rowcount
conn.commit()

print(f"✅ Marcati {updated} turni inviati come obsoleti (data < {cutoff_date})")
print(f"Database: {db_config['name']} @ {db_config['host']}")

cursor.close()
conn.close()
