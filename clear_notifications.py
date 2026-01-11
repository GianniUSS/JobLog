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

# Cancella il log delle notifiche push turni
cursor.execute('DELETE FROM push_notification_log WHERE kind = %s', ('turni_published',))
deleted = cursor.rowcount
conn.commit()

print(f"✅ Cancellate {deleted} notifiche turni_published dal database MySQL")
print(f"Database: {db_config['name']} @ {db_config['host']}")

cursor.close()
conn.close()
