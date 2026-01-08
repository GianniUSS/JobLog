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
print("✓ Connesso a MySQL")

cursor = db.cursor()
cursor.execute("""
    SELECT COUNT(*) as total, 
           SUM(CASE WHEN sent_to_webservice = 1 THEN 1 ELSE 0 END) as sent,
           SUM(CASE WHEN sent_to_webservice = 0 THEN 1 ELSE 0 END) as not_sent
    FROM rentman_plannings 
    WHERE planning_date = '2026-01-08'
""")
result = cursor.fetchone()
print(f"Total: {result[0]}, Sent (1): {result[1]}, Not Sent (0): {result[2]}")

# Mostra tutti i turni
print("\n=== TUTTI I TURNI PER 2026-01-08 ===")
cursor.execute("""
    SELECT id, crew_id, project_name, plan_start, plan_end, sent_to_webservice
    FROM rentman_plannings 
    WHERE planning_date = '2026-01-08'
    ORDER BY crew_id
""")
for row in cursor.fetchall():
    print(f"ID={row[0]}, crew_id={row[1]}, project={row[2]}, start={row[3]}, end={row[4]}, sent={row[5]}")
