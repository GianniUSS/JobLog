import mysql.connector
import json

conn = mysql.connector.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog'
)
cursor = conn.cursor(dictionary=True)

cursor.execute('''
    SELECT project_name, project_code, location_name, vehicle_data, vehicle_names,
           project_manager_name, remark, remark_planner
    FROM rentman_plannings 
    WHERE project_code = '3498'
    AND DATE(plan_start) = '2026-02-13'
''')

rows = cursor.fetchall()
print(f"Trovate {len(rows)} righe per progetto 3498 del 13/02/2026")
for r in rows:
    print(f"\nProject Name: '{r['project_name']}'")
    print(f"Location: '{r['location_name']}'")
    print(f"Vehicle Data: '{r['vehicle_data']}'")
    print(f"Vehicle Names: '{r['vehicle_names']}'")
    print(f"Project Manager: '{r['project_manager_name']}'")
    print(f"Remark: '{r['remark']}'")
    print(f"Remark Planner: '{r['remark_planner']}'")

# Check all columns
cursor.execute("SHOW COLUMNS FROM rentman_plannings")
cols = cursor.fetchall()
print("\n--- Tutte le colonne ---")
for c in cols:
    print(c['Field'])

cursor.close()
conn.close()
