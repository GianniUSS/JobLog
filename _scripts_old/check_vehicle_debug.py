import mysql.connector
import json

conn = mysql.connector.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog'
)
cursor = conn.cursor(dictionary=True)

# Ricostruisco esattamente la logica del template JS 
cursor.execute('''
    SELECT project_code, vehicle_data, vehicle_names
    FROM rentman_plannings 
    WHERE project_code = '3498'
    AND DATE(plan_start) = '2026-02-13'
    LIMIT 1
''')

row = cursor.fetchone()
if row:
    vd = row['vehicle_data']
    vn = row['vehicle_names']
    
    print(f"vehicle_names campo DB: '{vn}'")
    print(f"vehicle_data campo DB: '{vd}'")
    print()
    
    if vd:
        vehicles = json.loads(vd)
        print(f"Numero veicoli: {len(vehicles)}")
        for i, v in enumerate(vehicles):
            name = v.get('name', '')
            plate = v.get('plate', '')
            driver_name = v.get('driver_name', '')
            driver_crew_id = v.get('driver_crew_id', '')
            
            # Simulo la logica del template JS
            if name and plate:
                vLabel = f"{name} - {plate}"
            else:
                vLabel = name or plate
            
            isAssigned = bool(driver_name)
            cssClass = "assigned" if isAssigned else ""
            visibility = "HIDDEN (display:none)" if isAssigned else "VISIBLE"
            
            print(f"\nVeicolo {i+1}:")
            print(f"  name='{name}', plate='{plate}'")
            print(f"  driver_name='{driver_name}', driver_crew_id='{driver_crew_id}'")
            print(f"  vLabel='{vLabel}'")
            print(f"  isAssigned={isAssigned}, class='{cssClass}'")
            print(f"  {visibility}")
            print(f"  Output HTML: 🚛 {vLabel}")
            
            # Check per parentesi
            if '(' in vLabel:
                print(f"  ⚠️ CONTIENE PARENTESI APERTA!")
    
    # Ricostruisco il vehicle_names come lo fa il backend
    if vd:
        vehicles = json.loads(vd)
        vehicle_names_rebuilt = ", ".join(
            f"{v['name']} ({v['plate']})" if v.get('name') and v.get('plate')
            else v.get('name') or v.get('plate') or ''
            for v in vehicles
        )
        print(f"\nvehicle_names ricostruito: '{vehicle_names_rebuilt}'")
        if '(' in vehicle_names_rebuilt:
            print("⚠️ vehicle_names contiene parentesi!")

# Check anche le assegnazioni nella tabella vehicle_driver_assignments
cursor.execute('''
    SELECT * FROM vehicle_driver_assignments 
    WHERE planning_date = '2026-02-13'
    AND project_id = 3474
''')
rows = cursor.fetchall()
print(f"\nAssegnazioni autisti salvate: {len(rows)}")
for r in rows:
    print(f"  vehicle_id={r.get('vehicle_id')}, vehicle_name='{r.get('vehicle_name')}', driver='{r.get('driver_name')}'")

cursor.close()
conn.close()
