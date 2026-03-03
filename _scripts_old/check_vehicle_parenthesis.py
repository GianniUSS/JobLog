import mysql.connector
import json

conn = mysql.connector.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog'
)
cursor = conn.cursor()

# Cerca in tutti i planning se c'è vehicle_data con parentesi aperte
cursor.execute('''
    SELECT project_id, project_code, project_name, vehicle_data 
    FROM rentman_plannings 
    WHERE vehicle_data IS NOT NULL
   AND vehicle_data LIKE '%(%'
''')

rows = cursor.fetchall()
print(f"Trovate {len(rows)} righe con vehicle_data contenente '('")

for r in rows:
    project_code = r[1]
    project_name = r[2]
    vehicle_data_str = r[3]
    
    try:
        vehicles = json.loads(vehicle_data_str)
        for v in vehicles:
            name = v.get('name', '')
            plate = v.get('plate', '')
            if name == '(' or plate == '(' or (name and '(' in name and ')' not in name):
                print(f"\nProgetto {project_code} ({project_name}):")
                print(f"  Veicolo con problema: name='{name}', plate='{plate}'")
    except:
        print(f"Errore parsing JSON per progetto {project_code}")

cursor.close()
conn.close()
