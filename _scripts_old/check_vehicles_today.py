import mysql.connector, json

conn = mysql.connector.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
cursor = conn.cursor()
cursor.execute('SELECT DISTINCT project_code, project_name, vehicle_data, vehicle_names FROM rentman_plannings WHERE planning_date = %s AND is_obsolete = 0 ORDER BY project_code', ('2026-02-13',))
for row in cursor.fetchall():
    vd = []
    try:
        vd = json.loads(row[2]) if row[2] else []
    except:
        pass
    unassigned = [v for v in vd if not v.get('driver_name')]
    assigned = [v for v in vd if v.get('driver_name')]
    print(f'Progetto: {row[0]} | Nome: {row[1][:50]} | vehicle_names: {row[3]}')
    print(f'  Totale veicoli: {len(vd)} | Assegnati: {len(assigned)} | Non assegnati: {len(unassigned)}')
    for v in vd:
        name = v.get('name', '')
        plate = v.get('plate', '')
        driver = v.get('driver_name', 'None')
        print(f'  - {name} ({plate}) driver: {driver}')
    print()
conn.close()
