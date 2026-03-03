import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog'
)
cursor = conn.cursor()

cursor.execute('''
    SELECT project_id, project_code, project_name, vehicle_data, vehicle_names 
    FROM rentman_plannings 
    WHERE project_code LIKE "%3498%"
    LIMIT 1
''')

r = cursor.fetchone()
if r:
    print(f'Project ID: {r[0]}')
    print(f'Code: {r[1]}')
    print(f'Name: {r[2]}')
    print(f'Vehicle Data: {r[3]}')
    print(f'Vehicle Names: {r[4]}')
else:
    print('Nessun progetto trovato con 3498')

cursor.close()
conn.close()
