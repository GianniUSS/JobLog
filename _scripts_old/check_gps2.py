import pymysql

conn = pymysql.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
c = conn.cursor()

# Simulazione della logica dell'API turno-oggi
crew_id = 1923  # Donato
today = '2026-01-07'

c.execute("""
    SELECT project_code, project_name, function_name, plan_start, plan_end,
           hours_planned, remark, is_leader, transport, break_start, break_end, break_minutes,
           location_name, location_lat, location_lon, timbratura_gps_mode, gps_timbratura_location
    FROM rentman_plannings
    WHERE crew_id = %s AND planning_date = %s AND sent_to_webservice = 1
    ORDER BY plan_start ASC
""", (crew_id, today))

row = c.fetchone()
if row:
    print(f"Project: {row[1]}")
    print(f"Location Name: {row[12]}")
    print(f"Location Lat: {row[13]}")
    print(f"Location Lon: {row[14]}")
    print(f"GPS Mode: {row[15]}")
    print(f"Group GPS Location: {row[16]}")
    
    gps_mode = row[15] or 'group'
    location_name = row[12]
    location_lat = row[13]
    location_lon = row[14]
    gps_timbratura_location = row[16]
    
    print(f"\n--- Logica di calcolo ---")
    if gps_mode == 'location' and location_name:
        print(f"Modalità: location progetto")
        print(f"timbratura_location = {location_name}")
        print(f"timbratura_lat = {location_lat}")
        print(f"timbratura_lon = {location_lon}")
        has_coords = location_lat and location_lon
        print(f"Has Coords: {has_coords}")
    elif gps_timbratura_location:
        print(f"Modalità: sede gruppo")
        print(f"timbratura_location = {gps_timbratura_location}")
        print("(dovrebbe cercare coordinate nella config)")
else:
    print("Nessun turno trovato")

conn.close()
