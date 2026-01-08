import pymysql

conn = pymysql.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
c = conn.cursor()
c.execute("""
    SELECT crew_name, gps_timbratura_location, timbratura_gps_mode, location_name, location_lat, location_lon 
    FROM rentman_plannings 
    WHERE planning_date = '2026-01-07' AND crew_id = 1923
""")
row = c.fetchone()
print(f"Crew: {row[0]}")
print(f"GPS Mode: {row[2]}")
print(f"Group GPS Location: {row[1]}")
print(f"Project Location: {row[3]}")
print(f"Location Lat: {row[4]}")
print(f"Location Lon: {row[5]}")
conn.close()
