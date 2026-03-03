import pymysql
import json

conn = pymysql.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
c = conn.cursor()

crew_id = 1923  # Donato
today = '2026-01-07'

c.execute("""
    SELECT location_lat, location_lon
    FROM rentman_plannings
    WHERE crew_id = %s AND planning_date = %s AND sent_to_webservice = 1
""", (crew_id, today))

row = c.fetchone()
print(f"location_lat type: {type(row[0])}, value: {row[0]}")
print(f"location_lon type: {type(row[1])}, value: {row[1]}")

# Prova conversione JSON
try:
    data = {"lat": row[0], "lon": row[1]}
    json_str = json.dumps(data)
    print(f"JSON: {json_str}")
except Exception as e:
    print(f"JSON error: {e}")
    # Prova con conversione a float
    data = {"lat": float(row[0]) if row[0] else None, "lon": float(row[1]) if row[1] else None}
    json_str = json.dumps(data)
    print(f"JSON (float): {json_str}")

conn.close()
