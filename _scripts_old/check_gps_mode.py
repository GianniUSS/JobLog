import pymysql
import json

conn = pymysql.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
c = conn.cursor(pymysql.cursors.DictCursor)

# Verifica stato attuale con tutti gli ID
c.execute("""
    SELECT id, rentman_id, crew_name, location_name, gps_timbratura_location, timbratura_gps_mode 
    FROM rentman_plannings 
    WHERE planning_date = '2026-01-10'
""")

rows = c.fetchall()
print("=== DATI NEL DATABASE ===")
for r in rows:
    print(f"DB_ID={r['id']}, rentman_id={r['rentman_id']}, {r['crew_name']}: gps_mode={r['timbratura_gps_mode']}")

# Test update diretto
print("\n=== TEST UPDATE ===")
test_id = 837  # Donato
c.execute("SELECT id, timbratura_gps_mode FROM rentman_plannings WHERE id = %s", (test_id,))
before = c.fetchone()
print(f"Prima: id={test_id}, mode={before['timbratura_gps_mode'] if before else 'NON TROVATO'}")

if before:
    new_mode = 'location' if before['timbratura_gps_mode'] == 'group' else 'group'
    c.execute("UPDATE rentman_plannings SET timbratura_gps_mode = %s WHERE id = %s", (new_mode, test_id))
    conn.commit()
    
    c.execute("SELECT id, timbratura_gps_mode FROM rentman_plannings WHERE id = %s", (test_id,))
    after = c.fetchone()
    print(f"Dopo: id={test_id}, mode={after['timbratura_gps_mode']}")

conn.close()
