import mysql.connector
import json
import requests

# Login con sessione
session = requests.Session()
login_resp = session.post('http://localhost:5000/api/login', json={
    'username': 'admin',
    'password': 'admin'
}, allow_redirects=False)
print(f"Login status: {login_resp.status_code}")
print(f"Login response: {login_resp.json()}")

# Fetch saved plannings
resp = session.get('http://localhost:5000/api/admin/rentman-planning/saved?date=2026-02-13')
data = resp.json()
print(f"Plannings count: {data.get('count', 0)}")

# Check vehicle_data per progetto 3498
for p in data.get('plannings', []):
    if p.get('project_code') == '3498':
        vd = p.get('vehicle_data')
        vn = p.get('vehicle_names')
        print(f"\nCrew: {p.get('crew_name')}")
        print(f"  vehicle_data type: {type(vd).__name__}")
        print(f"  vehicle_data value: '{vd}'")
        print(f"  vehicle_names: '{vn}'")
        
        if vd:
            try:
                vehicles = json.loads(vd) if isinstance(vd, str) else vd
                print(f"  Parsed vehicles type: {type(vehicles).__name__}")
                print(f"  Parsed vehicles length: {len(vehicles)}")
                for v in vehicles:
                    name = v.get('name', '')
                    plate = v.get('plate', '')
                    driver = v.get('driver_name', '')
                    print(f"    Vehicle: name='{name}', plate='{plate}', driver='{driver}'")
                    
                    # Check label che il frontend genera
                    if name and plate:
                        label = f"{name} ({plate})"  # OLD code
                        label_new = f"{name} - {plate}"  # NEW code
                    else:
                        label = name or plate
                        label_new = name or plate
                    print(f"    Old label: '{label}'")
                    print(f"    New label: '{label_new}'")
                    print(f"    Has '(': {'(' in label}")
            except Exception as e:
                print(f"  Parse error: {e}")
        break
