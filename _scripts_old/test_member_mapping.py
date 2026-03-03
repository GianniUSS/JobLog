"""Test del mapping member_key -> username"""
import mysql.connector

conn = mysql.connector.connect(
    host='localhost', 
    user='tim_root', 
    password='gianni225524', 
    database='joblog'
)
cur = conn.cursor(dictionary=True)

# Simula la logica della funzione get_username_from_member_key
member_keys = [
    'rentman-crew-1903',  # Gentian Deda
    'rentman-crew-1904',  # Donato Laviola
    'rentman-crew-1905',  # Tonio Calianno
    'rentman-crew-1906',  # Marius Galan
    'rentman-crew-1940',  # Angelo Semeraro
]

print("Test mapping member_key -> username:\n")

for member_key in member_keys:
    rentman_id = int(member_key.replace('rentman-crew-', ''))
    
    # Step 1: Cerca crew_id da rentman_plannings
    cur.execute(
        'SELECT crew_id, crew_name FROM rentman_plannings WHERE rentman_id = %s LIMIT 1', 
        (rentman_id,)
    )
    planning = cur.fetchone()
    
    if planning:
        crew_id = planning['crew_id']
        crew_name = planning['crew_name']
        
        # Step 2: Cerca username da app_users
        cur.execute(
            'SELECT username FROM app_users WHERE rentman_crew_id = %s AND is_active = 1', 
            (crew_id,)
        )
        user = cur.fetchone()
        username = user['username'] if user else None
        
        status = "✓" if username else "✗"
        print(f"{status} {member_key}")
        print(f"   rentman_id={rentman_id} -> crew_name='{crew_name}', crew_id={crew_id}")
        print(f"   -> username='{username}'\n")
    else:
        print(f"✗ {member_key}")
        print(f"   rentman_id={rentman_id} -> NOT FOUND in rentman_plannings\n")

conn.close()
