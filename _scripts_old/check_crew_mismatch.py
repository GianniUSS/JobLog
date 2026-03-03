#!/usr/bin/env python3
from app import app, get_db

with app.app_context():
    db = get_db()
    
    print("=== CREW_ID NEGLI UTENTI DEL GRUPPO 7 ===")
    users = db.execute('SELECT username, rentman_crew_id FROM app_users WHERE group_id = 7 AND is_active = 1 ORDER BY username').fetchall()
    for u in users:
        print(f"  {u['username']:15} -> crew_id: {u['rentman_crew_id']}")
    
    print("\n=== CREW_ID NEI TURNI DEL 8 GENNAIO ===")
    crews = db.execute('SELECT DISTINCT crew_id, crew_name FROM rentman_plannings WHERE planning_date = "2026-01-08" ORDER BY crew_id').fetchall()
    for c in crews:
        print(f"  crew_id {c['crew_id']}: {c['crew_name']}")
