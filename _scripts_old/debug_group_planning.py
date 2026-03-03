#!/usr/bin/env python3
"""Debug script per verificare i dati del group-planning."""

import json
from app import app, get_db

with app.app_context():
    db = get_db()

    # Leggi user_groups
    print("=== Contenuto tabella user_groups ===")
    groups = db.execute("SELECT * FROM user_groups").fetchall()
    for g in groups:
        print(f"  ID: {g['id']}, Nome: {g['group_name']}")
    
    print("\n=== Group ID 7 (usato dal debug) ===")
    group_7 = db.execute("SELECT * FROM user_groups WHERE id = 7").fetchone()
    if group_7:
        print(f"  Nome: {group_7['group_name']}")
    else:
        print("  ❌ Non trovato!")
    
    print("\n=== Group ID 9 ===")
    group_9 = db.execute("SELECT * FROM user_groups WHERE id = 9").fetchone()
    if group_9:
        print(f"  Nome: {group_9['group_name']}")
    else:
        print("  ❌ Non trovato!")

    print("\n✅ Debug completato")
