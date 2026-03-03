#!/usr/bin/env python3
from app import app, get_db
from datetime import datetime

with app.app_context():
    db = get_db()
    
    print("=== TIPI DI RICHIESTA DISPONIBILI ===")
    types = db.execute("SELECT id, name FROM request_types WHERE active = 1 ORDER BY name").fetchall()
    for t in types:
        print(f"  ID {t['id']}: {t['name']}")
    
    print("\n=== FERIE AUTORIZZATE (status='approved') PER LA SETTIMANA 5-11 GENNAIO ===")
    
    # Utenti del gruppo 7
    users = db.execute("""
        SELECT username FROM app_users 
        WHERE group_id = 7 AND is_active = 1
        ORDER BY username
    """).fetchall()
    
    for u in users:
        username = u['username']
        # Ferie che intersecano con 5-11 gennaio
        requests = db.execute("""
            SELECT id, username, request_type_id, date_from, date_to, status, notes
            FROM user_requests
            WHERE username = %s 
              AND status = 'approved'
              AND date_from <= '2026-01-11'
              AND date_to >= '2026-01-05'
            ORDER BY date_from
        """, (username,)).fetchall()
        
        if requests:
            print(f"\n{username.upper()}:")
            for req in requests:
                # Leggi il tipo
                req_type = db.execute(
                    "SELECT name FROM request_types WHERE id = %s",
                    (req['request_type_id'],)
                ).fetchone()
                
                type_name = req_type['name'] if req_type else 'Unknown'
                print(f"  {req['date_from']} -> {req['date_to']}: {type_name}")
                print(f"    Note: {req['notes']}")
