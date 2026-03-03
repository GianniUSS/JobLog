import app
import json

with app.app.app_context():
    db = app.get_db()
    
    # Controlla tutti i permessi (tipo 3)
    permissions = db.execute("""
        SELECT id, username, request_type_id, date_from, date_to, extra_data, status
        FROM user_requests
        WHERE request_type_id = 3
        ORDER BY created_ts DESC
    """).fetchall()
    
    print(f"Trovati {len(permissions)} permessi:\n")
    for p in permissions:
        perm_id = p[0] if isinstance(p, tuple) else p['id']
        username = p[1] if isinstance(p, tuple) else p['username']
        date_from = p[3] if isinstance(p, tuple) else p['date_from']
        extra_data = p[5] if isinstance(p, tuple) else p['extra_data']
        status = p[6] if isinstance(p, tuple) else p['status']
        
        print(f"ID: {perm_id}, User: {username}, Date: {date_from}, Status: {status}")
        print(f"  Extra Data: {extra_data}")
        print()
