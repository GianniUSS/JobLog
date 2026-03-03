import app
import json

with app.app.app_context():
    db = app.get_db()
    
    # Aggiorna il permesso di donato con gli orari
    donato_permission = db.execute("""
        SELECT id FROM user_requests
        WHERE username = 'donato' AND request_type_id = 3
        ORDER BY created_ts DESC
        LIMIT 1
    """).fetchone()
    
    if donato_permission:
        perm_id = donato_permission[0] if isinstance(donato_permission, tuple) else donato_permission['id']
        extra_data = json.dumps({
            "time_start": "09:00",
            "time_end": "13:00"
        })
        
        db.execute(
            "UPDATE user_requests SET extra_data = ? WHERE id = ?",
            (extra_data, perm_id)
        )
        db.commit()
        print(f"Updated permission {perm_id} with times: 09:00 - 13:00")
    else:
        print("Permission not found")
