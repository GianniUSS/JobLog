#!/usr/bin/env python3
from app import app, get_db
import json

with app.app_context():
    db = get_db()
    
    # Verifica il tipo di extra_data
    req = db.execute('SELECT extra_data FROM user_requests LIMIT 1').fetchone()
    if req and req['extra_data']:
        try:
            data = json.loads(req['extra_data'])
            print("extra_data è JSON")
            print(f"Contenuto: {data}")
        except Exception as e:
            extra = req['extra_data']
            print(f"extra_data non è JSON valido: {extra}")
    else:
        print("extra_data è vuoto")
