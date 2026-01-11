#!/usr/bin/env python3
from app import app, get_db

with app.app_context():
    db = get_db()
    
    # Leggi colonne di user_requests
    cols = db.execute('DESCRIBE user_requests').fetchall()
    print("Colonne user_requests:")
    for col in cols:
        print(f"  {col['Field']}")
    
    # Vedi se c'è qualche campo con 'time', 'hour', 'start', 'end'
    print("\nCampi che potrebbero contenere orari:")
    for col in cols:
        field = col['Field'].lower()
        if any(x in field for x in ['time', 'hour', 'start', 'end', 'from', 'to']):
            field_name = col['Field']
            print(f"  ✓ {field_name}")
