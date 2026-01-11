#!/usr/bin/env python3
from app import app, get_db

with app.app_context():
    db = get_db()
    
    # Cerca tabelle che contengono 'ferie', 'leave', 'holiday', 'vacation', 'absence'
    tables = db.execute('''
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = DATABASE()
        ORDER BY TABLE_NAME
    ''').fetchall()
    
    keywords = ['ferie', 'leave', 'holiday', 'vacation', 'absence', 'request', 'permission', 'permit', 'day_off']
    
    print("=== TABELLE CHE POTREBBERO CONTENERE FERIE ===")
    for t in tables:
        name = t['TABLE_NAME'].lower()
        if any(kw in name for kw in keywords):
            table_name = t['TABLE_NAME']
            print(f"  ✓ {table_name}")
            
            # Mostra le colonne
            cols = db.execute(f"DESCRIBE {table_name}").fetchall()
            for col in cols:
                print(f"    - {col['Field']}")
