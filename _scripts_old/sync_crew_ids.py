#!/usr/bin/env python3
"""Sincronizza i crew_id corretti da Rentman a app_users"""

from app import app, get_db

with app.app_context():
    db = get_db()
    
    # Mapping tra crew_id corretti nei turni salvati
    correct_crew_ids = {
        'donato': 1923,
        'ciro': 1896,
        'angelo': 1921,
        'enzo': 1927,
        'marius': 1928,
        'tonio': 1932
    }
    
    print("=== AGGIORNAMENTO CREW_ID IN APP_USERS ===")
    
    for username, correct_crew_id in correct_crew_ids.items():
        current = db.execute(
            'SELECT rentman_crew_id FROM app_users WHERE username = %s',
            (username,)
        ).fetchone()
        
        if current:
            current_id = current['rentman_crew_id']
            if current_id != correct_crew_id:
                print(f"❌ {username}: {current_id} -> corretto: {correct_crew_id}")
                db.execute(
                    'UPDATE app_users SET rentman_crew_id = %s WHERE username = %s',
                    (correct_crew_id, username)
                )
                db.commit()
                print(f"   ✅ Aggiornato!")
            else:
                print(f"✅ {username}: {current_id} (corretto)")
        else:
            print(f"⚠️  {username}: non trovato")
    
    print("\n✅ Sincronizzazione completata!")
