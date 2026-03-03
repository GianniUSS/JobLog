#!/usr/bin/env python
"""Corregge i dati Fuori Flessibilità esistenti."""
import pymysql
import json
import sys

# Forza output immediato
sys.stdout = open('fix_result.txt', 'w')

conn = pymysql.connect(
    host='localhost',
    user='root', 
    password='musa',
    database='joblog',
    cursorclass=pymysql.cursors.DictCursor
)

with conn.cursor() as cur:
    # Trova la richiesta Fuori Flessibilità approvata per giannipi del 17/01/2026
    cur.execute('''
        SELECT id, extra_data, status 
        FROM user_requests 
        WHERE username = 'giannipi' 
        AND request_type_id = 17 
        AND date_from = '2026-01-17'
        ORDER BY id DESC LIMIT 1
    ''')
    req = cur.fetchone()
    print(f"Richiesta trovata: ID={req['id'] if req else 'None'}, status={req['status'] if req else ''}")
    
    if req and req['status'] == 'approved':
        # Aggiorna extra_data con ora_finale
        extra = json.loads(req['extra_data']) if req['extra_data'] else {}
        print(f"Extra data attuale: {extra}")
        extra['ora_finale'] = '19:00'
        cur.execute('UPDATE user_requests SET extra_data = %s WHERE id = %s', 
                   (json.dumps(extra), req['id']))
        print(f"Aggiornato extra_data con ora_finale=19:00")
        
        # Aggiorna timbrature
        cur.execute('''
            UPDATE timbrature 
            SET ora_mod = '19:00'
            WHERE username = 'giannipi' 
            AND data = '2026-01-17' 
            AND tipo = 'fine_giornata'
        ''')
        print(f"Aggiornate {cur.rowcount} righe in timbrature con ora_mod=19:00")
        
        conn.commit()
        print("✅ FATTO!")
    else:
        print("❌ Nessuna richiesta approvata trovata")

conn.close()
