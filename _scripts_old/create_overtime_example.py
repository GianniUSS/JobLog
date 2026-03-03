#!/usr/bin/env python
"""Script per creare un esempio di richiesta straordinario usando MySQL."""
import pymysql
import json
import time

# Configurazione database
config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'tim_root',
    'password': 'gianni225524',
    'database': 'joblog',
    'cursorclass': pymysql.cursors.DictCursor
}

db = pymysql.connect(**config)
cursor = db.cursor()

# Verifica/crea tipo Straordinario
cursor.execute('SELECT id FROM request_types WHERE name = %s', ('Straordinario',))
rt = cursor.fetchone()

if not rt:
    # Il tipo non esiste, proviamo a crearlo
    try:
        cursor.execute('''
            INSERT INTO request_types (name, value_type, description, active, sort_order)
            VALUES ('Straordinario', 'minutes', 'Richiesta ore di straordinario', 1, 99)
        ''')
        db.commit()
        cursor.execute('SELECT id FROM request_types WHERE name = %s', ('Straordinario',))
        rt = cursor.fetchone()
    except Exception as e:
        print(f"Errore creazione tipo (potrebbe essere constraint ENUM): {e}")
        # Prova con 'hours' come fallback
        cursor.execute('''
            INSERT INTO request_types (name, value_type, description, active, sort_order)
            VALUES ('Straordinario', 'hours', 'Richiesta ore di straordinario', 1, 99)
        ''')
        db.commit()
        cursor.execute('SELECT id FROM request_types WHERE name = %s', ('Straordinario',))
        rt = cursor.fetchone()

overtime_type_id = rt['id']
print(f'Tipo Straordinario ID: {overtime_type_id}')

# Trova un utente esistente
cursor.execute('SELECT username, display_name FROM app_users WHERE is_active = 1 LIMIT 1')
user = cursor.fetchone()

if not user:
    print('Nessun utente attivo trovato!')
else:
    username = user['username']
    display_name = user['display_name']
    print(f'Utente: {username} ({display_name})')
    
    # Dati esempio straordinario
    now_ts = int(time.time() * 1000)
    date_str = '2026-01-03'  # Ieri
    total_minutes = 75  # 1 ora e 15 minuti
    
    extra_data = {
        'session_id': 12345,
        'planning_id': 100,
        'shift_source': 'rentman',
        'planned_start': '09:00',
        'planned_end': '18:00',
        'actual_start': '08:30',
        'actual_end': '18:45',
        'extra_minutes_before': 30,
        'extra_minutes_after': 45,
        'overtime_type': 'both',
        'auto_detected': True
    }
    
    # Inserisci richiesta esempio
    cursor.execute('''
        INSERT INTO user_requests 
        (user_id, username, request_type_id, date_from, date_to, value_amount, 
         notes, cdc, attachment_path, tratte, extra_data, status, created_ts, updated_ts)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s, %s)
    ''', (0, username, overtime_type_id, date_str, date_str, total_minutes, 
          'Straordinario per completamento progetto urgente', None, None, None, 
          json.dumps(extra_data), now_ts, now_ts))
    
    db.commit()
    print('✅ Richiesta straordinario creata!')
    print(f'   📅 Data: {date_str}')
    hours = total_minutes // 60
    mins = total_minutes % 60
    print(f'   ⏱️  Minuti: {total_minutes} ({hours}h {mins}m)')
    print(f'   📋 Turno pianificato: 09:00 - 18:00')
    print(f'   ✅ Orario effettivo: 08:30 - 18:45')
    print(f'   📝 Status: pending')

cursor.close()
db.close()
