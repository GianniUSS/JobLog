#!/usr/bin/env python3
"""Fix extra_data for existing Extra Turno requests"""
import mysql.connector
import json

with open('config.json') as f:
    cfg = json.load(f)

db_cfg = cfg['database']
conn = mysql.connector.connect(
    host=db_cfg['host'],
    port=db_cfg['port'],
    user=db_cfg['user'],
    password=db_cfg['password'],
    database=db_cfg['name'],
    charset='utf8mb4'
)
cursor = conn.cursor(dictionary=True)

# Query Extra Turno requests that need to be fixed
cursor.execute('''
    SELECT id, extra_data 
    FROM user_requests 
    WHERE request_type_id = 9 
    AND extra_data NOT LIKE '%planned_start%'
''')
rows = cursor.fetchall()

print(f"Trovate {len(rows)} richieste da aggiornare\n")

for r in rows:
    try:
        ed = json.loads(r['extra_data']) if isinstance(r['extra_data'], str) else r['extra_data']
        
        # Extract existing values
        extra_type = ed.get('extra_type', ed.get('overtime_type', 'before_shift'))
        turno_time = ed.get('turno_time', '08:00')  # This was the shift start time
        ora_timbrata = ed.get('ora_timbrata', '')
        ora_mod = ed.get('ora_mod', '')
        extra_minutes = ed.get('extra_minutes', 0)
        
        # Add new fields based on extra_type
        if extra_type == 'before_shift':
            ed['planned_start'] = turno_time
            ed['planned_end'] = '17:00'  # Default, not known
            ed['actual_start'] = ora_timbrata[:5] if ora_timbrata else ''
            ed['actual_end'] = ''
            ed['rounded_start'] = ora_mod[:5] if ora_mod else ''
            ed['rounded_end'] = ''
            ed['extra_minutes_before'] = extra_minutes
            ed['extra_minutes_after'] = 0
        else:  # after_shift
            ed['planned_start'] = '08:00'  # Default, not known
            ed['planned_end'] = turno_time
            ed['actual_start'] = ''
            ed['actual_end'] = ora_timbrata[:5] if ora_timbrata else ''
            ed['rounded_start'] = ''
            ed['rounded_end'] = ora_mod[:5] if ora_mod else ''
            ed['extra_minutes_before'] = 0
            ed['extra_minutes_after'] = extra_minutes
        
        # Update in database
        cursor.execute('''
            UPDATE user_requests 
            SET extra_data = %s 
            WHERE id = %s
        ''', (json.dumps(ed), r['id']))
        
        print(f"ID {r['id']} aggiornato:")
        print(f"  planned_start: {ed.get('planned_start')}")
        print(f"  planned_end: {ed.get('planned_end')}")
        print(f"  actual_start: {ed.get('actual_start')}")
        print(f"  actual_end: {ed.get('actual_end')}")
        print(f"  rounded_start: {ed.get('rounded_start')}")
        print(f"  rounded_end: {ed.get('rounded_end')}")
        print(f"  extra_minutes_before: {ed.get('extra_minutes_before')}")
        print(f"  extra_minutes_after: {ed.get('extra_minutes_after')}")
        print()
        
    except Exception as e:
        print(f"Errore per ID {r['id']}: {e}")

conn.commit()
conn.close()
print("Aggiornamento completato!")
