#!/usr/bin/env python
"""Fix: Cambia oltre_flessibilita_action da 'block' a 'warn' per il gruppo Ufficio"""

import pymysql
import json

with open('config.json') as f:
    cfg = json.load(f)

db_cfg = cfg.get('database', {})
conn = pymysql.connect(
    host=db_cfg.get('host'),
    port=int(db_cfg.get('port', 3306)),
    user=db_cfg.get('user'),
    password=db_cfg.get('password'),
    database=db_cfg.get('name'),
    cursorclass=pymysql.cursors.DictCursor
)

# Cambia action da 'block' a 'warn' per il gruppo Ufficio (ID 9)
with conn.cursor() as cur:
    cur.execute(
        'UPDATE group_timbratura_rules SET oltre_flessibilita_action = %s WHERE group_id = %s',
        ('warn', 9)
    )
    print(f'Rows affected: {cur.rowcount}')
    
    # Verifica
    cur.execute('SELECT group_id, oltre_flessibilita_action FROM group_timbratura_rules WHERE group_id = %s', (9,))
    row = cur.fetchone()
    print(f'Nuovo valore: {row}')

conn.commit()
conn.close()
print('Fatto! Le timbrature fuori flessibilità ora verranno accettate con un avviso.')
