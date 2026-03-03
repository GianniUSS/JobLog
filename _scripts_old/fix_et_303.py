import pymysql, json
db = pymysql.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog', cursorclass=pymysql.cursors.DictCursor, autocommit=True)
cur = db.cursor()

# Aggiorna request #305 con campi blocco straordinario
cur.execute('SELECT id, extra_data FROM user_requests WHERE request_type_id=9 ORDER BY id DESC LIMIT 1')
r = cur.fetchone()
if r:
    req_id = r['id']
    ed = json.loads(r['extra_data'])
    
    # Aggiungi blocco straordinario e differenza
    # worked=546 nette, planned=480, extra_lordo=66, blocco=30 floor -> extra=60, diff=6
    ed.setdefault('worked_minutes', 546)
    ed.setdefault('planned_minutes', 480)
    ed.setdefault('pausa_effettiva_minuti', 60)
    ed.setdefault('pausa_pianificata_minuti', 60)
    ed['extra_minutes_lordo'] = ed.get('worked_minutes', 546) - ed.get('planned_minutes', 480)
    ed['blocco_straordinario_minuti'] = 30
    ed['tipo_arrotondamento'] = 'floor'
    ed['differenza_minuti'] = ed['extra_minutes_lordo'] - ed.get('extra_minutes', 60)
    ed.setdefault('break_confirmed', False)
    ed.setdefault('break_skipped', False)
    ed.setdefault('break_skip_reason', '')
    
    cur.execute('UPDATE user_requests SET extra_data=%s WHERE id=%s', (json.dumps(ed), req_id))
    print('Updated #%s:' % req_id)
    for k in ['extra_minutes_lordo', 'blocco_straordinario_minuti', 'tipo_arrotondamento', 'differenza_minuti', 'extra_minutes']:
        print('  %s: %s' % (k, ed.get(k)))

db.close()
