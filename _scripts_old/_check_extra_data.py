import json, pymysql
cfg = json.load(open('config.json'))['database']
db = pymysql.connect(host=cfg['host'],port=cfg['port'],user=cfg['user'],password=cfg['password'],db=cfg['name'],cursorclass=pymysql.cursors.DictCursor)
cur = db.cursor()
q = 'SELECT ur.id, ur.username, ur.date_from, ur.extra_data FROM user_requests ur JOIN request_types rt ON ur.request_type_id=rt.id WHERE rt.name=%s AND ur.status=%s ORDER BY ur.date_from DESC'
cur.execute(q, ('Deroga Pausa Ridotta', 'approved'))
for r in cur.fetchall():
    print(r['id'], r['username'], r['date_from'])
    ed = json.loads(r['extra_data']) if r['extra_data'] else {}
    print('  rounded_break_minutes:', ed.get('rounded_break_minutes', 'ASSENTE'))
    print('  effective_break_minutes:', ed.get('effective_break_minutes', 'ASSENTE'))
    print('  keys:', list(ed.keys()))
db.close()
