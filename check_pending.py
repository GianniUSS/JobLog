import json
with open('config.json') as f:
    cfg = json.load(f)

import pymysql
conn = pymysql.connect(
    host=cfg['db']['host'], 
    user=cfg['db']['user'], 
    password=cfg['db']['password'], 
    database=cfg['db']['database']
)
cur = conn.cursor()

# Verifica timbrature di oggi
cur.execute('''
    SELECT id, username, tipo, data, ora 
    FROM timbrature 
    WHERE data = CURDATE() AND username = %s
''', ('gianni.picoco',))
print('=== TIMBRATURE OGGI ===')
for r in cur.fetchall():
    print(r)

# Verifica cedolino_timbrature di oggi
cur.execute('''
    SELECT id, username, timeframe_id, data_riferimento, overtime_request_id, synced_ts, sync_error 
    FROM cedolino_timbrature 
    WHERE data_riferimento = CURDATE() AND username = %s
''', ('gianni.picoco',))
print('\n=== CEDOLINO_TIMBRATURE OGGI ===')
for r in cur.fetchall():
    print(r)

conn.close()
