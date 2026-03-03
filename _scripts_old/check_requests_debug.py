import pymysql

conn = pymysql.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog',
    cursorclass=pymysql.cursors.DictCursor
)

with conn.cursor() as cursor:
    print("=== ULTIME RICHIESTE ===")
    cursor.execute('''
        SELECT ur.id, ur.username, rt.name as type_name, rt.value_type, ur.status, ur.extra_data
        FROM user_requests ur
        JOIN request_types rt ON ur.request_type_id = rt.id
        WHERE ur.username = 'giannipi'
        ORDER BY ur.id DESC LIMIT 5
    ''')
    rows = cursor.fetchall()

    for r in rows:
        print("="*60)
        print(r)

    # Check cedolino_timbrature
    print("\n\n=== CEDOLINO TIMBRATURE ===")
    cursor.execute('''
        SELECT id, username, data_riferimento, ora_originale, ora_modificata, sync_error, overtime_request_id, synced_ts
        FROM cedolino_timbrature
        WHERE username = 'giannipi'
        ORDER BY id DESC LIMIT 5
    ''')
    rows2 = cursor.fetchall()

    for r in rows2:
        print(r)
