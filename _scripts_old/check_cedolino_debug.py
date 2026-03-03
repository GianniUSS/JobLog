import pymysql

conn = pymysql.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog',
    cursorclass=pymysql.cursors.DictCursor
)

with conn.cursor() as cursor:
    print("=== ULTIME CEDOLINO_TIMBRATURE ===")
    cursor.execute('''
        SELECT id, username, data_riferimento, ora_originale, ora_modificata, 
               sync_error, overtime_request_id, synced_ts, timeframe_id
        FROM cedolino_timbrature
        WHERE username = 'giannipi'
        ORDER BY id DESC LIMIT 10
    ''')
    rows = cursor.fetchall()

    for r in rows:
        print("="*70)
        # Converti timedelta in stringa leggibile
        ora_orig = str(r['ora_originale']) if r['ora_originale'] else None
        ora_mod = str(r['ora_modificata']) if r['ora_modificata'] else None
        print(f"ID: {r['id']}, Data: {r['data_riferimento']}")
        print(f"  ora_originale: {ora_orig}")
        print(f"  ora_modificata: {ora_mod}")
        print(f"  sync_error: {r['sync_error']}")
        print(f"  synced_ts: {r['synced_ts']}")
        print(f"  overtime_request_id: {r['overtime_request_id']}")
        print(f"  timeframe_id: {r['timeframe_id']}")
