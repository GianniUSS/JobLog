import sqlite3, json

conn = sqlite3.connect('joblog.db')
conn.row_factory = sqlite3.Row
rows = conn.execute('SELECT ts, kind, member_key, details FROM event_log ORDER BY ts LIMIT 40').fetchall()
for row in rows:
    details = json.loads(row['details']) if row['details'] else {}
    print(row['ts'], row['kind'], row['member_key'], details)
