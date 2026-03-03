import sqlite3

conn = sqlite3.connect('joblog.db')
conn.row_factory = sqlite3.Row

print("\n=== STATISTICHE DATABASE ===")
print(f"Eventi totali: {conn.execute('SELECT COUNT(*) FROM event_log').fetchone()[0]}")
print(f"Eventi move: {conn.execute('SELECT COUNT(*) FROM event_log WHERE kind=?', ('move',)).fetchone()[0]}")
print(f"Eventi finish: {conn.execute('SELECT COUNT(*) FROM event_log WHERE kind=?', ('finish_activity',)).fetchone()[0]}")
print(f"Eventi pause: {conn.execute('SELECT COUNT(*) FROM event_log WHERE kind=?', ('pause_member',)).fetchone()[0]}")
print(f"Eventi resume: {conn.execute('SELECT COUNT(*) FROM event_log WHERE kind=?', ('resume_member',)).fetchone()[0]}")

print("\n=== ULTIMI 5 EVENTI ===")
cursor = conn.execute('''
    SELECT el.kind, el.member_key, ms.member_name, el.details
    FROM event_log el
    LEFT JOIN member_state ms ON el.member_key = ms.member_key
    ORDER BY el.ts DESC
    LIMIT 5
''')

for row in cursor:
    print(f"Kind: {row['kind']}, Member: {row['member_name']}, Details: {row['details'][:50] if row['details'] else 'N/A'}")

conn.close()
