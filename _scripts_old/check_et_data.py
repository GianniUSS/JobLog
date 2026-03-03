import sqlite3, json
db = sqlite3.connect('instance/joblog.db')
db.row_factory = sqlite3.Row
r = db.execute('SELECT id, extra_data, value_amount, status FROM user_requests WHERE request_type_id=9 ORDER BY id DESC LIMIT 1').fetchone()
if r:
    print(f"Request #{r['id']}, status={r['status']}, value={r['value_amount']}")
    ed = json.loads(r['extra_data']) if r['extra_data'] else {}
    for k, v in ed.items():
        print(f"  {k}: {v}")
    # Check for break fields
    has_break = any(k.startswith('break_') or k == 'pausa_pianificata_minuti' for k in ed.keys())
    print(f"\nHas break fields: {has_break}")
else:
    print('No Extra Turno request found')
db.close()
