import pymysql, json
conn = pymysql.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog', cursorclass=pymysql.cursors.DictCursor, charset='utf8mb4')
cur = conn.cursor()

print("=== MEMBER_STATE ===")
cur.execute("SELECT member_key, member_name, activity_id, running, start_ts, elapsed_cached FROM member_state WHERE project_code=%s", ('4589',))
for r in cur.fetchall(): print(r)

print("\n=== EVENT_LOG (last 15) ===")
cur.execute("SELECT id, ts, kind, member_key, LEFT(details, 150) as d FROM event_log WHERE project_code=%s ORDER BY ts DESC LIMIT 15", ('4589',))
for r in cur.fetchall(): print(r)

print("\n=== PHASE_PROGRESS ===")
cur.execute("SELECT * FROM project_phase_progress WHERE project_key=%s ORDER BY function_key, phase_order", ('4589',))
for r in cur.fetchall(): print(r)

print("\n=== FUNCTION_PHASES CONFIG ===")
cur.execute("SELECT value FROM company_settings WHERE key_name='custom_settings'")
row = cur.fetchone()
if row:
    val = json.loads(row['value'])
    phases = val.get('function_phases', {})
    for k, v in phases.items():
        print(f"[{k}]:", json.dumps(v, ensure_ascii=False))

conn.close()
