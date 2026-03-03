"""Verify that open-sessions now returns operators with phase progress."""
import pymysql, json, re
from datetime import datetime

conn = pymysql.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog', cursorclass=pymysql.cursors.DictCursor, charset='utf8mb4')
cur = conn.cursor()

# Get function phases config
cur.execute("SHOW COLUMNS FROM company_settings")
cols = [r['Field'] for r in cur.fetchall()]
print("company_settings columns:", cols)

print("\n=== Simulating open-sessions logic ===")
today_str = datetime.now().strftime("%Y-%m-%d")

# Get phase progress
cur.execute("SELECT function_key, phase_name, completed FROM project_phase_progress WHERE project_date=%s AND project_key=%s", (today_str, '4589'))
phase_progress = {}
for r in cur.fetchall():
    fk = r['function_key']
    if fk not in phase_progress:
        phase_progress[fk] = {}
    phase_progress[fk][r['phase_name']] = bool(r['completed'])

print("Phase progress:", json.dumps(phase_progress, ensure_ascii=False, indent=2))

# Get member_state
cur.execute("""SELECT ms.member_key, ms.member_name, ms.activity_id, ms.running, 
               ms.start_ts, ms.elapsed_cached, ms.pause_start, ms.entered_ts,
               a.label AS activity_label
        FROM member_state ms
        LEFT JOIN activities a ON ms.activity_id = a.activity_id AND ms.project_code = a.project_code
        WHERE ms.activity_id IS NOT NULL AND ms.project_code = %s""", ('4589',))

for row in cur.fetchall():
    running = int(row['running'])
    start_ts = row['start_ts']
    elapsed = row['elapsed_cached'] or 0
    pause_start = row['pause_start']
    
    would_skip_old = (running != 1 and start_ts is None and elapsed == 0 and pause_start is None)
    
    # New logic: check phase progress
    has_phase_progress = False
    act_label = row['activity_label'] or ''
    fn = re.sub(r'\s*\[ID\s+\d+\]$', '', act_label, flags=re.IGNORECASE).strip()
    if fn:
        for fk in phase_progress:
            if fk.lower().strip() == fn.lower():
                has_phase_progress = any(phase_progress[fk].values())
                break
    
    would_skip_new = would_skip_old and not has_phase_progress
    
    print(f"\n{row['member_name']}:")
    print(f"  activity: {act_label}, function: {fn}")
    print(f"  running={running}, start_ts={start_ts}, elapsed={elapsed}, pause_start={pause_start}")
    print(f"  OLD filter would skip: {would_skip_old}")
    print(f"  has_phase_progress: {has_phase_progress}")
    print(f"  NEW filter would skip: {would_skip_new}")

conn.close()
