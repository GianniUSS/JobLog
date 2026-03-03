import sys; sys.path.insert(0,'.')
from app import app, get_db
from datetime import datetime

with app.app_context():
    db = get_db()
    rows = db.execute("""
        SELECT member_key, member_name, activity_id, running, start_ts, entered_ts, 
               elapsed_cached, pause_start, project_code, current_phase
        FROM member_state WHERE activity_id IS NOT NULL
    """).fetchall()
    print(f"Righe attive: {len(rows)}")
    for r in rows:
        st = r["start_ts"]
        et = r["entered_ts"]
        st_str = datetime.fromtimestamp(st/1000).strftime("%Y-%m-%d %H:%M:%S") if st else "NULL"
        et_str = datetime.fromtimestamp(et/1000).strftime("%Y-%m-%d %H:%M:%S") if et else "NULL"
        print(f"  {r['member_name']} ({r['member_key']})")
        print(f"    activity={r['activity_id']} running={r['running']} phase={r['current_phase']}")
        print(f"    project_code={r['project_code']}")
        print(f"    start_ts={st} ({st_str})")
        print(f"    entered_ts={et} ({et_str})")
        print(f"    elapsed_cached={r['elapsed_cached']} pause_start={r['pause_start']}")
