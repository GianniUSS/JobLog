import sys; sys.path.insert(0,'.')
from app import app, get_db
from datetime import datetime

with app.app_context():
    db = get_db()
    
    # Tutti gli event_log
    events = db.execute("SELECT * FROM event_log ORDER BY ts ASC").fetchall()
    print(f"=== Tutti gli eventi in event_log ({len(events)}) ===")
    for e in events:
        ts_str = datetime.fromtimestamp(e["ts"]/1000).strftime("%Y-%m-%d %H:%M:%S") if e["ts"] else "NULL"
        print(f"  [{ts_str}] {e['kind']} member={e['member_key']} details={e['details'][:100] if e['details'] else 'NULL'}")
    
    # Tutte le warehouse_sessions
    wh = db.execute("SELECT * FROM warehouse_sessions ORDER BY created_ts DESC LIMIT 20").fetchall()
    print(f"\n=== Warehouse sessions (ultimi 20): {len(wh)} ===")
    for w in wh:
        ts_str = datetime.fromtimestamp(w["created_ts"]/1000).strftime("%Y-%m-%d %H:%M:%S") if w["created_ts"] else "NULL"
        print(f"  [{ts_str}] {w['username']}: {w['activity_label']} ({w['project_code']}) elapsed={w['elapsed_ms']}ms")
    
    # member_state completo
    ms = db.execute("SELECT * FROM member_state").fetchall()
    print(f"\n=== member_state completo ({len(ms)}) ===")
    for r in ms:
        act = r.get("activity_id") or "NULL"
        pc = r.get("project_code") or "NULL"
        run = r.get("running", 0)
        st = r.get("start_ts")
        st_str = datetime.fromtimestamp(st/1000).strftime("%H:%M:%S") if st else "NULL"
        print(f"  {r['member_name']} ({r['member_key']}): activity={act} project={pc} running={run} start={st_str}")
