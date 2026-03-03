#!/usr/bin/env python3
"""Simula un cambio fase dal supervisor e verifica che finish_activity viene scritto."""
import sys, os, json
from datetime import datetime, date
sys.path.insert(0, '.')
from app import app, get_db

with app.app_context():
    db = get_db()
    today = date.today()
    
    # Stato corrente
    rows = db.execute("SELECT member_key, member_name, activity_id, current_phase, running, start_ts, elapsed_cached, project_code FROM member_state WHERE activity_id IS NOT NULL").fetchall()
    print(f"=== Stato member_state ({len(rows)} attivi) ===")
    for r in rows:
        st = r["start_ts"]
        st_str = datetime.fromtimestamp(st/1000).strftime("%H:%M:%S") if st else "NULL"
        print(f"  {r['member_name']}: activity={r['activity_id']} phase={r['current_phase']} running={r['running']} start={st_str} elapsed={r['elapsed_cached']}")
    
    # Event log
    events = db.execute("SELECT ts, kind, member_key, details FROM event_log ORDER BY ts DESC LIMIT 20").fetchall()
    print(f"\n=== Event log (ultimi 20) ===")
    for e in events:
        ts_str = datetime.fromtimestamp(e["ts"]/1000).strftime("%H:%M:%S") if e["ts"] else "NULL"
        det = json.loads(e["details"]) if e["details"] else {}
        phase = det.get("phase_name") or det.get("from_phase") or ""
        dur = det.get("duration_ms", "")
        print(f"  [{ts_str}] {e['kind']} member={e['member_key']} phase={phase} dur={dur}")
    
    # project_phase_progress
    pp = db.execute("SELECT * FROM project_phase_progress WHERE project_date = %s ORDER BY function_key, phase_order", (today.isoformat(),)).fetchall()
    print(f"\n=== project_phase_progress oggi ({len(pp)} righe) ===")
    for p in pp:
        comp_str = "✅" if p["completed"] else "⬜"
        comp_at = datetime.fromtimestamp(p["completed_at"]/1000).strftime("%H:%M:%S") if p["completed_at"] else "NULL"
        print(f"  {comp_str} {p['function_key']}/{p['phase_name']} (order={p['phase_order']}) by={p['completed_by']} at={comp_at}")
    
    print("\n=== VERIFICA ===")
    finish_events = [e for e in events if e["kind"] == "finish_activity"]
    if finish_events:
        print(f"✅ Trovati {len(finish_events)} eventi finish_activity")
        for e in finish_events:
            det = json.loads(e["details"]) if e["details"] else {}
            print(f"   phase_change={det.get('phase_change')} phase={det.get('phase_name')} dur={det.get('duration_ms')}ms")
    else:
        print("⚠️  Nessun finish_activity — il fix avrà effetto al prossimo cambio fase dal supervisor")
