#!/usr/bin/env python3
"""Diagnostica completa: stato member_state, warehouse_active_timers, event_log, build_session_rows."""
import sys, os, json
from datetime import datetime, date, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app import app, get_db, build_session_rows

with app.app_context():
    db = get_db()
    today = date.today()
    start_dt = datetime.combine(today, datetime.min.time())
    end_dt = start_dt + timedelta(days=1)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    print(f"=== DIAGNOSTICA DASHBOARD — {today.isoformat()} ===\n")

    # 1. member_state
    ms_rows = db.execute("SELECT member_key, member_name, activity_id, running, start_ts, elapsed_cached, pause_start, current_phase FROM member_state").fetchall()
    ms_active = [r for r in ms_rows if r["activity_id"] is not None]
    print(f"1) member_state: {len(ms_rows)} righe totali, {len(ms_active)} con activity_id NOT NULL")
    for r in ms_active:
        print(f"   {r['member_name']} ({r['member_key']}): activity={r['activity_id']} running={r['running']} phase={r['current_phase']}")
    if not ms_active:
        print("   => NESSUNA sessione aperta (member_state vuoto)")

    # 2. warehouse_active_timers
    wat_rows = db.execute("SELECT * FROM warehouse_active_timers WHERE running=1").fetchall()
    print(f"\n2) warehouse_active_timers running=1: {len(wat_rows)} righe")
    for r in wat_rows:
        print(f"   {r['username']}: {r['activity_label']} ({r['project_code']}) elapsed={r['elapsed_ms']}ms")

    # 3. event_log per oggi
    el_rows = db.execute(
        "SELECT kind, COUNT(*) as cnt FROM event_log WHERE ts >= %s AND ts < %s GROUP BY kind ORDER BY cnt DESC",
        (start_ms, end_ms)
    ).fetchall()
    el_total = db.execute("SELECT COUNT(*) as cnt FROM event_log").fetchone()["cnt"]
    print(f"\n3) event_log: {el_total} totale, per oggi:")
    for r in el_rows:
        print(f"   {r['kind']}: {r['cnt']}")
    if not el_rows:
        print("   => NESSUN EVENTO OGGI")

    # 4. Tutti gli eventi (qualsiasi data) per capire il range
    range_rows = db.execute("SELECT MIN(ts) as min_ts, MAX(ts) as max_ts FROM event_log").fetchone()
    if range_rows and range_rows["min_ts"]:
        min_d = datetime.fromtimestamp(range_rows["min_ts"]/1000).strftime("%Y-%m-%d %H:%M")
        max_d = datetime.fromtimestamp(range_rows["max_ts"]/1000).strftime("%Y-%m-%d %H:%M")
        print(f"   Range eventi: {min_d} → {max_d}")

    # 5. build_session_rows per oggi
    sessions = build_session_rows(db, start_date=today, end_date=today)
    completed = [s for s in sessions if s["status"] == "completed"]
    running = [s for s in sessions if s["status"] == "running"]
    print(f"\n4) build_session_rows oggi: {len(sessions)} totali ({len(completed)} completed, {len(running)} running)")
    for s in sessions:
        print(f"   [{s['status']}] {s['member_name']} - {s['activity_label']} (prj {s['project_code']}) net={s['net_ms']}ms ({round(s['net_ms']/60000,1)} min)")

    # 6. warehouse_sessions oggi
    wh_rows = db.execute(
        "SELECT project_code, activity_label, elapsed_ms, username FROM warehouse_sessions WHERE created_ts >= %s AND created_ts < %s ORDER BY created_ts DESC",
        (start_ms, end_ms)
    ).fetchall()
    print(f"\n5) warehouse_sessions oggi: {len(wh_rows)} righe")
    for r in wh_rows:
        print(f"   {r['username']}: {r['activity_label']} ({r['project_code']}) elapsed={r['elapsed_ms']}ms")

    # 7. Verifica API open-sessions (simula)
    print(f"\n6) Simulazione /api/admin/open-sessions:")
    open_rows = db.execute("""
        SELECT ms.member_key, ms.member_name, ms.activity_id, ms.running,
               ms.start_ts, ms.elapsed_cached, ms.pause_start, ms.current_phase,
               ms.project_code
        FROM member_state ms
        WHERE ms.activity_id IS NOT NULL
    """).fetchall()
    print(f"   Righe: {len(open_rows)}")
    for r in open_rows:
        print(f"   {r['member_name']}: activity={r['activity_id']} project={r['project_code']}")

    print("\n=== FINE DIAGNOSTICA ===")
