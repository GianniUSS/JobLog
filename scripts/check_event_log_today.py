#!/usr/bin/env python3
"""Verifica se ci sono eventi in event_log per oggi e cosa restituirebbe build_session_rows."""
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

    # Check event_log
    rows = db.execute(
        "SELECT kind, COUNT(*) as cnt FROM event_log WHERE ts >= %s AND ts < %s GROUP BY kind ORDER BY cnt DESC",
        (start_ms, end_ms)
    ).fetchall()
    print(f"=== Event log per oggi ({today.isoformat()}) ===")
    if rows:
        for r in rows:
            print(f"  {r['kind']}: {r['cnt']}")
    else:
        print("  NESSUN EVENTO!")

    # Check all event_log (any date)
    total = db.execute("SELECT COUNT(*) as cnt FROM event_log").fetchone()
    print(f"\nTotale eventi in event_log: {total['cnt']}")

    # Check warehouse_sessions today
    wh = db.execute(
        "SELECT COUNT(*) as cnt FROM warehouse_sessions WHERE created_ts >= %s AND created_ts < %s",
        (start_ms, end_ms)
    ).fetchall()
    print(f"\nWarehouse sessions oggi: {wh[0]['cnt']}")

    # Try build_session_rows
    print(f"\n=== build_session_rows per oggi ===")
    sessions = build_session_rows(db, start_date=today, end_date=today)
    print(f"Sessioni trovate: {len(sessions)}")
    for s in sessions[:10]:
        print(f"  [{s['status']}] {s['member_name']} - {s['activity_label']} ({s['project_code']}) net={s['net_ms']}ms")
