import sys, json; sys.path.insert(0,'.')
from app import app, get_db, build_session_rows
from datetime import datetime, date, timezone

with app.app_context():
    db = get_db()
    today = date.today()
    now = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    
    # Simula /api/admin/open-sessions
    rows = db.execute("""
        SELECT ms.member_key, ms.member_name, ms.activity_id, ms.running, 
               ms.start_ts, ms.elapsed_cached, ms.pause_start, ms.entered_ts,
               ms.project_code, ms.current_phase,
               a.label AS activity_label
        FROM member_state ms
        LEFT JOIN activities a ON ms.activity_id = a.activity_id AND ms.project_code = a.project_code
        WHERE ms.activity_id IS NOT NULL
    """).fetchall()
    
    print(f"=== Simulazione /api/admin/open-sessions ===")
    print(f"Sessioni totali: {len(rows)}")
    for r in rows:
        start_ts = r["entered_ts"] or r["start_ts"] or now  # STESSA logica del fix
        status = "▶️ In corso" if r["running"] == 1 else ("📋 Assegnata" if r["activity_id"] else "—")
        ts_str = datetime.fromtimestamp(start_ts/1000).strftime("%H:%M:%S")
        print(f"  {r['member_name']}: {r['activity_label'] or r['activity_id']} [{status}] start={ts_str} project={r['project_code']}")
    
    # Simula /api/admin/day-sessions - team_sessions
    sessions = build_session_rows(db, start_date=today, end_date=today)
    completed = [s for s in sessions if s["status"] == "completed"]
    running = [s for s in sessions if s["status"] == "running"]
    print(f"\n=== Simulazione /api/admin/day-sessions ===")
    print(f"  team_sessions: {len(sessions)} ({len(completed)} completed, {len(running)} running)")
    for s in sessions:
        print(f"  [{s['status']}] {s['member_name']} - {s['activity_label']}")
    
    print(f"\n=== RIEPILOGO ===")
    print(f"Sessioni Aperte attese in dashboard: {len(rows)} (tutte, incluse quelle senza start_ts)")
    print(f"Sessioni Registrate attese: {len(completed)} completed + 0 warehouse = {len(completed)}")
    if len(completed) == 0:
        print("  ⚠️  Nessuna sessione completata: le sessioni running appariranno solo in 'Sessioni Aperte'")
        print("  ℹ️  Per avere sessioni registrate, un'attività deve essere conclusa (finish_activity)")
