import sys; sys.path.insert(0,'.')
from app import app, get_db
with app.app_context():
    db = get_db()
    rows = db.execute('SELECT member_key, activity_id, running FROM member_state WHERE activity_id IS NOT NULL').fetchall()
    print(f'Righe con activity_id NOT NULL: {len(rows)}')
    for r in rows:
        print(f'  {r["member_key"]} -> activity={r["activity_id"]} running={r["running"]}')
