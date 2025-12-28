from __future__ import annotations

import os
import sys
from datetime import date

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app import MySQLConnection, DATABASE_SETTINGS, build_session_rows


def main() -> None:
    db = MySQLConnection(DATABASE_SETTINGS)
    try:
        today = date.today()
        rows = build_session_rows(db, start_date=today, end_date=today)
        missing = [r for r in rows if not r.get('project_code')]
        present = sorted({str(r.get('project_code')) for r in rows if r.get('project_code')})
        print('rows=', len(rows))
        print('project_codes=', present)
        print('missing_project_code_rows=', len(missing))
        if missing:
            sample = missing[0]
            print('sample_missing=', {k: sample.get(k) for k in ('member_name','activity_label','net_ms','start_ts','end_ts')})
    finally:
        try:
            db.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()
