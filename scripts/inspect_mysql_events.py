import json
import datetime
import pymysql


def main() -> None:
    cfg = json.load(open('config.json', 'r', encoding='utf-8'))
    db = cfg['database']

    conn = pymysql.connect(
        host=db['host'],
        port=int(db['port']),
        user=db['user'],
        password=db['password'],
        database=db['name'],
        cursorclass=pymysql.cursors.DictCursor,
    )

    with conn.cursor() as cur:
        cur.execute("SELECT `key`, value FROM app_state WHERE `key` IN ('project_code','project_name')")
        state_rows = cur.fetchall()
        if state_rows:
            print('--- app_state ---')
            for r in state_rows:
                print(r.get('key'), '=', r.get('value'))
            print('---------------')

        cur.execute(
            """
            SELECT ts, kind, member_key, details
            FROM event_log
            WHERE kind IN ('move', 'finish_activity')
            ORDER BY ts DESC
            LIMIT 30
            """
        )
        rows = cur.fetchall()

        cur.execute(
            """
            SELECT ts, kind, details
            FROM event_log
            WHERE kind = 'project_load'
            ORDER BY ts DESC
            LIMIT 10
            """
        )
        load_rows = cur.fetchall()

    for r in rows:
        details_raw = r.get('details')
        try:
            details = json.loads(details_raw) if details_raw else {}
        except Exception:
            details = {}

        ts = int(r.get('ts') or 0)
        iso = datetime.datetime.utcfromtimestamp(ts / 1000).isoformat() + 'Z'
        print(
            iso,
            r.get('kind'),
            r.get('member_key'),
            'proj=', details.get('project_code'),
            'to=', details.get('to'),
            'act=', details.get('activity_id'),
        )

    if load_rows:
        print('--- recent project_load ---')
        for r in load_rows:
            details_raw = r.get('details')
            try:
                details = json.loads(details_raw) if details_raw else {}
            except Exception:
                details = {}
            ts = int(r.get('ts') or 0)
            iso = datetime.datetime.utcfromtimestamp(ts / 1000).isoformat() + 'Z'
            print(iso, 'project_load', 'proj=', details.get('project_code'))
        print('---------------------------')

    conn.close()


if __name__ == '__main__':
    main()
