import app
import json

with app.app.app_context():
    db = app.get_db()

    # Check permission for donato
    result = db.execute("""
        SELECT id, username, request_type_id, date_from, date_to, extra_data
        FROM user_requests
        WHERE username = 'donato' AND request_type_id = 3
        ORDER BY created_ts DESC
        LIMIT 1
    """).fetchone()

    if result:
        print("Permission found:")
        if isinstance(result, tuple):
            print(f"  ID: {result[0]}")
            print(f"  Extra Data (raw): '{result[5]}'")
        else:
            print(f"  ID: {result['id']}")
            print(f"  Extra Data (raw): '{result['extra_data']}'")
            if result['extra_data']:
                try:
                    ed = json.loads(result['extra_data'])
                    print(f"  Extra Data (parsed): {ed}")
                except Exception as e:
                    print(f"  Error parsing: {e}")
    else:
        print("No permission found for donato")
