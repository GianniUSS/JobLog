import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from app import app
import json

with app.test_client() as c:
    with c.session_transaction() as sess:
        sess['is_admin'] = True
        sess['username'] = 'admin'
        sess['user'] = 'admin'
    rv = c.get('/api/admin/user-requests')
    print('STATUS:', rv.status_code)
    try:
        print(json.dumps(rv.get_json(), indent=2, ensure_ascii=False))
    except Exception as e:
        print('ERROR PARSING JSON:', e)
        print(rv.get_data(as_text=True))
