import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from app import app
import json

def to_iso_date(val):
    if val is None:
        return None
    if isinstance(val, str):
        s = val[:10]
        # try parse common formats
        try:
            # handle RFC formats
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(val)
            return dt.date().isoformat()
        except Exception:
            # fallback to slicing
            return val[:10]
    return None

with app.test_client() as c:
    with c.session_transaction() as sess:
        sess['is_admin'] = True
        sess['username'] = 'admin'
        sess['user'] = 'admin'
    rv = c.get('/api/admin/user-requests')
    data = rv.get_json()
    reqs = data.get('requests', [])

    # Test filters
    filterGroupVal = '7'
    filterDateFrom = '2026-02-23'
    filterDateTo = ''
    filterType = ''
    searchUser = ''
    currentFilter = 'all'

    def matches(r):
        if currentFilter != 'all' and r.get('status') != currentFilter:
            return False
        if filterType and r.get('request_type_id') != int(filterType):
            return False
        reqGroupId = None if r.get('group_id') is None else str(r.get('group_id'))
        if filterGroupVal and (reqGroupId is None or reqGroupId != filterGroupVal):
            return False
        if searchUser and searchUser.lower() not in r.get('username','').lower():
            return False
        if filterDateFrom:
            reqDateFrom = to_iso_date(r.get('date_from'))
            reqDateTo = to_iso_date(r.get('date_to')) or reqDateFrom
            if reqDateTo and reqDateTo < filterDateFrom:
                return False
        if filterDateTo:
            reqDateFrom = to_iso_date(r.get('date_from'))
            if reqDateFrom and reqDateFrom > filterDateTo:
                return False
        return True

    filtered = [r for r in reqs if matches(r)]
    print('Total returned:', len(reqs))
    print('Filtered count:', len(filtered))
    print('Filtered ids:', [r['id'] for r in filtered])
