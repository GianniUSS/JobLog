"""Debug script per verificare perché Extra Turno non scatta."""
import json
import mysql.connector

with open('config.json') as f:
    cfg = json.load(f)

db = cfg['database']
conn = mysql.connector.connect(
    host=db['host'],
    user=db['user'],
    password=db['password'],
    database=db['name'],
    port=db.get('port', 3306)
)
cur = conn.cursor(dictionary=True)

print("=== TIPI RICHIESTA EXTRA TURNO ===")
cur.execute("SELECT id, name FROM request_types WHERE name LIKE '%Extra%' OR name LIKE '%traordinari%'")
for r in cur.fetchall():
    print(r)

print("\n=== RICHIESTE DONATO OGGI ===")
cur.execute("""
    SELECT id, username, request_type_id, date_from, notes, status, created_ts 
    FROM user_requests WHERE username = 'donato' AND date_from = '2026-02-13'
    ORDER BY created_ts DESC
""")
for r in cur.fetchall():
    print(r)

print("\n=== TIMBRATURE DONATO OGGI ===")
cur.execute("""
    SELECT id, tipo, ora, ora_mod, data 
    FROM timbrature WHERE username = 'donato' AND data = '2026-02-13'
    ORDER BY ora ASC
""")
for r in cur.fetchall():
    print(r)

print("\n=== TURNI RENTMAN DONATO OGGI ===")
cur.execute("SELECT rentman_crew_id FROM app_users WHERE username = 'donato'")
user = cur.fetchone()
if user and user['rentman_crew_id']:
    crew_id = user['rentman_crew_id']
    print(f"crew_id = {crew_id}")
    cur.execute("""
        SELECT id, plan_start, plan_end, planning_date 
        FROM rentman_plannings 
        WHERE crew_id = %s AND planning_date = '2026-02-13'
        AND (is_obsolete IS NULL OR is_obsolete = 0)
    """, (crew_id,))
    total_planned = 0
    for r in cur.fetchall():
        print(r)
        ps = r['plan_start']
        pe = r['plan_end']
        if ps and pe:
            if hasattr(ps, 'hour'):
                ps_min = ps.hour * 60 + ps.minute
            else:
                ps_str = str(ps)[:5]
                ps_min = int(ps_str.split(':')[0]) * 60 + int(ps_str.split(':')[1])
            if hasattr(pe, 'hour'):
                pe_min = pe.hour * 60 + pe.minute
            else:
                pe_str = str(pe)[:5]
                pe_min = int(pe_str.split(':')[0]) * 60 + int(pe_str.split(':')[1])
            total_planned += (pe_min - ps_min)
    print(f"Totale minuti pianificati: {total_planned} ({total_planned // 60}h{total_planned % 60:02d}m)")
else:
    print("Nessun crew_id per donato")

print("\n=== MODULO STRAORDINARI ===")
cur.execute("SELECT modules_enabled FROM company_settings WHERE id = 1")
row = cur.fetchone()
if row:
    import json as j
    modules = j.loads(row['modules_enabled'] or '{}')
    print(f"straordinari = {modules.get('straordinari', 'NON DEFINITO (default True)')}")
else:
    print("Nessuna company_settings")

conn.close()
print("\n=== DONE ===")
