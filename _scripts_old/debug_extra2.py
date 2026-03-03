"""Debug Extra Turno - verifica calcolo."""
import json
import mysql.connector

with open('config.json') as f:
    cfg = json.load(f)

db = cfg['database']
conn = mysql.connector.connect(
    host=db['host'], user=db['user'], password=db['password'],
    database=db['name'], port=db.get('port', 3306)
)
cur = conn.cursor(dictionary=True)

print("=== TIMBRATURE DONATO OGGI ===")
cur.execute("""
    SELECT id, tipo, ora, ora_mod 
    FROM timbrature WHERE username = 'donato' AND data = '2026-02-13'
    ORDER BY ora_mod ASC
""")
timbrature = cur.fetchall()
for t in timbrature:
    print(t)

print("\n=== RICHIESTE DONATO OGGI ===")
cur.execute("""
    SELECT id, request_type_id, date_from, notes, status, created_ts 
    FROM user_requests WHERE username = 'donato' AND date_from = '2026-02-13'
""")
for r in cur.fetchall():
    print(r)

print("\n=== CALCOLO MANUALE ===")
# Turni pianificati
cur.execute("SELECT rentman_crew_id FROM app_users WHERE username = 'donato'")
user = cur.fetchone()
crew_id = user['rentman_crew_id']

cur.execute("""
    SELECT plan_start, plan_end FROM rentman_plannings 
    WHERE crew_id = %s AND planning_date = '2026-02-13'
    AND (is_obsolete IS NULL OR is_obsolete = 0)
""", (crew_id,))
planned_minutes = 0
for r in cur.fetchall():
    ps = r['plan_start']
    pe = r['plan_end']
    if ps and pe:
        ps_min = ps.hour * 60 + ps.minute
        pe_min = pe.hour * 60 + pe.minute
        planned_minutes += (pe_min - ps_min)

print(f"Minuti pianificati: {planned_minutes} ({planned_minutes//60}h{planned_minutes%60:02d}m)")

# Ore lavorate
worked_minutes = 0
inizio_min = None
for t in timbrature:
    tipo = t['tipo']
    ora_mod = t['ora_mod']
    if isinstance(ora_mod, str):
        parts = ora_mod[:5].split(':')
        t_min = int(parts[0]) * 60 + int(parts[1])
    else:
        # timedelta
        total_sec = int(ora_mod.total_seconds())
        t_min = total_sec // 60
    
    if tipo == 'inizio_giornata':
        inizio_min = t_min
        print(f"  inizio_giornata: {t_min//60:02d}:{t_min%60:02d}")
    elif tipo == 'fine_giornata' and inizio_min is not None:
        worked_minutes += (t_min - inizio_min)
        print(f"  fine_giornata: {t_min//60:02d}:{t_min%60:02d} (+{t_min - inizio_min} min)")
        inizio_min = None

print(f"Minuti lavorati LORDI: {worked_minutes} ({worked_minutes//60}h{worked_minutes%60:02d}m)")

# Pausa timbrata
pausa_timbrata = 0
inizio_pausa_min = None
for t in timbrature:
    tipo = t['tipo']
    ora_mod = t['ora_mod']
    if isinstance(ora_mod, str):
        parts = ora_mod[:5].split(':')
        t_min = int(parts[0]) * 60 + int(parts[1])
    else:
        total_sec = int(ora_mod.total_seconds())
        t_min = total_sec // 60
    
    if tipo == 'inizio_pausa':
        inizio_pausa_min = t_min
    elif tipo == 'fine_pausa' and inizio_pausa_min is not None:
        pausa_timbrata = t_min - inizio_pausa_min
        print(f"Pausa timbrata: {pausa_timbrata} min")

worked_netti = worked_minutes - pausa_timbrata
print(f"Minuti lavorati NETTI: {worked_netti} ({worked_netti//60}h{worked_netti%60:02d}m)")

extra = worked_netti - planned_minutes
print(f"\n>>> EXTRA TURNO: {extra} minuti <<<")

if extra > 0:
    print("DOVREBBE scattare il popup Extra Turno!")
else:
    print("NON dovrebbe scattare (extra <= 0)")

conn.close()
