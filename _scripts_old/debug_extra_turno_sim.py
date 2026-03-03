"""Simula il calcolo Extra Turno per donato per diagnosticare perché non scatta."""
import json, mysql.connector

with open('config.json') as f:
    cfg = json.load(f)
db = cfg['database']
conn = mysql.connector.connect(
    host=db['host'], user=db['user'], password=db['password'],
    database=db['name'], port=db.get('port', 3306)
)
cur = conn.cursor(dictionary=True)

username = 'donato'
today = '2026-02-13'

# 1. Planned minutes from Rentman
cur.execute('SELECT rentman_crew_id FROM app_users WHERE username=%s', (username,))
cr = cur.fetchone()
crew_id = cr['rentman_crew_id']
print(f"crew_id = {crew_id}")

cur.execute(
    'SELECT plan_start, plan_end FROM rentman_plannings WHERE crew_id=%s AND planning_date=%s AND (is_obsolete IS NULL OR is_obsolete=0)',
    (crew_id, today)
)
planned_minutes = 0
for tr in cur.fetchall():
    ps = tr['plan_start']
    pe = tr['plan_end']
    ps_min = ps.hour * 60 + ps.minute
    pe_min = pe.hour * 60 + pe.minute
    dur = max(0, pe_min - ps_min)
    planned_minutes += dur
    print(f"  turno {ps.hour:02d}:{ps.minute:02d}-{pe.hour:02d}:{pe.minute:02d} = {dur} min")
print(f"planned_minutes (lordo) = {planned_minutes}")

# 2. Planned break from employee_shifts
cur.execute('SELECT break_start, break_end FROM employee_shifts WHERE username=%s AND is_active=1', (username,))
br = cur.fetchone()
planned_break = 0
print(f"employee_shifts break row = {br}")
if br:
    bs = br.get('break_start')
    be = br.get('break_end')
    if bs and be:
        if hasattr(bs, 'total_seconds'):
            bs_min = int(bs.total_seconds()) // 60
            be_min = int(be.total_seconds()) // 60
        else:
            bs_min = int(str(bs)[:2]) * 60 + int(str(bs)[3:5])
            be_min = int(str(be)[:2]) * 60 + int(str(be)[3:5])
        planned_break = be_min - bs_min
        print(f"  break: {bs_min} -> {be_min} = {planned_break} min")

print(f"planned_break = {planned_break}")
if planned_break > 0:
    planned_minutes -= planned_break
print(f"planned_minutes (netto) = {planned_minutes}")

# 3. Timbrature in DB
cur.execute(
    'SELECT tipo, ora_mod FROM timbrature WHERE username=%s AND data=%s ORDER BY ora_mod ASC',
    (username, today)
)
all_t = []
print("\nTimbrature nel DB:")
for t in cur.fetchall():
    tipo = t['tipo']
    ora = t['ora_mod']
    if hasattr(ora, 'total_seconds'):
        total_sec = int(ora.total_seconds())
        ora_str = f"{total_sec // 3600:02d}:{(total_sec % 3600) // 60:02d}"
    elif hasattr(ora, 'strftime'):
        ora_str = ora.strftime("%H:%M")
    else:
        ora_str = str(ora)[:5]
    all_t.append((tipo, ora_str))
    print(f"  {tipo}: {ora_str}")

# NOTE: nel codice reale, la fine_giornata corrente viene aggiunta PRIMA dell'INSERT,
# quindi NON è ancora nel DB. MA qui ce l'abbiamo già nel DB.
# Simuliamo come se NON fosse nel DB (rimuoviamo la fine_giornata e la riaggiungiamo)
print("\n--- SIMULAZIONE come se fine_giornata non fosse ancora nel DB ---")
all_t_sim = [(tipo, ora) for (tipo, ora) in all_t if tipo != 'fine_giornata']
ora_mod_current = '17:15'
all_t_sim.append(('fine_giornata', ora_mod_current))
print(f"Timbrature per calcolo (sim):")
for tipo, ora in all_t_sim:
    print(f"  {tipo}: {ora}")

# 4. Calculate worked_minutes
worked_minutes = 0
inizio = None
first_inizio_min = None

for t_tipo, t_ora in all_t_sim:
    parts = t_ora.split(':')
    t_min = int(parts[0]) * 60 + int(parts[1])
    
    if t_tipo == 'inizio_giornata':
        if first_inizio_min is None:
            first_inizio_min = t_min
        inizio = t_min
    elif t_tipo == 'fine_giornata' and inizio is not None:
        worked_minutes += (t_min - inizio)
        inizio = None

print(f"\nworked_minutes (lordo, senza pausa) = {worked_minutes}")

# Break info: nel test, supponiamo nessun break_info (l'utente ha detto "senza pausa")
break_info = None
total_pausa_effettiva = planned_break if planned_break else 0

if break_info and break_info.get('break_skipped'):
    total_pausa_effettiva = 0
    print("Pausa saltata dall'utente")
else:
    worked_minutes -= total_pausa_effettiva
    print(f"Sottratta pausa pianificata: {total_pausa_effettiva} min")

print(f"worked_minutes (netto) = {worked_minutes}")
extra = worked_minutes - planned_minutes
print(f"extra_minutes = {extra}")
print(f"Should trigger extra turno: {extra > 0}")

# 5. Verifica anti-duplicato
cur.execute(
    'SELECT id, name FROM request_types WHERE name LIKE %s', ('%Extra%',)
)
for r in cur.fetchall():
    print(f"\nrequest_type Extra Turno: id={r['id']}, name={r['name']}")

cur.execute(
    'SELECT id FROM user_requests WHERE username=%s AND request_type_id=9 AND date_from=%s AND status=%s',
    (username, today, 'pending')
)
existing = cur.fetchone()
print(f"Existing pending Extra Turno request: {existing}")

conn.close()
