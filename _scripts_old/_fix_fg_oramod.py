"""
Fix una-tantum: ricalcola fine_giornata.ora_mod per le deroga pausa ridotta già approvate
usando pausa arrotondata (rounded_break_minutes) invece di effective_break_minutes.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import json, pymysql
from datetime import datetime

config = json.load(open('config.json'))['database']
db = pymysql.connect(
    host=config['host'], port=config['port'],
    user=config['user'], password=config['password'],
    db=config['name'], cursorclass=pymysql.cursors.DictCursor
)
cur = db.cursor()

# Trova tutte le deroga approvate nel mese corrente
cur.execute("""
    SELECT ur.id, ur.username, ur.date_from, ur.extra_data
    FROM user_requests ur
    JOIN request_types rt ON ur.request_type_id = rt.id
    WHERE rt.name = 'Deroga Pausa Ridotta'
      AND ur.status = 'approved'
    ORDER BY ur.date_from DESC
""")
rows = cur.fetchall()
print(f"Trovate {len(rows)} deroge approvate")

for r in rows:
    username = r['username']
    date_str = str(r['date_from'])[:10]
    ed = {}
    if r['extra_data']:
        try: ed = json.loads(r['extra_data'])
        except: pass

    effective = int(ed.get('effective_break_minutes', 0) or 0)
    rounded   = int(ed.get('rounded_break_minutes', 0) or 0)
    # Se rounded_break non salvato (record vecchi), calcolalo arrotondando al blocco
    if not rounded and effective:
        # Blocco dal gruppo (default 15 se None)
        cur2 = db.cursor()
        cur2.execute("""SELECT gtr.arrotondamento_giornaliero_minuti
            FROM app_users u
            LEFT JOIN user_groups ug ON u.group_id=ug.id
            LEFT JOIN group_timbratura_rules gtr ON gtr.group_id=ug.id
            WHERE u.username=%s""", (r['username'],))
        g2 = cur2.fetchone() or {}
        blk2 = int(g2.get('arrotondamento_giornaliero_minuti') or 15)
        import math
        rounded = math.ceil(effective / blk2) * blk2
    forced = rounded or effective  # usa arrotondato se disponibile

    if not forced:
        print(f"  SKIP {username} {date_str}: nessuna pausa in extra_data")
        continue

    # Leggi inizio e fine giornata
    cur.execute("""
        SELECT tipo, ora, ora_mod FROM timbrature
        WHERE username=%s AND data=%s AND tipo IN ('inizio_giornata','fine_giornata')
        ORDER BY tipo ASC
    """, (username, date_str))
    trs = {t['tipo']: t for t in cur.fetchall()}

    fg = trs.get('fine_giornata')
    ig = trs.get('inizio_giornata')
    if not fg or not ig:
        print(f"  SKIP {username} {date_str}: mancano timbrature ig/fg")
        continue

    def to_min(v):
        if v is None: return None
        if hasattr(v, 'total_seconds'):
            s = int(v.total_seconds())
            return s // 60
        s = str(v)[:5]
        h, m = map(int, s.split(':'))
        return h * 60 + m

    inizio_min = to_min(ig['ora_mod'] or ig['ora'])
    fine_min   = to_min(fg['ora'])

    if inizio_min is None or fine_min is None:
        print(f"  SKIP {username} {date_str}: ora non leggibile")
        continue

    ore_lorde = fine_min - inizio_min
    ore_nette = ore_lorde - forced

    # Leggi regole dal gruppo (blocco e tipo arrotondamento)
    cur.execute("""
        SELECT gtr.arrotondamento_giornaliero_minuti, gtr.arrotondamento_giornaliero_tipo
        FROM app_users u
        LEFT JOIN user_groups ug ON u.group_id = ug.id
        LEFT JOIN group_timbratura_rules gtr ON gtr.group_id = ug.id
        WHERE u.username = %s
    """, (username,))
    grp = cur.fetchone() or {}
    blocco      = int(grp.get('arrotondamento_giornaliero_minuti') or 30)
    tipo_arrot  = grp.get('arrotondamento_giornaliero_tipo') or 'floor'

    # Calcola turno_base dai turni del giorno
    day_of_week = datetime.strptime(date_str, '%Y-%m-%d').weekday()
    cur.execute("""
        SELECT start_time, end_time, break_start, break_end
        FROM employee_shifts WHERE username=%s AND is_active=1 AND day_of_week=%s
        LIMIT 1
    """, (username, day_of_week))
    sh = cur.fetchone()
    if sh:
        def t2m(v):
            if v is None: return 0
            if hasattr(v, 'total_seconds'): return int(v.total_seconds()) // 60
            h2, m2 = map(int, str(v)[:5].split(':'))
            return h2*60 + m2
        ts = t2m(sh['start_time']); te = t2m(sh['end_time'])
        bs = t2m(sh['break_start']); be = t2m(sh['break_end'])
        turno_base = (te - ts) - (be - bs) if bs and be else (te - ts)
        turno_base = max(turno_base, 1)
    else:
        turno_base = 480  # default 8h

    straordinario = max(0, ore_nette - turno_base)
    if tipo_arrot == 'floor':
        str_arr = (straordinario // blocco) * blocco
    elif tipo_arrot == 'ceil':
        str_arr = ((straordinario + blocco - 1) // blocco) * blocco
    else:
        str_arr = round(straordinario / blocco) * blocco

    ore_arr = turno_base + str_arr if ore_nette >= turno_base else (ore_nette // blocco) * blocco
    differenza = ore_nette - ore_arr
    ora_mod_min = fine_min - differenza

    h = ora_mod_min // 60
    m = ora_mod_min % 60
    new_ora_mod = f"{h:02d}:{m:02d}:00"

    old_ora_mod = str(fg['ora_mod'])[:5] if fg['ora_mod'] else 'NULL'
    print(f"  {username} {date_str}: pausa_forced={forced}min, ore_nette={ore_nette}min, "
          f"str_arr={str_arr}min, ora_mod {old_ora_mod} → {new_ora_mod}")

    cur.execute("""
        UPDATE timbrature SET ora_mod=%s
        WHERE username=%s AND data=%s AND tipo='fine_giornata'
    """, (new_ora_mod, username, date_str))
    
    try:
        cur.execute("""
            UPDATE cedolino_timbrature SET ora_modificata=%s, synced_ts=NULL
            WHERE username=%s AND data_riferimento=%s AND timeframe_id=8
        """, (new_ora_mod, username, date_str))
    except Exception as ce:
        print(f"    cedolino skip: {ce}")

db.commit()
db.close()
print("Done.")
