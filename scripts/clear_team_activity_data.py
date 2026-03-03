import sys, os

# Assicura il project root nel path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import app

with app.app.app_context():
    db = app.get_db()

    # 1) Imposta gestione_squadra = 0 su tutti i turni
    try:
        cur = db.execute("SELECT COUNT(*) FROM rentman_plannings WHERE gestione_squadra = 1")
        row = cur.fetchone()
        cnt = int(row[0]) if row else 0
    except Exception:
        print("Tabella rentman_plannings non trovata o errore DB")
        cnt = 0

    if cnt:
        db.execute("UPDATE rentman_plannings SET gestione_squadra = 0 WHERE gestione_squadra = 1")
        try:
            db.commit()
        except Exception:
            pass
        print(f"Impostato gestione_squadra=0 per {cnt} turni")
    else:
        print("Nessun turno con gestione_squadra=1 trovato")

    # 2) Azzerare current_phase in member_state
    try:
        cur = db.execute("SELECT COUNT(*) FROM member_state WHERE current_phase IS NOT NULL AND current_phase != ''")
        row = cur.fetchone()
        mcnt = int(row[0]) if row else 0
    except Exception:
        print("Tabella member_state non trovata o errore DB")
        mcnt = 0

    if mcnt:
        db.execute("UPDATE member_state SET current_phase = NULL WHERE current_phase IS NOT NULL AND current_phase != ''")
        try:
            db.commit()
        except Exception:
            pass
        print(f"Azzerato current_phase per {mcnt} righe in member_state")
    else:
        print("Nessun current_phase da azzerare in member_state")

    # 3) Resetta stato fasi progetto: set completed = 0, completed_by = NULL, date = NULL
    try:
        cur = db.execute("SELECT COUNT(*) FROM project_phase_progress")
        row = cur.fetchone()
        pcount = int(row[0]) if row else 0
    except Exception:
        print("Tabella project_phase_progress non trovata o errore DB")
        pcount = 0

    if pcount:
        db.execute("UPDATE project_phase_progress SET completed = 0, completed_by = NULL, date = NULL")
        try:
            db.commit()
        except Exception:
            pass
        print(f"Reset completamento per {pcount} righe in project_phase_progress")
    else:
        print("Nessuna riga in project_phase_progress da resettare")

    print("Pulizia dati gestione squadra completata.")
