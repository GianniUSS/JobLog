import sys, os
# Ensure project root is in sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import app

with app.app.app_context():
    db = app.get_db()

    # Conta quanti turni hanno gestione_squadra attiva
    try:
        cur = db.execute("SELECT COUNT(*) FROM rentman_plannings WHERE gestione_squadra = 1")
        row = cur.fetchone()
        cnt = row[0] if isinstance(row, tuple) or row is not None else (row[0] if row else 0)
    except Exception:
        print("Tabella rentman_plannings non trovata o errore nel DB")
        cnt = 0

    print(f"Turni con gestione_squadra=1: {cnt}")

    if cnt:
        db.execute("UPDATE rentman_plannings SET gestione_squadra = 0 WHERE gestione_squadra = 1")
        try:
            db.commit()
        except Exception:
            pass
        print(f"Impostato gestione_squadra=0 per {cnt} turni")

    # Azzerare current_phase per tutti i membri (scope: tutti i progetti)
    try:
        cur2 = db.execute("SELECT COUNT(*) FROM member_state WHERE current_phase IS NOT NULL AND current_phase != ''")
        row2 = cur2.fetchone()
        mcnt = row2[0] if isinstance(row2, tuple) or row2 is not None else (row2[0] if row2 else 0)
    except Exception:
        print("Tabella member_state non trovata o errore nel DB")
        mcnt = 0

    print(f"Member_state con current_phase impostato: {mcnt}")
    if mcnt:
        db.execute("UPDATE member_state SET current_phase = NULL")
        try:
            db.commit()
        except Exception:
            pass
        print(f"Azzerato current_phase per {mcnt} righe in member_state")

    print("Operazione completata.")
