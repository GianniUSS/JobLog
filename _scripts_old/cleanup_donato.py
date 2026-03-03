import mysql.connector
from datetime import date

conn = mysql.connector.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
cur = conn.cursor()

print("=== PULIZIA DATI DONATO ===\n")

# 1. Elimina timer attivo
cur.execute("DELETE FROM warehouse_active_timers WHERE username='donato'")
print(f"1. Timer cancellati: {cur.rowcount}")

# 2. Elimina timbrature del 3 febbraio (ieri)
cur.execute("DELETE FROM timbrature WHERE username='donato' AND data='2026-02-03'")
print(f"2. Timbrature 3/2 cancellate: {cur.rowcount}")

# 3. Elimina cedolino_timbrature del 3 febbraio
cur.execute("DELETE FROM cedolino_timbrature WHERE username='donato' AND data_riferimento='2026-02-03'")
print(f"3. Cedolino timbrature cancellate: {cur.rowcount}")

# 4. Elimina richieste user_requests pending per donato
cur.execute("DELETE FROM user_requests WHERE username='donato' AND status='pending'")
print(f"4. User requests pending cancellate: {cur.rowcount}")

# 5. Elimina sessioni warehouse del 3 febbraio
cur.execute("DELETE FROM warehouse_sessions WHERE username='donato' AND DATE(FROM_UNIXTIME(start_ts/1000))='2026-02-03'")
print(f"5. Warehouse sessions cancellate: {cur.rowcount}")

conn.commit()
print("\n=== PULIZIA COMPLETATA ===")
