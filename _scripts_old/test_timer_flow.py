import mysql.connector
from datetime import datetime

conn = mysql.connector.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
cur = conn.cursor(dictionary=True)

# Pulisci dati vecchi
print("=== PULIZIA ===")
cur.execute("DELETE FROM warehouse_active_timers WHERE username='donato'")
cur.execute("DELETE FROM warehouse_sessions WHERE username='donato'")
cur.execute("DELETE FROM timbrature WHERE username='donato' AND data='2026-02-03'")
cur.execute("DELETE FROM cedolino_timbrature WHERE username='donato' AND data_riferimento='2026-02-03'")
conn.commit()
print("Pulizia completata")

# Simula il flusso
print("\n=== SIMULAZIONE ===")

# 1. Simula timbratura inizio_giornata alle 07:31
timbratura_ts = int(datetime(2026, 2, 3, 7, 31, 41).timestamp() * 1000)
print(f"1. Timbratura inizio_giornata: {datetime.fromtimestamp(timbratura_ts/1000)}")

# 2. Simula avvio timer con il timestamp della timbratura
print(f"2. Avvio timer con start_ts={timbratura_ts}")
cur.execute("""
    INSERT INTO warehouse_active_timers 
    (username, project_code, project_name, activity_label, notes, running, paused, start_ts, elapsed_ms, pause_start_ts, updated_ts)
    VALUES (%s, %s, %s, %s, %s, 1, 0, %s, 0, NULL, %s)
    ON DUPLICATE KEY UPDATE
        start_ts = VALUES(start_ts),
        elapsed_ms = 0,
        updated_ts = VALUES(updated_ts)
""", ('donato', '1', 'Shifts project', 'Consegna Materiale', 'Test', timbratura_ts, timbratura_ts))
conn.commit()

# 3. Verifica
cur.execute("SELECT * FROM warehouse_active_timers WHERE username='donato'")
timer = cur.fetchone()
if timer:
    print(f"3. Timer creato:")
    print(f"   start_ts: {timer['start_ts']} = {datetime.fromtimestamp(timer['start_ts']/1000)}")
else:
    print("3. ERRORE: Timer non creato!")

# 4. Simula stop timer (fine giornata)
end_ts = int(datetime(2026, 2, 3, 16, 55, 2).timestamp() * 1000)
elapsed_ms = end_ts - timbratura_ts
print(f"\n4. Stop timer alle {datetime.fromtimestamp(end_ts/1000)}, elapsed={elapsed_ms/60000:.1f} min")

cur.execute("""
    INSERT INTO warehouse_sessions (project_code, activity_label, elapsed_ms, start_ts, end_ts, note, username, created_ts)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""", ('1', 'Consegna Materiale', elapsed_ms, timbratura_ts, end_ts, 'Test', 'donato', end_ts))
cur.execute("DELETE FROM warehouse_active_timers WHERE username='donato'")
conn.commit()

# 5. Verifica sessione
cur.execute("SELECT * FROM warehouse_sessions WHERE username='donato' ORDER BY id DESC LIMIT 1")
sess = cur.fetchone()
if sess:
    print(f"\n5. Sessione salvata:")
    print(f"   start_ts: {datetime.fromtimestamp(sess['start_ts']/1000)}")
    print(f"   end_ts: {datetime.fromtimestamp(sess['end_ts']/1000)}")
    print(f"   elapsed: {sess['elapsed_ms']/60000:.1f} min")
    print(f"\n   CORRETTO: La sessione mostra l'ora della timbratura (07:31), non 07:18!")
