#!/usr/bin/env python3
"""
Script per simulare timbrature di Gennaio 2026 per test.
Genera timbrature realistiche per 31 giorni (esclusi weekend).
"""

import json
import random
import sys
from datetime import datetime, timedelta

# Carica configurazione database
try:
    with open("config.json", "r") as f:
        config = json.load(f)
    DB_VENDOR = config.get("database", {}).get("vendor", "sqlite")
except:
    DB_VENDOR = "sqlite"

if DB_VENDOR == "mysql":
    import pymysql
    pymysql.install_as_MySQLdb()
    
    with open("config.json", "r") as f:
        config = json.load(f)
    db_config = config.get("database", {})
    
    conn = pymysql.connect(
        host=db_config.get("host", "localhost"),
        port=db_config.get("port", 3306),
        user=db_config.get("user", "root"),
        password=db_config.get("password", ""),
        database=db_config.get("name", "joblog"),
        charset='utf8mb4'
    )
    cursor = conn.cursor()
    placeholder = "%s"
else:
    import sqlite3
    conn = sqlite3.connect("joblog.db")
    cursor = conn.cursor()
    placeholder = "?"

# Username per cui generare le timbrature (da argomento o default)
if len(sys.argv) > 1:
    USERNAME = sys.argv[1]
else:
    USERNAME = "admin"

print(f"\n🗓️ Generazione timbrature per {USERNAME} - Gennaio 2026\n")

# Elimina timbrature esistenti di gennaio 2026 per questo utente
cursor.execute(f"""
    DELETE FROM timbrature 
    WHERE username = {placeholder} 
    AND data >= '2026-01-01' 
    AND data < '2026-02-01'
""", (USERNAME,))
print(f"✓ Eliminate timbrature esistenti di gennaio 2026")

# Genera timbrature per ogni giorno di gennaio
inserted = 0
for day in range(1, 32):
    date_str = f"2026-01-{day:02d}"
    date_obj = datetime(2026, 1, day)
    weekday = date_obj.weekday()  # 0=Lunedì, 6=Domenica
    
    # Salta weekend (Sabato=5, Domenica=6)
    if weekday >= 5:
        print(f"  {date_str} - Weekend, saltato")
        continue
    
    # Orari casuali ma realistici
    # Inizio: tra 7:30 e 9:00
    start_hour = random.choice([7, 8, 8, 8, 9])
    start_min = random.choice([0, 15, 30, 45, 0, 30])
    if start_hour == 7:
        start_min = random.choice([30, 45])
    
    # Fine: tra 17:00 e 19:00
    end_hour = random.choice([17, 17, 18, 18, 18, 19])
    end_min = random.choice([0, 15, 30, 45, 0, 30])
    
    # Pausa pranzo: tra 12:30 e 13:30, durata 30-60 min
    pause_start_hour = random.choice([12, 13])
    pause_start_min = random.choice([0, 15, 30, 45]) if pause_start_hour == 13 else random.choice([30, 45])
    pause_duration = random.choice([30, 45, 60])
    
    pause_end = datetime(2026, 1, day, pause_start_hour, pause_start_min) + timedelta(minutes=pause_duration)
    pause_end_hour = pause_end.hour
    pause_end_min = pause_end.minute
    
    # Metodo casuale
    method = random.choice(["GPS", "GPS", "GPS", "QR", "Mancata Timbratura"])
    location = random.choice(["Ufficio", "Sede Centrale", "Magazzino", None, None])
    
    # Timestamp
    ts = int(datetime(2026, 1, day, start_hour, start_min).timestamp() * 1000)
    
    # Inserisci inizio giornata
    cursor.execute(f"""
        INSERT INTO timbrature (username, tipo, data, ora, created_ts, method, location_name)
        VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})
    """, (USERNAME, "inizio_giornata", date_str, f"{start_hour:02d}:{start_min:02d}:00", ts, method, location))
    
    # Inserisci inizio pausa
    ts_pause = int(datetime(2026, 1, day, pause_start_hour, pause_start_min).timestamp() * 1000)
    cursor.execute(f"""
        INSERT INTO timbrature (username, tipo, data, ora, created_ts, method, location_name)
        VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})
    """, (USERNAME, "inizio_pausa", date_str, f"{pause_start_hour:02d}:{pause_start_min:02d}:00", ts_pause, method, location))
    
    # Inserisci fine pausa
    ts_pause_end = int(pause_end.timestamp() * 1000)
    cursor.execute(f"""
        INSERT INTO timbrature (username, tipo, data, ora, created_ts, method, location_name)
        VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})
    """, (USERNAME, "fine_pausa", date_str, f"{pause_end_hour:02d}:{pause_end_min:02d}:00", ts_pause_end, method, location))
    
    # Inserisci fine giornata
    ts_end = int(datetime(2026, 1, day, end_hour, end_min).timestamp() * 1000)
    cursor.execute(f"""
        INSERT INTO timbrature (username, tipo, data, ora, created_ts, method, location_name)
        VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})
    """, (USERNAME, "fine_giornata", date_str, f"{end_hour:02d}:{end_min:02d}:00", ts_end, method, location))
    
    # Calcola ore
    total_min = (end_hour * 60 + end_min) - (start_hour * 60 + start_min)
    net_min = total_min - pause_duration
    hours = net_min // 60
    mins = net_min % 60
    
    print(f"  {date_str} ({['Lu','Ma','Me','Gi','Ve'][weekday]}) - {start_hour:02d}:{start_min:02d} → {end_hour:02d}:{end_min:02d} | Pausa: {pause_duration}min | Ore: {hours}h{mins}m")
    inserted += 4

conn.commit()
conn.close()

print(f"\n✅ Inserite {inserted} timbrature per {inserted // 4} giorni lavorativi")
print(f"🔗 Vai su http://localhost:5000/user/storico-timbrature per verificare")
