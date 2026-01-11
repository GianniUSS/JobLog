#!/usr/bin/env python3
"""
Script di debug per verificare i turni di giovedì 8 gennaio nel database.
Eseguire sul server: python debug_thursday_shifts.py
"""

import json
from pathlib import Path

# Carica config
CONFIG_FILE = Path(__file__).with_name("config.json")
config = json.loads(CONFIG_FILE.read_text()) if CONFIG_FILE.exists() else {}
db_config = config.get("database", {})

import pymysql
from pymysql.cursors import DictCursor

conn = pymysql.connect(
    host=db_config.get("host", "localhost"),
    port=db_config.get("port", 3306),
    user=db_config.get("user", "root"),
    password=db_config.get("password", ""),
    database=db_config.get("name", "joblog"),
    cursorclass=DictCursor
)

cursor = conn.cursor()

print("=" * 80)
print("DEBUG TURNI GIOVEDÌ 8 GENNAIO 2026")
print("=" * 80)

# 1. Tutti i turni del 8 gennaio
print("\n1️⃣  TUTTI i turni del 8 gennaio (senza filtri):")
cursor.execute("""
    SELECT id, crew_id, crew_name, project_code, project_name, 
           plan_start, plan_end, sent_to_webservice, is_obsolete
    FROM rentman_plannings 
    WHERE planning_date = '2026-01-08'
    ORDER BY crew_name, plan_start
""")
rows = cursor.fetchall()
print(f"   Trovati: {len(rows)} turni")
for r in rows:
    print(f"   - ID={r['id']}, crew={r['crew_name']}, project={r['project_code']}, "
          f"{r['plan_start']}-{r['plan_end']}, sent={r['sent_to_webservice']}, obsolete={r['is_obsolete']}")

# 2. Solo turni attivi (sent=1, obsolete=0)
print("\n2️⃣  Turni ATTIVI (sent_to_webservice=1 AND is_obsolete=0):")
cursor.execute("""
    SELECT id, crew_id, crew_name, project_code, project_name, 
           plan_start, plan_end
    FROM rentman_plannings 
    WHERE planning_date = '2026-01-08'
      AND sent_to_webservice = 1
      AND (is_obsolete = 0 OR is_obsolete IS NULL)
    ORDER BY crew_name, plan_start
""")
rows = cursor.fetchall()
print(f"   Trovati: {len(rows)} turni")
for r in rows:
    print(f"   - ID={r['id']}, crew={r['crew_name']}, project={r['project_code']}, "
          f"{r['plan_start']}-{r['plan_end']}")

# 3. Turni obsoleti
print("\n3️⃣  Turni OBSOLETI (is_obsolete=1):")
cursor.execute("""
    SELECT id, crew_id, crew_name, project_code, sent_to_webservice
    FROM rentman_plannings 
    WHERE planning_date = '2026-01-08' AND is_obsolete = 1
""")
rows = cursor.fetchall()
print(f"   Trovati: {len(rows)} turni obsoleti")

# 4. Turni non inviati
print("\n4️⃣  Turni NON INVIATI (sent_to_webservice=0):")
cursor.execute("""
    SELECT id, crew_id, crew_name, project_code, is_obsolete
    FROM rentman_plannings 
    WHERE planning_date = '2026-01-08' AND sent_to_webservice = 0
""")
rows = cursor.fetchall()
print(f"   Trovati: {len(rows)} turni non inviati")
for r in rows:
    print(f"   - ID={r['id']}, crew={r['crew_name']}, project={r['project_code']}, obsolete={r['is_obsolete']}")

# 5. Riepilogo per utente loggato (esempio: cerca tutti gli utenti con turni quel giorno)
print("\n5️⃣  Utenti con turni l'8 gennaio:")
cursor.execute("""
    SELECT DISTINCT u.username, u.rentman_crew_id, rp.crew_name,
           COUNT(*) as num_turni,
           SUM(CASE WHEN rp.sent_to_webservice = 1 AND (rp.is_obsolete = 0 OR rp.is_obsolete IS NULL) THEN 1 ELSE 0 END) as turni_attivi
    FROM app_users u
    JOIN rentman_plannings rp ON u.rentman_crew_id = rp.crew_id
    WHERE rp.planning_date = '2026-01-08'
    GROUP BY u.username, u.rentman_crew_id, rp.crew_name
""")
rows = cursor.fetchall()
for r in rows:
    print(f"   - {r['username']} (crew_id={r['rentman_crew_id']}, {r['crew_name']}): "
          f"{r['num_turni']} totali, {r['turni_attivi']} attivi")

print("\n" + "=" * 80)
print("SOLUZIONE:")
print("Se i turni ci sono ma sono obsoleti, esegui:")
print("UPDATE rentman_plannings SET is_obsolete = 0 WHERE planning_date = '2026-01-08' AND sent_to_webservice = 1;")
print("=" * 80)

conn.close()
