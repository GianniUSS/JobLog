#!/usr/bin/env python3
"""
Script per correggere i turni obsoleti.
Eseguire sul server: python3 fix_obsolete_shifts.py
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
    cursorclass=DictCursor,
    autocommit=True
)

cursor = conn.cursor()

print("=" * 80)
print("FIX TURNI OBSOLETI")
print("=" * 80)

# Mostra stato attuale
print("\n📊 Stato PRIMA della correzione:")
cursor.execute("""
    SELECT planning_date, 
           SUM(CASE WHEN is_obsolete = 0 OR is_obsolete IS NULL THEN 1 ELSE 0 END) as attivi,
           SUM(CASE WHEN is_obsolete = 1 THEN 1 ELSE 0 END) as obsoleti,
           COUNT(*) as totale
    FROM rentman_plannings 
    WHERE planning_date >= '2026-01-05' AND planning_date <= '2026-01-12'
      AND sent_to_webservice = 1
    GROUP BY planning_date
    ORDER BY planning_date
""")
for row in cursor.fetchall():
    print(f"   {row['planning_date']}: {row['attivi']} attivi, {row['obsoleti']} obsoleti (totale: {row['totale']})")

# Correggi TUTTI i turni della settimana
print("\n🔧 Correzione in corso...")
cursor.execute("""
    UPDATE rentman_plannings 
    SET is_obsolete = 0 
    WHERE planning_date >= '2026-01-05' AND planning_date <= '2026-01-12'
      AND sent_to_webservice = 1
""")
affected = cursor.rowcount
print(f"   ✅ Corretti {affected} turni")

# Mostra stato dopo
print("\n📊 Stato DOPO la correzione:")
cursor.execute("""
    SELECT planning_date, 
           SUM(CASE WHEN is_obsolete = 0 OR is_obsolete IS NULL THEN 1 ELSE 0 END) as attivi,
           SUM(CASE WHEN is_obsolete = 1 THEN 1 ELSE 0 END) as obsoleti,
           COUNT(*) as totale
    FROM rentman_plannings 
    WHERE planning_date >= '2026-01-05' AND planning_date <= '2026-01-12'
      AND sent_to_webservice = 1
    GROUP BY planning_date
    ORDER BY planning_date
""")
for row in cursor.fetchall():
    print(f"   {row['planning_date']}: {row['attivi']} attivi, {row['obsoleti']} obsoleti (totale: {row['totale']})")

print("\n" + "=" * 80)
print("✅ FATTO! Ricarica la pagina per vedere i turni.")
print("=" * 80)

conn.close()
