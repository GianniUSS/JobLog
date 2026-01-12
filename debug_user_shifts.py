#!/usr/bin/env python3
"""
Script di debug completo per verificare i turni di un utente.
Eseguire sul server: python3 debug_user_shifts.py [username]
"""

import sys
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

# Prendi username da argomento o mostra tutti gli utenti
username = sys.argv[1] if len(sys.argv) > 1 else None

print("=" * 80)
print("DEBUG TURNI UTENTE")
print("=" * 80)

if not username:
    print("\n📋 Utenti con rentman_crew_id configurato:")
    cursor.execute("""
        SELECT username, display_name, rentman_crew_id, role
        FROM app_users 
        WHERE rentman_crew_id IS NOT NULL
        ORDER BY username
    """)
    users = cursor.fetchall()
    for u in users:
        print(f"   - {u['username']} (crew_id={u['rentman_crew_id']}, {u['role']})")
    
    print("\n⚠️  Usa: python3 debug_user_shifts.py <username>")
    print("   Esempio: python3 debug_user_shifts.py donato")
    conn.close()
    sys.exit(0)

# Trova l'utente
print(f"\n🔍 Cercando utente: {username}")
cursor.execute("""
    SELECT username, display_name, rentman_crew_id, role
    FROM app_users 
    WHERE username = %s OR display_name LIKE %s
""", (username, f"%{username}%"))
user = cursor.fetchone()

if not user:
    print(f"   ❌ Utente '{username}' non trovato!")
    conn.close()
    sys.exit(1)

print(f"   ✅ Trovato: {user['display_name']} (username={user['username']})")
print(f"   📌 rentman_crew_id: {user['rentman_crew_id']}")
print(f"   👤 Ruolo: {user['role']}")

crew_id = user['rentman_crew_id']

if not crew_id:
    print("\n   ⚠️  ATTENZIONE: Questo utente NON ha un rentman_crew_id!")
    print("   Non vedrà turni da Rentman, solo da employee_shifts.")
    conn.close()
    sys.exit(0)

# Cerca i turni per questo crew_id
print(f"\n📊 Turni per crew_id={crew_id} (settimana 5-12 gennaio):")
cursor.execute("""
    SELECT planning_date, project_code, project_name, 
           TIME_FORMAT(plan_start, '%%H:%%i') as ora_inizio,
           TIME_FORMAT(plan_end, '%%H:%%i') as ora_fine,
           hours_planned,
           sent_to_webservice, is_obsolete, id
    FROM rentman_plannings 
    WHERE crew_id = %s 
      AND planning_date >= '2026-01-05' AND planning_date <= '2026-01-12'
    ORDER BY planning_date, plan_start
""", (crew_id,))
rows = cursor.fetchall()

if not rows:
    print(f"   ❌ Nessun turno trovato per crew_id={crew_id}")
else:
    for r in rows:
        status = "✅ VISIBILE" if r['sent_to_webservice'] == 1 and r['is_obsolete'] == 0 else "❌ NASCOSTO"
        reason = ""
        if r['sent_to_webservice'] != 1:
            reason += " (non inviato)"
        if r['is_obsolete'] == 1:
            reason += " (obsoleto)"
        print(f"   {r['planning_date']} | {r['ora_inizio']}-{r['ora_fine']} | {r['hours_planned']}h | {status}{reason}")
        print(f"      → {r['project_code']}: {r['project_name'][:50]}")

# Riepilogo
print("\n📈 Riepilogo:")
cursor.execute("""
    SELECT 
        SUM(CASE WHEN sent_to_webservice = 1 AND (is_obsolete = 0 OR is_obsolete IS NULL) THEN 1 ELSE 0 END) as visibili,
        SUM(CASE WHEN sent_to_webservice = 0 THEN 1 ELSE 0 END) as non_inviati,
        SUM(CASE WHEN is_obsolete = 1 THEN 1 ELSE 0 END) as obsoleti,
        COUNT(*) as totale
    FROM rentman_plannings 
    WHERE crew_id = %s 
      AND planning_date >= '2026-01-05' AND planning_date <= '2026-01-12'
""", (crew_id,))
summary = cursor.fetchone()
print(f"   Turni VISIBILI all'utente: {summary['visibili']}")
print(f"   Turni NON inviati: {summary['non_inviati']}")
print(f"   Turni OBSOLETI: {summary['obsoleti']}")
print(f"   TOTALE: {summary['totale']}")

if summary['non_inviati'] > 0:
    print("\n⚠️  PROBLEMA: Alcuni turni non sono stati INVIATI!")
    print("   Vai su admin/rentman-planning, seleziona i turni e clicca 'Invia Selezionati'")

if summary['obsoleti'] > 0:
    print("\n⚠️  PROBLEMA: Alcuni turni sono OBSOLETI!")
    print("   Esegui: python3 fix_obsolete_shifts.py")

print("\n" + "=" * 80)
conn.close()
