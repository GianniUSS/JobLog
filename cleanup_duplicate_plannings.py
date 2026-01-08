#!/usr/bin/env python3
"""
Script per rimuovere i turni duplicati da rentman_plannings.
Mantiene il record più recente (updated_ts massimo) e elimina i duplicati.
"""

import sqlite3
import mysql.connector
import os
import json
from typing import Optional

# Configurazione database
DB_VENDOR = os.environ.get("DB_VENDOR", "mysql")  # Default a MySQL
DB_PATH = os.environ.get("DB_PATH", "data.db")

def get_db_connection():
    """Crea una connessione al database."""
    config = json.load(open("config.json"))
    db_config = config.get("database", {})
    
    if db_config.get("vendor") == "mysql" or DB_VENDOR == "mysql":
        return mysql.connector.connect(
            host=db_config.get("host", "localhost"),
            port=db_config.get("port", 3306),
            user=db_config.get("user"),
            password=db_config.get("password"),
            database=db_config.get("name")
        )
    else:
        # Prova i database SQLite disponibili
        for db_file in ["joblog.db", "data.db", "app.db"]:
            if os.path.exists(db_file):
                print(f"Usando database: {db_file}")
                return sqlite3.connect(db_file)
        return sqlite3.connect(DB_PATH)

def cleanup_duplicates():
    """Rimuove i record duplicati da rentman_plannings."""
    db = get_db_connection()
    cursor = db.cursor()
    
    if DB_VENDOR == "mysql":
        cursor.execute("SELECT DATABASE()")
        print(f"Database MySQL: {cursor.fetchone()[0]}")
    else:
        print(f"Database SQLite: {DB_PATH}")
    
    # 1. Identifica i duplicati (stesso crew_id, planning_date, plan_start, plan_end)
    if DB_VENDOR == "mysql":
        # Trova i gruppi di duplicati
        query = """
        SELECT crew_id, planning_date, plan_start, plan_end, COUNT(*) as cnt
        FROM rentman_plannings
        WHERE crew_id IS NOT NULL
        GROUP BY crew_id, planning_date, plan_start, plan_end
        HAVING COUNT(*) > 1
        ORDER BY crew_id, planning_date
        """
        cursor.execute(query)
        duplicates = cursor.fetchall()
        
        print(f"\n✓ Trovati {len(duplicates)} gruppi di duplicati:\n")
        
        total_deleted = 0
        for group in duplicates:
            crew_id, planning_date, plan_start, plan_end, count = group
            print(f"  crew_id={crew_id}, date={planning_date}, start={plan_start}, end={plan_end}: {count} record")
            
            # Trova l'ID da mantenere (il più recente)
            cursor.execute("""
                SELECT id FROM rentman_plannings
                WHERE crew_id = %s AND planning_date = %s 
                  AND plan_start = %s AND plan_end = %s
                ORDER BY updated_ts DESC
                LIMIT 1
            """, (crew_id, planning_date, plan_start, plan_end))
            
            keep_id = cursor.fetchone()[0]
            
            # Elimina tutti gli altri
            cursor.execute("""
                DELETE FROM rentman_plannings
                WHERE crew_id = %s AND planning_date = %s 
                  AND plan_start = %s AND plan_end = %s
                  AND id != %s
            """, (crew_id, planning_date, plan_start, plan_end, keep_id))
            
            deleted = cursor.rowcount
            total_deleted += deleted
            print(f"    → Mantenuto id={keep_id}, eliminati {deleted} duplicati")
    
    else:  # SQLite
        # Trova i gruppi di duplicati
        query = """
        SELECT crew_id, planning_date, plan_start, plan_end, COUNT(*) as cnt
        FROM rentman_plannings
        WHERE crew_id IS NOT NULL
        GROUP BY crew_id, planning_date, plan_start, plan_end
        HAVING COUNT(*) > 1
        ORDER BY crew_id, planning_date
        """
        cursor.execute(query)
        duplicates = cursor.fetchall()
        
        print(f"\n✓ Trovati {len(duplicates)} gruppi di duplicati:\n")
        
        total_deleted = 0
        for group in duplicates:
            crew_id, planning_date, plan_start, plan_end, count = group
            print(f"  crew_id={crew_id}, date={planning_date}, start={plan_start}, end={plan_end}: {count} record")
            
            # Trova l'ID da mantenere (il più recente)
            cursor.execute("""
                SELECT id FROM rentman_plannings
                WHERE crew_id = ? AND planning_date = ? 
                  AND plan_start = ? AND plan_end = ?
                ORDER BY updated_ts DESC
                LIMIT 1
            """, (crew_id, planning_date, plan_start, plan_end))
            
            keep_id = cursor.fetchone()[0]
            
            # Elimina tutti gli altri
            cursor.execute("""
                DELETE FROM rentman_plannings
                WHERE crew_id = ? AND planning_date = ? 
                  AND plan_start = ? AND plan_end = ?
                  AND id != ?
            """, (crew_id, planning_date, plan_start, plan_end, keep_id))
            
            deleted = cursor.rowcount
            total_deleted += deleted
            print(f"    → Mantenuto id={keep_id}, eliminati {deleted} duplicati")
    
    db.commit()
    print(f"\n✅ Totale record eliminati: {total_deleted}")
    
    # 2. Verifica il risultato
    if DB_VENDOR == "mysql":
        cursor.execute("""
        SELECT crew_id, planning_date, COUNT(*) as cnt
        FROM rentman_plannings
        WHERE crew_id IS NOT NULL
        GROUP BY crew_id, planning_date
        ORDER BY cnt DESC
        """)
    else:
        cursor.execute("""
        SELECT crew_id, planning_date, COUNT(*) as cnt
        FROM rentman_plannings
        WHERE crew_id IS NOT NULL
        GROUP BY crew_id, planning_date
        ORDER BY cnt DESC
        """)
    
    results = cursor.fetchall()
    max_count = max([r[2] for r in results]) if results else 0
    
    print(f"\n📊 Verifica finale:")
    print(f"  Massimo turni per (crew_id, date): {max_count}")
    
    if max_count > 1:
        print(f"  ⚠️ Ancora ci sono duplicati!")
        for row in results:
            if row[2] > 1:
                print(f"    crew_id={row[0]}, date={row[1]}: {row[2]} turni")
    else:
        print(f"  ✅ Nessun duplicato rimasto!")
    
    cursor.close()
    db.close()

if __name__ == "__main__":
    print("=" * 70)
    print("PULIZIA DUPLICATI - rentman_plannings")
    print("=" * 70)
    cleanup_duplicates()
    print("\n✅ Completato!")
