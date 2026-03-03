#!/usr/bin/env python3
"""
Script di migrazione per aggiornare la tabella cedolino_timbrature.
Aggiunge le colonne: username, ora_originale, ora_modificata
Modifica member_key per essere nullable.
"""

import json
from pathlib import Path

CONFIG_FILE = Path(__file__).with_name("config.json")

def load_config():
    if not CONFIG_FILE.exists():
        return {}
    with CONFIG_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)

def main():
    config = load_config()
    db_config = config.get("database", {})
    vendor = db_config.get("vendor", "sqlite")
    
    if vendor == "mysql":
        import mysql.connector
        conn = mysql.connector.connect(
            host=db_config.get("host", "localhost"),
            port=db_config.get("port", 3306),
            user=db_config.get("user"),
            password=db_config.get("password"),
            database=db_config.get("name"),
        )
        cursor = conn.cursor()
        
        # Verifica se la tabella esiste
        cursor.execute("SHOW TABLES LIKE 'cedolino_timbrature'")
        if not cursor.fetchone():
            print("Tabella cedolino_timbrature non esiste, verrà creata automaticamente all'avvio dell'app.")
            cursor.close()
            conn.close()
            return
        
        # Verifica e aggiungi colonne mancanti
        cursor.execute("SHOW COLUMNS FROM cedolino_timbrature")
        existing_columns = {row[0] for row in cursor.fetchall()}
        
        migrations = []
        
        if 'username' not in existing_columns:
            migrations.append("ALTER TABLE cedolino_timbrature ADD COLUMN username VARCHAR(190) DEFAULT NULL AFTER member_name")
            migrations.append("CREATE INDEX idx_cedolino_username ON cedolino_timbrature(username)")
        
        if 'ora_originale' not in existing_columns:
            migrations.append("ALTER TABLE cedolino_timbrature ADD COLUMN ora_originale TIME NOT NULL DEFAULT '00:00:00' AFTER data_riferimento")
        
        if 'ora_modificata' not in existing_columns:
            migrations.append("ALTER TABLE cedolino_timbrature ADD COLUMN ora_modificata TIME NOT NULL DEFAULT '00:00:00' AFTER ora_originale")
        
        # Modifica member_key per essere nullable
        cursor.execute("SHOW COLUMNS FROM cedolino_timbrature LIKE 'member_key'")
        col_info = cursor.fetchone()
        if col_info and col_info[2] == 'NO':  # IS_NULLABLE
            migrations.append("ALTER TABLE cedolino_timbrature MODIFY COLUMN member_key VARCHAR(255) DEFAULT NULL")
        
        if migrations:
            for sql in migrations:
                print(f"Esecuzione: {sql}")
                try:
                    cursor.execute(sql)
                except Exception as e:
                    print(f"  Errore (potrebbe essere già applicata): {e}")
            conn.commit()
            print("Migrazione completata!")
        else:
            print("Nessuna migrazione necessaria, tabella già aggiornata.")
        
        cursor.close()
        conn.close()
    else:
        import sqlite3
        db_path = Path(__file__).with_name("joblog.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Verifica se la tabella esiste
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cedolino_timbrature'")
        if not cursor.fetchone():
            print("Tabella cedolino_timbrature non esiste, verrà creata automaticamente all'avvio dell'app.")
            cursor.close()
            conn.close()
            return
        
        # SQLite: verifica colonne esistenti
        cursor.execute("PRAGMA table_info(cedolino_timbrature)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        
        migrations = []
        
        if 'username' not in existing_columns:
            migrations.append("ALTER TABLE cedolino_timbrature ADD COLUMN username TEXT DEFAULT NULL")
        
        if 'ora_originale' not in existing_columns:
            migrations.append("ALTER TABLE cedolino_timbrature ADD COLUMN ora_originale TEXT NOT NULL DEFAULT '00:00:00'")
        
        if 'ora_modificata' not in existing_columns:
            migrations.append("ALTER TABLE cedolino_timbrature ADD COLUMN ora_modificata TEXT NOT NULL DEFAULT '00:00:00'")
        
        if migrations:
            for sql in migrations:
                print(f"Esecuzione: {sql}")
                try:
                    cursor.execute(sql)
                except Exception as e:
                    print(f"  Errore (potrebbe essere già applicata): {e}")
            
            # Crea indice username se non esiste
            if 'username' not in existing_columns:
                try:
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cedolino_username ON cedolino_timbrature(username)")
                except Exception as e:
                    print(f"  Errore creazione indice: {e}")
            
            conn.commit()
            print("Migrazione completata!")
        else:
            print("Nessuna migrazione necessaria, tabella già aggiornata.")
        
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
