#!/usr/bin/env python3
"""
Script per verificare e aggiustare il database MySQL del server.
Controlla se la colonna timbratura_gps_mode esiste e la crea/popola se necessaria.
"""

import mysql.connector
from mysql.connector import Error
import json
import os

# Carica la configurazione
try:
    with open('config.json', 'r') as f:
        config = json.load(f)
        db_config = config.get('database', {})
except:
    print("❌ Errore: non riesco a leggere config.json")
    exit(1)

try:
    # Connessione
    conn = mysql.connector.connect(
        host=db_config.get('host', 'localhost'),
        user=db_config.get('user', 'root'),
        password=db_config.get('password', ''),
        database=db_config.get('database', 'joblog')
    )
    
    cursor = conn.cursor(dictionary=True)
    
    print("=" * 80)
    print("VERIFICA E AGGIUSTAMENTO DATABASE MySQL")
    print("=" * 80)
    
    # 1. Verifica se la colonna timbratura_gps_mode esiste
    print("\n1️⃣  Verificando colonna 'timbratura_gps_mode'...")
    cursor.execute("""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'rentman_plannings' 
        AND COLUMN_NAME = 'timbratura_gps_mode'
    """)
    
    result = cursor.fetchone()
    if result:
        print("   ✅ Colonna 'timbratura_gps_mode' ESISTE")
    else:
        print("   ❌ Colonna 'timbratura_gps_mode' NON ESISTE - la creiamo...")
        cursor.execute("""
            ALTER TABLE rentman_plannings 
            ADD COLUMN timbratura_gps_mode VARCHAR(20) DEFAULT 'group' 
            AFTER location_lon
        """)
        conn.commit()
        print("   ✅ Colonna creata")
    
    # 2. Verifica se la colonna is_obsolete esiste
    print("\n2️⃣  Verificando colonna 'is_obsolete'...")
    cursor.execute("""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'rentman_plannings' 
        AND COLUMN_NAME = 'is_obsolete'
    """)
    
    result = cursor.fetchone()
    if result:
        print("   ✅ Colonna 'is_obsolete' ESISTE")
    else:
        print("   ❌ Colonna 'is_obsolete' NON ESISTE - la creiamo...")
        cursor.execute("""
            ALTER TABLE rentman_plannings 
            ADD COLUMN is_obsolete TINYINT(1) DEFAULT 0 COMMENT 'Turno rimosso da Rentman'
        """)
        conn.commit()
        print("   ✅ Colonna creata")
    
    # 3. Conteggio valori NULL per timbratura_gps_mode
    print("\n3️⃣  Conteggio record con timbratura_gps_mode NULL...")
    cursor.execute("""
        SELECT COUNT(*) as null_count FROM rentman_plannings 
        WHERE timbratura_gps_mode IS NULL OR timbratura_gps_mode = ''
    """)
    null_count = cursor.fetchone()['null_count']
    print(f"   Record con valore NULL/vuoto: {null_count}")
    
    # 4. Aggiorna i NULL a 'group'
    if null_count > 0:
        print(f"\n4️⃣  Aggiornando {null_count} record a timbratura_gps_mode='group'...")
        cursor.execute("""
            UPDATE rentman_plannings 
            SET timbratura_gps_mode = 'group' 
            WHERE timbratura_gps_mode IS NULL OR timbratura_gps_mode = ''
        """)
        conn.commit()
        print(f"   ✅ Aggiornati {cursor.rowcount} record")
    else:
        print("   ✅ Nessun record da aggiornare - tutti hanno valore")
    
    # 5. Verifica il risultato finale
    print("\n5️⃣  Verifica finale...")
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN timbratura_gps_mode = 'group' THEN 1 ELSE 0 END) as group_count,
            SUM(CASE WHEN timbratura_gps_mode = 'location' THEN 1 ELSE 0 END) as location_count,
            SUM(CASE WHEN timbratura_gps_mode IS NULL THEN 1 ELSE 0 END) as null_count
        FROM rentman_plannings
    """)
    
    stats = cursor.fetchone()
    print(f"   📊 Statistiche finali:")
    print(f"      • Record totali: {stats['total']}")
    print(f"      • Con modo 'group': {stats['group_count']}")
    print(f"      • Con modo 'location': {stats['location_count']}")
    print(f"      • Con valore NULL: {stats['null_count']}")
    
    print("\n" + "=" * 80)
    print("✅ VERIFICAZIONE E AGGIUSTAMENTO COMPLETATI")
    print("=" * 80)
    
    cursor.close()
    conn.close()
    
except Error as e:
    print(f"\n❌ Errore MySQL: {e}")
    exit(1)
except Exception as e:
    print(f"\n❌ Errore: {e}")
    exit(1)
