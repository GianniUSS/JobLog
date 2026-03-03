#!/usr/bin/env python
"""
Script per correggere i record 'fuori flessibilità' già approvati
che hanno il dettaglio giornata (tabella timbrature) non aggiornato.
"""

import json
import pymysql
from datetime import datetime

# Configurazione database (stessa di app.py)
with open("config.json", "r") as f:
    config = json.load(f)

db_config = config.get("database", {})
DB_HOST = db_config.get("host", "localhost")
DB_PORT = db_config.get("port", 3306)
DB_USER = db_config.get("user", "root")
DB_PASS = db_config.get("password", "")
DB_NAME = db_config.get("name", "joblog")  # Nota: "name" non "database"

def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )

def main():
    conn = get_connection()
    cursor = conn.cursor()
    
    # Trova tutte le richieste "fuori flessibilità" approvate
    cursor.execute("""
        SELECT ur.id, ur.username, ur.date_from, ur.extra_data, ur.status,
               rt.name as type_name
        FROM user_requests ur
        JOIN request_types rt ON ur.request_type_id = rt.id
        WHERE rt.name = 'Fuori Flessibilità'
          AND ur.status = 'approved'
        ORDER BY ur.date_from DESC
    """)
    
    requests = cursor.fetchall()
    print(f"\n=== Trovate {len(requests)} richieste 'Fuori Flessibilità' approvate ===\n")
    
    fixed_count = 0
    error_count = 0
    already_ok_count = 0
    
    for req in requests:
        req_id = req['id']
        username = req['username']
        date_from = str(req['date_from'])
        extra_data_str = req.get('extra_data')
        
        print(f"--- Request ID: {req_id}, User: {username}, Data: {date_from} ---")
        
        # Parse extra_data
        extra_data = {}
        if extra_data_str:
            try:
                extra_data = json.loads(extra_data_str) if isinstance(extra_data_str, str) else extra_data_str
            except:
                print(f"  ERRORE: impossibile parsare extra_data: {extra_data_str}")
                error_count += 1
                continue
        
        tipo_timbratura = extra_data.get("tipo_timbratura")
        ora_finale = extra_data.get("ora_finale")
        ora_timbrata = extra_data.get("ora_timbrata")
        rounded_time = extra_data.get("rounded_time")
        
        print(f"  extra_data: tipo_timbratura={tipo_timbratura}, ora_finale={ora_finale}, ora_timbrata={ora_timbrata}, rounded_time={rounded_time}")
        
        if not tipo_timbratura:
            print(f"  ATTENZIONE: tipo_timbratura mancante, provo a determinarlo...")
            # Prova a determinare il tipo dalla timbratura
            cursor.execute("""
                SELECT tipo, ora, ora_mod FROM timbrature
                WHERE username = %s AND data = %s
                ORDER BY ora DESC
            """, (username, date_from))
            timbrature = cursor.fetchall()
            print(f"  Timbrature trovate: {timbrature}")
            
            # Usa l'ultima timbratura del giorno (spesso è quella di "fine giornata")
            if timbrature:
                tipo_timbratura = timbrature[0]['tipo']
                print(f"  Uso tipo_timbratura={tipo_timbratura} dall'ultima timbratura")
            else:
                print(f"  ERRORE: nessuna timbratura trovata per {username} in data {date_from}")
                error_count += 1
                continue
        
        # Determina l'ora corretta da usare
        ora_da_usare = ora_finale or rounded_time or ora_timbrata
        if ora_da_usare:
            ora_da_usare = str(ora_da_usare)[:5]  # Prendi solo HH:MM
        
        if not ora_da_usare:
            print(f"  ERRORE: nessun orario disponibile per aggiornare")
            error_count += 1
            continue
        
        print(f"  Ora da usare: {ora_da_usare}")
        
        # Verifica lo stato attuale nella tabella timbrature
        cursor.execute("""
            SELECT id, tipo, ora, ora_mod FROM timbrature
            WHERE username = %s AND data = %s AND tipo = %s
        """, (username, date_from, tipo_timbratura))
        timbratura = cursor.fetchone()
        
        if not timbratura:
            # Prova senza filtro tipo
            cursor.execute("""
                SELECT id, tipo, ora, ora_mod FROM timbrature
                WHERE username = %s AND data = %s
                ORDER BY ora DESC LIMIT 1
            """, (username, date_from))
            timbratura = cursor.fetchone()
            if timbratura:
                tipo_timbratura = timbratura['tipo']
                print(f"  NOTA: trovata timbratura con tipo diverso: {tipo_timbratura}")
        
        if not timbratura:
            print(f"  ERRORE: nessuna timbratura trovata per tipo={tipo_timbratura}")
            error_count += 1
            continue
        
        current_ora_mod = timbratura.get('ora_mod')
        current_ora_mod_str = str(current_ora_mod)[:5] if current_ora_mod else None
        
        print(f"  Timbratura attuale: id={timbratura['id']}, tipo={timbratura['tipo']}, ora={timbratura['ora']}, ora_mod={current_ora_mod}")
        
        # Confronta
        if current_ora_mod_str == ora_da_usare:
            print(f"  OK: ora_mod già corretto ({ora_da_usare})")
            already_ok_count += 1
            continue
        
        # Aggiorna
        print(f"  AGGIORNO: ora_mod da '{current_ora_mod_str}' a '{ora_da_usare}'")
        cursor.execute("""
            UPDATE timbrature SET ora_mod = %s
            WHERE id = %s
        """, (ora_da_usare, timbratura['id']))
        
        # Aggiorna anche extra_data se manca ora_finale
        if not ora_finale:
            extra_data['ora_finale'] = ora_da_usare
            cursor.execute("""
                UPDATE user_requests SET extra_data = %s WHERE id = %s
            """, (json.dumps(extra_data), req_id))
            print(f"  Aggiornato anche extra_data con ora_finale={ora_da_usare}")
        
        fixed_count += 1
        print(f"  FATTO!")
    
    conn.commit()
    conn.close()
    
    print(f"\n=== RIEPILOGO ===")
    print(f"Totale richieste: {len(requests)}")
    print(f"Già corretti: {already_ok_count}")
    print(f"Corretti: {fixed_count}")
    print(f"Errori: {error_count}")

if __name__ == "__main__":
    main()
