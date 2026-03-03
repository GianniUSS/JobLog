"""
Fix temporaneo per correggere ora_mod della timbratura fine_giornata di giannipi
"""
import mysql.connector
from datetime import datetime, timedelta

conn = mysql.connector.connect(
    host='localhost',
    user='root', 
    password='admin',
    database='joblog'
)
cursor = conn.cursor(dictionary=True)

username = 'giannipi'
data = '2026-01-20'

# 1. Recupera le timbrature del giorno
cursor.execute('''
    SELECT id, tipo, ora, ora_mod FROM timbrature 
    WHERE username = %s AND data = %s
    ORDER BY ora
''', (username, data))
timbrature = cursor.fetchall()

print("Timbrature attuali:")
for t in timbrature:
    print(f"  {t['tipo']}: ora={t['ora']}, ora_mod={t['ora_mod']}")

# 2. Trova inizio e fine
inizio = None
fine = None
for t in timbrature:
    if t['tipo'] == 'inizio_giornata':
        inizio = t
    elif t['tipo'] == 'fine_giornata':
        fine = t

if inizio and fine:
    # Converti in minuti
    ora_inizio = inizio['ora_mod'] or inizio['ora']
    if hasattr(ora_inizio, 'total_seconds'):
        inizio_min = int(ora_inizio.total_seconds()) // 60
    else:
        parts = str(ora_inizio).split(':')
        inizio_min = int(parts[0]) * 60 + int(parts[1])
    
    ora_fine = fine['ora']  # Ora EFFETTIVA
    if hasattr(ora_fine, 'total_seconds'):
        fine_min = int(ora_fine.total_seconds()) // 60
    else:
        parts = str(ora_fine).split(':')
        fine_min = int(parts[0]) * 60 + int(parts[1])
    
    # Pausa turno (da employee_shifts)
    pausa_turno_min = 60  # Default 1 ora
    
    # Calcola ore nette effettive
    ore_lorde = fine_min - inizio_min
    ore_nette = ore_lorde - pausa_turno_min
    
    # Arrotonda (blocco 30, floor)
    blocco = 30
    ore_arrotondate = (ore_nette // blocco) * blocco
    
    # Calcola ora_mod corretta
    ora_mod_min = inizio_min + ore_arrotondate + pausa_turno_min
    h = ora_mod_min // 60
    m = ora_mod_min % 60
    ora_mod_corretta = f"{h:02d}:{m:02d}:00"
    
    print(f"\nCalcolo:")
    print(f"  Inizio (ora_mod): {inizio_min // 60:02d}:{inizio_min % 60:02d}")
    print(f"  Fine (effettiva): {fine_min // 60:02d}:{fine_min % 60:02d}")
    print(f"  Ore lorde: {ore_lorde} min ({ore_lorde // 60}:{ore_lorde % 60:02d})")
    print(f"  Pausa turno: {pausa_turno_min} min")
    print(f"  Ore nette: {ore_nette} min ({ore_nette // 60}:{ore_nette % 60:02d})")
    print(f"  Ore arrotondate (blocco {blocco}): {ore_arrotondate} min ({ore_arrotondate // 60}:{ore_arrotondate % 60:02d})")
    print(f"  ora_mod corretta: {ora_mod_corretta}")
    print(f"  ora_mod attuale: {fine['ora_mod']}")
    
    # 3. Aggiorna automaticamente
    print(f"\nAggiorno ora_mod da {fine['ora_mod']} a {ora_mod_corretta}...")
    cursor.execute('''
        UPDATE timbrature SET ora_mod = %s WHERE id = %s
    ''', (ora_mod_corretta, fine['id']))
    conn.commit()
    print(f"Aggiornato! ora_mod = {ora_mod_corretta}")

conn.close()
