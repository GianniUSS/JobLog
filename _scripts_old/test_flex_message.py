# Test del nuovo formato messaggio flessibilità
# Simula la generazione del messaggio con i dati della timbrata delle 10:00

def verifica_flessibilita_test(ora, tipo, turno_start, turno_end, flessibilita_minuti):
    """Simula la funzione verifica_flessibilita_timbrata"""
    from datetime import datetime
    
    # Parse ora timbrata
    if isinstance(ora, str):
        ora_parts = ora.replace(':', ' ').split()
        ora_min = int(ora_parts[0]) * 60 + int(ora_parts[1])
    
    # Parse turno
    def parse_time(t):
        parts = t.replace(':', ' ').split()
        return int(parts[0]) * 60 + int(parts[1])
    
    turno_start_min = parse_time(turno_start)
    turno_end_min = parse_time(turno_end)
    
    # Calcola differenza rispetto al riferimento
    if tipo == 'inizio_giornata':
        ref_min = turno_start_min
    else:
        ref_min = turno_end_min
    
    diff = ora_min - ref_min
    flessibilita = flessibilita_minuti
    within_flex = abs(diff) <= flessibilita
    
    message = ''
    if not within_flex:
        oltre_flex = abs(diff) - flessibilita
        if tipo == 'inizio_giornata':
            if diff < 0:
                message = f"Ingresso {abs(diff)} min prima del turno ({oltre_flex} min oltre flessibilità)"
            else:
                message = f"Ingresso {diff} min dopo il turno ({oltre_flex} min oltre flessibilità)"
        else:
            if diff < 0:
                message = f"Uscita {abs(diff)} min prima del turno ({oltre_flex} min oltre flessibilità)"
            else:
                message = f"Uscita {diff} min dopo il turno ({oltre_flex} min oltre flessibilità)"
    
    return {
        'within_flex': within_flex,
        'diff_minutes': diff,
        'oltre_flex_minutes': abs(diff) - flessibilita if not within_flex else 0,
        'message': message
    }


# Simula i tuoi dati:
# - Timbrata: 10:00
# - Turno: 09:00 - 18:00
# - Flessibilità: 30 minuti

print("=" * 60)
print("TEST NUOVO FORMATO MESSAGGIO FLESSIBILITÀ")
print("=" * 60)

result = verifica_flessibilita_test(
    ora="10:00:52",
    tipo="inizio_giornata",
    turno_start="09:00",
    turno_end="18:00",
    flessibilita_minuti=30
)

print(f"\nDati test:")
print(f"  Ora timbrata: 10:00:52")
print(f"  Turno: 09:00 - 18:00")
print(f"  Flessibilità: 30 minuti")
print()
print(f"Risultato:")
print(f"  Entro flessibilità: {result['within_flex']}")
print(f"  Differenza dal turno: {result['diff_minutes']} minuti")
print(f"  Oltre flessibilità: {result['oltre_flex_minutes']} minuti")
print()
print(f"📝 NUOVO MESSAGGIO:")
print(f"   \"{result['message']}\"")
print()
print("VS vecchio messaggio:")
print(f"   \"Timbrata {abs(result['diff_minutes'])} minuti oltre la flessibilità\"")
