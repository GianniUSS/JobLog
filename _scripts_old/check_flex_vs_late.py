import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog'
)
cursor = conn.cursor()

# Verifica richieste Fuori Flessibilità
print('=== RICHIESTE FUORI FLESSIBILITÀ per donato ===')
cursor.execute('''
    SELECT r.id, rt.name, r.date_from, r.status, r.created_ts, r.notes
    FROM user_requests r
    JOIN request_types rt ON r.request_type_id = rt.id
    WHERE r.username = 'donato' AND rt.name LIKE "%Flessibilit%"
    ORDER BY r.created_ts DESC
    LIMIT 5
''')
flex_requests = cursor.fetchall()
if flex_requests:
    for req in flex_requests:
        print(f'  ID: {req[0]}, Tipo: {req[1]}, Data: {req[2]}, Status: {req[3]}')
else:
    print('  Nessuna richiesta Fuori Flessibilità')

# Verifica regole gruppo 7
print('\n=== REGOLE GRUPPO 7 (Produzione) ===')
cursor.execute('''
    SELECT 
        late_threshold_minutes,
        flessibilita_ingresso_minuti,
        flessibilita_uscita_minuti,
        rounding_mode,
        arrotondamento_giornaliero_minuti
    FROM group_timbratura_rules
    WHERE group_id = 7
''')
rules = cursor.fetchone()
if rules:
    print(f'  late_threshold_minutes: {rules[0]}')
    print(f'  flessibilita_ingresso_minuti: {rules[1]}')
    print(f'  flessibilita_uscita_minuti: {rules[2]}')
    print(f'  rounding_mode: {rules[3]}')
    print(f'  arrotondamento_giornaliero_minuti: {rules[4]}')

conn.close()
