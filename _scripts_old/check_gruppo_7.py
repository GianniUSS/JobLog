import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog'
)
cursor = conn.cursor()

# Tutti i gruppi
print('=== TUTTI I GRUPPI ===')
cursor.execute('SELECT id, name, description, is_production, is_active FROM user_groups ORDER BY id')
groups = cursor.fetchall()
for g in groups:
    print(f'ID: {g[0]}, Nome: {g[1]}, Desc: {g[2]}, Produzione: {g[3]}, Attivo: {g[4]}')

# Gruppo 7 (gruppo di donato)
print('\n=== GRUPPO 7 (Donato) ===')
cursor.execute('SELECT * FROM user_groups WHERE id = 7')
cursor.execute('SHOW COLUMNS FROM user_groups')
cols = [c[0] for c in cursor.fetchall()]
cursor.execute('SELECT * FROM user_groups WHERE id = 7')
group = cursor.fetchone()
if group:
    group_dict = dict(zip(cols, group))
    for k, v in group_dict.items():
        print(f'  {k}: {v}')
else:
    print('  GRUPPO NON TROVATO!')

# Regole timbratura per gruppo 7
print('\n=== REGOLE TIMBRATURA GRUPPO 7 ===')
cursor.execute('SELECT * FROM group_timbratura_rules WHERE group_id = 7')
cursor.execute('SHOW COLUMNS FROM group_timbratura_rules')
rule_cols = [c[0] for c in cursor.fetchall()]
cursor.execute('SELECT * FROM group_timbratura_rules WHERE group_id = 7')
rules = cursor.fetchone()
if rules:
    rules_dict = dict(zip(rule_cols, rules))
    print('  Regole trovate:')
    for k, v in rules_dict.items():
        print(f'    {k}: {v}')
else:
    print('  NESSUNA REGOLA TROVATA PER GRUPPO 7!')
    print('\n  Regole esistenti:')
    cursor.execute('SELECT group_id, late_threshold_minutes, is_active FROM group_timbratura_rules')
    existing = cursor.fetchall()
    for r in existing:
        print(f'    group_id: {r[0]}, late_threshold: {r[1]}, active: {r[2]}')

conn.close()
