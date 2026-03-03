import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog'
)
cursor = conn.cursor()

# Verifica gruppi esistenti
print('=== GRUPPI DISPONIBILI ===')
cursor.execute('SHOW TABLES LIKE "%group%"')
tables = cursor.fetchall()
for t in tables:
    print(f'  Tabella: {t[0]}')

# Cerca tabella gruppi
cursor.execute('SHOW TABLES')
all_tables = [t[0] for t in cursor.fetchall()]

if 'app_groups' in all_tables:
    print('\n=== GRUPPI IN app_groups ===')
    cursor.execute('SELECT * FROM app_groups')
    cursor.execute('SHOW COLUMNS FROM app_groups')
    cols = [c[0] for c in cursor.fetchall()]
    cursor.execute('SELECT * FROM app_groups')
    groups = cursor.fetchall()
    for g in groups:
        print(dict(zip(cols, g)))

if 'groups' in all_tables:
    print('\n=== GRUPPI IN groups ===')
    cursor.execute('SHOW COLUMNS FROM groups')
    cols = [c[0] for c in cursor.fetchall()]
    cursor.execute('SELECT * FROM groups')
    groups = cursor.fetchall()
    for g in groups:
        print(dict(zip(cols, g)))

# Utente donato
print('\n=== UTENTE DONATO ===')
cursor.execute('SHOW COLUMNS FROM app_users')
user_cols = [c[0] for c in cursor.fetchall()]
print(f'Colonne app_users: {", ".join(user_cols)}')

cursor.execute('SELECT * FROM app_users WHERE username = %s', ('donato',))
user = cursor.fetchone()
if user:
    user_dict = dict(zip(user_cols, user))
    print(f'\nDati donato:')
    for k, v in user_dict.items():
        print(f'  {k}: {v}')

conn.close()
