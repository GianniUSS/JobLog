import mysql.connector
from datetime import date

conn = mysql.connector.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
cur = conn.cursor(dictionary=True)

today = date.today().isoformat()
print(f"=== Debug revisione donato - {today} ===\n")

# 1. Timbrature di oggi
print("1. TIMBRATURE DI OGGI:")
cur.execute("SELECT id, tipo, data, ora, ora_mod FROM timbrature WHERE username='donato' AND data=%s ORDER BY id", (today,))
for r in cur.fetchall():
    print(f"   ID {r['id']}: {r['tipo']} ora={r['ora']} ora_mod={r['ora_mod']}")

# 2. Turno previsto
print("\n2. TURNO PREVISTO OGGI:")
cur.execute("SELECT rentman_crew_id FROM app_users WHERE username='donato'")
user = cur.fetchone()
if user and user['rentman_crew_id']:
    cur.execute("""SELECT project_code, project_name, function_name, plan_start, plan_end 
                   FROM rentman_plannings 
                   WHERE crew_id=%s AND planning_date=%s AND sent_to_webservice=1 
                   AND (is_obsolete=0 OR is_obsolete IS NULL)""", 
                (user['rentman_crew_id'], today))
    for r in cur.fetchall():
        print(f"   Progetto: {r['project_code']} - {r['project_name']}")
        print(f"   Mansione: {r['function_name']}")
        print(f"   Orario: {r['plan_start']} - {r['plan_end']}")

# 3. Richieste overtime pending
print("\n3. RICHIESTE OVERTIME PENDING:")
cur.execute("SELECT * FROM overtime_requests WHERE username='donato' AND status='pending' ORDER BY id DESC LIMIT 5")
for r in cur.fetchall():
    print(f"   ID {r['id']}: date={r['date']} status={r['status']}")
    print(f"   planned: {r['planned_start']}-{r['planned_end']}, actual: {r['actual_start']}-{r['actual_end']}")
    print(f"   extra_minutes: before={r['extra_minutes_before']} after={r['extra_minutes_after']} total={r['total_extra_minutes']}")
    print(f"   overtime_type: {r['overtime_type']}")
    print(f"   notes: {r['notes']}")
    print()

# 4. Regole flessibilità per il gruppo
print("\n4. REGOLE FLESSIBILITA:")
cur.execute("SELECT group_id FROM app_users WHERE username='donato'")
user = cur.fetchone()
if user and user['group_id']:
    cur.execute("SELECT * FROM timbratura_rules WHERE group_id=%s", (user['group_id'],))
    rules = cur.fetchone()
    if rules:
        print(f"   Gruppo ID: {user['group_id']}")
        print(f"   flex_enabled: {rules.get('flex_enabled')}")
        print(f"   flex_minutes: {rules.get('flex_minutes')}")
        print(f"   extra_turno_threshold: {rules.get('extra_turno_threshold')}")
    else:
        print("   Nessuna regola specifica")
