import pymysql, json
conn = pymysql.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog', cursorclass=pymysql.cursors.DictCursor, charset='utf8mb4')
cur = conn.cursor()

# All users
print('=== ALL USERS ===')
cur.execute("SELECT username, display_name, role, group_id FROM app_users ORDER BY username")
for u in cur.fetchall(): print(u)

# All plannings today
print('\n=== ALL PLANNINGS TODAY ===')
cur.execute("SELECT crew_name, crew_id, project_code, project_name, function_name, gestione_squadra, is_leader FROM rentman_plannings WHERE planning_date='2026-02-23' ORDER BY crew_name")
for p in cur.fetchall(): print(p)

conn.close()
