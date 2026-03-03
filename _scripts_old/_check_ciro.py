import pymysql, json
conn = pymysql.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog', cursorclass=pymysql.cursors.DictCursor, charset='utf8mb4')
cur = conn.cursor()

# Find Ciro's username
cur.execute("SELECT username, display_name, role, group_id FROM app_users WHERE username LIKE '%ciro%' OR display_name LIKE '%ciro%'")
users = cur.fetchall()
for u in users: print('USER:', u)

# Check his shifts for today
cur.execute("SELECT crew_name, project_code, project_name, function_name, gestione_squadra, is_leader FROM rentman_plannings WHERE planning_date='2026-02-23' AND (crew_name LIKE '%iro%' OR crew_name LIKE '%Ciro%')")
plans = cur.fetchall()
for p in plans: print('PLANNING:', p)

# Also check all plannings for today to see all crew
cur.execute("SELECT DISTINCT crew_name, project_code, function_name, gestione_squadra FROM rentman_plannings WHERE planning_date='2026-02-23'")
all_plans = cur.fetchall()
print('\n=== ALL PLANNINGS TODAY ===')
for p in all_plans: print(p)

conn.close()
