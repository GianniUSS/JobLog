import pymysql
conn = pymysql.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog', cursorclass=pymysql.cursors.DictCursor, charset='utf8mb4')
cur = conn.cursor()
cur.execute("SELECT crew_name, project_code, function_name, sent_to_webservice, is_obsolete, gestione_squadra FROM rentman_plannings WHERE planning_date='2026-02-23' AND crew_id=1896")
for r in cur.fetchall(): print(r)

# Also check crew_id mapping for ciro
cur.execute("SELECT username, display_name, external_id FROM app_users WHERE username='ciro'")
for r in cur.fetchall(): print('USER:', r)

# Check user_crew_id resolution
cur.execute("SELECT id, rentman_id, name FROM crew_members WHERE name LIKE '%Ciro%' OR name LIKE '%ciro%'")
for r in cur.fetchall(): print('CREW:', r)

conn.close()
