import mysql.connector

conn = mysql.connector.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
cur = conn.cursor(dictionary=True)
cur.execute('SELECT crew_name, remark, remark_planner FROM rentman_plannings WHERE planning_date = %s', ('2026-01-16',))
rows = cur.fetchall()
print('Note nel database:')
for r in rows:
    print(f"  {r['crew_name']}: remark='{r['remark']}', remark_planner='{r['remark_planner']}'")
conn.close()
