import pymysql
import json

with open('config.json', 'r') as f:
    config = json.load(f)

db_cfg = config.get('database', {})
conn = pymysql.connect(
    host=db_cfg.get('host', 'localhost'),
    user=db_cfg.get('user'),
    password=db_cfg.get('password'),
    database=db_cfg.get('name'),
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

with conn.cursor() as cursor:
    # Rimuovi i turni vecchi/errati per donato e tonio del 5 gennaio con progetto 2899
    print('=== RIMOZIONE TURNI ERRATI ===')
    cursor.execute('''
        DELETE FROM rentman_plannings 
        WHERE planning_date = '2026-01-05' 
          AND project_code = '2899'
    ''')
    print(f'Rimossi {cursor.rowcount} turni con progetto 2899')
    conn.commit()
    
    print()
    print('=== PIANIFICAZIONI RIMANENTI 2026-01-05 ===')
    cursor.execute('''
        SELECT crew_id, crew_name, project_code, function_name, plan_start, plan_end
        FROM rentman_plannings 
        WHERE planning_date = '2026-01-05' AND sent_to_webservice = 1
        ORDER BY crew_name, plan_start
    ''')
    for row in cursor.fetchall():
        print(f'crew_id={row["crew_id"]}, crew_name={row["crew_name"]}: {row["project_code"]} - {row["function_name"]} ({row["plan_start"]} - {row["plan_end"]})')

conn.close()
