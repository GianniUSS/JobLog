import mysql.connector
import time

conn = mysql.connector.connect(host='localhost', user='tim_root', password='gianni225524', database='joblog')
cur = conn.cursor(dictionary=True)
cur.execute("SELECT * FROM warehouse_active_timers WHERE username = 'donato'")
timer = cur.fetchone()
print('Timer nel DB:')
for k, v in timer.items():
    print(f'  {k}: {v}')

now_ms = int(time.time() * 1000)
print()
print(f'Now (ms): {now_ms}')
print(f'start_ts: {timer["start_ts"]}')

if timer['start_ts']:
    diff_ms = now_ms - timer['start_ts']
    diff_sec = diff_ms / 1000
    diff_min = diff_sec / 60
    print(f'Differenza: {diff_ms} ms = {diff_sec:.1f} sec = {diff_min:.1f} min')
    
    # Verifica ora di avvio
    from datetime import datetime
    start_dt = datetime.fromtimestamp(timer['start_ts'] / 1000)
    print(f'Timer avviato alle: {start_dt.strftime("%H:%M:%S")}')
    print(f'Ora attuale: {datetime.now().strftime("%H:%M:%S")}')
