import pymysql

conn = pymysql.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog'
)

cur = conn.cursor()
cur.execute("UPDATE cedolino_timbrature SET sync_error = NULL, synced_ts = NULL, ora_modificata = '08:00:00' WHERE id = 239")
conn.commit()
print("OK - corretta timbrata 239")
