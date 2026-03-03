import sqlite3
conn = sqlite3.connect('app.db')
cur = conn.cursor()
cur.execute('SELECT id, title, file_path, file_name, target_users, target_all FROM user_documents')
rows = cur.fetchall()
with open('docs_output.txt', 'w') as f:
    f.write("Documents in DB:\n")
    for r in rows:
        f.write(f"ID: {r[0]}, Title: {r[1]}\n")
        f.write(f"  file_path: {r[2]}\n")
        f.write(f"  file_name: {r[3]}\n")
        f.write(f"  target_users: {r[4]}\n")
        f.write(f"  target_all: {r[5]}\n\n")
conn.close()
print("Output written to docs_output.txt")
