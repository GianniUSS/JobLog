"""Script per testare e creare la tabella group_timbratura_rules"""
import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog'
)
c = conn.cursor(dictionary=True)

# Crea tabella se non esiste
c.execute("""
CREATE TABLE IF NOT EXISTS group_timbratura_rules (
    id INT AUTO_INCREMENT PRIMARY KEY,
    group_id INT NOT NULL,
    rounding_mode ENUM('single', 'daily') NOT NULL DEFAULT 'single',
    flessibilita_ingresso_minuti INT DEFAULT 30,
    flessibilita_uscita_minuti INT DEFAULT 30,
    arrotondamento_giornaliero_minuti INT DEFAULT 15,
    arrotondamento_giornaliero_tipo ENUM('floor', 'ceil', 'nearest') DEFAULT 'floor',
    oltre_flessibilita_action ENUM('allow', 'warn', 'block') DEFAULT 'allow',
    usa_regole_pausa_standard TINYINT(1) DEFAULT 1,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_ts BIGINT NOT NULL,
    updated_ts BIGINT NOT NULL,
    updated_by VARCHAR(100),
    UNIQUE KEY uk_group_rules (group_id),
    FOREIGN KEY (group_id) REFERENCES user_groups(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""")
conn.commit()
print("Tabella group_timbratura_rules creata/verificata!")

# Verifica struttura
c.execute("DESCRIBE group_timbratura_rules")
print("\nStruttura tabella:")
for col in c.fetchall():
    print(f"  {col['Field']}: {col['Type']}")

# Mostra gruppi esistenti
c.execute("SELECT id, name FROM user_groups WHERE is_active = 1")
groups = c.fetchall()
print(f"\nGruppi attivi: {len(groups)}")
for g in groups:
    print(f"  - {g['id']}: {g['name']}")

# Test: ottieni regole per utente giannipi (gruppo Ufficio = 9)
c.execute("SELECT group_id FROM app_users WHERE username = 'giannipi'")
user = c.fetchone()
print(f"\ngiannipi appartiene al gruppo: {user['group_id']}")

# Verifica se ci sono regole per il suo gruppo
c.execute("SELECT * FROM group_timbratura_rules WHERE group_id = %s", (user['group_id'],))
rules = c.fetchone()
if rules:
    print(f"Regole personalizzate trovate: mode={rules['rounding_mode']}")
else:
    print("Nessuna regola personalizzata - userà regole globali")

conn.close()
print("\n✅ Test completato!")
