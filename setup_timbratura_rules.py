"""Script per creare le strutture DB per le regole timbrature."""
import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    user='tim_root',
    password='gianni225524',
    database='joblog'
)
c = conn.cursor()

# Verifica e crea tabella timbratura_rules
c.execute("SHOW TABLES LIKE 'timbratura_rules'")
if c.fetchone():
    print("Tabella timbratura_rules già esistente")
else:
    c.execute("""
        CREATE TABLE timbratura_rules (
            id INT PRIMARY KEY DEFAULT 1,
            anticipo_max_minuti INT DEFAULT 30,
            tolleranza_ritardo_minuti INT DEFAULT 5,
            arrotondamento_ingresso_minuti INT DEFAULT 15,
            arrotondamento_uscita_minuti INT DEFAULT 15,
            pausa_blocco_minimo_minuti INT DEFAULT 30,
            pausa_incremento_minuti INT DEFAULT 15,
            pausa_tolleranza_minuti INT DEFAULT 5,
            updated_ts BIGINT,
            updated_by VARCHAR(100)
        )
    """)
    print("Tabella timbratura_rules creata!")

# Verifica se ci sono dati
c.execute("SELECT * FROM timbratura_rules WHERE id = 1")
if c.fetchone():
    print("Record di default già esistente")
else:
    c.execute("""
        INSERT INTO timbratura_rules (id, anticipo_max_minuti, tolleranza_ritardo_minuti,
            arrotondamento_ingresso_minuti, arrotondamento_uscita_minuti,
            pausa_blocco_minimo_minuti, pausa_incremento_minuti, pausa_tolleranza_minuti)
        VALUES (1, 30, 5, 15, 15, 30, 15, 5)
    """)
    print("Record di default inserito!")

# Verifica e crea colonna ora_mod
c.execute("SHOW COLUMNS FROM timbrature LIKE 'ora_mod'")
if c.fetchone():
    print("Colonna ora_mod già esistente")
else:
    c.execute("ALTER TABLE timbrature ADD COLUMN ora_mod VARCHAR(8)")
    print("Colonna ora_mod aggiunta!")

conn.commit()
conn.close()
print("\nSetup completato con successo!")
