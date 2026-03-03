import json, sys, os
sys.path.insert(0, '.')
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from app import get_function_phases_config, get_phases_for_function, DATABASE_SETTINGS, DB_VENDOR

# Create DB connection directly
if DB_VENDOR == "mysql":
    from app import MySQLConnection
    db = MySQLConnection(DATABASE_SETTINGS)
else:
    import sqlite3
    db = sqlite3.connect('joblog.db')
    db.row_factory = sqlite3.Row

# 1. Check function_phases config
config = get_function_phases_config(db)
print("=== FUNCTION PHASES CONFIG ===")
if not config:
    print("  EMPTY - No function phases configured!")
else:
    for k, v in config.items():
        phases = v.get('phases', [])
        match_mode = v.get('match_mode', 'N/A')
        print(f"  Key: [{k}] match_mode={match_mode} phases={[p['name'] for p in phases]}")

# 2. Test matching for the specific function
test_fn = "montaggio Teli Masseria Eccellenza"
result = get_phases_for_function(db, test_fn)
print(f"\n=== PHASES FOR '{test_fn}' ===")
print(f"  Result: {result}")

# 3. Also test simpler names
for fn in ["Montaggio", "montaggio", "Allestimento"]:
    result2 = get_phases_for_function(db, fn)
    print(f"  Phases for '{fn}': {result2}")

db.close()
