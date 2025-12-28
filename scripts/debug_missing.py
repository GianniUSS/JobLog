"""Debug: cerca i progetti mancanti 4410 e 4419"""
import sys
sys.path.insert(0, '.')
from app import get_rentman_client, parse_reference

client = get_rentman_client()

missing_codes = ['4410', '4419']
print('Cerco progetti mancanti...')

for code in missing_codes:
    print(f'\n=== Progetto {code} ===')
    # Trova il progetto per numero
    for proj in client.iter_projects(limit_total=500, params={'number': code}):
        proj_id = proj.get('id')
        print(f'Trovato project_id={proj_id}')
        # Recupera i suoi subprojects
        subs = client.get_project_subprojects(proj_id)
        print(f'Subprojects: {len(subs)}')
        for sub in subs:
            status_ref = sub.get('status')
            status_id = parse_reference(status_ref)
            sub_id = sub.get('id')
            print(f'  Sub {sub_id}: status={status_ref} (id={status_id})')
            print(f'    equipment_period: {sub.get("equipment_period_from")} -> {sub.get("equipment_period_to")}')
            print(f'    usageperiod: {sub.get("usageperiod_start")} -> {sub.get("usageperiod_end")}')
            print(f'    planperiod: {sub.get("planperiod_start")} -> {sub.get("planperiod_end")}')
