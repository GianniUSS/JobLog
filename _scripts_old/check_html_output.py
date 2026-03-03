"""Ricostruzione del HTML generato dal renderPlannings per il progetto 3498"""
import json

# Dati dal DB (confermati via API)
vehicle_data_str = '[{"id": 25, "name": "CASSONATO FORD", "plate": "GW681XV", "driver_crew_id": 1923, "driver_name": "Donato Laviola"}, {"id": 30, "name": "500 X ", "plate": ""}]'

vehicles = json.loads(vehicle_data_str)

print("=== SEZIONE VEHICLE HEADER (project-vehicles-row) ===")
print(f"vehicle_data.length = {len(vehicles)} > 0 → MOSTRA SEZIONE")
print()

for v in vehicles:
    name = v.get('name', '')
    plate = v.get('plate', '')
    driver_name = v.get('driver_name', '')
    
    # JS logic: const vLabel = v.name && v.plate ? `${v.name} - ${v.plate}` : (v.name || v.plate);
    if name and plate:
        vLabel = f"{name} - {plate}"
    else:
        vLabel = name or plate
    
    # JS logic: const isAssigned = !!currentDriver;
    isAssigned = bool(driver_name)
    cssClass = f"vehicle-driver-item {'assigned' if isAssigned else ''}"
    
    # CSS: .vehicle-driver-item.assigned { display: none; }
    visible = "NASCOSTO (display:none)" if isAssigned else "VISIBILE"
    
    print(f"<div class=\"{cssClass}\">")
    print(f"  <span class=\"vehicle-name-label\">🚛 {vLabel}</span>")
    print(f"  <span class=\"vehicle-no-driver\">trascina su operatore</span>")
    print(f"</div>")
    print(f"  [{visible}]")
    print()

# Controlla se ci sono parentesi in QUALUNQUE contenuto visibile
print("=== CHECK PARENTESI ===")
all_visible_text = []
for v in vehicles:
    name = v.get('name', '')
    plate = v.get('plate', '')
    driver_name = v.get('driver_name', '')
    isAssigned = bool(driver_name)
    
    if not isAssigned:  # Solo veicoli visibili
        if name and plate:
            vLabel = f"{name} - {plate}"
        else:
            vLabel = name or plate
        all_visible_text.append(f"🚛 {vLabel}")
        all_visible_text.append("trascina su operatore")

visible_html = ' '.join(all_visible_text)
print(f"Testo visibile totale: '{visible_html}'")
print(f"Contiene '(': {'(' in visible_html}")
print(f"Contiene ')': {')' in visible_html}")

# Verifica anche: l'intero progetto header
project_name = "EXO25_Rosalba Cardone_Leonardo Trulli Resort_Locorotondo (Ba)"
print(f"\nNome progetto: '{project_name}'")
print(f"  Contiene '(': {'(' in project_name} → MA questo è nella riga del titolo, NON sotto 'Gestione squadra'")
