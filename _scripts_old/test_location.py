"""Test per verificare il recupero della location da Rentman."""
from rentman_client import RentmanClient
import json

client = RentmanClient()

# Test: recupera una pianificazione
plannings = client.get_crew_plannings_by_date('2026-01-03')
print(f'Pianificazioni trovate: {len(plannings)}')

if plannings:
    p = plannings[0]
    print(f'\n=== Planning ID: {p.get("id")} ===')
    
    # Recupera function
    func_ref = p.get('function', '')
    print(f'Function ref: {func_ref}')
    
    if func_ref:
        func_id = int(func_ref.split('/')[-1])
        func = client.get_project_function(func_id)
        if func:
            print(f'Function name: {func.get("name")}')
            print(f'Subproject ref: {func.get("subproject")}')
            
            # Recupera subproject
            subproj_ref = func.get('subproject', '')
            if subproj_ref:
                subproj_id = int(subproj_ref.split('/')[-1])
                subproj = client.get_subproject(subproj_id)
                if subproj:
                    print(f'\n=== Subproject ===')
                    print(f'Name: {subproj.get("name")}')
                    print(f'Location ref: {subproj.get("location")}')
                    
                    # Recupera contact (location)
                    loc_ref = subproj.get('location', '')
                    if loc_ref:
                        loc_id = int(loc_ref.split('/')[-1])
                        contact = client.get_contact(loc_id)
                        if contact:
                            print(f'\n=== Location (Contact) ===')
                            print(f'Name: {contact.get("name")}')
                            print(f'Displayname: {contact.get("displayname")}')
                            print(f'City: {contact.get("city")}')
                            print(f'Street: {contact.get("street")}')
                            print(f'Latitude: {contact.get("latitude")}')
                            print(f'Longitude: {contact.get("longitude")}')
                            
                            # Mostra tutti i campi del contatto per vedere cosa c'è
                            print(f'\n=== Tutti i campi del contact ===')
                            for key, value in sorted(contact.items()):
                                if value is not None and value != '':
                                    print(f'  {key}: {value}')
                        else:
                            print('Contact non trovato')
                    else:
                        print('Nessuna location nel subproject')
                else:
                    print('Subproject non trovato')
            else:
                print('Nessun subproject ref nella function')
        else:
            print('Function non trovata')
else:
    print('Nessuna pianificazione trovata')
