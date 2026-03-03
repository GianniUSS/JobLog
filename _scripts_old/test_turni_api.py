#!/usr/bin/env python
"""Test API /api/user/turni"""
import requests

# Test l'API turni (simulando sessione)
session = requests.Session()

# Prima login
login_resp = session.post('http://localhost:5000/login', data={
    'username': 'giannipi',
    'password': 'gianni'  # Sostituisci con la password corretta
}, allow_redirects=False)

print(f"Login status: {login_resp.status_code}")
print(f"Cookies: {session.cookies.get_dict()}")

# Poi chiama API turni
turni_resp = session.get('http://localhost:5000/api/user/turni')
print(f"\nTurni status: {turni_resp.status_code}")

if turni_resp.ok:
    data = turni_resp.json()
    turni = data.get('turni', [])
    print(f"Numero turni: {len(turni)}")
    
    if turni:
        # Mostra primo turno
        t = turni[0]
        print(f"\n=== Primo turno ===")
        print(f"  date: {t.get('date')}")
        print(f"  location_name: {t.get('location_name')}")
        print(f"  timbratura_location: {t.get('timbratura_location')}")
        print(f"  timbratura_lat: {t.get('timbratura_lat')}")
        print(f"  timbratura_lon: {t.get('timbratura_lon')}")
else:
    print(f"Errore: {turni_resp.text}")
