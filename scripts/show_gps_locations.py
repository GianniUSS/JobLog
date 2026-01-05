#!/usr/bin/env python
"""Mostra le coordinate GPS delle sedi configurate"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importa l'app Flask per usare il suo contesto DB
from app import app, get_db, get_company_settings

with app.app_context():
    db = get_db()
    settings = get_company_settings(db)
    
    custom = settings.get('custom_settings', {})
    timbratura = custom.get('timbratura', {})
    locations = timbratura.get('gps_locations', [])
    
    print("\n" + "="*60)
    print("🏢 SEDI GPS CONFIGURATE")
    print("="*60)
    
    if not locations:
        print("  Nessuna sede GPS configurata!")
    else:
        for i, loc in enumerate(locations, 1):
            print(f"\n  [{i}] {loc.get('name', 'Senza nome')}")
            print(f"      Latitudine:  {loc.get('latitude')}")
            print(f"      Longitudine: {loc.get('longitude')}")
            print(f"      Raggio:      {loc.get('radius_meters', 100)} metri")
    
    print("\n" + "="*60)
    print("📍 IMPOSTAZIONI GPS")
    print("="*60)
    print(f"  GPS abilitato:       {timbratura.get('gps_enabled', False)}")
    print(f"  Precisione massima:  {timbratura.get('gps_max_accuracy_meters', 50)} metri")
    print()
