"""Script di test per elencare i progetti attivi da Rentman.

Secondo la documentazione API Rentman (oas.json):
- I progetti (projects) NON hanno un campo status diretto
- Lo STATUS è nel SUBPROJECT (/subprojects) come riferimento "/statuses/ID"
- Ogni progetto ha almeno un subproject

Quindi: recuperiamo i subprojects, filtriamo per status e data,
poi raggruppiamo per progetto padre.
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import (
    _parse_date_any,
    get_rentman_client,
    parse_reference,
)

ALLOWED_STATUS_KEYWORDS = (
    "confermat",
    "confirm",
    "in location",
    "locat",
    "pronto",
    "ready",
)
CANCELLED_KEYWORDS = ("annull", "cancel")


def normalize_status(value: Optional[str]) -> str:
    cleaned = str(value or "").strip().lower()
    cleaned = cleaned.replace("_", " ")
    return " ".join(cleaned.split())


def parse_date_value(value: Any) -> Optional[date]:
    """Converte un valore in date."""
    return _parse_date_any(value)


def subproject_matches_date(subproject: Mapping[str, Any], target: date) -> bool:
    """Verifica se il subproject copre la data target."""
    date_fields = [
        ("equipment_period_from", "equipment_period_to"),
        ("usageperiod_start", "usageperiod_end"),
        ("planperiod_start", "planperiod_end"),
    ]
    
    for start_key, end_key in date_fields:
        start_val = parse_date_value(subproject.get(start_key))
        end_val = parse_date_value(subproject.get(end_key))
        
        if start_val is not None or end_val is not None:
            # Se abbiamo almeno una data, verifichiamo
            if start_val is None:
                start_val = end_val
            if end_val is None:
                end_val = start_val
            if start_val and end_val:
                if end_val < start_val:
                    start_val, end_val = end_val, start_val
                if start_val <= target <= end_val:
                    return True
    
    return False


def is_status_allowed(status_name: str) -> bool:
    """Verifica se lo status è tra quelli consentiti."""
    normalized = normalize_status(status_name)
    if any(kw in normalized for kw in CANCELLED_KEYWORDS):
        return False
    if any(kw in normalized for kw in ALLOWED_STATUS_KEYWORDS):
        return True
    return False


def list_active_projects(target_date_str: str) -> Dict[str, Any]:
    """Elenca i progetti attivi per la data specificata."""
    client = get_rentman_client()
    if not client:
        return {"error": "Rentman client non disponibile", "projects": []}

    target = _parse_date_any(target_date_str)
    if target is None:
        return {"error": f"Data non valida: {target_date_str}", "projects": []}

    # 1) Recupera tutti gli status disponibili
    print("Recupero status da Rentman...")
    status_map: Dict[int, str] = {}
    try:
        for entry in client.get_project_statuses():
            status_id = entry.get("id")
            if isinstance(status_id, int):
                status_name = entry.get("displayname") or entry.get("name") or str(status_id)
                status_map[status_id] = str(status_name)
    except Exception as e:
        print(f"Errore recupero status: {e}")

    print(f"Status trovati: {len(status_map)}")
    for sid, sname in status_map.items():
        print(f"  [{sid}] {sname} -> allowed={is_status_allowed(sname)}")

    # 2) Recupera tutti i subprojects (aumentato limite a 15000)
    print("\nRecupero subprojects da Rentman...")
    subprojects: List[Dict[str, Any]] = []
    try:
        # Usiamo la paginazione per recuperare tutti i subprojects
        for sub in client.iter_collection("/subprojects", limit_total=15000):
            subprojects.append(sub)
    except Exception as e:
        print(f"Errore recupero subprojects: {e}")

    print(f"Subprojects totali recuperati: {len(subprojects)}")

    # 3) Filtra i subprojects per status e data
    valid_subprojects: List[Dict[str, Any]] = []
    projects_seen: Set[str] = set()
    
    for sub in subprojects:
        # Estrai l'ID dello status dal riferimento "/statuses/123"
        status_ref = sub.get("status")  # es: "/statuses/2"
        status_id = parse_reference(status_ref)
        status_name = status_map.get(status_id, "") if isinstance(status_id, int) else ""
        
        # Verifica status
        if not is_status_allowed(status_name):
            continue
        
        # Verifica data
        if not subproject_matches_date(sub, target):
            continue
        
        # Estrai il riferimento al progetto padre "/projects/123"
        project_ref = sub.get("project")  # es: "/projects/456"
        if not project_ref:
            continue
        
        valid_subprojects.append({
            "subproject_id": sub.get("id"),
            "subproject_name": sub.get("displayname") or sub.get("name"),
            "project_ref": project_ref,
            "status_id": status_id,
            "status_name": status_name,
        })
        projects_seen.add(project_ref)

    print(f"\nSubprojects validi (status ok + data ok): {len(valid_subprojects)}")
    print(f"Progetti unici coinvolti: {len(projects_seen)}")

    # 4) Recupera i dettagli dei progetti padre
    print("\nRecupero dettagli progetti...")
    projects_result: List[Dict[str, Any]] = []
    
    for project_ref in sorted(projects_seen):
        project_id = parse_reference(project_ref)
        if project_id is None:
            continue
        
        try:
            payload = client._request("GET", f"/projects/{project_id}")
            project_data = payload.get("data", {})
            
            code = str(project_data.get("number") or project_data.get("reference") or project_id).strip().upper()
            name = project_data.get("displayname") or project_data.get("name") or code
            
            # Trova i subprojects associati
            related_subs = [s for s in valid_subprojects if s["project_ref"] == project_ref]
            
            projects_result.append({
                "code": code,
                "name": name,
                "project_id": project_id,
                "subprojects": related_subs,
            })
        except Exception as e:
            print(f"Errore recupero progetto {project_id}: {e}")

    projects_result.sort(key=lambda p: (p["code"], p["name"]))

    return {
        "date": target_date_str,
        "count": len(projects_result),
        "status_map_count": len(status_map),
        "subprojects_total": len(subprojects),
        "subprojects_valid": len(valid_subprojects),
        "projects": projects_result,
    }


if __name__ == "__main__":
    raw_input_value = input("Inserisci la data (YYYY-MM-DD), default oggi: ").strip()

    if not raw_input_value:
        target_date = datetime.now().date().isoformat()
    else:
        parsed = _parse_date_any(raw_input_value)
        if parsed is not None:
            target_date = parsed.isoformat()
        else:
            # Prova a invertire giorno/mese se il formato sembra errato
            normalized = raw_input_value.replace("/", "-")
            parts = [p.strip() for p in normalized.split("-") if p.strip()]
            if len(parts) == 3 and all(p.isdigit() for p in parts):
                year, first, second = parts
                swapped = f"{year}-{second}-{first}"
                parsed_swapped = _parse_date_any(swapped)
                if parsed_swapped is not None:
                    target_date = parsed_swapped.isoformat()
                else:
                    print(f"Data non valida: {raw_input_value}. Usa formato YYYY-MM-DD.")
                    sys.exit(1)
            else:
                print(f"Data non valida: {raw_input_value}. Usa formato YYYY-MM-DD.")
                sys.exit(1)

    print(f"\n=== Ricerca progetti attivi per {target_date} ===\n")
    results = list_active_projects(target_date)
    
    print("\n" + "=" * 50)
    print("RISULTATO FINALE:")
    print("=" * 50)
    print(json.dumps(results, ensure_ascii=False, indent=2))
