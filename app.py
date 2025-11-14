from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from flask import Flask, g, jsonify, render_template, request
from rentman_client import (
    RentmanAPIError,
    RentmanAuthError,
    RentmanClient,
    RentmanNotFound,
)


app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    force=True,
)
app.logger.handlers = logging.getLogger().handlers
app.logger.setLevel(logging.INFO)
app.logger.propagate = True
DATABASE = Path(__file__).with_name("joblog.db")
PROJECTS_FILE = Path(__file__).with_name("projects.json")
CONFIG_FILE = Path(__file__).with_name("config.json")
DEMO_PROJECT_CODE = "1001"

MOCK_PROJECTS: Dict[str, Dict[str, Any]] = {
    "1001": {
        "project_code": "1001",
        "project_name": "Allestimento Stand Futuro",
        "activities": [
            {"id": "PREP", "label": "Preparazione materiali"},
            {"id": "MONT", "label": "Montaggio struttura"},
            {"id": "LGT", "label": "Illuminazione e cablaggi"},
            {"id": "FIN", "label": "Finiture e collaudo"},
        ],
        "team": [
            {"key": "anna", "name": "Anna Rossi"},
            {"key": "luca", "name": "Luca Bianchi"},
            {"key": "mario", "name": "Mario Verdi"},
            {"key": "giulia", "name": "Giulia Neri"},
        ],
    },
    "1002": {
        "project_code": "1002",
        "project_name": "Evento Corporate Milano",
        "activities": [
            {"id": "LOAD", "label": "Carico materiali"},
            {"id": "STAGE", "label": "Montaggio palco"},
            {"id": "AUDIO", "label": "Audio & video"},
            {"id": "DECOR", "label": "Decorazioni"},
        ],
        "team": [
            {"key": "federico", "name": "Federico Cattaneo"},
            {"key": "ilaria", "name": "Ilaria Riva"},
            {"key": "paolo", "name": "Paolo Gallo"},
            {"key": "roberta", "name": "Roberta Colombo"},
        ],
    },
}

_RENTMAN_CLIENT: Optional[RentmanClient] = None
_RENTMAN_CLIENT_TOKEN: Optional[str] = None
_CONFIG_CACHE: Optional[Dict[str, Any]] = None
_CONFIG_CACHE_MTIME: Optional[float] = None


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y", "si"}
    if isinstance(value, (int, float)):
        return value != 0
    return False


def _normalize_datetime(value: Any) -> Optional[str]:
    """Normalizza un valore data/ora in formato ISO 8601 (se possibile)."""

    if value is None:
        return None

    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
        except (ValueError, OSError):
            return None

    if isinstance(value, str):
        slug = value.strip()
        if not slug:
            return None
        normalized = slug.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            return slug
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()

    return None


def _normalize_date(value: Any) -> Optional[str]:
    """Normalizza un valore data (YYYY-MM-DD)."""

    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc).date().isoformat()
        except (ValueError, OSError):
            return None

    if isinstance(value, str):
        slug = value.strip()
        if not slug:
            return None
        for candidate in (slug, slug.replace("/", "-")):
            try:
                dt = datetime.fromisoformat(candidate)
            except ValueError:
                try:
                    dt = datetime.strptime(candidate, "%Y-%m-%d")
                except ValueError:
                    continue
            return dt.date().isoformat()
        return None

    return None


def _extract_iso_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        if len(value) >= 10:
            return value[:10]
        return None
    return dt.date().isoformat()


def _activity_matches_date(plan_start: Optional[str], plan_end: Optional[str], expected: str) -> bool:
    """Verifica se la data selezionata cade nell'intervallo dell'attività."""
    start_date = _extract_iso_date(plan_start)
    end_date = _extract_iso_date(plan_end)
    
    # Se non abbiamo date valide, escludiamo l'attività
    if not start_date and not end_date:
        return False
    
    # Se abbiamo solo una data, verifichiamo l'uguaglianza
    if start_date and not end_date:
        return start_date == expected
    if end_date and not start_date:
        return end_date == expected
    
    # Se abbiamo entrambe le date, verifichiamo che expected sia nell'intervallo
    # A questo punto sappiamo che entrambe sono stringhe non-None
    assert start_date is not None and end_date is not None
    return start_date <= expected <= end_date


def load_external_projects() -> Dict[str, Dict[str, Any]]:
    """Legge un catalogo di progetti personalizzati da projects.json."""

    if not PROJECTS_FILE.exists():
        return {}

    try:
        content = PROJECTS_FILE.read_text(encoding="utf-8")
    except OSError:
        return {}

    try:
        payload = json.loads(content) if content.strip() else {}
    except json.JSONDecodeError:
        return {}

    def normalize(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        code_raw = entry.get("project_code") or entry.get("code")
        code = str(code_raw or "").strip().upper()
        if not code:
            return None

        name_raw = entry.get("project_name") or entry.get("name")
        project_name = str(name_raw or code).strip()

        activities_payload = entry.get("activities") or []
        activities: List[Dict[str, Any]] = []
        for index, item in enumerate(activities_payload, start=1):
            if not isinstance(item, dict):
                continue
            raw_id = item.get("id") or item.get("code")
            activity_id = str(raw_id or "").strip().upper() or f"ACT{index:02}"
            raw_label = item.get("label") or item.get("name")
            label = str(raw_label or activity_id).strip() or activity_id
            activities.append(
                {
                    "id": activity_id,
                    "label": label,
                    "plan_start": _normalize_datetime(item.get("plan_start")),
                    "plan_end": _normalize_datetime(item.get("plan_end")),
                }
            )

        team_payload = entry.get("team") or entry.get("members") or []
        team: List[Dict[str, Any]] = []
        for member in team_payload:
            if not isinstance(member, dict):
                continue
            raw_key = member.get("key") or member.get("id")
            key = str(raw_key or "").strip()
            raw_name = member.get("name") or member.get("full_name")
            name = str(raw_name or key or "Operatore").strip()
            activity_id = member.get("activity_id")
            if isinstance(activity_id, str):
                activity_id = activity_id.strip().upper() or None
            else:
                activity_id = None
            team.append({
                "key": key,
                "name": name,
                "activity_id": activity_id,
            })

        return {
            "project_code": code,
            "project_name": project_name,
            "activities": activities,
            "team": team,
        }

    catalog: Dict[str, Dict[str, Any]] = {}

    if isinstance(payload, dict):
        for key, value in payload.items():
            if not isinstance(value, dict):
                continue
            entry = dict(value)
            entry.setdefault("project_code", key)
            normalized = normalize(entry)
            if normalized:
                catalog[normalized["project_code"]] = normalized
    elif isinstance(payload, list):
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            normalized = normalize(entry)
            if normalized:
                catalog[normalized["project_code"]] = normalized

    return catalog


def load_config() -> Dict[str, Any]:
    """Carica config.json quando disponibile e mantiene una cache in memoria."""

    global _CONFIG_CACHE, _CONFIG_CACHE_MTIME

    if not CONFIG_FILE.exists():
        _CONFIG_CACHE = {}
        _CONFIG_CACHE_MTIME = None
        return {}

    try:
        mtime = CONFIG_FILE.stat().st_mtime
    except OSError:
        return {}

    if _CONFIG_CACHE is not None and _CONFIG_CACHE_MTIME == mtime:
        return _CONFIG_CACHE

    try:
        content = CONFIG_FILE.read_text(encoding="utf-8")
        data = json.loads(content) if content.strip() else {}
        if not isinstance(data, dict):
            raise ValueError("Config root deve essere un oggetto JSON")
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        app.logger.warning("Config: impossibile leggere %s (%s)", CONFIG_FILE.name, exc)
        _CONFIG_CACHE = {}
        _CONFIG_CACHE_MTIME = mtime
        return {}

    _CONFIG_CACHE = data
    _CONFIG_CACHE_MTIME = mtime
    return data


def get_rentman_client() -> Optional[RentmanClient]:
    """Istanzia il client Rentman solo quando disponibile un token valido."""

    global _RENTMAN_CLIENT, _RENTMAN_CLIENT_TOKEN

    token = (os.environ.get("RENTMAN_API_TOKEN") or "").strip()
    if not token:
        config = load_config()
        token = str(config.get("rentman_api_token") or "").strip()
        if token:
            app.logger.warning("Rentman: uso token da config.json")

    if not token:
        app.logger.warning("Rentman: token non trovato (env o config)")
        return None

    if _RENTMAN_CLIENT and _RENTMAN_CLIENT_TOKEN == token:
        return _RENTMAN_CLIENT

    try:
        client = RentmanClient(token=token)
    except RentmanAuthError as exc:
        app.logger.warning("Rentman: token non valido (%s)", exc)
        return None

    app.logger.debug("Rentman: client inizializzato con token attivo")

    _RENTMAN_CLIENT = client
    _RENTMAN_CLIENT_TOKEN = token
    return client


def parse_reference(reference: Any) -> Optional[int]:
    """Estrae l'identificativo numerico da un riferimento Rentman."""

    if reference is None:
        return None

    if isinstance(reference, int):
        return reference

    if isinstance(reference, dict):
        candidate = reference.get("id") or reference.get("href") or reference.get("resource")
        if isinstance(candidate, int):
            return candidate
        reference = candidate

    if isinstance(reference, str):
        slug = reference.strip()
        if not slug:
            return None
        slug = slug.strip("/")
        last_segment = slug.split("/")[-1]
        last_segment = last_segment.split("?")[0]
        try:
            return int(last_segment)
        except ValueError:
            return None

    return None


def fetch_rentman_plan(project_code: str, project_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Recupera da Rentman le funzioni equiparate alle attività e il relativo crew."""

    client = get_rentman_client()
    if not client:
        return None

    app.logger.warning("Rentman: ricerca progetto per codice '%s' (data: %s)", project_code, project_date)

    try:
        project = client.find_project(project_code)
        app.logger.info(
            "Rentman: payload progetto=\n%s",
            json.dumps(project, ensure_ascii=False, indent=2) if project else "{}",
        )
    except RentmanNotFound:
        app.logger.warning("Rentman: progetto %s non trovato", project_code)
        return None
    except RentmanAuthError as exc:
        app.logger.error("Rentman: autenticazione fallita (%s)", exc)
        return None
    except RentmanAPIError as exc:
        app.logger.error("Rentman: errore recuperando il progetto %s: %s", project_code, exc)
        return None

    if not project:
        app.logger.warning(
            "Rentman: nessun progetto associato al codice %s (number/reference)",
            project_code,
        )
        return None

    project_id = parse_reference(project.get("id")) or project.get("id")
    if not isinstance(project_id, int):
        app.logger.error("Rentman: progetto %s senza id valido", project_code)
        return None

    try:
        subprojects = client.get_project_subprojects(project_id)
        app.logger.info(
            "Rentman: payload subprojects=\n%s",
            json.dumps(subprojects, ensure_ascii=False, indent=2),
        )
    except RentmanNotFound:
        subprojects = []
    except RentmanAPIError as exc:
        app.logger.error(
            "Rentman: errore leggendo i subprojects del progetto %s: %s",
            project_code,
            exc,
        )
        subprojects = []

    allowed_subprojects: Set[int] = set()
    subproject_labels: Dict[int, str] = {}
    for sub in subprojects:
        sub_id = parse_reference(sub.get("id")) or sub.get("id")
        if not isinstance(sub_id, int):
            continue

        label = (
            sub.get("displayname")
            or sub.get("name")
            or sub.get("description")
            or str(sub_id)
        )
        subproject_labels[sub_id] = str(label)

        if _is_truthy(sub.get("is_planning")):
            allowed_subprojects.add(sub_id)

    app.logger.info("Rentman: subprojects pianificati=%s", sorted(allowed_subprojects))
    filter_subprojects = bool(allowed_subprojects)
    if subprojects and not allowed_subprojects:
        app.logger.warning(
            "Rentman: subprojects presenti ma nessuno con is_planning=true per %s",
            project_code,
        )

    try:
        functions = client.get_project_functions(project_id)
        app.logger.info(
            "Rentman: payload funzioni=\n%s",
            json.dumps(functions, ensure_ascii=False, indent=2),
        )
    except RentmanNotFound:
        functions = []
    except RentmanAPIError as exc:
        app.logger.error(
            "Rentman: errore leggendo le funzioni del progetto %s: %s",
            project_code,
            exc,
        )
        functions = []

    filtered_functions: List[Dict[str, Any]] = []
    function_ids: List[int] = []
    seen_function_ids: Set[int] = set()
    for entry in functions:
        func_id = parse_reference(entry.get("id")) or entry.get("id")
        if not isinstance(func_id, int):
            continue
        subproject_id = parse_reference(entry.get("subproject")) or entry.get("subproject")
        if filter_subprojects:
            if not isinstance(subproject_id, int):
                continue
            if subproject_id not in allowed_subprojects:
                continue
        if func_id in seen_function_ids:
            continue
        filtered_functions.append(entry)
        function_ids.append(func_id)
        seen_function_ids.add(func_id)

    crew_assignments: List[Dict[str, Any]] = []
    if function_ids:
        try:
            crew_assignments = client.get_project_crew_by_function_ids(function_ids)
            app.logger.info(
                "Rentman: payload crew assignments=\n%s",
                json.dumps(crew_assignments, ensure_ascii=False, indent=2),
            )
        except RentmanNotFound:
            crew_assignments = []
        except RentmanAPIError as exc:
            app.logger.error(
                "Rentman: errore leggendo il planning crew del progetto %s: %s",
                project_code,
                exc,
            )
            crew_assignments = []

    activity_lookup: Dict[int, str] = {}
    activities: List[Dict[str, Any]] = []
    for entry in filtered_functions:
        func_id = parse_reference(entry.get("id")) or entry.get("id")
        if not isinstance(func_id, int) or func_id in activity_lookup:
            continue

        subproject_ref = entry.get("subproject")
        subproject_id = parse_reference(subproject_ref)
        subproject_label: Optional[str] = None
        if isinstance(subproject_id, int):
            subproject_label = subproject_labels.get(subproject_id) or str(subproject_id)
        elif isinstance(subproject_ref, str):
            if subproject_ref.startswith("/subprojects/"):
                subproject_label = subproject_ref.split("/")[-1]
            else:
                subproject_label = subproject_ref

        normalized_subproject = (
            str(subproject_label).strip().lower() if subproject_label else ""
        )
        if "produzione" not in normalized_subproject:
            app.logger.debug(
                "Rentman: funzione %s esclusa per sottoprogetto '%s'",
                func_id,
                subproject_label,
            )
            continue

        plan_start = (
            _normalize_datetime(entry.get("planperiod_start"))
            or _normalize_datetime(entry.get("usageperiod_start"))
        )
        plan_end = (
            _normalize_datetime(entry.get("planperiod_end"))
            or _normalize_datetime(entry.get("usageperiod_end"))
        )

        if project_date:
            if not _activity_matches_date(plan_start, plan_end, project_date):
                app.logger.debug(
                    "Rentman: funzione %s esclusa per data '%s' (start: %s, end: %s)",
                    func_id,
                    project_date,
                    plan_start,
                    plan_end,
                )
                continue

        activity_id = f"rentman-f-{func_id}"
        label = (
            entry.get("name")
            or entry.get("displayname")
            or entry.get("description")
            or f"Funzione {func_id}"
        )

        label = f"{label} [ID {func_id}]"

        activities.append(
            {
                "id": activity_id,
                "label": str(label),
                "plan_start": plan_start,
                "plan_end": plan_end,
            }
        )
        activity_lookup[func_id] = activity_id

    activities.sort(key=lambda item: item["label"].lower())
    app.logger.info(
        "Rentman: funzioni considerate=%s",
        json.dumps(activities, ensure_ascii=False, indent=2),
    )

    valid_function_ids: Set[int] = set(activity_lookup)
    crew_ids: Set[int] = set()
    for assignment in crew_assignments:
        member_id = parse_reference(assignment.get("crewmember"))
        function_id = parse_reference(assignment.get("function"))
        if not isinstance(function_id, int) or function_id not in valid_function_ids:
            continue
        if isinstance(member_id, int):
            crew_ids.add(member_id)

    crew_details: List[Dict[str, Any]] = []
    if crew_ids:
        try:
            crew_details = client.get_crew_members_by_ids(crew_ids)
            app.logger.info(
                "Rentman: payload crew details=\n%s",
                json.dumps(crew_details, ensure_ascii=False, indent=2),
            )
        except RentmanNotFound:
            crew_details = []
        except RentmanAPIError as exc:
            app.logger.error(
                "Rentman: errore leggendo i membri crew del progetto %s: %s",
                project_code,
                exc,
            )
            crew_details = []

    crew_map: Dict[int, Dict[str, Any]] = {}
    for member in crew_details:
        member_id = parse_reference(member.get("id")) or member.get("id")
        if isinstance(member_id, int):
            crew_map[member_id] = member

    team: List[Dict[str, Any]] = []
    seen_members: Set[str] = set()
    for assignment in crew_assignments:
        assignment_id = assignment.get("id")
        member_id = parse_reference(assignment.get("crewmember"))
        function_id = parse_reference(assignment.get("function"))

        if (
            not isinstance(assignment_id, int)
            or member_id is None
            or function_id is None
            or function_id not in valid_function_ids
        ):
            continue

        activity_id = activity_lookup.get(function_id)
        if not activity_id:
            continue

        crew_info = crew_map.get(member_id, {})
        display_name = (
            crew_info.get("displayname")
            or crew_info.get("name")
            or assignment.get("displayname")
            or assignment.get("name")
            or "Operatore"
        )

        member_key = f"rentman-crew-{assignment_id}"
        if member_key in seen_members:
            continue
        seen_members.add(member_key)

        team.append(
            {
                "key": member_key,
                "name": str(display_name),
                "activity_id": activity_id,
            }
        )

    project_name = (
        project.get("name")
        or project.get("displayname")
        or project.get("description")
        or project_code
    )

    plan = {
        "project_code": str(project.get("number") or project_code),
        "project_name": str(project_name),
        "activities": activities,
        "team": team,
    }

    return plan


def now_ms() -> int:
    return int(time.time() * 1000)


def init_db() -> None:
    db = sqlite3.connect(DATABASE)
    try:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS activities (
                activity_id TEXT PRIMARY KEY,
                label TEXT NOT NULL,
                sort_order INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS member_state (
                member_key TEXT PRIMARY KEY,
                member_name TEXT NOT NULL,
                activity_id TEXT,
                running INTEGER NOT NULL DEFAULT 0,
                start_ts INTEGER,
                elapsed_cached INTEGER NOT NULL DEFAULT 0,
                pause_start INTEGER,
                FOREIGN KEY(activity_id) REFERENCES activities(activity_id)
            );

            CREATE TABLE IF NOT EXISTS event_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                kind TEXT NOT NULL,
                member_key TEXT,
                details TEXT
            );

            CREATE TABLE IF NOT EXISTS app_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        purge_legacy_seed(db)
        db.commit()
    finally:
        db.close()


def purge_legacy_seed(db: sqlite3.Connection) -> None:
    try:
        row = db.execute(
            "SELECT value FROM app_state WHERE key='project_code'"
        ).fetchone()
    except sqlite3.OperationalError:
        return

    if not row:
        return

    project_code = row[0]
    if project_code != DEMO_PROJECT_CODE:
        return

    db.execute("DELETE FROM activities")
    db.execute("DELETE FROM member_state")
    db.execute("DELETE FROM event_log")
    db.execute("DELETE FROM app_state WHERE key IN ('project_code','project_name')")


def mock_fetch_project(project_code: str, project_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
    code = (project_code or "").strip().upper()
    if not code:
        return None
    plan = fetch_rentman_plan(code, project_date)
    if plan:
        app.logger.warning("Rentman: piano caricato da API per %s (data: %s)", code, project_date)
        return plan
    external = load_external_projects().get(code)
    plan = external or MOCK_PROJECTS.get(code)
    if plan is None:
        app.logger.warning("Rentman: nessun piano disponibile per %s", code)
        return None
    app.logger.warning("Rentman: uso piano locale per %s", code)
    result = deepcopy(plan)
    result["project_code"] = code
    return result


def set_app_state(db: sqlite3.Connection, key: str, value: str) -> None:
    db.execute(
        """
        INSERT INTO app_state(key, value) VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (key, value),
    )


def get_app_state(db: sqlite3.Connection, key: str) -> Optional[str]:
    row = db.execute("SELECT value FROM app_state WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


def clear_project_state(db: sqlite3.Connection) -> None:
    """Rimuove il progetto attivo e i relativi dati dal database."""

    db.execute("DELETE FROM activities")
    db.execute("DELETE FROM member_state")
    db.execute("DELETE FROM event_log")
    db.execute(
        "DELETE FROM app_state WHERE key IN ('project_code','project_name','activity_plan_meta')"
    )


def apply_project_plan(db: sqlite3.Connection, plan: Dict[str, Any]) -> None:
    clear_project_state(db)

    activities = list(plan.get("activities") or [])
    team = list(plan.get("team") or [])
    project_code = str(plan.get("project_code") or "UNKNOWN")
    project_name = str(plan.get("project_name") or project_code)

    db.executemany(
        "INSERT INTO activities(activity_id, label, sort_order) VALUES(?,?,?)",
        [
            (activity["id"], activity["label"], index)
            for index, activity in enumerate(activities, start=1)
        ],
    )

    now = now_ms()
    member_rows: List[tuple] = []
    seen_keys = set()
    for member in team:
        raw_key = (member.get("key") or "").strip()
        name = (member.get("name") or raw_key or "Operatore").strip()
        key = raw_key or name.lower().replace(" ", "-")
        while key in seen_keys:
            key = f"{key}-dup"
        seen_keys.add(key)
        activity_id = (member.get("activity_id") or "").strip() or None
        # Non avviare automaticamente il timer, il capo squadra lo avvierà manualmente
        running = 0
        start_ts = None
        member_rows.append(
            (key, name, activity_id, running, start_ts, 0, None)
        )

    if member_rows:
        db.executemany(
            """
            INSERT INTO member_state(
                member_key, member_name, activity_id, running, start_ts, elapsed_cached, pause_start
            ) VALUES(?,?,?,?,?,?,?)
            """,
            member_rows,
        )

    activity_meta = {
        activity["id"]: {
            "plan_start": activity.get("plan_start"),
            "plan_end": activity.get("plan_end"),
        }
        for activity in activities
        if activity.get("id")
    }

    set_app_state(db, "project_code", project_code)
    set_app_state(db, "project_name", project_name)
    set_app_state(db, "activity_plan_meta", json.dumps(activity_meta))

    db.execute(
        "INSERT INTO event_log(ts, kind, details) VALUES(?,?,?)",
        (
            now,
            "project_load",
            json.dumps({"project_code": project_code, "project_name": project_name}),
        ),
    )


def seed_demo_data(db: sqlite3.Connection) -> None:
    plan = mock_fetch_project(DEMO_PROJECT_CODE, None)
    if plan is None:
        raise RuntimeError("Demo project configuration missing")
    apply_project_plan(db, plan)


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        conn = sqlite3.connect(DATABASE)
        conn.row_factory = sqlite3.Row
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(_: BaseException | None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def compute_elapsed(row: sqlite3.Row, reference: int) -> int:
    elapsed = row["elapsed_cached"] or 0
    if row["running"]:
        start_ts = row["start_ts"] or reference
        elapsed += max(0, reference - start_ts)
    return elapsed


def fetch_member(db: sqlite3.Connection, member_key: str) -> Optional[sqlite3.Row]:
    if not member_key:
        return None
    return db.execute(
        "SELECT * FROM member_state WHERE member_key=?",
        (member_key,),
    ).fetchone()


def format_duration_ms(ms: Any) -> Optional[str]:
    if not isinstance(ms, (int, float)):
        return None
    total_seconds = max(0, int(ms // 1000))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours:02}:{minutes:02}:{seconds:02}"
    return f"{minutes:02}:{seconds:02}"


def describe_event(kind: str, details: Dict[str, Any], activity_labels: Dict[str, str]) -> str:
    def label_for(activity_id: Optional[str]) -> str:
        if not activity_id:
            return "Disponibili"
        return activity_labels.get(activity_id, activity_id)

    if kind == "move":
        member_name = details.get("member_name") or "Operatore"
        origin = label_for(details.get("from"))
        destination = label_for(details.get("to"))
        summary = f"{member_name}: {origin} → {destination}"
        duration = format_duration_ms(details.get("duration_ms"))
        if duration:
            summary += f" · {duration}"
        return summary

    if kind == "project_load":
        code = details.get("project_code") or "--"
        name = details.get("project_name") or ""
        if name:
            return f"Progetto {code} · {name}"
        return f"Progetto {code} attivato"

    if kind == "pause_all":
        affected = details.get("affected") or 0
        return f"Pausa collettiva ({affected} operatori)"

    if kind == "resume_all":
        affected = details.get("affected") or 0
        return f"Ripresa collettiva ({affected} operatori)"

    if kind == "pause_member":
        member_name = details.get("member_name") or "Operatore"
        activity_label = label_for(details.get("activity_id"))
        duration = format_duration_ms(details.get("duration_ms"))
        summary = f"{member_name}: Pausa {activity_label}"
        if duration:
            summary += f" · {duration}"
        return summary

    if kind == "resume_member":
        member_name = details.get("member_name") or "Operatore"
        activity_label = label_for(details.get("activity_id"))
        return f"{member_name}: Ripresa {activity_label}"

    if kind == "finish_activity":
        member_name = details.get("member_name") or "Operatore"
        activity_label = label_for(details.get("activity_id"))
        summary = f"{member_name}: Fine {activity_label}"
        duration = format_duration_ms(details.get("duration_ms"))
        if duration:
            summary += f" · {duration}"
        return summary

    if kind == "finish_all":
        affected = details.get("affected") or 0
        return f"Fine attività collettiva ({affected} operatori)"

    return kind.replace("_", " ").title()


@app.route("/")
def home() -> str:
    return render_template("index.html")


@app.get("/api/activities")
def api_activities():
    db = get_db()
    rows = db.execute(
        "SELECT activity_id, label FROM activities ORDER BY sort_order, label"
    ).fetchall()
    return jsonify({"activities": [dict(row) for row in rows]})


@app.get("/api/state")
def api_state():
    db = get_db()
    now = now_ms()

    project_code = get_app_state(db, "project_code")
    project_name = get_app_state(db, "project_name")

    if not project_code:
        return jsonify(
            {
                "team": [],
                "activities": [],
                "allPaused": True,
                "timestamp": now,
                "project": None,
            }
        )

    activity_rows = db.execute(
        "SELECT activity_id, label FROM activities ORDER BY sort_order, label"
    ).fetchall()

    meta_raw = get_app_state(db, "activity_plan_meta")
    if meta_raw:
        try:
            activity_meta = json.loads(meta_raw)
        except json.JSONDecodeError:
            activity_meta = {}
    else:
        activity_meta = {}

    activity_map: Dict[str, Dict[str, Any]] = {
        row["activity_id"]: {
            "activity_id": row["activity_id"],
            "label": row["label"],
            "members": [],
            "plan_start": (activity_meta.get(row["activity_id"]) or {}).get("plan_start"),
            "plan_end": (activity_meta.get(row["activity_id"]) or {}).get("plan_end"),
        }
        for row in activity_rows
    }

    members = db.execute(
        "SELECT * FROM member_state ORDER BY member_name"
    ).fetchall()

    team: List[Dict[str, Any]] = []
    active_members: List[Dict[str, Any]] = []
    paused_keys = {
        row["member_key"]
        for row in db.execute(
            "SELECT member_key FROM member_state WHERE running=0 AND pause_start IS NOT NULL"
        ).fetchall()
    }
    for row in members:
        member = {
            "member_key": row["member_key"],
            "member_name": row["member_name"],
            "activity_id": row["activity_id"],
            "running": bool(row["running"]),
            "elapsed": compute_elapsed(row, now),
            "paused": row["member_key"] in paused_keys,
        }
        if row["activity_id"] and row["activity_id"] in activity_map:
            activity_map[row["activity_id"]]["members"].append(member)
            active_members.append(member)
        else:
            team.append(member)

    for activity in activity_map.values():
        activity["members"].sort(key=lambda m: m["member_name"])

    all_paused = not any(m["running"] for m in team + active_members)
    project_info = {
        "code": project_code,
        "name": project_name or project_code,
    }

    return jsonify(
        {
            "team": team,
            "activities": list(activity_map.values()),
            "allPaused": all_paused,
            "timestamp": now,
            "project": project_info,
        }
    )


@app.get("/api/events")
def api_events():
    db = get_db()
    project_code = get_app_state(db, "project_code")
    if not project_code:
        return jsonify({"events": []})

    activity_labels = {
        row["activity_id"]: row["label"]
        for row in db.execute("SELECT activity_id, label FROM activities")
    }

    rows = db.execute(
        "SELECT id, ts, kind, member_key, details FROM event_log ORDER BY ts DESC LIMIT 25"
    ).fetchall()

    events: List[Dict[str, Any]] = []
    for row in rows:
        details_raw = row["details"]
        details: Dict[str, Any]
        if details_raw:
            try:
                details = json.loads(details_raw)
            except json.JSONDecodeError:
                details = {}
        else:
            details = {}

        summary = describe_event(row["kind"], details, activity_labels)
        events.append(
            {
                "id": row["id"],
                "timestamp": row["ts"],
                "kind": row["kind"],
                "summary": summary,
            }
        )

    return jsonify({"events": events})


@app.post("/api/load_project")
def api_load_project():
    data = request.get_json(silent=True) or {}
    project_code = (data.get("project_code") or "").strip().upper()
    project_date = (data.get("project_date") or "").strip()
    if not project_code:
        return jsonify({"ok": False, "error": "missing_project_code"}), 400
    if not project_date:
        return jsonify({"ok": False, "error": "missing_project_date"}), 400

    plan = mock_fetch_project(project_code, project_date)
    db = get_db()
    if plan is None:
        clear_project_state(db)
        db.commit()
        return jsonify({"ok": False, "error": "project_not_found"}), 404

    apply_project_plan(db, plan)
    db.commit()

    return jsonify(
        {
            "ok": True,
            "project": {
                "code": plan["project_code"],
                "name": plan.get("project_name"),
            },
        }
    )


@app.post("/api/move")
def api_move():
    data = request.get_json(silent=True) or {}
    member_key = (data.get("member_key") or "").strip()
    member_name = (data.get("member_name") or "").strip()
    activity_id = data.get("activity_id")

    if isinstance(activity_id, str):
        activity_id = activity_id.strip()
    if activity_id == "":
        activity_id = None

    if not member_key or not member_name:
        return jsonify({"ok": False, "error": "invalid_payload"}), 400

    db = get_db()
    now = now_ms()

    if activity_id:
        exists = db.execute(
            "SELECT 1 FROM activities WHERE activity_id=?",
            (activity_id,),
        ).fetchone()
        if exists is None:
            return jsonify({"ok": False, "error": "unknown_activity"}), 400

    existing = db.execute(
        "SELECT * FROM member_state WHERE member_key=?",
        (member_key,),
    ).fetchone()

    if existing is None:
        db.execute(
            """
            INSERT INTO member_state(
                member_key, member_name, activity_id, running, start_ts, elapsed_cached, pause_start
            ) VALUES(?,?,?,?,?,?,?)
            """,
            (member_key, member_name, None, 0, None, 0, None),
        )
        existing = db.execute(
            "SELECT * FROM member_state WHERE member_key=?",
            (member_key,),
        ).fetchone()
    else:
        db.execute(
            "UPDATE member_state SET member_name=? WHERE member_key=?",
            (member_name, member_key),
        )

    previous_activity = existing["activity_id"]
    prev_elapsed = compute_elapsed(existing, now)

    running = 1 if activity_id else 0
    start_ts = now if running else None

    db.execute(
        """
        UPDATE member_state
        SET activity_id=?, running=?, start_ts=?, elapsed_cached=?, pause_start=NULL
        WHERE member_key=?
        """,
        (activity_id, running, start_ts, 0, member_key),
    )

    move_details = {
        "from": previous_activity,
        "to": activity_id,
        "member_name": member_name,
        "duration_ms": prev_elapsed,
    }
    db.execute(
        "INSERT INTO event_log(ts, kind, member_key, details) VALUES(?,?,?,?)",
        (now, "move", member_key, json.dumps(move_details)),
    )

    db.commit()
    return jsonify({"ok": True})


@app.post("/api/start_activity")
def api_start_activity():
    """Avvia i timer per tutti i membri di una specifica attività."""
    data = request.get_json(silent=True) or {}
    activity_id = (data.get("activity_id") or "").strip()
    
    if not activity_id:
        return jsonify({"ok": False, "error": "missing_activity_id"}), 400
    
    now = now_ms()
    db = get_db()

    # Verifica che l'attività esista
    activity_exists = db.execute(
        "SELECT 1 FROM activities WHERE activity_id=?",
        (activity_id,),
    ).fetchone()
    
    if not activity_exists:
        return jsonify({"ok": False, "error": "activity_not_found"}), 404

    # Trova tutti i membri assegnati a questa attività con timer non avviato
    rows = db.execute(
        "SELECT member_key FROM member_state WHERE activity_id=? AND running=0",
        (activity_id,),
    ).fetchall()

    if not rows:
        return jsonify({"ok": True, "affected": 0})

    affected = 0
    for row in rows:
        db.execute(
            "UPDATE member_state SET running=1, start_ts=?, pause_start=NULL WHERE member_key=?",
            (now, row["member_key"]),
        )
        affected += 1

    db.execute(
        "INSERT INTO event_log(ts, kind, details) VALUES(?,?,?)",
        (now, "start_activity", json.dumps({"activity_id": activity_id, "affected": affected})),
    )

    db.commit()
    return jsonify({"ok": True, "affected": affected})


@app.post("/api/start_member")
def api_start_member():
    """Avvia il timer per un singolo membro."""
    data = request.get_json(silent=True) or {}
    member_key = (data.get("member_key") or "").strip()
    
    if not member_key:
        return jsonify({"ok": False, "error": "missing_member_key"}), 400
    
    now = now_ms()
    db = get_db()

    # Verifica che il membro esista e abbia un'attività assegnata
    member = db.execute(
        "SELECT member_key, activity_id, running FROM member_state WHERE member_key=?",
        (member_key,),
    ).fetchone()
    
    if not member:
        return jsonify({"ok": False, "error": "member_not_found"}), 404
    
    if not member["activity_id"]:
        return jsonify({"ok": False, "error": "no_activity_assigned"}), 400
    
    if member["running"]:
        return jsonify({"ok": False, "error": "already_running"}), 400

    # Avvia il timer
    db.execute(
        "UPDATE member_state SET running=1, start_ts=?, pause_start=NULL WHERE member_key=?",
        (now, member_key),
    )

    db.execute(
        "INSERT INTO event_log(ts, kind, details) VALUES(?,?,?)",
        (now, "start_member", json.dumps({"member_key": member_key})),
    )

    db.commit()
    return jsonify({"ok": True})


@app.post("/api/start_all")
def api_start_all():
    """Avvia i timer per tutti i membri che hanno un'attività assegnata."""
    now = now_ms()
    db = get_db()

    # Trova tutti i membri con activity_id assegnato ma non in esecuzione
    rows = db.execute(
        "SELECT member_key FROM member_state WHERE activity_id IS NOT NULL AND running=0"
    ).fetchall()

    if not rows:
        return jsonify({"ok": True, "affected": 0})

    affected = 0
    for row in rows:
        db.execute(
            "UPDATE member_state SET running=1, start_ts=?, pause_start=NULL WHERE member_key=?",
            (now, row["member_key"]),
        )
        affected += 1

    db.execute(
        "INSERT INTO event_log(ts, kind, details) VALUES(?,?,?)",
        (now, "start_all", json.dumps({"affected": affected})),
    )

    db.commit()
    return jsonify({"ok": True, "affected": affected})


@app.post("/api/pause_all")
def api_pause_all():
    now = now_ms()
    db = get_db()

    rows = db.execute(
        "SELECT member_key, start_ts, elapsed_cached FROM member_state WHERE running=1"
    ).fetchall()

    for row in rows:
        start_ts = row["start_ts"] or now
        elapsed = (row["elapsed_cached"] or 0) + max(0, now - start_ts)
        db.execute(
            """
            UPDATE member_state
            SET running=0, start_ts=NULL, elapsed_cached=?, pause_start=?
            WHERE member_key=?
            """,
            (elapsed, now, row["member_key"]),
        )

    if rows:
        db.execute(
            "INSERT INTO event_log(ts, kind, details) VALUES(?,?,?)",
            (now, "pause_all", json.dumps({"affected": len(rows)})),
        )

    db.commit()
    return jsonify({"ok": True})


@app.post("/api/resume_all")
def api_resume_all():
    now = now_ms()
    db = get_db()

    rows = db.execute(
        "SELECT member_key FROM member_state WHERE running=0 AND pause_start IS NOT NULL"
    ).fetchall()

    for row in rows:
        db.execute(
            "UPDATE member_state SET running=1, start_ts=?, pause_start=NULL WHERE member_key=?",
            (now, row["member_key"]),
        )

    if rows:
        db.execute(
            "INSERT INTO event_log(ts, kind, details) VALUES(?,?,?)",
            (now, "resume_all", json.dumps({"affected": len(rows)})),
        )

    db.commit()
    return jsonify({"ok": True})


@app.post("/api/finish_all")
def api_finish_all():
    now = now_ms()
    db = get_db()

    rows = db.execute(
        "SELECT * FROM member_state WHERE activity_id IS NOT NULL"
    ).fetchall()

    affected = 0
    for row in rows:
        elapsed = compute_elapsed(row, now)
        db.execute(
            """
            UPDATE member_state
            SET activity_id=NULL, running=0, start_ts=NULL, elapsed_cached=0, pause_start=NULL
            WHERE member_key=?
            """,
            (row["member_key"],),
        )
        db.execute(
            "INSERT INTO event_log(ts, kind, member_key, details) VALUES(?,?,?,?)",
            (
                now,
                "finish_activity",
                row["member_key"],
                json.dumps(
                    {
                        "member_name": row["member_name"],
                        "activity_id": row["activity_id"],
                        "duration_ms": elapsed,
                    }
                ),
            ),
        )
        affected += 1

    if affected:
        db.execute(
            "INSERT INTO event_log(ts, kind, details) VALUES(?,?,?)",
            (
                now,
                "finish_all",
                json.dumps({"affected": affected}),
            ),
        )

    db.commit()
    return jsonify({"ok": True, "affected": affected})


@app.post("/api/member/pause")
def api_member_pause():
    data = request.get_json(silent=True) or {}
    member_key = (data.get("member_key") or "").strip()
    if not member_key:
        return jsonify({"ok": False, "error": "missing_member_key"}), 400

    db = get_db()
    member = fetch_member(db, member_key)
    if member is None:
        return jsonify({"ok": False, "error": "member_not_found"}), 404

    if not member["activity_id"]:
        return jsonify({"ok": False, "error": "member_not_assigned"}), 400

    if member["pause_start"] is not None:
        return jsonify({"ok": True, "already_paused": True})

    if not member["running"]:
        return jsonify({"ok": False, "error": "member_not_running"}), 400

    now = now_ms()
    elapsed = compute_elapsed(member, now)

    db.execute(
        """
        UPDATE member_state
        SET running=0, start_ts=NULL, elapsed_cached=?, pause_start=?
        WHERE member_key=?
        """,
        (elapsed, now, member_key),
    )

    db.execute(
        "INSERT INTO event_log(ts, kind, member_key, details) VALUES(?,?,?,?)",
        (
            now,
            "pause_member",
            member_key,
            json.dumps(
                {
                    "member_name": member["member_name"],
                    "activity_id": member["activity_id"],
                    "duration_ms": elapsed,
                }
            ),
        ),
    )

    db.commit()
    return jsonify({"ok": True})


@app.post("/api/member/resume")
def api_member_resume():
    data = request.get_json(silent=True) or {}
    member_key = (data.get("member_key") or "").strip()
    if not member_key:
        return jsonify({"ok": False, "error": "missing_member_key"}), 400

    db = get_db()
    member = fetch_member(db, member_key)
    if member is None:
        return jsonify({"ok": False, "error": "member_not_found"}), 404

    if not member["activity_id"]:
        return jsonify({"ok": False, "error": "member_not_assigned"}), 400

    if member["running"]:
        return jsonify({"ok": True, "already_running": True})

    if member["pause_start"] is None:
        return jsonify({"ok": False, "error": "member_not_paused"}), 400

    now = now_ms()

    db.execute(
        "UPDATE member_state SET running=1, start_ts=?, pause_start=NULL WHERE member_key=?",
        (now, member_key),
    )

    db.execute(
        "INSERT INTO event_log(ts, kind, member_key, details) VALUES(?,?,?,?)",
        (
            now,
            "resume_member",
            member_key,
            json.dumps(
                {
                    "member_name": member["member_name"],
                    "activity_id": member["activity_id"],
                }
            ),
        ),
    )

    db.commit()
    return jsonify({"ok": True})


@app.post("/api/member/finish")
def api_member_finish():
    data = request.get_json(silent=True) or {}
    member_key = (data.get("member_key") or "").strip()
    if not member_key:
        return jsonify({"ok": False, "error": "missing_member_key"}), 400

    db = get_db()
    member = fetch_member(db, member_key)
    if member is None:
        return jsonify({"ok": False, "error": "member_not_found"}), 404

    if not member["activity_id"]:
        return jsonify({"ok": False, "error": "member_not_assigned"}), 400

    now = now_ms()
    elapsed = compute_elapsed(member, now)

    db.execute(
        """
        UPDATE member_state
        SET activity_id=NULL, running=0, start_ts=NULL, elapsed_cached=0, pause_start=NULL
        WHERE member_key=?
        """,
        (member_key,),
    )

    db.execute(
        "INSERT INTO event_log(ts, kind, member_key, details) VALUES(?,?,?,?)",
        (
            now,
            "finish_activity",
            member_key,
            json.dumps(
                {
                    "member_name": member["member_name"],
                    "activity_id": member["activity_id"],
                    "duration_ms": elapsed,
                }
            ),
        ),
    )

    db.commit()
    return jsonify({"ok": True})


@app.post("/api/_reset")
def api_reset():
    db = get_db()
    seed_demo_data(db)
    db.commit()
    return jsonify({"ok": True})


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
