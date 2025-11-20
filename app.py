from __future__ import annotations

import atexit
import base64
import csv
import hashlib
import io
import json
import logging
import os
import secrets
import sqlite3
import time
from copy import deepcopy
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from threading import Event, Thread
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, TypeAlias, cast

try:
    import pymysql  # type: ignore[import]
    from pymysql import err as pymysql_err  # type: ignore[import]
    from pymysql.cursors import DictCursor  # type: ignore[import]
except ImportError:  # pragma: no cover - fallback when MySQL client not installed
    pymysql = None
    pymysql_err = None
    DictCursor = None

from flask import Flask, g, jsonify, redirect, render_template, request, send_file, session, url_for
from flask.typing import ResponseReturnValue
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet
from pywebpush import WebPushException, webpush
from rentman_client import (
    RentmanAPIError,
    RentmanAuthError,
    RentmanClient,
    RentmanNotFound,
)


app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', secrets.token_hex(32))
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['PERMANENT_SESSION_LIFETIME'] = 86400  # 24 hours

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
USERS_FILE = Path(__file__).with_name("users.json")
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


@app.get("/sw.js")
def service_worker() -> ResponseReturnValue:
    response = app.send_static_file("sw.js")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response


class RowMapping(dict):
    """Row helper that mimics sqlite3.Row access for dict-based cursors."""

    __slots__ = ("_ordered",)

    def __init__(self, data: Dict[str, Any], columns: Sequence[str]):
        super().__init__(data)
        self._ordered = [data.get(col) for col in columns]

    def __getitem__(self, key: Any) -> Any:  # type: ignore[override]
        if isinstance(key, int):
            return self._ordered[key]
        return super().__getitem__(key)


class CursorWrapper:
    """Minimal cursor wrapper to align PyMySQL cursor behaviour with sqlite3."""

    __slots__ = ("_cursor", "_columns", "_closed")

    def __init__(self, cursor):
        self._cursor = cursor
        self._columns = [col[0] for col in cursor.description] if cursor.description else []
        self._closed = False

    @property
    def description(self):
        return self._cursor.description

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        if isinstance(row, dict):
            return RowMapping(row, self._columns)
        return row

    def fetchall(self):
        rows = self._cursor.fetchall()
        try:
            if rows and isinstance(rows[0], dict):
                mapped = [RowMapping(row, self._columns) for row in rows]
            else:
                mapped = rows
        finally:
            self.close()
        return mapped

    def __iter__(self):
        for row in self._cursor:
            if isinstance(row, dict):
                yield RowMapping(row, self._columns)
            else:
                yield row
        self.close()

    def close(self):
        if not self._closed:
            try:
                self._cursor.close()
            finally:
                self._closed = True

    def __del__(self):  # pragma: no cover - best effort cleanup
        self.close()


class MySQLConnection:
    """Adapter to expose a sqlite-like interface backed by PyMySQL."""

    def __init__(self, settings: Dict[str, Any]):
        if pymysql is None or DictCursor is None:
            raise RuntimeError(
                "PyMySQL non è installato. Esegui 'pip install PyMySQL' per usare il backend MySQL."
            )
        self._settings = settings
        self._conn = self._connect_with_autocreate()

    # Internal helpers -------------------------------------------------
    def _base_connect(self, include_db: bool = True):
        kwargs = {
            "host": self._settings["host"],
            "port": int(self._settings.get("port", 3306)),
            "user": self._settings["user"],
            "password": self._settings["password"],
            "charset": "utf8mb4",
            "cursorclass": DictCursor,
            "autocommit": False,
        }
        if include_db:
            kwargs["database"] = self._settings["name"]
        return pymysql.connect(**kwargs)  # type: ignore[union-attr]

    def _connect_with_autocreate(self):
        try:
            return self._base_connect(include_db=True)
        except Exception as exc:  # pragma: no cover - MySQL specific bootstrap
            if pymysql_err is not None and isinstance(exc, pymysql_err.OperationalError):
                err_code = exc.args[0] if exc.args else None
                if err_code == 1049:  # Unknown database
                    bootstrap = self._base_connect(include_db=False)
                    try:
                        with bootstrap.cursor() as cursor:
                            cursor.execute(
                                f"CREATE DATABASE IF NOT EXISTS `{self._settings['name']}` "
                                "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                            )
                        bootstrap.commit()
                    finally:
                        bootstrap.close()
                    return self._base_connect(include_db=True)
            raise

    @staticmethod
    def _prepare_sql(sql: str) -> str:
        if "%s" in sql or "%(" in sql:
            return sql
        return sql.replace("?", "%s")

    @staticmethod
    def _prepare_params(params: Any) -> tuple:
        if params is None:
            return ()
        if isinstance(params, (list, tuple)):
            return tuple(params)
        return (params,)

    # Public API -------------------------------------------------------
    def execute(self, sql: str, params: Any = None) -> CursorWrapper:
        cursor = self._conn.cursor()
        cursor.execute(self._prepare_sql(sql), self._prepare_params(params))
        return CursorWrapper(cursor)

    def executemany(self, sql: str, seq_of_params: Iterable[Iterable[Any]]) -> CursorWrapper:
        cursor = self._conn.cursor()
        prepared_sql = self._prepare_sql(sql)
        prepared_params = [self._prepare_params(params) for params in seq_of_params]
        if prepared_params:
            cursor.executemany(prepared_sql, prepared_params)
        return CursorWrapper(cursor)

    def executescript(self, script: str) -> None:
        for statement in script.split(";"):
            stmt = statement.strip()
            if stmt:
                self.execute(stmt)

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()


DatabaseLike: TypeAlias = sqlite3.Connection | MySQLConnection

_RENTMAN_CLIENT: Optional[RentmanClient] = None
_RENTMAN_CLIENT_TOKEN: Optional[str] = None
_CONFIG_CACHE: Optional[Dict[str, Any]] = None
_CONFIG_CACHE_MTIME: Optional[float] = None
_WEBPUSH_SETTINGS: Optional[Dict[str, Optional[str]]] = None
_NOTIFICATION_THREAD: Optional[Thread] = None
_NOTIFICATION_STOP: Optional[Event] = None

NOTIFICATION_INTERVAL_SECONDS = int(os.environ.get("JOBLOG_NOTIFICATION_INTERVAL", "60"))
ACTIVITY_OVERDUE_GRACE_MS = 10 * 60 * 1000  # 10 minuti di ritardo tollerato
PUSH_NOTIFIED_STATE_KEY = "push_notified_activities"
LONG_RUNNING_STATE_KEY = "long_running_member_notifications"
LONG_RUNNING_THRESHOLD_MS = 2 * 60 * 1000  # 2 minuti
OVERDUE_PUSH_TTL_SECONDS = max(300, int(os.environ.get("JOBLOG_OVERDUE_PUSH_TTL", "3600")))


# Authentication helpers
def hash_password(password: str) -> str:
    """Hash a password using SHA-256."""
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


def verify_password(password: str, hashed: str) -> bool:
    """Verify a password against a hash."""
    return hash_password(password) == hashed


def compute_initials(value: str) -> str:
    """Return up to two initials from the provided value."""
    if not value:
        return "?"
    cleaned = value.replace("-", " ")
    tokens = [part for part in cleaned.split() if part]
    if not tokens:
        return value[:2].upper()
    initials = "".join(part[0] for part in tokens[:2]).upper()
    if initials:
        return initials
    return value[:2].upper()


def load_users() -> Dict[str, Dict[str, str]]:
    """Load users from users.json file."""
    if not USERS_FILE.exists():
        return {}
    try:
        with open(USERS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def login_required(f):
    """Decorator to require login for a route."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Authentication required'}), 401
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


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


def _coerce_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        slug = value.strip()
        if not slug:
            return None
        try:
            return int(slug)
        except ValueError:
            return None
    return None


def compute_planned_duration_ms(
    plan_start: Optional[str],
    plan_end: Optional[str],
    planned_members: Optional[int],
) -> Optional[int]:
    start_ms = parse_iso_to_ms(plan_start)
    end_ms = parse_iso_to_ms(plan_end)
    if start_ms is None or end_ms is None:
        return None
    base = max(0, end_ms - start_ms)
    if base == 0:
        return 0
    normalized_members = planned_members if isinstance(planned_members, int) else None
    if normalized_members is None:
        normalized_members = _coerce_int(planned_members)
    if normalized_members is None or normalized_members <= 0:
        normalized_members = 1
    return base * normalized_members


def load_activity_meta(db: DatabaseLike) -> Dict[str, Any]:
    raw = get_app_state(db, "activity_plan_meta")
    if not raw:
        return {}
    try:
        meta = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if isinstance(meta, dict):
        return meta
    return {}


def save_activity_meta(db: DatabaseLike, meta: Mapping[str, Any]) -> None:
    set_app_state(db, "activity_plan_meta", json.dumps(meta))


def increment_activity_runtime(meta: Dict[str, Any], activity_id: Optional[str], delta_ms: int) -> bool:
    if not activity_id:
        return False
    try:
        contribution = int(delta_ms)
    except (TypeError, ValueError):
        return False
    if contribution <= 0:
        return False

    entry = meta.get(activity_id)
    if not isinstance(entry, dict):
        entry = {}
    current_value = entry.get("actual_runtime_ms")
    try:
        current_int = int(current_value)
    except (TypeError, ValueError):
        current_int = 0
    entry["actual_runtime_ms"] = current_int + contribution
    meta[activity_id] = entry
    return True


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

    global _CONFIG_CACHE, _CONFIG_CACHE_MTIME, _DATABASE_SETTINGS, _WEBPUSH_SETTINGS

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
    _DATABASE_SETTINGS = None
    _WEBPUSH_SETTINGS = None
    return data


MYSQL_SCHEMA_STATEMENTS: List[str] = [
    """
    CREATE TABLE IF NOT EXISTS activities (
        activity_id VARCHAR(255) PRIMARY KEY,
        label VARCHAR(255) NOT NULL,
        sort_order INT NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS member_state (
        member_key VARCHAR(255) PRIMARY KEY,
        member_name VARCHAR(255) NOT NULL,
        activity_id VARCHAR(255),
        running TINYINT(1) NOT NULL DEFAULT 0,
        start_ts BIGINT,
        elapsed_cached BIGINT NOT NULL DEFAULT 0,
        pause_start BIGINT,
        CONSTRAINT fk_member_activity
            FOREIGN KEY (activity_id)
            REFERENCES activities(activity_id)
            ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS event_log (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ts BIGINT NOT NULL,
        kind VARCHAR(64) NOT NULL,
        member_key VARCHAR(255),
        details LONGTEXT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS app_state (
        `key` VARCHAR(128) PRIMARY KEY,
        value TEXT NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS push_subscriptions (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(190) NOT NULL,
        endpoint VARCHAR(500) NOT NULL UNIQUE,
        p256dh VARCHAR(255) NOT NULL,
        auth VARCHAR(128) NOT NULL,
        content_encoding VARCHAR(32) DEFAULT NULL,
        user_agent VARCHAR(255) DEFAULT NULL,
        expiration_time BIGINT DEFAULT NULL,
        created_ts BIGINT NOT NULL,
        updated_ts BIGINT NOT NULL,
        INDEX idx_push_username (username)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS push_notification_log (
        id INT AUTO_INCREMENT PRIMARY KEY,
        kind VARCHAR(32) NOT NULL,
        activity_id VARCHAR(255) DEFAULT NULL,
        username VARCHAR(190) DEFAULT NULL,
        title VARCHAR(255) NOT NULL,
        body TEXT,
        payload LONGTEXT,
        sent_ts BIGINT NOT NULL,
        created_ts BIGINT NOT NULL,
        INDEX idx_push_log_user (username),
        INDEX idx_push_log_sent (sent_ts)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]


_DATABASE_SETTINGS: Optional[Dict[str, Any]] = None


def get_database_settings(force_refresh: bool = False) -> Dict[str, Any]:
    """Restituisce le impostazioni DB combinando env e config.json."""

    global _DATABASE_SETTINGS
    if _DATABASE_SETTINGS is not None and not force_refresh:
        return _DATABASE_SETTINGS

    config = load_config()
    db_section = config.get("database")
    raw_db: Dict[str, Any] = db_section if isinstance(db_section, dict) else {}

    def read(key: str, env_key: str, default: Any = None) -> Any:
        env_value = os.environ.get(env_key)
        if env_value not in (None, ""):
            return env_value
        if key in raw_db:
            value = raw_db.get(key)
            if value not in (None, ""):
                return value
        return default

    vendor = str(read("vendor", "JOBLOG_DB_VENDOR", "sqlite") or "sqlite").lower()
    port_value = read("port", "JOBLOG_DB_PORT", 3306)
    try:
        port = int(port_value)
    except (TypeError, ValueError):  # pragma: no cover - configurazione invalida
        port = 3306

    settings = {
        "vendor": vendor,
        "host": read("host", "JOBLOG_DB_HOST", "localhost"),
        "port": port,
        "user": read("user", "JOBLOG_DB_USER", "root"),
        "password": read("password", "JOBLOG_DB_PASSWORD", ""),
        "name": read("name", "JOBLOG_DB_NAME", "joblog"),
    }

    _DATABASE_SETTINGS = settings
    return settings


DATABASE_SETTINGS = get_database_settings()
DB_VENDOR = DATABASE_SETTINGS["vendor"]
APP_STATE_KEY_COLUMN = "`key`" if DB_VENDOR == "mysql" else "key"


def get_webpush_settings(force_refresh: bool = False) -> Optional[Dict[str, str]]:
    """Restituisce le impostazioni VAPID per il Web Push, se configurate."""

    global _WEBPUSH_SETTINGS
    if _WEBPUSH_SETTINGS is not None and not force_refresh:
        return cast(Optional[Dict[str, str]], _WEBPUSH_SETTINGS)

    config = load_config()
    section = config.get("webpush")
    raw_section: Dict[str, Any] = section if isinstance(section, dict) else {}

    def read(key: str, env_key: str) -> Optional[str]:
        value = os.environ.get(env_key)
        if value not in (None, ""):
            return value.strip()
        candidate = raw_section.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
        return None

    public = read("vapid_public", "WEBPUSH_VAPID_PUBLIC")
    private = read("vapid_private", "WEBPUSH_VAPID_PRIVATE")
    subject = read("subject", "WEBPUSH_VAPID_SUBJECT")

    if not public or not private or not subject:
        _WEBPUSH_SETTINGS = None
        return None

    _WEBPUSH_SETTINGS = {
        "vapid_public": public,
        "vapid_private": private,
        "subject": subject,
    }
    return cast(Dict[str, str], _WEBPUSH_SETTINGS)


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
    if DB_VENDOR == "mysql":
        db = MySQLConnection(DATABASE_SETTINGS)
        try:
            for statement in MYSQL_SCHEMA_STATEMENTS:
                cursor = db.execute(statement)
                cursor.close()
            purge_legacy_seed(db)
            db.commit()
        finally:
            db.close()
        return

    db = sqlite3.connect(DATABASE)
    try:
        db.row_factory = sqlite3.Row
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

            CREATE TABLE IF NOT EXISTS push_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                endpoint TEXT NOT NULL UNIQUE,
                p256dh TEXT NOT NULL,
                auth TEXT NOT NULL,
                content_encoding TEXT,
                user_agent TEXT,
                expiration_time INTEGER,
                created_ts INTEGER NOT NULL,
                updated_ts INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_push_username ON push_subscriptions(username);

            CREATE TABLE IF NOT EXISTS push_notification_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                activity_id TEXT,
                username TEXT,
                title TEXT NOT NULL,
                body TEXT,
                payload TEXT,
                sent_ts INTEGER NOT NULL,
                created_ts INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_push_log_user ON push_notification_log(username);
            CREATE INDEX IF NOT EXISTS idx_push_log_sent ON push_notification_log(sent_ts);
            """
        )
        purge_legacy_seed(db)
        db.commit()
    finally:
        db.close()


def purge_legacy_seed(db: DatabaseLike) -> None:
    try:
        project_code = get_app_state(db, "project_code")
    except Exception:  # pragma: no cover - ignora errori iniziali
        return

    if project_code != DEMO_PROJECT_CODE:
        return

    db.execute("DELETE FROM activities")
    db.execute("DELETE FROM member_state")
    db.execute("DELETE FROM event_log")
    db.execute(
        f"DELETE FROM app_state WHERE {APP_STATE_KEY_COLUMN} IN ('project_code','project_name','activity_plan_meta','push_notified_activities','long_running_member_notifications')"
    )


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


def set_app_state(db: DatabaseLike, key: str, value: str) -> None:
    if DB_VENDOR == "mysql":
        db.execute(
            """
            INSERT INTO app_state(`key`, value) VALUES(?, ?)
            ON DUPLICATE KEY UPDATE value=VALUES(value)
            """,
            (key, value),
        )
        return

    db.execute(
        """
        INSERT INTO app_state(key, value) VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (key, value),
    )


def get_app_state(db: DatabaseLike, key: str) -> Optional[str]:
    try:
        query = f"SELECT value FROM app_state WHERE {APP_STATE_KEY_COLUMN}=?"
        row = db.execute(query, (key,)).fetchone()
    except sqlite3.OperationalError:
        return None
    except Exception as exc:  # pragma: no cover - gestione MySQL
        if pymysql_err is not None and isinstance(exc, pymysql_err.ProgrammingError):
            return None
        raise
    return row["value"] if row else None


def get_push_notified_map(db: DatabaseLike) -> Dict[str, Any]:
    raw = get_app_state(db, PUSH_NOTIFIED_STATE_KEY)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if isinstance(data, dict):
        return data
    return {}


def save_push_notified_map(db: DatabaseLike, payload: Mapping[str, Any]) -> None:
    set_app_state(db, PUSH_NOTIFIED_STATE_KEY, json.dumps(payload))


def get_long_running_notified_map(db: DatabaseLike) -> Dict[str, Any]:
    raw = get_app_state(db, LONG_RUNNING_STATE_KEY)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if isinstance(data, dict):
        return data
    return {}


def save_long_running_notified_map(db: DatabaseLike, payload: Mapping[str, Any]) -> None:
    set_app_state(db, LONG_RUNNING_STATE_KEY, json.dumps(payload))


def parse_iso_to_ms(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    slug = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(slug)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def clear_project_state(db: DatabaseLike) -> None:
    """Rimuove il progetto attivo e i relativi dati dal database."""

    db.execute("DELETE FROM activities")
    db.execute("DELETE FROM member_state")
    db.execute("DELETE FROM event_log")
    db.execute(
        f"DELETE FROM app_state WHERE {APP_STATE_KEY_COLUMN} IN ('project_code','project_name','activity_plan_meta','push_notified_activities','long_running_member_notifications')"
    )


def apply_project_plan(db: DatabaseLike, plan: Dict[str, Any]) -> None:
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

    planned_counts: Dict[str, int] = {}
    for member in team:
        activity_id = (member.get("activity_id") or "").strip()
        if activity_id:
            planned_counts[activity_id] = planned_counts.get(activity_id, 0) + 1

    activity_meta = {}
    for activity in activities:
        activity_id = activity.get("id")
        if not activity_id:
            continue
        key = str(activity_id)
        plan_start = activity.get("plan_start")
        plan_end = activity.get("plan_end")
        planned_members = planned_counts.get(key, 0)
        activity_meta[key] = {
            "plan_start": plan_start,
            "plan_end": plan_end,
            "planned_members": planned_members,
            "planned_duration_ms": compute_planned_duration_ms(
                plan_start,
                plan_end,
                planned_members,
            ),
            "actual_runtime_ms": 0,
        }

    set_app_state(db, "project_code", project_code)
    set_app_state(db, "project_name", project_name)
    set_app_state(db, "activity_plan_meta", json.dumps(activity_meta))
    set_app_state(db, PUSH_NOTIFIED_STATE_KEY, json.dumps({}))
    set_app_state(db, LONG_RUNNING_STATE_KEY, json.dumps({}))

    db.execute(
        "INSERT INTO event_log(ts, kind, details) VALUES(?,?,?)",
        (
            now,
            "project_load",
            json.dumps({"project_code": project_code, "project_name": project_name}),
        ),
    )


def seed_demo_data(db: DatabaseLike) -> None:
    plan = mock_fetch_project(DEMO_PROJECT_CODE, None)
    if plan is None:
        raise RuntimeError("Demo project configuration missing")
    apply_project_plan(db, plan)


def get_db() -> DatabaseLike:
    if "db" not in g:
        if DB_VENDOR == "mysql":
            g.db = MySQLConnection(DATABASE_SETTINGS)
        else:
            conn = sqlite3.connect(DATABASE)
            conn.row_factory = sqlite3.Row
            g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(_: BaseException | None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def compute_elapsed(row: Mapping[str, Any], reference: int) -> int:
    elapsed = row["elapsed_cached"] or 0
    if row["running"]:
        start_ts = row["start_ts"] or reference
        elapsed += max(0, reference - start_ts)
    return elapsed


def fetch_member(db: DatabaseLike, member_key: str) -> Optional[Mapping[str, Any]]:
    if not member_key:
        return None
    return db.execute(
        "SELECT * FROM member_state WHERE member_key=?",
        (member_key,),
    ).fetchone()


def fetch_push_subscriptions(db: DatabaseLike) -> List[Mapping[str, Any]]:
    rows = db.execute(
        "SELECT username, endpoint, p256dh, auth, content_encoding, user_agent FROM push_subscriptions"
    ).fetchall()
    return [dict(row) for row in rows]  # type: ignore[list-item]


def remove_push_subscription(db: DatabaseLike, endpoint: str) -> None:
    if not endpoint:
        return
    db.execute("DELETE FROM push_subscriptions WHERE endpoint=?", (endpoint,))


def record_push_notification(
    db: DatabaseLike,
    *,
    kind: str,
    title: str,
    body: Optional[str],
    payload: Mapping[str, Any],
    activity_id: Optional[str] = None,
    username: Optional[str] = None,
) -> None:
    sent_ts = now_ms()
    try:
        serialized = json.dumps(payload, ensure_ascii=False)
    except TypeError:
        serialized = json.dumps({"payload_repr": repr(payload)}, ensure_ascii=False)
    db.execute(
        """
        INSERT INTO push_notification_log(
            kind, activity_id, username, title, body, payload, sent_ts, created_ts
        ) VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            kind,
            activity_id,
            username,
            title,
            body,
            serialized,
            sent_ts,
            sent_ts,
        ),
    )


def fetch_recent_push_notifications(
    db: DatabaseLike,
    *,
    username: str,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    sql = """
        SELECT id, kind, activity_id, username, title, body, payload, sent_ts, created_ts
        FROM push_notification_log
        WHERE username = ?
        ORDER BY sent_ts DESC, id DESC
    """
    params: List[Any] = [username]
    if limit is not None and limit > 0:
        safe_limit = max(1, min(limit, 1000))
        sql += " LIMIT ?"
        params.append(safe_limit)
    rows = db.execute(sql, tuple(params)).fetchall()

    items: List[Dict[str, Any]] = []
    for row in rows:
        raw_payload = row["payload"]
        try:
            payload = json.loads(raw_payload) if raw_payload else None
        except json.JSONDecodeError:
            payload = None
        items.append(
            {
                "id": row["id"],
                "kind": row["kind"],
                "activity_id": row["activity_id"],
                "username": row["username"],
                "title": row["title"],
                "body": row["body"],
                "payload": payload,
                "sent_ts": row["sent_ts"],
                "created_ts": row["created_ts"],
            }
        )
    return items


def format_duration_ms(ms: Any) -> Optional[str]:
    if not isinstance(ms, (int, float)):
        return None
    total_seconds = max(0, int(ms // 1000))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours:02}:{minutes:02}:{seconds:02}"
    return f"{minutes:02}:{seconds:02}"


def evaluate_overdue_activities(db: DatabaseLike) -> List[Dict[str, Any]]:
    meta = load_activity_meta(db)
    if not meta:
        return []

    now = now_ms()
    activity_labels = {
        row["activity_id"]: row["label"]
        for row in db.execute("SELECT activity_id, label FROM activities")
    }

    notified = get_push_notified_map(db)
    overdue: List[Dict[str, Any]] = []

    for activity_id, entry in meta.items():
        if not isinstance(entry, Mapping):
            app.logger.info(
                "Push worker: meta inatteso per attività %s (%s)", activity_id, type(entry)
            )
            continue

        member_rows = db.execute(
            "SELECT running, start_ts, elapsed_cached, pause_start FROM member_state WHERE activity_id=?",
            (activity_id,),
        ).fetchall()

        assigned_count = len(member_rows)
        running_rows = [row for row in member_rows if row["running"]]
        paused_count = sum(1 for row in member_rows if row["pause_start"] is not None)
        running_count = len(running_rows)

        if assigned_count == 0:
            continue

        plan_start_ms = parse_iso_to_ms(cast(Optional[str], entry.get("plan_start")))
        plan_end_ms = parse_iso_to_ms(cast(Optional[str], entry.get("plan_end")))
        planned_members = _coerce_int(entry.get("planned_members"))
        planned_duration_ms = _coerce_int(entry.get("planned_duration_ms"))

        if planned_duration_ms is None:
            if plan_start_ms is None or plan_end_ms is None:
                app.logger.info(
                    "Push worker: pianificazione assente/illeggibile per attività %s",
                    activity_id,
                )
                continue
            base_duration_ms = max(0, plan_end_ms - plan_start_ms)
            if base_duration_ms == 0:
                continue
            normalized_members = planned_members if planned_members and planned_members > 0 else assigned_count
            if normalized_members <= 0:
                normalized_members = 1
            planned_duration_ms = base_duration_ms * normalized_members

        if planned_duration_ms <= 0:
            continue

        running_total_ms = 0
        for row in running_rows:
            elapsed = int(row["elapsed_cached"] or 0)
            start_ts = row["start_ts"]
            start_value: Optional[int]
            try:
                start_value = int(start_ts) if start_ts is not None else None
            except (TypeError, ValueError):
                start_value = None
            if start_value is not None:
                elapsed += max(0, now - start_value)
            running_total_ms += elapsed

        threshold_ms = planned_duration_ms + ACTIVITY_OVERDUE_GRACE_MS
        if running_total_ms <= threshold_ms:
            app.logger.info(
                "Push worker: attività %s ancora entro il margine (tempo %sms, soglia %sms)",
                activity_id,
                running_total_ms,
                threshold_ms,
            )
            continue

        previous = notified.get(activity_id)
        if isinstance(previous, Mapping):
            previous_signature = previous.get("planned_duration_ms")
            if previous_signature is None:
                previous_signature = previous.get("plan_end_ms")
            try:
                previous_signature_int = (
                    int(previous_signature) if previous_signature is not None else None
                )
            except (TypeError, ValueError):
                previous_signature_int = None
            if previous_signature_int == planned_duration_ms:
                app.logger.info(
                    "Push worker: attività %s già notificata per questa durata prevista",
                    activity_id,
                )
                continue

        overdue_minutes = max(1, int((running_total_ms - planned_duration_ms) // 60000))
        app.logger.info(
            "Push worker: attività %s supera la durata prevista di %s minuti",
            activity_id,
            overdue_minutes,
        )
        overdue.append(
            {
                "activity_id": activity_id,
                "activity_label": activity_labels.get(activity_id, activity_id),
                "planned_duration_ms": planned_duration_ms,
                "overdue_minutes": overdue_minutes,
                "assigned_members": assigned_count,
                "active_members": running_count,
                "paused_members": paused_count,
                "actual_running_ms": running_total_ms,
            }
        )

    return overdue


def deliver_overdue_notifications(
    db: DatabaseLike,
    overdue_items: Sequence[Mapping[str, Any]],
    settings: Mapping[str, str],
) -> Set[str]:
    if not overdue_items:
        return set()

    subscriptions = fetch_push_subscriptions(db)
    if not subscriptions:
        app.logger.info(
            "Push worker: nessuna subscription attiva, skip notifica %s",
            [item.get("activity_id") for item in overdue_items],
        )
        return set()

    invalid_endpoints: Set[str] = set()
    delivered: Set[str] = set()

    for item in overdue_items:
        activity_id = cast(str, item.get("activity_id"))
        label = cast(str, item.get("activity_label", activity_id))
        overdue_minutes = cast(int, item.get("overdue_minutes", 0))
        planned_duration_ms = int(cast(Optional[int], item.get("planned_duration_ms")) or 0)
        actual_running_ms = int(cast(Optional[int], item.get("actual_running_ms")) or 0)
        planned_label = format_duration_ms(planned_duration_ms) or "Durata prevista"
        actual_label = format_duration_ms(actual_running_ms) or "Tempo in corso"
        assigned_members = int(cast(Optional[int], item.get("assigned_members")) or 0)
        active_members = int(cast(Optional[int], item.get("active_members")) or 0)
        paused_members = int(cast(Optional[int], item.get("paused_members")) or 0)

        if active_members:
            status_suffix = f" · {active_members} operatori ancora attivi"
        elif paused_members:
            status_suffix = f" · {paused_members} operatori in pausa"
        else:
            status_suffix = " · Nessun timer attivo"

        payload = {
            "title": "Attività oltre il termine",
            "body": (
                f"{label}: il tempo in corso ({actual_label}) supera la durata prevista ({planned_label})"
                f" di {overdue_minutes} minuti{status_suffix}"
            ),
            "data": {
                "activity_id": activity_id,
                "notification_type": "overdue_activity",
                "overdue_minutes": overdue_minutes,
                "planned_duration_ms": planned_duration_ms,
                "actual_running_ms": actual_running_ms,
                "assigned_members": assigned_members,
                "active_members": active_members,
                "paused_members": paused_members,
            },
        }

        delivered_this_round = False

        for sub in subscriptions:
            endpoint = sub.get("endpoint")
            if not endpoint or endpoint in invalid_endpoints:
                continue
            key_p256dh = sub.get("p256dh")
            key_auth = sub.get("auth")
            if not key_p256dh or not key_auth:
                invalid_endpoints.add(str(endpoint))
                continue
            subscription_info = {
                "endpoint": endpoint,
                "keys": {
                    "p256dh": key_p256dh,
                    "auth": key_auth,
                },
            }
            encoding = sub.get("content_encoding") or "aes128gcm"
            try:
                webpush(
                    subscription_info=subscription_info,
                    data=json.dumps(payload),
                    vapid_private_key=settings["vapid_private"],
                    vapid_claims={"sub": settings["subject"]},
                    ttl=OVERDUE_PUSH_TTL_SECONDS,
                    content_encoding=encoding,
                )
                delivered_this_round = True
                record_push_notification(
                    db,
                    kind="overdue_activity",
                    title=payload.get("title", "Notifica"),
                    body=payload.get("body"),
                    payload=payload,
                    activity_id=activity_id,
                    username=sub.get("username"),
                )
            except WebPushException as exc:
                status = getattr(exc.response, "status_code", None)
                app.logger.warning("WebPush fallita (%s): %s", status, exc)
                if status in (404, 410):
                    invalid_endpoints.add(endpoint)
            except Exception as exc:  # pragma: no cover - logging best effort
                app.logger.exception("Errore imprevisto nell'invio push", exc_info=exc)

        if delivered_this_round:
            delivered.add(activity_id)

    if invalid_endpoints:
        for endpoint in invalid_endpoints:
            remove_push_subscription(db, endpoint)
        db.commit()
        app.logger.info(
            "Push worker: rimossa %s subscription invalida", len(invalid_endpoints)
        )

    return delivered


def evaluate_long_running_members(db: DatabaseLike) -> List[Dict[str, Any]]:
    rows = db.execute(
        """
        SELECT
            ms.member_key,
            ms.member_name,
            ms.activity_id,
            ms.start_ts,
            a.label AS activity_label
        FROM member_state ms
        LEFT JOIN activities a ON a.activity_id = ms.activity_id
        WHERE ms.running=1 AND ms.start_ts IS NOT NULL
        """,
    ).fetchall()

    now = now_ms()
    notified = get_long_running_notified_map(db)
    active_starts = {
        row["member_key"]: int(row["start_ts"])
        for row in rows
        if row["start_ts"] is not None
    }

    # Pulisce lo stato dei long running non più validi
    cleaned_state: Dict[str, Any] = {}
    changed = False
    for key, value in notified.items():
        if not isinstance(value, Mapping):
            changed = True
            continue
        try:
            recorded_start = int(value.get("start_ts"))
        except (TypeError, ValueError):
            changed = True
            continue
        if active_starts.get(key) == recorded_start:
            cleaned_state[key] = value
        else:
            changed = True

    if changed:
        save_long_running_notified_map(db, cleaned_state)
        notified = cleaned_state

    long_running: List[Dict[str, Any]] = []

    for row in rows:
        member_key = row["member_key"]
        start_ts = row["start_ts"]
        if start_ts is None:
            continue
        duration = max(0, now - int(start_ts))
        if duration < LONG_RUNNING_THRESHOLD_MS:
            continue
        previous = notified.get(member_key)
        previous_start = None
        if isinstance(previous, Mapping):
            try:
                previous_start = int(previous.get("start_ts"))
            except (TypeError, ValueError):
                previous_start = None
        if previous_start == int(start_ts):
            continue
        long_running.append(
            {
                "member_key": member_key,
                "member_name": row["member_name"],
                "activity_id": row["activity_id"],
                "activity_label": row["activity_label"] or row["activity_id"],
                "start_ts": int(start_ts),
                "duration_ms": duration,
            }
        )

    return long_running


def deliver_long_running_notifications(
    db: DatabaseLike,
    items: Sequence[Mapping[str, Any]],
    settings: Mapping[str, str],
) -> Set[str]:
    if not items:
        return set()

    subscriptions = fetch_push_subscriptions(db)
    if not subscriptions:
        app.logger.info(
            "Push worker: nessuna subscription attiva, skip avvisi operatori oltre soglia"
        )
        return set()

    invalid_endpoints: Set[str] = set()
    delivered_members: Set[str] = set()

    for item in items:
        member_key = cast(str, item.get("member_key"))
        member_name = cast(str, item.get("member_name", member_key))
        activity_label = cast(str, item.get("activity_label"))
        duration_ms = cast(int, item.get("duration_ms", 0))
        duration_label = format_duration_ms(duration_ms) or "02:00"

        payload = {
            "title": "Operatore in attività",
            "body": f"{member_name} è su {activity_label} da {duration_label}",
            "data": {
                "notification_type": "long_running_member",
                "member_key": member_key,
                "member_name": member_name,
                "activity_id": item.get("activity_id"),
                "activity_label": activity_label,
                "start_ts": item.get("start_ts"),
                "duration_ms": duration_ms,
            },
        }

        delivered_this_round = False

        for sub in subscriptions:
            endpoint = sub.get("endpoint")
            if not endpoint or endpoint in invalid_endpoints:
                continue
            key_p256dh = sub.get("p256dh")
            key_auth = sub.get("auth")
            if not key_p256dh or not key_auth:
                invalid_endpoints.add(str(endpoint))
                continue
            subscription_info = {
                "endpoint": endpoint,
                "keys": {
                    "p256dh": key_p256dh,
                    "auth": key_auth,
                },
            }
            encoding = sub.get("content_encoding") or "aes128gcm"
            try:
                webpush(
                    subscription_info=subscription_info,
                    data=json.dumps(payload),
                    vapid_private_key=settings["vapid_private"],
                    vapid_claims={"sub": settings["subject"]},
                    ttl=120,
                    content_encoding=encoding,
                )
                delivered_this_round = True
                record_push_notification(
                    db,
                    kind="long_running_member",
                    title=payload.get("title", "Notifica"),
                    body=payload.get("body"),
                    payload=payload,
                    activity_id=cast(Optional[str], item.get("activity_id")),
                    username=sub.get("username"),
                )
            except WebPushException as exc:
                status = getattr(exc.response, "status_code", None)
                app.logger.warning("WebPush fallita (%s): %s", status, exc)
                if status in (404, 410):
                    invalid_endpoints.add(endpoint)
            except Exception as exc:  # pragma: no cover - logging best effort
                app.logger.exception("Errore imprevisto nell'invio push", exc_info=exc)

        if delivered_this_round:
            delivered_members.add(member_key)

    if invalid_endpoints:
        for endpoint in invalid_endpoints:
            remove_push_subscription(db, endpoint)
        db.commit()
        app.logger.info(
            "Push worker: rimossa %s subscription invalida (avvisi long running)",
            len(invalid_endpoints),
        )

    return delivered_members


def _notification_worker() -> None:
    stop_event = _NOTIFICATION_STOP
    if stop_event is None:
        return

    app.logger.info(
        "Push worker: avviato (intervallo %ss)", NOTIFICATION_INTERVAL_SECONDS
    )

    while not stop_event.is_set():
        try:
            with app.app_context():
                settings = get_webpush_settings()
                if not settings:
                    app.logger.info("Push worker: impostazioni VAPID mancanti")
                    continue
                db = get_db()
                overdue_items = evaluate_overdue_activities(db)
                if overdue_items:
                    overdue_ids = [str(item.get("activity_id")) for item in overdue_items]
                    app.logger.info(
                        "Push worker: trovate %s attività in ritardo %s",
                        len(overdue_items),
                        overdue_ids,
                    )
                    delivered = deliver_overdue_notifications(db, overdue_items, settings)
                    if delivered:
                        app.logger.info(
                            "Push worker: notifiche inviate a %s",
                            sorted(delivered),
                        )
                        state = get_push_notified_map(db)
                        now_sent = now_ms()
                        for item in overdue_items:
                            activity_id = item.get("activity_id")
                            if activity_id in delivered:
                                state[str(activity_id)] = {
                                    "planned_duration_ms": item.get("planned_duration_ms"),
                                    "sent_ts": now_sent,
                                }
                        save_push_notified_map(db, state)
                        db.commit()
                    else:
                        app.logger.info(
                            "Push worker: nessuna notifica consegnata per %s",
                            overdue_ids,
                        )
                else:
                    app.logger.info("Push worker: nessuna attività oltre il termine")

                # Notifiche "operatore long running" disattivate su richiesta
        except Exception as exc:  # pragma: no cover - worker should never crash
            app.logger.exception("Worker notifiche push in errore", exc_info=exc)
        finally:
            stop_event.wait(NOTIFICATION_INTERVAL_SECONDS)


def start_notification_worker() -> None:
    global _NOTIFICATION_THREAD, _NOTIFICATION_STOP

    if _NOTIFICATION_THREAD and _NOTIFICATION_THREAD.is_alive():
        return

    _NOTIFICATION_STOP = Event()
    _NOTIFICATION_THREAD = Thread(target=_notification_worker, name="joblog-push-worker", daemon=True)
    _NOTIFICATION_THREAD.start()
    app.logger.info("Push worker: thread avviato")


def stop_notification_worker() -> None:
    global _NOTIFICATION_THREAD, _NOTIFICATION_STOP

    stop_event = _NOTIFICATION_STOP
    thread = _NOTIFICATION_THREAD

    if stop_event is not None:
        stop_event.set()

    if thread and thread.is_alive():
        thread.join(timeout=5)

    _NOTIFICATION_THREAD = None
    _NOTIFICATION_STOP = None


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


# Authentication routes
@app.route("/login")
def login():
    """Render login page. Redirect to home if already logged in."""
    if 'user' in session:
        return redirect(url_for('home'))
    return render_template("login.html")


@app.post("/api/login")
def api_login():
    """Handle login POST request."""
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'Dati non validi'}), 400
    
    username_input = data.get('username', '').strip()
    password = data.get('password', '')
    
    if not username_input or not password:
        return jsonify({'success': False, 'error': 'Username e password richiesti'}), 400
    
    users = load_users()
    username_key = username_input.lower()
    user = users.get(username_input) or users.get(username_key)
    
    if not user or not verify_password(password, user.get('password', '')):
        return jsonify({'success': False, 'error': 'Credenziali non valide'}), 401
    
    display = user.get('display') or username_input
    full_name = user.get('name') or display
    session.permanent = True
    session['user'] = username_key
    session['user_display'] = display
    session['user_name'] = full_name
    session['user_initials'] = compute_initials(full_name)
    return jsonify({'success': True})


@app.route("/logout")
def logout():
    """Clear session and redirect to login."""
    session.clear()
    return redirect(url_for('login'))


@app.route("/")
def home() -> ResponseReturnValue:
    if 'user' not in session:
        return redirect(url_for('login'))
    display_name = session.get('user_display') or session.get('user_name') or session.get('user')
    primary_name = session.get('user_name') or display_name or session.get('user')
    initials = session.get('user_initials') or compute_initials(primary_name or "")
    return render_template(
        "index.html",
        user_name=primary_name,
        user_display=display_name,
        user_initials=initials,
    )


@app.get("/api/activities")
@login_required
def api_activities():
    db = get_db()
    rows = db.execute(
        "SELECT activity_id, label FROM activities ORDER BY sort_order, label"
    ).fetchall()
    return jsonify({"activities": [dict(row) for row in rows]})


@app.get("/api/push/status")
@login_required
def api_push_status():
    settings = get_webpush_settings()
    if not settings:
        return jsonify({"enabled": False})

    db = get_db()
    username = session.get("user")
    subscribed = False
    if username:
        row = db.execute(
            "SELECT 1 FROM push_subscriptions WHERE username=? LIMIT 1",
            (username,),
        ).fetchone()
        subscribed = row is not None

    return jsonify(
        {
            "enabled": True,
            "publicKey": settings["vapid_public"],
            "subscribed": subscribed,
        }
    )


@app.post("/api/push/subscribe")
@login_required
def api_push_subscribe():
    settings = get_webpush_settings()
    if not settings:
        return jsonify({"ok": False, "error": "push_not_configured"}), 400

    data = request.get_json(silent=True) or {}
    endpoint_raw = data.get("endpoint")
    if not isinstance(endpoint_raw, str):
        return jsonify({"ok": False, "error": "invalid_subscription"}), 400
    endpoint = endpoint_raw.strip()

    keys = data.get("keys") or {}
    p256dh_raw = keys.get("p256dh")
    auth_raw = keys.get("auth")
    if not isinstance(p256dh_raw, str) or not isinstance(auth_raw, str):
        return jsonify({"ok": False, "error": "invalid_subscription"}), 400
    p256dh = p256dh_raw.strip()
    auth_key = auth_raw.strip()

    if not endpoint or not p256dh or not auth_key:
        return jsonify({"ok": False, "error": "invalid_subscription"}), 400

    encoding_raw = data.get("contentEncoding") or data.get("content_encoding")
    encoding = (
        encoding_raw.strip()
        if isinstance(encoding_raw, str) and encoding_raw.strip()
        else "aes128gcm"
    )

    expiration_raw = data.get("expirationTime")
    expiration_time: Optional[int]
    if isinstance(expiration_raw, (int, float)):
        expiration_time = int(expiration_raw)
    elif isinstance(expiration_raw, str) and expiration_raw.isdigit():
        expiration_time = int(expiration_raw)
    else:
        expiration_time = None

    user_agent = data.get("userAgent")
    if not isinstance(user_agent, str) or not user_agent.strip():
        user_agent = request.headers.get("User-Agent")
    if isinstance(user_agent, str):
        user_agent = user_agent[:255]

    username = session.get("user") or "anonymous"
    now = now_ms()

    db = get_db()
    params = (
        username,
        endpoint,
        p256dh,
        auth_key,
        encoding,
        user_agent,
        expiration_time,
        now,
        now,
    )

    if DB_VENDOR == "mysql":
        db.execute(
            """
            INSERT INTO push_subscriptions(
                username, endpoint, p256dh, auth, content_encoding, user_agent, expiration_time, created_ts, updated_ts
            ) VALUES(?,?,?,?,?,?,?,?,?)
            ON DUPLICATE KEY UPDATE
                username=VALUES(username),
                p256dh=VALUES(p256dh),
                auth=VALUES(auth),
                content_encoding=VALUES(content_encoding),
                user_agent=VALUES(user_agent),
                expiration_time=VALUES(expiration_time),
                updated_ts=VALUES(updated_ts)
            """,
            params,
        )
    else:
        db.execute(
            """
            INSERT INTO push_subscriptions(
                username, endpoint, p256dh, auth, content_encoding, user_agent, expiration_time, created_ts, updated_ts
            ) VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(endpoint) DO UPDATE SET
                username=excluded.username,
                p256dh=excluded.p256dh,
                auth=excluded.auth,
                content_encoding=excluded.content_encoding,
                user_agent=excluded.user_agent,
                expiration_time=excluded.expiration_time,
                updated_ts=excluded.updated_ts
            """,
            params,
        )

    db.commit()
    return jsonify({"ok": True})


@app.post("/api/push/test")
@login_required
def api_push_test():
    settings = get_webpush_settings()
    if not settings:
        return jsonify({"ok": False, "error": "push_not_configured"}), 400

    db = get_db()
    username = session.get("user")
    if not username:
        return jsonify({"ok": False, "error": "missing_user"}), 400

    rows = db.execute(
        "SELECT endpoint, p256dh, auth, content_encoding FROM push_subscriptions WHERE username=?",
        (username,),
    ).fetchall()

    if not rows:
        return jsonify({"ok": False, "error": "no_subscription"}), 404

    subscriptions = [dict(row) for row in rows]
    invalid_endpoints = set()
    delivered = 0

    payload = {
        "title": "JobLog",
        "body": "Notifica di prova inviata con successo",
        "data": {
            "notification_type": "test_message",
            "issued_at": datetime.now(timezone.utc).isoformat(),
        },
    }

    for sub in subscriptions:
        endpoint = sub.get("endpoint") or ""
        if not endpoint or endpoint in invalid_endpoints:
            continue
        key_p256dh = sub.get("p256dh")
        key_auth = sub.get("auth")
        if not key_p256dh or not key_auth:
            invalid_endpoints.add(endpoint)
            continue

        subscription_info = {
            "endpoint": endpoint,
            "keys": {
                "p256dh": key_p256dh,
                "auth": key_auth,
            },
        }
        encoding = sub.get("content_encoding") or "aes128gcm"

        try:
            webpush(
                subscription_info=subscription_info,
                data=json.dumps(payload),
                vapid_private_key=settings["vapid_private"],
                vapid_claims={"sub": settings["subject"]},
                ttl=60,
                content_encoding=encoding,
            )
            delivered += 1
            record_push_notification(
                db,
                kind="test_message",
                title=payload.get("title", "JobLog"),
                body=payload.get("body"),
                payload=payload,
                activity_id=None,
                username=username,
            )
        except WebPushException as exc:
            status = getattr(exc.response, "status_code", None)
            app.logger.warning("WebPush test fallita (%s): %s", status, exc)
            if status in (404, 410):
                invalid_endpoints.add(endpoint)
        except Exception as exc:  # pragma: no cover - logging best effort
            app.logger.exception("Errore generico nell'invio della notifica di prova", exc_info=exc)

    if invalid_endpoints:
        for endpoint in invalid_endpoints:
            remove_push_subscription(db, endpoint)

    db.commit()
    return jsonify({"ok": True, "delivered": delivered, "invalid": list(invalid_endpoints)})


@app.post("/api/push/unsubscribe")
@login_required
def api_push_unsubscribe():
    data = request.get_json(silent=True) or {}
    endpoint = str(data.get("endpoint") or "").strip()
    if not endpoint:
        return jsonify({"ok": False, "error": "invalid_endpoint"}), 400

    db = get_db()
    username = session.get("user")
    if username:
        db.execute(
            "DELETE FROM push_subscriptions WHERE endpoint=? AND username=?",
            (endpoint, username),
        )
    else:
        db.execute("DELETE FROM push_subscriptions WHERE endpoint=?", (endpoint,))

    db.commit()
    return jsonify({"ok": True})


@app.get("/api/push/notifications")
@login_required
def api_push_notifications():
    username = session.get("user")
    if not username:
        return jsonify({"items": []})

    limit_arg = request.args.get("limit", default="20")
    parsed_limit: Optional[int]
    if isinstance(limit_arg, str) and limit_arg.strip().lower() in {"all", "tutti"}:
        parsed_limit = None
    else:
        try:
            parsed_limit = int(limit_arg)
        except (TypeError, ValueError):
            parsed_limit = 20
        else:
            if parsed_limit <= 0:
                parsed_limit = None

    db = get_db()
    items = fetch_recent_push_notifications(db, username=username, limit=parsed_limit)
    return jsonify({"items": items})


@app.get("/api/state")
@login_required
def api_state():
    db = get_db()
    now = now_ms()

    project_code = get_app_state(db, "project_code")
    project_name = get_app_state(db, "project_name") or project_code

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

    activity_meta = load_activity_meta(db)

    activity_map: Dict[str, Dict[str, Any]] = {}
    for row in activity_rows:
        activity_id = row["activity_id"]
        activity_key = str(activity_id)
        meta_entry = activity_meta.get(activity_key)
        if not isinstance(meta_entry, dict):
            meta_entry = {}
            activity_meta[activity_key] = meta_entry

        activity_map[activity_id] = {
            "activity_id": activity_id,
            "label": row["label"],
            "members": [],
            "plan_start": meta_entry.get("plan_start"),
            "plan_end": meta_entry.get("plan_end"),
            "planned_members": meta_entry.get("planned_members"),
            "planned_duration_ms": meta_entry.get("planned_duration_ms"),
            "actual_runtime_ms": meta_entry.get("actual_runtime_ms", 0),
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

    meta_dirty = False
    for activity_id, activity in activity_map.items():
        activity_key = str(activity_id)
        activity["members"].sort(key=lambda m: m["member_name"])
        meta_entry = activity_meta.get(activity_key)
        if not isinstance(meta_entry, dict):
            meta_entry = {}
            activity_meta[activity_key] = meta_entry
        stored_value = _coerce_int(meta_entry.get("planned_members"))
        activity["planned_members"] = stored_value

        plan_start_meta = meta_entry.get("plan_start")
        plan_end_meta = meta_entry.get("plan_end")
        planned_duration_ms = _coerce_int(meta_entry.get("planned_duration_ms"))
        if planned_duration_ms is None:
            computed_duration = compute_planned_duration_ms(
                plan_start_meta,
                plan_end_meta,
                stored_value,
            )
            if computed_duration is not None:
                planned_duration_ms = computed_duration
                meta_entry["planned_duration_ms"] = computed_duration
                meta_dirty = True
        activity["planned_duration_ms"] = planned_duration_ms

    if meta_dirty:
        save_activity_meta(db, activity_meta)

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
@login_required
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
@login_required
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
@login_required
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

    if existing is None:
        app.logger.error("Member state insert fallita per %s", member_key)
        return jsonify({"ok": False, "error": "member_state_error"}), 500

    previous_activity = existing["activity_id"]
    prev_elapsed = compute_elapsed(existing, now)
    normalized_previous = str(previous_activity) if previous_activity else None
    normalized_target = str(activity_id) if activity_id else None
    same_activity = normalized_previous is not None and normalized_previous == normalized_target

    running = 1 if activity_id else 0
    start_ts = now if running else None
    reset_elapsed = bool(activity_id) and not same_activity
    elapsed_cached = 0 if reset_elapsed else prev_elapsed

    activity_meta = load_activity_meta(db)
    meta_changed = False
    if normalized_previous and normalized_previous != normalized_target and prev_elapsed > 0:
        meta_changed = increment_activity_runtime(activity_meta, normalized_previous, prev_elapsed)

    db.execute(
        """
        UPDATE member_state
        SET activity_id=?, running=?, start_ts=?, elapsed_cached=?, pause_start=NULL
        WHERE member_key=?
        """,
        (activity_id, running, start_ts, elapsed_cached, member_key),
    )

    if meta_changed:
        save_activity_meta(db, activity_meta)

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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
def api_finish_all():
    now = now_ms()
    db = get_db()

    rows = db.execute(
        "SELECT * FROM member_state WHERE activity_id IS NOT NULL"
    ).fetchall()

    activity_meta = load_activity_meta(db)
    meta_changed = False

    affected = 0
    for row in rows:
        elapsed = compute_elapsed(row, now)
        if row["activity_id"] and elapsed > 0:
            meta_changed |= increment_activity_runtime(activity_meta, str(row["activity_id"]), elapsed)
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

    if meta_changed:
        save_activity_meta(db, activity_meta)

    db.commit()
    return jsonify({"ok": True, "affected": affected})


@app.post("/api/member/pause")
@login_required
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
@login_required
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
@login_required
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
    activity_meta = load_activity_meta(db)
    meta_changed = False
    if member["activity_id"] and elapsed > 0:
        meta_changed = increment_activity_runtime(activity_meta, str(member["activity_id"]), elapsed)

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

    if meta_changed:
        save_activity_meta(db, activity_meta)

    db.commit()
    return jsonify({"ok": True})


@app.get("/api/export")
@login_required
def api_export():
    """Esporta i dati delle attività in formato Excel o CSV."""
    export_format = request.args.get("format", "excel").lower()
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()
    project_filter = request.args.get("project_code", "").strip()

    db = get_db()
    
    # Ottieni informazioni progetto corrente
    raw_project_code = get_app_state(db, "project_code")
    project_name = get_app_state(db, "project_name") or raw_project_code or ""

    if not raw_project_code:
        return jsonify({"ok": False, "error": "no_active_project"}), 400

    project_code = str(raw_project_code)
    project_name = str(project_name or project_code)

    # Se c'è un filtro progetto e non corrisponde al progetto attivo, errore
    if project_filter and project_filter != project_code:
        return jsonify({"ok": False, "error": "project_mismatch"}), 400

    # Ottieni tutte le attività
    activity_rows = db.execute(
        "SELECT activity_id, label FROM activities ORDER BY sort_order, label"
    ).fetchall()
    
    activity_map = {row["activity_id"]: row["label"] for row in activity_rows}

    # Query per ottenere i log delle attività completate
    query = """
        SELECT 
            el.ts,
            el.kind,
            el.member_key,
            el.details,
            ms.member_name
        FROM event_log el
        LEFT JOIN member_state ms ON el.member_key = ms.member_key
        WHERE el.kind IN ('move', 'finish_activity', 'pause_member', 'resume_member')
        ORDER BY el.ts ASC
    """
    
    event_rows = db.execute(query).fetchall()

    # Raggruppa eventi per operatore e attività per calcolare sessioni di lavoro
    sessions = {}  # Key: (member_key, activity_id) -> lista di eventi
    
    app.logger.info(f"Export: trovati {len(event_rows)} eventi da processare")
    
    for row in event_rows:
        try:
            details = json.loads(row["details"]) if row["details"] else {}
        except json.JSONDecodeError:
            details = {}
        
        ts = row["ts"]
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        
        # Filtro per data
        if start_date:
            filter_date = datetime.fromisoformat(start_date).date()
            if dt.date() < filter_date:
                continue
        if end_date:
            filter_date = datetime.fromisoformat(end_date).date()
            if dt.date() > filter_date:
                continue
        
        member_key = row["member_key"]
        member_name = row["member_name"] or details.get("member_name", "N/D")
        
        # Determina l'activity_id dall'evento
        if row["kind"] == "move":
            activity_id = details.get("to")
        else:
            activity_id = details.get("activity_id")
        
        if not activity_id or not member_key:
            continue
        
        session_key = (member_key, activity_id)
        
        if session_key not in sessions:
            sessions[session_key] = {
                "member_name": member_name,
                "activity_id": activity_id,
                "events": []
            }
        
        sessions[session_key]["events"].append({
            "ts": ts,
            "dt": dt,
            "kind": row["kind"],
            "details": details
        })
    
    # Calcola statistiche per ogni sessione
    export_data = []
    
    app.logger.info(f"Export: trovate {len(sessions)} sessioni uniche (operatore + attività)")
    
    for (member_key, activity_id), session in sessions.items():
        events = sorted(session["events"], key=lambda e: e["ts"])
        
        if not events:
            continue
        
        member_name = session["member_name"]
        activity_label = activity_map.get(activity_id, "N/D")
        
        # Trova inizio e fine
        start_event = None
        end_event = None
        pause_events = []
        resume_events = []
        
        for event in events:
            if event["kind"] == "move" and event["details"].get("to") == activity_id:
                if not start_event:
                    start_event = event
            elif event["kind"] == "pause_member":
                pause_events.append(event)
            elif event["kind"] == "resume_member":
                resume_events.append(event)
            elif event["kind"] == "finish_activity":
                end_event = event
        
        # Se non c'è evento di move, usa il primo evento come inizio
        if not start_event and events:
            start_event = events[0]
        
        # Se ancora non c'è inizio, salta
        if not start_event:
            continue
        
        start_dt = start_event["dt"]
        end_dt = end_event["dt"] if end_event else datetime.now(tz=timezone.utc)
        
        # Calcola durata totale (tempo trascorso dall'inizio alla fine)
        total_duration_ms = int((end_dt - start_dt).total_seconds() * 1000)
        
        # Calcola tempo netto (durata registrata nell'evento finish)
        net_duration_ms = 0
        if end_event:
            net_duration_ms = end_event["details"].get("duration_ms", 0)
        else:
            # Se l'attività è ancora in corso, usa la durata totale
            net_duration_ms = total_duration_ms
        
        # Calcola tempo in pausa (differenza tra totale e netto)
        pause_duration_ms = max(0, total_duration_ms - net_duration_ms)
        
        # Conta numero di pause
        num_pauses = len(pause_events)
        
        export_data.append({
            "operatore": member_name,
            "attivita": activity_label,
            "data_inizio": start_dt.strftime("%d/%m/%Y"),
            "ora_inizio": start_dt.strftime("%H:%M:%S"),
            "data_fine": end_dt.strftime("%d/%m/%Y") if end_event else "In corso",
            "ora_fine": end_dt.strftime("%H:%M:%S") if end_event else "-",
            "durata_netta": format_duration_ms(net_duration_ms) or "00:00:00",
            "tempo_pausa": format_duration_ms(pause_duration_ms) or "00:00:00",
            "num_pause": str(num_pauses),
            "stato": "Completato" if end_event else "In corso"
        })
    
    app.logger.info(f"Export: generati {len(export_data)} record per l'export")

    # Genera file in base al formato
    if export_format == "csv":
        return generate_csv_export(export_data, project_code, project_name)
    else:
        return generate_excel_export(export_data, project_code, project_name)


def generate_excel_export(data: List[Dict[str, Any]], project_code: str, project_name: str):
    """Genera un file Excel con template professionale."""
    wb = Workbook()
    ws_raw = wb.active
    if ws_raw is None:  # pragma: no cover - openpyxl should always provide an active sheet
        ws_raw = wb.create_sheet(title="Report Attività")
    ws: Worksheet = cast(Worksheet, ws_raw)
    ws.title = "Report Attività"

    # Stili
    header_font = Font(name="Calibri", size=14, bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="0EA5E9", end_color="0EA5E9", fill_type="solid")
    header_alignment = Alignment(horizontal="center", vertical="center")
    
    title_font = Font(name="Calibri", size=18, bold=True, color="1E293B")
    title_alignment = Alignment(horizontal="left", vertical="center")
    
    cell_font = Font(name="Calibri", size=11)
    cell_alignment = Alignment(horizontal="left", vertical="center")
    
    border_thin = Border(
        left=Side(style="thin", color="CBD5E1"),
        right=Side(style="thin", color="CBD5E1"),
        top=Side(style="thin", color="CBD5E1"),
        bottom=Side(style="thin", color="CBD5E1"),
    )

    # Titolo report
    ws["A1"] = f"🔷 JobLOG - Report Attività"
    ws.merge_cells("A1:J1")
    title_cell = ws["A1"]
    title_cell.font = title_font
    title_cell.alignment = title_alignment
    
    # Info progetto
    ws["A2"] = f"Progetto: {project_code} - {project_name or project_code}"
    ws.merge_cells("A2:J2")
    project_cell = ws["A2"]
    project_cell.font = Font(name="Calibri", size=12, color="64748B")
    project_cell.alignment = Alignment(horizontal="left", vertical="center")
    
    # Data generazione
    now = datetime.now()
    ws["A3"] = f"Generato il: {now.strftime('%d/%m/%Y alle %H:%M')}"
    ws.merge_cells("A3:J3")
    date_cell = ws["A3"]
    date_cell.font = Font(name="Calibri", size=10, color="94A3B8")
    date_cell.alignment = Alignment(horizontal="left", vertical="center")

    # Riga vuota
    ws.append([])

    # Header colonne
    headers = ["Operatore", "Attività", "Data Inizio", "Ora Inizio", "Data Fine", "Ora Fine", "Durata Netta", "Tempo Pausa", "N° Pause", "Stato"]
    ws.append(headers)
    
    header_row = ws.max_row
    for col_num, header in enumerate(headers, start=1):
        cell = ws.cell(row=header_row, column=col_num)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_alignment
        cell.border = border_thin

    # Dati
    for row_data in data:
        ws.append([
            row_data["operatore"],
            row_data["attivita"],
            row_data["data_inizio"],
            row_data["ora_inizio"],
            row_data["data_fine"],
            row_data["ora_fine"],
            row_data["durata_netta"],
            row_data["tempo_pausa"],
            row_data["num_pause"],
            row_data["stato"],
        ])
        
        row_num = ws.max_row
        for col_num in range(1, 11):
            cell = ws.cell(row=row_num, column=col_num)
            cell.font = cell_font
            cell.alignment = cell_alignment
            cell.border = border_thin
            
            # Alternating row colors
            if row_num % 2 == 0:
                cell.fill = PatternFill(start_color="F8FAFC", end_color="F8FAFC", fill_type="solid")

    # Totale sessioni
    total_row = ws.max_row + 2
    total_cell_ref = f"A{total_row}"
    ws[total_cell_ref] = f"Totale Sessioni: {len(data)}"
    ws.merge_cells(f"A{total_row}:I{total_row}")
    total_cell = ws[total_cell_ref]
    total_cell.font = Font(name="Calibri", size=12, bold=True, color="1E293B")
    total_cell.alignment = Alignment(horizontal="right", vertical="center")

    # Auto-fit colonne
    column_widths = {
        "A": 20,  # Operatore
        "B": 30,  # Attività
        "C": 12,  # Data Inizio
        "D": 11,  # Ora Inizio
        "E": 12,  # Data Fine
        "F": 11,  # Ora Fine
        "G": 14,  # Durata Netta
        "H": 13,  # Tempo Pausa
        "I": 10,  # N° Pause
        "J": 12,  # Stato
    }
    
    for col_letter, width in column_widths.items():
        ws.column_dimensions[col_letter].width = width

    # Salva in memoria
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"joblog_report_{project_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=filename,
    )


def generate_csv_export(data: List[Dict[str, Any]], project_code: str, project_name: str):
    """Genera un file CSV con encoding UTF-8 BOM."""
    output = io.StringIO()
    
    # UTF-8 BOM per compatibilità Excel
    output.write("\ufeff")
    
    writer = csv.writer(output, delimiter=";", quoting=csv.QUOTE_MINIMAL)
    
    # Header informativo
    writer.writerow([f"JobLOG - Report Attività"])
    writer.writerow([f"Progetto: {project_code} - {project_name or project_code}"])
    writer.writerow([f"Generato il: {datetime.now().strftime('%d/%m/%Y alle %H:%M')}"])
    writer.writerow([])  # Riga vuota
    
    # Header colonne
    writer.writerow(["Operatore", "Attività", "Data Inizio", "Ora Inizio", "Data Fine", "Ora Fine", "Durata Netta", "Tempo Pausa", "N° Pause", "Stato"])
    
    # Dati
    for row in data:
        writer.writerow([
            row["operatore"],
            row["attivita"],
            row["data_inizio"],
            row["ora_inizio"],
            row["data_fine"],
            row["ora_fine"],
            row["durata_netta"],
            row["tempo_pausa"],
            row["num_pause"],
            row["stato"],
        ])
    
    # Totale
    writer.writerow([])
    writer.writerow(["", "", "", "", "", "", "", "", f"Totale Sessioni: {len(data)}", ""])

    # Prepara per download
    output.seek(0)
    bytes_output = io.BytesIO(output.getvalue().encode("utf-8"))
    bytes_output.seek(0)

    filename = f"joblog_report_{project_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    return send_file(
        bytes_output,
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


@app.post("/api/_reset")
@login_required
def api_reset():
    db = get_db()
    seed_demo_data(db)
    db.commit()
    return jsonify({"ok": True})


# Registrazione lazy del worker: si assicura che il thread sia attivo al primo accesso
@app.before_request
def _ensure_notification_worker() -> None:
    if _NOTIFICATION_THREAD is None or not _NOTIFICATION_THREAD.is_alive():
        start_notification_worker()


atexit.register(stop_notification_worker)


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
