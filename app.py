from __future__ import annotations

import atexit
import base64
import csv
import hashlib
import io
import json
import logging
import os
import random
import secrets
import sqlite3
import time
import re
from decimal import Decimal
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
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

from flask import Flask, abort, flash, g, jsonify, redirect, render_template, request, send_file, send_from_directory, session, url_for
from flask_session import Session
from flask.typing import ResponseReturnValue
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet
from pywebpush import WebPushException, webpush
import qrcode
from rentman_client import (
    RentmanAPIError,
    RentmanAuthError,
    RentmanClient,
    RentmanError,
    RentmanNotFound,
    MAX_LIMIT,
)


SECRET_FILE = Path(__file__).with_name('.flask_secret')


def _load_or_create_secret_key() -> str:
    override = os.environ.get('FLASK_SECRET_KEY')
    if override:
        return override
    if SECRET_FILE.exists():
        try:
            value = SECRET_FILE.read_text(encoding='utf-8').strip()
            if value:
                return value
        except OSError:
            pass
    generated = secrets.token_hex(32)
    try:
        SECRET_FILE.write_text(generated, encoding='utf-8')
    except OSError:
        logging.getLogger(__name__).warning("Impossibile salvare la chiave segreta su %s", SECRET_FILE)
    return generated


app = Flask(__name__)
app.secret_key = _load_or_create_secret_key()
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=24)

PERSISTENT_SESSION_COOKIE_NAME = os.environ.get('JOBLOG_PERSISTENT_COOKIE_NAME', 'joblog_auth')
PERSISTENT_SESSION_MAX_AGE = int(os.environ.get('JOBLOG_PERSISTENT_SESSION_MAX_AGE', str(30 * 86400)))
app.config['PERSISTENT_SESSION_COOKIE_NAME'] = PERSISTENT_SESSION_COOKIE_NAME
app.config['PERSISTENT_SESSION_MAX_AGE'] = PERSISTENT_SESSION_MAX_AGE

SESSION_STORAGE = Path(__file__).with_name('.flask_session')
SESSION_STORAGE.mkdir(parents=True, exist_ok=True)
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_FILE_DIR'] = str(SESSION_STORAGE)
app.config['SESSION_FILE_THRESHOLD'] = 1024
app.config['SESSION_PERMANENT'] = True
session_manager = Session(app)

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


def static_version(filename: str) -> str:
    """Return cache-busted static URL using file mtime as version."""
    safe_path = filename.lstrip("/")
    static_folder = app.static_folder or os.path.join(app.root_path, "static")
    file_path = os.path.join(static_folder, safe_path)
    try:
        version = int(os.path.getmtime(file_path))
    except (OSError, TypeError):
        version = int(time.time())
    return url_for("static", filename=safe_path, v=version)


app.jinja_env.globals["static_version"] = static_version

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
              raise RuntimeError("PyMySQL non è installato. Esegui 'pip install PyMySQL' per usare il backend MySQL.")
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
_CEDOLINO_RETRY_THREAD: Optional[Thread] = None
_CEDOLINO_RETRY_STOP: Optional[Event] = None

NOTIFICATION_INTERVAL_SECONDS = int(os.environ.get("JOBLOG_NOTIFICATION_INTERVAL", "60"))
CEDOLINO_RETRY_INTERVAL_SECONDS = int(os.environ.get("JOBLOG_CEDOLINO_RETRY_INTERVAL", "300"))  # 5 minuti
ACTIVITY_OVERDUE_GRACE_MS = 10 * 60 * 1000  # 10 minuti di ritardo tollerato
PUSH_NOTIFIED_STATE_KEY = "push_notified_activities"
LONG_RUNNING_STATE_KEY = "long_running_member_notifications"
LONG_RUNNING_THRESHOLD_MS = 2 * 60 * 1000  # 2 minuti
OVERDUE_PUSH_TTL_SECONDS = max(300, int(os.environ.get("JOBLOG_OVERDUE_PUSH_TTL", "3600")))

RUN_STATE_PAUSED = 0
RUN_STATE_RUNNING = 1
RUN_STATE_FINISHED = 2
VALID_RUN_STATES = {RUN_STATE_PAUSED, RUN_STATE_RUNNING, RUN_STATE_FINISHED}

ROLE_USER = "user"
ROLE_SUPERVISOR = "supervisor"
ROLE_ADMIN = "admin"
ROLE_MAGAZZINO = "magazzino"
VALID_USER_ROLES = {ROLE_USER, ROLE_SUPERVISOR, ROLE_ADMIN, ROLE_MAGAZZINO}


# Permission check helpers
def is_admin_or_supervisor() -> bool:
    """Check if current user is admin or supervisor (responsabile squadra)."""
    return bool(session.get("is_admin") or session.get("is_supervisor"))


def is_admin_only() -> bool:
    """Check if current user is admin only."""
    return bool(session.get("is_admin"))


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


def load_users_file() -> Dict[str, Dict[str, Any]]:
    """Return the legacy users.json payload (if present) for migrations."""
    if not USERS_FILE.exists():
        return {}
    try:
        with open(USERS_FILE, 'r', encoding='utf-8') as f:
            payload = json.load(f)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _normalize_username(value: str) -> str:
    return value.strip().lower()


def _normalize_role(value: Optional[str]) -> str:
    if not value:
        return ROLE_USER
    candidate = value.strip().lower()
    if candidate in VALID_USER_ROLES:
        return candidate
    return ROLE_USER


def _role_from_legacy_entry(entry: Mapping[str, Any]) -> str:
    roles_field = entry.get("roles")
    candidates: List[str] = []
    if isinstance(roles_field, str):
        candidates.append(roles_field)
    elif isinstance(roles_field, Iterable):
        for role in roles_field:
            if isinstance(role, str):
                candidates.append(role)

    for role in candidates:
        normalized = role.strip().lower()
        if normalized == ROLE_ADMIN:
            return ROLE_ADMIN
        if normalized == ROLE_SUPERVISOR:
            return ROLE_SUPERVISOR
        if normalized == ROLE_MAGAZZINO:
            return ROLE_MAGAZZINO

    is_admin_flag = entry.get("is_admin")
    if isinstance(is_admin_flag, bool) and is_admin_flag:
        return ROLE_ADMIN
    if isinstance(is_admin_flag, str) and is_admin_flag.strip().lower() in {"1", "true", "yes", "y"}:
        return ROLE_ADMIN
    return ROLE_USER


def fetch_user_record(db: DatabaseLike, username: str) -> Optional[Mapping[str, Any]]:
    if not username:
        return None
    return db.execute(
        """
        SELECT username, password_hash, display_name, full_name, role, is_active,
               current_project_code, current_project_name
        FROM app_users
        WHERE LOWER(username)=LOWER(?)
        """,
        (username,),
    ).fetchone()


def _persistent_cookie_name() -> str:
    return app.config.get('PERSISTENT_SESSION_COOKIE_NAME', PERSISTENT_SESSION_COOKIE_NAME)


def _persistent_session_max_age() -> int:
    value = app.config.get('PERSISTENT_SESSION_MAX_AGE', PERSISTENT_SESSION_MAX_AGE)
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return PERSISTENT_SESSION_MAX_AGE
    return max(300, parsed)


def _hash_persistent_token(token: str) -> str:
    return hashlib.sha256(token.encode('utf-8')).hexdigest()


def _request_metadata() -> Tuple[Optional[str], Optional[str]]:
    user_agent = request.headers.get('User-Agent', '').strip()
    forwarded_for = request.headers.get('X-Forwarded-For', '').strip()
    if forwarded_for:
        ip_candidate = forwarded_for.split(',')[0].strip()
    else:
        ip_candidate = request.remote_addr or ''
    user_agent = user_agent or None
    ip_candidate = ip_candidate or None
    return user_agent, ip_candidate


def _store_persistent_session(username: str) -> Tuple[str, int]:
    token = secrets.token_urlsafe(48)
    token_hash = _hash_persistent_token(token)
    now = now_ms()
    max_age_ms = _persistent_session_max_age() * 1000
    expires_ts = now + max_age_ms
    user_agent, ip_addr = _request_metadata()
    db = get_db()
    db.execute(
        """
        INSERT INTO persistent_sessions(token_hash, username, user_agent, ip_address, created_ts, last_seen_ts, expires_ts)
        VALUES(?,?,?,?,?,?,?)
        """,
        (token_hash, username, user_agent, ip_addr, now, now, expires_ts),
    )
    try:
        db.commit()
    except Exception:
        pass
    return token, expires_ts


def _delete_persistent_session(token_value: Optional[str]) -> None:
    if not token_value:
        return
    token_hash = _hash_persistent_token(token_value)
    db = get_db()
    db.execute("DELETE FROM persistent_sessions WHERE token_hash=?", (token_hash,))
    try:
        db.commit()
    except Exception:
        pass


def _load_persistent_session(token_value: str) -> Optional[Tuple[str, int]]:
    if not token_value:
        return None
    token_hash = _hash_persistent_token(token_value)
    db = get_db()
    row = db.execute(
        "SELECT username, expires_ts FROM persistent_sessions WHERE token_hash=?",
        (token_hash,),
    ).fetchone()
    if not row:
        return None
    username = row.get('username') if isinstance(row, Mapping) else row[0]
    expires_raw = row.get('expires_ts') if isinstance(row, Mapping) else row[1]
    expires_ts = _coerce_int(expires_raw) or 0
    now = now_ms()
    if expires_ts <= now:
        db.execute("DELETE FROM persistent_sessions WHERE token_hash=?", (token_hash,))
        try:
            db.commit()
        except Exception:
            pass
        return None
    new_expires = now + _persistent_session_max_age() * 1000
    db.execute(
        "UPDATE persistent_sessions SET last_seen_ts=?, expires_ts=? WHERE token_hash=?",
        (now, new_expires, token_hash),
    )
    try:
        db.commit()
    except Exception:
        pass
    return username, new_expires


def _set_persistent_cookie(response, token_value: str, expires_ts: Optional[int] = None) -> None:
    max_age = _persistent_session_max_age()
    expires = expires_ts or (now_ms() + max_age * 1000)
    expires_dt = datetime.fromtimestamp(expires / 1000, tz=timezone.utc)
    response.set_cookie(
        _persistent_cookie_name(),
        token_value,
        max_age=max_age,
        expires=expires_dt,
        httponly=True,
        secure=app.config.get('SESSION_COOKIE_SECURE', False),
        samesite=app.config.get('SESSION_COOKIE_SAMESITE', 'Lax'),
        path='/',
    )


def _apply_user_session(user_row: Mapping[str, Any]) -> None:
    username = user_row.get('username') or ''
    display = user_row.get('display_name') or username
    full_name = user_row.get('full_name') or display
    role = _normalize_role(user_row.get('role'))
    session.permanent = True
    session['user'] = username
    session['user_display'] = display
    session['user_name'] = full_name
    session['user_initials'] = compute_initials(full_name)
    session['user_role'] = role
    session['is_admin'] = role == ROLE_ADMIN
    session['is_supervisor'] = role in {ROLE_SUPERVISOR, ROLE_ADMIN}
    session['is_magazzino'] = role in {ROLE_MAGAZZINO, ROLE_ADMIN}
    
    # Ripristina il progetto salvato per i supervisor
    if role in {ROLE_SUPERVISOR, ROLE_ADMIN}:
        current_project_code = user_row.get('current_project_code')
        current_project_name = user_row.get('current_project_name')
        app.logger.info("Login %s: recupero progetto salvato = %s", username, current_project_code)
        if current_project_code:
            session['supervisor_project_code'] = current_project_code
            session['supervisor_project_name'] = current_project_name or current_project_code


def _magazzino_only() -> Optional[ResponseReturnValue]:
    role = session.get('user_role')
    if role not in {ROLE_MAGAZZINO, ROLE_ADMIN}:
        if request.path.startswith('/api/'):
            return jsonify({"error": "forbidden"}), 403
        return ("Forbidden", 403)
    return None


@app.before_request
def _restore_persistent_session() -> None:
    if 'user' in session:
        return
    token = request.cookies.get(_persistent_cookie_name())
    if not token:
        return
    loaded = _load_persistent_session(token)
    if not loaded:
        return
    username, new_expires = loaded
    db = get_db()
    user_row = fetch_user_record(db, username)
    if not user_row or not user_row.get('is_active'):
        _delete_persistent_session(token)
        return
    _apply_user_session(user_row)
    g.persistent_cookie_refresh = (token, new_expires)


@app.after_request
def _refresh_persistent_cookie(response):
    pending = g.pop('persistent_cookie_refresh', None)
    if pending:
        token_value, expires_ts = pending
        _set_persistent_cookie(response, token_value, expires_ts)
    return response


def migrate_users_file(db: DatabaseLike) -> bool:
    legacy = load_users_file()
    if not legacy:
        return False

    rows: List[tuple] = []
    now = now_ms()
    for raw_username, entry in legacy.items():
        if not isinstance(entry, Mapping):
            continue
        username = _normalize_username(str(raw_username))
        password_hash = entry.get("password") or entry.get("password_hash")
        if not username or not isinstance(password_hash, str) or not password_hash.strip():
            continue
        display = str(entry.get("display") or raw_username).strip() or username
        full_name = str(entry.get("name") or display).strip() or display
        role = _role_from_legacy_entry(entry)
        rows.append((username, password_hash.strip(), display, full_name, role, 1, now, now))

    if not rows:
        return False

    if DB_VENDOR == "mysql":
        insert_sql = (
            "INSERT INTO app_users("
            "username, password_hash, display_name, full_name, role, is_active, created_ts, updated_ts"
            ") VALUES(?,?,?,?,?,?,?,?) "
            "ON DUPLICATE KEY UPDATE username=VALUES(username)"
        )
    else:
        insert_sql = (
            "INSERT INTO app_users("
            "username, password_hash, display_name, full_name, role, is_active, created_ts, updated_ts"
            ") VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(username) DO NOTHING"
        )

    db.executemany(insert_sql, rows)
    app.logger.info("Importati %s utenti da %s", len(rows), USERS_FILE.name)
    return True


def bootstrap_user_store(db: DatabaseLike) -> None:
    try:
        ensure_app_users_table(db)
    except Exception:
        app.logger.exception("Impossibile preparare la tabella app_users")
        return

    try:
        row = db.execute("SELECT COUNT(*) AS total FROM app_users").fetchone()
        existing = int(row["total"] if row else 0)
    except Exception:
        app.logger.exception("Impossibile verificare il conteggio degli utenti")
        return

    if existing > 0:
        return

    try:
        migrated = migrate_users_file(db)
    except Exception:
        app.logger.exception("Migrazione utenti legacy fallita")
        migrated = False

    if migrated:
        return

    bootstrap_user = os.environ.get("JOBLOG_BOOTSTRAP_ADMIN_USER", "admin")
    bootstrap_password = os.environ.get("JOBLOG_BOOTSTRAP_ADMIN_PASSWORD")
    if not bootstrap_password:
        app.logger.warning(
            "Nessun utente configurato: crea un account con 'python manage_users.py create <utente> --role admin'"
        )
        return

    now = now_ms()
    username = _normalize_username(bootstrap_user)
    db.execute(
        """
        INSERT INTO app_users(username, password_hash, display_name, full_name, role, is_active, created_ts, updated_ts)
        VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            username,
            hash_password(bootstrap_password),
            bootstrap_user,
            bootstrap_user,
            ROLE_ADMIN,
            1,
            now,
            now,
        ),
    )
    app.logger.warning(
        "Creato automaticamente l'utente amministratore '%s' dalle variabili JOBLOG_BOOTSTRAP_ADMIN_*",
        bootstrap_user,
    )


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


def _parse_date_any(value: Any) -> Optional[date]:
    """Parsa una data da vari formati (ISO, timestamp, dd/mm/yyyy, dd-mm-yyyy)."""

    if value is None:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc).date()
        except (ValueError, OSError):
            return None

    if isinstance(value, str):
        slug = value.strip()
        if not slug:
            return None
        slug = slug.replace("Z", "+00:00")
        # ISO 8601 (date o datetime)
        try:
            return datetime.fromisoformat(slug).date()
        except ValueError:
            pass
        # Se contiene un datetime ma non ISO perfetto, prova a prendere YYYY-MM-DD
        if len(slug) >= 10:
            head = slug[:10]
            try:
                return datetime.strptime(head, "%Y-%m-%d").date()
            except ValueError:
                pass
        # Formati europei
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y"):
            try:
                return datetime.strptime(slug[:10], fmt).date()
            except ValueError:
                continue
        return None

    return None


def _coerce_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float, Decimal)):
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


def _normalize_running(value: Any, default: int | None = None) -> int:
    coerced = _coerce_int(value)
    if coerced in VALID_RUN_STATES:
        return int(coerced)
    if default is None:
        return RUN_STATE_PAUSED
    return default


def _slugify(value: str) -> str:
    """Crea uno slug alfanumerico minuscolo."""

    normalized = value.strip().lower()
    if not normalized:
        return ""
    buffer: List[str] = []
    previous_dash = False
    for char in normalized:
        if char.isalnum():
            buffer.append(char)
            previous_dash = False
        elif not previous_dash:
            buffer.append("-")
            previous_dash = True
    slug = "".join(buffer).strip("-")
    return slug


def _normalize_activity_id(value: str) -> str:
    slug = _slugify(value)
    if not slug:
        return ""
    return slug.replace("-", "_").upper()


def _generate_activity_id(db: DatabaseLike, label: str) -> str:
    base = _normalize_activity_id(label) or "ATTIVITA"
    base = base[:24]
    if not base:
        base = "ATTIVITA"
    candidate = base
    suffix = 1
    while True:
        row = db.execute("SELECT 1 FROM activities WHERE activity_id=?", (candidate,)).fetchone()
        if row is None:
            return candidate
        suffix += 1
        padded = f"{suffix:02d}"
        truncated = base[: max(4, 24 - len(padded) - 1)]
        candidate = f"{truncated}_{padded}"


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


def refresh_activity_meta(db: DatabaseLike) -> Dict[str, Any]:
    rows = db.execute(
        "SELECT activity_id, plan_start, plan_end, planned_members, planned_duration_ms FROM activities"
    ).fetchall()
    current = load_activity_meta(db)
    next_meta: Dict[str, Any] = {}
    for row in rows:
        activity_id = row["activity_id"]
        if not activity_id:
            continue
        previous = current.get(activity_id)
        previous_runtime = 0
        if isinstance(previous, Mapping):
            try:
                previous_runtime = int(previous.get("actual_runtime_ms") or 0)
            except (TypeError, ValueError):
                previous_runtime = 0
        next_meta[str(activity_id)] = {
            "plan_start": row["plan_start"],
            "plan_end": row["plan_end"],
            "planned_members": row["planned_members"],
            "planned_duration_ms": row["planned_duration_ms"],
            "actual_runtime_ms": previous_runtime,
        }
    save_activity_meta(db, next_meta)
    return next_meta


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
    expected_date = _parse_date_any(expected)
    start = _parse_date_any(plan_start)
    end = _parse_date_any(plan_end)

    if expected_date is None:
        return False

    # Se non abbiamo date valide, escludiamo l'attività
    if start is None and end is None:
        return False

    # Se abbiamo solo una data, verifichiamo l'uguaglianza
    if start is not None and end is None:
        return start == expected_date
    if end is not None and start is None:
        return end == expected_date

    assert start is not None and end is not None
    if end < start:
        start, end = end, start
    return start <= expected_date <= end


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


APP_USERS_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS app_users (
    username VARCHAR(190) PRIMARY KEY,
    password_hash VARCHAR(255) NOT NULL,
    display_name VARCHAR(255) NOT NULL,
    full_name VARCHAR(255) DEFAULT NULL,
    role VARCHAR(32) NOT NULL DEFAULT 'user',
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_ts BIGINT NOT NULL,
    updated_ts BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

APP_USERS_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS app_users (
    username TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    display_name TEXT NOT NULL,
    full_name TEXT,
    role TEXT NOT NULL DEFAULT 'user',
    is_active INTEGER NOT NULL DEFAULT 1,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
)
"""

SESSION_OVERRIDES_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS activity_session_overrides (
    id INT AUTO_INCREMENT PRIMARY KEY,
    member_key VARCHAR(255) NOT NULL,
    member_name VARCHAR(255) NOT NULL,
    activity_id VARCHAR(255) NOT NULL,
    activity_label VARCHAR(255) DEFAULT NULL,
    project_code VARCHAR(64) DEFAULT NULL,
    start_ts BIGINT NOT NULL,
    end_ts BIGINT DEFAULT NULL,
    net_ms BIGINT DEFAULT NULL,
    pause_ms BIGINT DEFAULT NULL,
    pause_count INT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'completed',
    source_member_key VARCHAR(255) DEFAULT NULL,
    source_activity_id VARCHAR(255) DEFAULT NULL,
    source_start_ts BIGINT DEFAULT NULL,
    manual_entry TINYINT(1) NOT NULL DEFAULT 0,
    note TEXT,
    created_by VARCHAR(190) DEFAULT NULL,
    updated_by VARCHAR(190) DEFAULT NULL,
    created_ts BIGINT NOT NULL,
    updated_ts BIGINT NOT NULL,
    INDEX idx_session_override_source (source_member_key, source_activity_id, source_start_ts),
    INDEX idx_session_override_member (member_key),
    INDEX idx_session_override_activity (activity_id),
    INDEX idx_session_override_project (project_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

SESSION_OVERRIDES_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS activity_session_overrides (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    member_key TEXT NOT NULL,
    member_name TEXT NOT NULL,
    activity_id TEXT NOT NULL,
    activity_label TEXT,
    project_code TEXT,
    start_ts INTEGER NOT NULL,
    end_ts INTEGER,
    net_ms INTEGER,
    pause_ms INTEGER,
    pause_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'completed',
    source_member_key TEXT,
    source_activity_id TEXT,
    source_start_ts INTEGER,
    manual_entry INTEGER NOT NULL DEFAULT 0,
    note TEXT,
    created_by TEXT,
    updated_by TEXT,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_session_override_source ON activity_session_overrides(source_member_key, source_activity_id, source_start_ts);
CREATE INDEX IF NOT EXISTS idx_session_override_member ON activity_session_overrides(member_key);
CREATE INDEX IF NOT EXISTS idx_session_override_activity ON activity_session_overrides(activity_id);
CREATE INDEX IF NOT EXISTS idx_session_override_project ON activity_session_overrides(project_code);
"""

PERSISTENT_SESSIONS_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS persistent_sessions (
    token_hash CHAR(64) PRIMARY KEY,
    username VARCHAR(190) NOT NULL,
    user_agent VARCHAR(255) DEFAULT NULL,
    ip_address VARCHAR(64) DEFAULT NULL,
    created_ts BIGINT NOT NULL,
    last_seen_ts BIGINT NOT NULL,
    expires_ts BIGINT NOT NULL,
    INDEX idx_persistent_sessions_user (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

PERSISTENT_SESSIONS_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS persistent_sessions (
    token_hash TEXT PRIMARY KEY,
    username TEXT NOT NULL,
    user_agent TEXT,
    ip_address TEXT,
    created_ts INTEGER NOT NULL,
    last_seen_ts INTEGER NOT NULL,
    expires_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_persistent_sessions_user ON persistent_sessions(username);
"""


EQUIPMENT_CHECKS_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS equipment_checks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    project_code VARCHAR(128) NOT NULL,
    item_key VARCHAR(512) NOT NULL,
    checked_ts BIGINT NOT NULL,
    username VARCHAR(190) DEFAULT NULL,
    created_ts BIGINT NOT NULL,
    updated_ts BIGINT NOT NULL,
    UNIQUE KEY uq_equipment_project_item (project_code, item_key),
    INDEX idx_equipment_project (project_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


EQUIPMENT_CHECKS_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS equipment_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_code TEXT NOT NULL,
    item_key TEXT NOT NULL,
    checked_ts INTEGER NOT NULL,
    username TEXT,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_equipment_project_item ON equipment_checks(project_code, item_key);
CREATE INDEX IF NOT EXISTS idx_equipment_project ON equipment_checks(project_code);
"""


LOCAL_EQUIPMENT_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS local_equipment (
    id INT AUTO_INCREMENT PRIMARY KEY,
    project_code VARCHAR(128) NOT NULL,
    name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    notes TEXT,
    group_name VARCHAR(255) DEFAULT 'Attrezzature extra',
    created_ts BIGINT NOT NULL,
    updated_ts BIGINT NOT NULL,
    INDEX idx_local_equipment_project (project_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


LOCAL_EQUIPMENT_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS local_equipment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_code TEXT NOT NULL,
    name TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    notes TEXT,
    group_name TEXT DEFAULT 'Attrezzature extra',
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_local_equipment_project ON local_equipment(project_code);
"""


PROJECT_PHOTOS_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS project_photos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    project_code VARCHAR(128) NOT NULL,
    filename VARCHAR(255) NOT NULL,
    original_name VARCHAR(255) NOT NULL,
    mime_type VARCHAR(100) NOT NULL,
    file_size INT NOT NULL,
    caption TEXT,
    created_ts BIGINT NOT NULL,
    INDEX idx_project_photos_project (project_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


PROJECT_PHOTOS_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS project_photos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_code TEXT NOT NULL,
    filename TEXT NOT NULL,
    original_name TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    caption TEXT,
    created_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_project_photos_project ON project_photos(project_code);
"""


PROJECT_MATERIALS_CACHE_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS project_materials_cache (
    project_code VARCHAR(128) PRIMARY KEY,
    project_name VARCHAR(255) NOT NULL,
    data_json LONGTEXT NOT NULL,
    created_ts BIGINT NOT NULL,
    updated_ts BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


PROJECT_MATERIALS_CACHE_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS project_materials_cache (
    project_code TEXT PRIMARY KEY,
    project_name TEXT NOT NULL,
    data_json TEXT NOT NULL,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  CEDOLINO TIMBRATURE - Integrazione CedolinoWeb
# ═══════════════════════════════════════════════════════════════════════════════

CEDOLINO_TIMBRATURE_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS cedolino_timbrature (
    id INT AUTO_INCREMENT PRIMARY KEY,
    member_key VARCHAR(255) DEFAULT NULL,
    member_name VARCHAR(255) NOT NULL,
    username VARCHAR(190) DEFAULT NULL,
    external_id VARCHAR(255) NOT NULL,
    timeframe_id INT NOT NULL COMMENT '1=inizio giornata, 4=inizio pausa, 5=fine pausa, 8=fine giornata',
    timestamp_ms BIGINT NOT NULL,
    data_riferimento DATE NOT NULL,
    ora_originale TIME NOT NULL,
    ora_modificata TIME NOT NULL,
    project_code VARCHAR(128) DEFAULT NULL,
    activity_id VARCHAR(255) DEFAULT NULL,
    synced_ts BIGINT DEFAULT NULL,
    sync_error TEXT DEFAULT NULL,
    sync_attempts INT NOT NULL DEFAULT 0,
    created_ts BIGINT NOT NULL,
    INDEX idx_cedolino_member (member_key),
    INDEX idx_cedolino_username (username),
    INDEX idx_cedolino_external (external_id),
    INDEX idx_cedolino_synced (synced_ts),
    INDEX idx_cedolino_data (data_riferimento)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

CEDOLINO_TIMBRATURE_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS cedolino_timbrature (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    member_key TEXT,
    member_name TEXT NOT NULL,
    username TEXT,
    external_id TEXT NOT NULL,
    timeframe_id INTEGER NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    data_riferimento TEXT NOT NULL,
    ora_originale TEXT NOT NULL,
    ora_modificata TEXT NOT NULL,
    project_code TEXT,
    activity_id TEXT,
    synced_ts INTEGER,
    sync_error TEXT,
    sync_attempts INTEGER NOT NULL DEFAULT 0,
    created_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cedolino_member ON cedolino_timbrature(member_key);
CREATE INDEX IF NOT EXISTS idx_cedolino_username ON cedolino_timbrature(username);
CREATE INDEX IF NOT EXISTS idx_cedolino_external ON cedolino_timbrature(external_id);
CREATE INDEX IF NOT EXISTS idx_cedolino_synced ON cedolino_timbrature(synced_ts);
CREATE INDEX IF NOT EXISTS idx_cedolino_data ON cedolino_timbrature(data_riferimento);
"""

# Costanti timeframe CedolinoWeb
TIMEFRAME_INIZIO_GIORNATA = 1
TIMEFRAME_INIZIO_PAUSA = 4
TIMEFRAME_FINE_PAUSA = 5
TIMEFRAME_FINE_GIORNATA = 8

CEDOLINO_WEB_ENDPOINT = "http://80.211.18.30/WebServices/crea_timbrata_elaborata"
CEDOLINO_CODICE_TERMINALE = "musa_mobile"


MYSQL_SCHEMA_STATEMENTS: List[str] = [
    """
    CREATE TABLE IF NOT EXISTS activities (
        activity_id VARCHAR(255) NOT NULL,
        project_code VARCHAR(64) NOT NULL DEFAULT '',
        label VARCHAR(255) NOT NULL,
        sort_order INT NOT NULL,
        plan_start TEXT NULL,
        plan_end TEXT NULL,
        planned_members INT NULL,
        planned_duration_ms BIGINT NULL,
        notes TEXT NULL,
        PRIMARY KEY (activity_id, project_code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS member_state (
        member_key VARCHAR(255) NOT NULL,
        project_code VARCHAR(64) NOT NULL DEFAULT '',
        member_name VARCHAR(255) NOT NULL,
        activity_id VARCHAR(255),
        running TINYINT(1) NOT NULL DEFAULT 0,
        start_ts BIGINT,
        elapsed_cached BIGINT NOT NULL DEFAULT 0,
        pause_start BIGINT,
        entered_ts BIGINT,
        PRIMARY KEY (member_key, project_code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS event_log (
        id INT AUTO_INCREMENT PRIMARY KEY,
        project_code VARCHAR(64) NOT NULL DEFAULT '',
        ts BIGINT NOT NULL,
        kind VARCHAR(64) NOT NULL,
        member_key VARCHAR(255),
        details LONGTEXT,
        INDEX idx_event_project (project_code)
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
    APP_USERS_TABLE_MYSQL,
    SESSION_OVERRIDES_TABLE_MYSQL,
    EQUIPMENT_CHECKS_TABLE_MYSQL,
    PROJECT_MATERIALS_CACHE_TABLE_MYSQL,
    LOCAL_EQUIPMENT_TABLE_MYSQL,
]


_DATABASE_SETTINGS: Optional[Dict[str, Any]] = None


REQUIRED_ACTIVITY_COLUMNS: Dict[str, str] = {
    "plan_start": "TEXT",
    "plan_end": "TEXT",
    "planned_members": "INTEGER",
    "planned_duration_ms": "BIGINT",
    "notes": "TEXT",
}

_ACTIVITY_SCHEMA_READY = False


def _get_existing_columns(db: DatabaseLike, table: str) -> Set[str]:
    columns: Set[str] = set()
    if DB_VENDOR == "mysql":
        query = (
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA=? AND TABLE_NAME=?"
        )
        rows = db.execute(query, (DATABASE_SETTINGS["name"], table)).fetchall()
        for row in rows:
            if isinstance(row, Mapping):
                columns.add(str(row.get("COLUMN_NAME")))
            elif isinstance(row, Sequence) and row:
                columns.add(str(row[0]))
    else:
        rows = db.execute(f"PRAGMA table_info({table})").fetchall()
        for row in rows:
            if isinstance(row, Mapping):
                columns.add(str(row.get("name")))
            elif isinstance(row, Sequence) and len(row) > 1:
                columns.add(str(row[1]))
    return {str(col).lower() for col in columns if col}


def ensure_activity_schema(db: DatabaseLike) -> None:
    global _ACTIVITY_SCHEMA_READY
    if _ACTIVITY_SCHEMA_READY:
        return
    try:
        existing = _get_existing_columns(db, "activities")
    except Exception:
        return
    alter_template = (
        "ALTER TABLE activities ADD COLUMN {name} {definition}"
        if DB_VENDOR == "mysql"
        else "ALTER TABLE activities ADD COLUMN {name} {definition}"
    )
    for name, definition in REQUIRED_ACTIVITY_COLUMNS.items():
        if name.lower() in existing:
            continue
        db.execute(alter_template.format(name=name, definition=definition))
    _ACTIVITY_SCHEMA_READY = True


_PROJECT_CODE_MIGRATION_DONE = False


def ensure_project_code_columns(db: DatabaseLike) -> None:
    """Migra le tabelle esistenti per aggiungere la colonna project_code."""
    global _PROJECT_CODE_MIGRATION_DONE
    if _PROJECT_CODE_MIGRATION_DONE:
        return
    
    tables_to_migrate = {
        "activities": "VARCHAR(64) NOT NULL DEFAULT ''" if DB_VENDOR == "mysql" else "TEXT NOT NULL DEFAULT ''",
        "member_state": "VARCHAR(64) NOT NULL DEFAULT ''" if DB_VENDOR == "mysql" else "TEXT NOT NULL DEFAULT ''",
        "event_log": "VARCHAR(64) NOT NULL DEFAULT ''" if DB_VENDOR == "mysql" else "TEXT NOT NULL DEFAULT ''",
    }
    
    for table, col_def in tables_to_migrate.items():
        try:
            existing = _get_existing_columns(db, table)
            if "project_code" not in existing:
                db.execute(f"ALTER TABLE {table} ADD COLUMN project_code {col_def}")
                app.logger.info("Aggiunta colonna project_code a tabella %s", table)
        except Exception as e:
            app.logger.warning("Impossibile aggiungere project_code a %s: %s", table, e)
    
    # Aggiungi indice su event_log se non esiste
    try:
        if DB_VENDOR == "mysql":
            # MySQL: verifica se l'indice esiste
            idx_check = db.execute(
                "SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA=%s AND TABLE_NAME='event_log' AND INDEX_NAME='idx_event_project'",
                (DATABASE_SETTINGS["name"],)
            ).fetchone()
            cnt = idx_check["cnt"] if isinstance(idx_check, Mapping) else idx_check[0]
            if cnt == 0:
                db.execute("CREATE INDEX idx_event_project ON event_log(project_code)")
        else:
            db.execute("CREATE INDEX IF NOT EXISTS idx_event_project ON event_log(project_code)")
    except Exception as e:
        app.logger.warning("Impossibile creare indice idx_event_project: %s", e)
    
    _PROJECT_CODE_MIGRATION_DONE = True


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


def _normalize_attachment_name(entry: Mapping[str, Any]) -> str:
    for key in (
        "readable_name",
        "displayname",
        "friendly_name_without_extension",
        "name_without_extension",
    ):
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    identifier = entry.get("id")
    return f"Allegato {identifier}" if identifier is not None else "Allegato"


def _normalize_attachment_extension(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    slug = value.strip().lstrip(".")
    return slug.upper()

ATTACHMENT_IMAGE_EXTENSIONS: Set[str] = {
    "JPG",
    "JPEG",
    "PNG",
    "GIF",
    "WEBP",
    "BMP",
    "TIFF",
    "HEIC",
}


def _attachment_is_image(entry: Mapping[str, Any]) -> bool:
    if _is_truthy(entry.get("image")):
        return True
    extension = _normalize_attachment_extension(entry.get("extension") or entry.get("type"))
    if extension and extension in ATTACHMENT_IMAGE_EXTENSIONS:
        return True
    description = entry.get("description")
    if isinstance(description, str):
        slug = description.lower()
        if any(token in slug for token in ("photo", "immagine", "preview")):
            return True
    return False


def _folder_display_name(entry: Mapping[str, Any]) -> str:
    for key in ("displayname", "name", "readable_name"):
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    identifier = entry.get("id")
    return f"Cartella {identifier}" if identifier is not None else "Cartella"


def _build_folder_path(folder_id: int, lookup: Mapping[int, Mapping[str, Any]], max_depth: int = 20) -> str:
    parts: List[str] = []
    current = folder_id
    visited: Set[int] = set()
    depth = 0
    while isinstance(current, int) and current not in visited and depth < max_depth:
        visited.add(current)
        entry = lookup.get(current)
        if not entry:
            break
        parts.append(_folder_display_name(entry))
        current = parse_reference(entry.get("parent"))
        depth += 1
    if not parts:
        return ""
    return " / ".join(reversed(parts))


def _collect_project_folders(client: RentmanClient, project_id: int) -> List[Dict[str, Any]]:
    try:
        raw_folders = client.get_project_file_folders(project_id)
    except RentmanError as exc:
        app.logger.error("Rentman: errore recuperando le cartelle file del progetto %s: %s", project_id, exc)
        return []

    folder_lookup: Dict[int, Mapping[str, Any]] = {}
    for entry in raw_folders:
        folder_id = parse_reference(entry.get("id")) or entry.get("id")
        if isinstance(folder_id, int):
            folder_lookup[folder_id] = entry

    try:
        raw_files = client.get_project_files(project_id, exhaustive=False)
    except RentmanError as exc:
        app.logger.error("Rentman: errore recuperando i file per il progetto %s: %s", project_id, exc)
        raw_files = []

    folder_files: Dict[int, List[Dict[str, Any]]] = {}
    for entry in raw_files:
        folder_id = parse_reference(entry.get("folder"))
        if not isinstance(folder_id, int):
            continue
        normalized = {
            "id": entry.get("id"),
            "name": _normalize_attachment_name(entry),
            "extension": _normalize_attachment_extension(entry.get("extension") or entry.get("type")),
            "url": entry.get("url"),
            "preview_url": entry.get("proxy_url") or entry.get("url"),
            "image": _attachment_is_image(entry),
        }
        folder_files.setdefault(folder_id, []).append(normalized)

    folders: List[Dict[str, Any]] = []
    for folder_id, entry in folder_lookup.items():
        parent_id = parse_reference(entry.get("parent"))
        path_value = entry.get("path") or _build_folder_path(folder_id, folder_lookup)
        files = folder_files.get(folder_id, [])
        image_file = next((item for item in files if item.get("image")), None)
        if not image_file and files:
            image_file = files[0]
        folders.append(
            {
                "id": folder_id,
                "name": _folder_display_name(entry),
                "parent_id": parent_id,
                "path": path_value or _folder_display_name(entry),
                "file_count": len(files),
                "photo": {
                    "name": image_file.get("name"),
                    "url": image_file.get("url"),
                    "preview_url": image_file.get("preview_url"),
                    "extension": image_file.get("extension"),
                }
                if image_file
                else None,
            }
        )

    folders.sort(key=lambda item: str(item.get("path") or item.get("name") or "").lower())
    return folders


def _equipment_group_display_name(entry: Mapping[str, Any]) -> str:
    for key in ("path", "displayname", "name", "description"):
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    identifier = entry.get("id")
    return f"Gruppo {identifier}" if identifier is not None else "Gruppo"


def _build_equipment_group_path(
    group_id: int,
    lookup: Mapping[int, Mapping[str, Any]],
    *,
    max_depth: int = 20,
) -> str:
    parts: List[str] = []
    current = group_id
    visited: Set[int] = set()
    depth = 0
    while isinstance(current, int) and current not in visited and depth < max_depth:
        visited.add(current)
        entry = lookup.get(current)
        if not entry:
            break
        parts.append(_equipment_group_display_name(entry))
        current = parse_reference(entry.get("parent"))
        depth += 1
    if not parts:
        return ""
    return " / ".join(reversed(parts))


def _collect_material_groups(client: RentmanClient, project_id: int) -> Dict[int, Dict[str, Any]]:
    try:
        raw_groups = client.get_project_equipment_groups(project_id)
    except RentmanError as exc:
        app.logger.error("Rentman: errore recuperando i gruppi materiali del progetto %s: %s", project_id, exc)
        return {}

    group_lookup: Dict[int, Mapping[str, Any]] = {}
    for entry in raw_groups:
        group_id = parse_reference(entry.get("id")) or entry.get("id")
        if isinstance(group_id, int):
            group_lookup[group_id] = entry

    result: Dict[int, Dict[str, Any]] = {}
    for group_id, entry in group_lookup.items():
        parent_id = parse_reference(entry.get("parent"))
        path_value = entry.get("path")
        if not isinstance(path_value, str) or not path_value.strip():
            path_value = _build_equipment_group_path(group_id, group_lookup)
        result[group_id] = {
            "id": group_id,
            "name": _equipment_group_display_name(entry),
            "parent_id": parent_id,
            "path": path_value or _equipment_group_display_name(entry),
        }

    return result


def fetch_project_attachments(project_code: Optional[str], *, exhaustive: bool = False) -> List[Dict[str, Any]]:
    attachments: List[Dict[str, Any]] = []
    code = (project_code or "").strip()
    if not code:
        return attachments

    client = get_rentman_client()
    if not client:
        return attachments

    try:
        project = client.find_project(code)
    except RentmanNotFound:
        app.logger.warning("Rentman: nessun progetto per allegati %s", code)
        return attachments
    except (RentmanAuthError, RentmanAPIError) as exc:
        app.logger.error("Rentman: errore durante la ricerca degli allegati per %s: %s", code, exc)
        return attachments

    if not project:
        app.logger.info("Rentman: progetto %s non trovato, nessun allegato", code)
        return attachments

    project_id = parse_reference(project.get("id")) or project.get("id")
    if not isinstance(project_id, int):
        app.logger.warning("Rentman: allegati non disponibili, id progetto non valido per %s (%s)", code, project.get("id"))
        return attachments

    try:
        app.logger.info(
            "Rentman: recupero allegati progetto %s (id=%s, exhaustive=%s)",
            code,
            project_id,
            exhaustive,
        )
        files = client.get_project_files(project_id, exhaustive=exhaustive)
        app.logger.info(
            "Rentman: payload files raw (primi 3)=\n%s",
            json.dumps(files[:3], ensure_ascii=False, indent=2) if files else "[]",
        )
    except RentmanNotFound:
        app.logger.warning("Rentman: endpoint files non trovato (404) per progetto %s", code)
        files = []
    except RentmanAuthError as exc:
        app.logger.error("Rentman: autenticazione fallita leggendo allegati per %s: %s", code, exc)
        files = []
    except RentmanAPIError as exc:
        app.logger.error(
            "Rentman: errore leggendo gli allegati del progetto %s: %s",
            code,
            exc,
        )
        files = []

    app.logger.info("Rentman: ricevuti %s allegati per progetto %s", len(files), code)

    for entry in files:
        if not isinstance(entry, Mapping):
            continue
        attachments.append(
            {
                "id": entry.get("id"),
                "name": _normalize_attachment_name(entry),
                "created": entry.get("created"),
                "type": _normalize_attachment_extension(entry.get("extension") or entry.get("type")),
                "size": entry.get("size"),
                "url": entry.get("url"),
                "preview_url": entry.get("proxy_url") or entry.get("url"),
            }
        )

    attachments.sort(key=lambda item: str(item.get("name") or "").lower())
    return attachments


def _normalize_material_name(entry: Mapping[str, Any]) -> str:
    for key in ("displayname", "name", "description"):
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    identifier = entry.get("id")
    return f"Materiale {identifier}" if identifier is not None else "Materiale"


def _extract_material_quantity(entry: Mapping[str, Any]) -> Tuple[Optional[float], str]:
    numeric_candidate = entry.get("quantity_total")
    quantity_label = ""
    quantity_value: Optional[float] = None
    if isinstance(numeric_candidate, (int, float)):
        quantity_value = float(numeric_candidate)

    raw_quantity = entry.get("quantity")
    if quantity_value is None:
        if isinstance(raw_quantity, (int, float)):
            quantity_value = float(raw_quantity)
        elif isinstance(raw_quantity, str):
            slug = raw_quantity.strip().replace(",", ".")
            try:
                quantity_value = float(slug)
            except ValueError:
                quantity_value = None

    if quantity_value is not None:
        if quantity_value.is_integer():
            quantity_label = str(int(quantity_value))
        else:
            quantity_label = f"{quantity_value:.2f}".rstrip("0").rstrip(".")

    if not quantity_label and isinstance(raw_quantity, str) and raw_quantity.strip():
        quantity_label = raw_quantity.strip()
    elif not quantity_label and isinstance(numeric_candidate, (int, float)):
        quantity_label = str(numeric_candidate)

    return quantity_value, quantity_label or "0"


def _material_status(entry: Mapping[str, Any]) -> Tuple[str, str]:
    if _is_truthy(entry.get("has_missings")):
        return "missing", "Mancanze"
    if _is_truthy(entry.get("delay_notified")):
        return "delayed", "In ritardo"
    subrent = _coerce_int(entry.get("subrent_reservations")) or 0
    if subrent > 0:
        return "subrent", "Subnoleggio"
    reserved = _coerce_int(entry.get("warehouse_reservations")) or 0
    if reserved > 0:
        return "reserved", "Riservato"
    if _is_truthy(entry.get("is_option")):
        return "option", "Opzione"
    return "planned", "Pianificato"


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        slug = value.strip().replace(",", ".")
        if not slug:
            return None
        try:
            return float(slug)
        except ValueError:
            return None
    return None


def _format_dimension_value(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    if value.is_integer():
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _format_dimensions_label(length: Optional[float], width: Optional[float], height: Optional[float]) -> str:
    components: List[str] = []
    for component in (length, width, height):
        label = _format_dimension_value(component)
        components.append(label or "0")
    if not any(component for component in components if component != "0"):
        return "---"
    return "x".join(components)


def _format_weight_label(value: Optional[float]) -> str:
    if value is None:
        return "---"
    label = _format_dimension_value(value)
    if not label:
        return "---"
    return f"{label} kg"


def _resolve_equipment_meta(
    equipment_ref: Any,
    client: RentmanClient,
    cache: Dict[int, Optional[Mapping[str, Any]]],
) -> Optional[Mapping[str, Any]]:
    equipment_id = parse_reference(equipment_ref)
    if not isinstance(equipment_id, int):
        return None
    if equipment_id in cache:
        return cache[equipment_id]
    try:
        meta = client.get_equipment(equipment_id)
    except RentmanError as exc:
        app.logger.error("Rentman: errore recuperando equipment %s: %s", equipment_id, exc)
        cache[equipment_id] = None
        return None
    cache[equipment_id] = meta
    return meta


def _resolve_photo_payload(
    reference: Any,
    client: RentmanClient,
    cache: Dict[int, Optional[Mapping[str, Any]]],
) -> Optional[Dict[str, Any]]:
    if not reference:
        return None

    if isinstance(reference, str) and reference.startswith("http"):
        return {"name": "Foto materiale", "url": reference, "preview_url": reference}

    file_id = parse_reference(reference)
    if not isinstance(file_id, int):
        return None

    if file_id in cache:
        file_entry = cache[file_id]
    else:
        try:
            file_entry = client.get_file(file_id)
        except RentmanError as exc:
            app.logger.error("Rentman: errore recuperando file %s: %s", file_id, exc)
            file_entry = None
        cache[file_id] = file_entry

    if not file_entry:
        return None

    url = file_entry.get("url")
    preview = file_entry.get("proxy_url") or url
    if not url and not preview:
        return None

    return {
        "name": _normalize_attachment_name(file_entry),
        "url": url or preview,
        "preview_url": preview or url,
        "extension": _normalize_attachment_extension(file_entry.get("extension") or file_entry.get("type")),
    }


def fetch_project_materials(project_code: Optional[str]) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {"items": [], "folders": []}
    code = (project_code or "").strip()
    if not code:
        return result

    client = get_rentman_client()
    if not client:
        return result

    try:
        project = client.find_project(code)
    except RentmanNotFound:
        app.logger.warning("Rentman: nessun progetto per materiali %s", code)
        return result
    except (RentmanAuthError, RentmanAPIError) as exc:
        app.logger.error("Rentman: errore durante la ricerca dei materiali per %s: %s", code, exc)
        return result

    if not project:
        app.logger.info("Rentman: progetto %s non trovato, nessun materiale", code)
        return result

    project_id = parse_reference(project.get("id")) or project.get("id")
    if not isinstance(project_id, int):
        app.logger.warning("Rentman: materiali non disponibili, id progetto non valido per %s (%s)", code, project.get("id"))
        return result

    try:
        records = client.get_project_planned_equipment(project_id)
        app.logger.info(
            "Rentman: materiali pianificati raw (primi 3)=\n%s",
            json.dumps(records[:3], ensure_ascii=False, indent=2) if records else "[]",
        )
    except RentmanError as exc:
        app.logger.error("Rentman: errore leggendo i materiali del progetto %s: %s", code, exc)
        return result

    equipment_cache: Dict[int, Optional[Mapping[str, Any]]] = {}
    file_cache: Dict[int, Optional[Mapping[str, Any]]] = {}
    group_lookup = _collect_material_groups(client, project_id)
    default_group_label = "Altri materiali"
    materials: List[Dict[str, Any]] = []
    for entry in records:
        if not isinstance(entry, Mapping):
            continue
        quantity_value, quantity_label = _extract_material_quantity(entry)
        status_code, status_label = _material_status(entry)
        equipment_meta = _resolve_equipment_meta(entry.get("equipment"), client, equipment_cache)
        length = _coerce_float(entry.get("length")) or ( _coerce_float(equipment_meta.get("length")) if equipment_meta else None)
        width = _coerce_float(entry.get("width")) or ( _coerce_float(equipment_meta.get("width")) if equipment_meta else None)
        height = _coerce_float(entry.get("height")) or ( _coerce_float(equipment_meta.get("height")) if equipment_meta else None)
        weight_value = _coerce_float(entry.get("weight"))
        if weight_value is None and equipment_meta:
            weight_value = _coerce_float(equipment_meta.get("weight"))
        dimensions_label = _format_dimensions_label(length, width, height)
        weight_label = _format_weight_label(weight_value)
        image_reference = entry.get("image") or (equipment_meta.get("image") if equipment_meta else None)
        photo_payload = _resolve_photo_payload(image_reference, client, file_cache)
        group_id = parse_reference(entry.get("equipment_group"))
        group_entry = group_lookup.get(group_id) if isinstance(group_id, int) else None
        group_name = group_entry.get("name") if group_entry else default_group_label
        group_path = group_entry.get("path") if group_entry else group_name
        notes: List[str] = []
        for key in ("internal_remark", "external_remark"):
            value = entry.get(key)
            if isinstance(value, str):
                stripped = value.strip()
                if stripped:
                    notes.append(stripped)
        note_text = " · ".join(dict.fromkeys(notes)) if notes else ""
        materials.append(
            {
                "id": entry.get("id"),
                "name": _normalize_material_name(entry),
                "quantity": quantity_value,
                "quantity_label": quantity_label,
                "period_start": entry.get("planperiod_start"),
                "period_end": entry.get("planperiod_end"),
                "note": note_text,
                "status": status_label,
                "status_code": status_code,
                "has_missings": bool(_is_truthy(entry.get("has_missings"))),
                "is_option": bool(_is_truthy(entry.get("is_option"))),
                "dimensions_label": dimensions_label,
                "weight_label": weight_label,
                "photo": photo_payload,
                "group_id": group_id,
                "group_name": group_name,
                "group_path": group_path,
            }
        )

    materials.sort(
        key=lambda item: (
            str(item.get("group_path") or item.get("group_name") or "").lower(),
            item.get("status_code"),
            str(item.get("name") or "").lower(),
        )
    )

    folders = _collect_project_folders(client, project_id)
    result["items"] = materials
    result["folders"] = folders
    return result


def now_ms() -> int:
    return int(time.time() * 1000)


def ensure_app_users_table(db: DatabaseLike) -> None:
    statement = APP_USERS_TABLE_MYSQL if DB_VENDOR == "mysql" else APP_USERS_TABLE_SQLITE
    cursor = db.execute(statement)
    try:
        cursor.close()
    except AttributeError:
        pass
    
    # Migrazione: aggiungi colonna full_name se non esiste
    if DB_VENDOR == "mysql":
        try:
            db.execute("ALTER TABLE app_users ADD COLUMN full_name VARCHAR(255) DEFAULT NULL")
            db.commit()
        except Exception:
            pass  # Colonna già esistente
        # Migrazione: aggiungi colonna rentman_crew_id se non esiste
        try:
            db.execute("ALTER TABLE app_users ADD COLUMN rentman_crew_id INT DEFAULT NULL")
            db.commit()
        except Exception:
            pass  # Colonna già esistente
        # Migrazione: aggiungi colonne per progetto corrente supervisor
        try:
            db.execute("ALTER TABLE app_users ADD COLUMN current_project_code VARCHAR(64) DEFAULT NULL")
            db.commit()
        except Exception:
            pass
        try:
            db.execute("ALTER TABLE app_users ADD COLUMN current_project_name VARCHAR(255) DEFAULT NULL")
            db.commit()
        except Exception:
            pass
    else:
        # SQLite
        try:
            db.execute("ALTER TABLE app_users ADD COLUMN full_name TEXT")
            db.commit()
        except Exception:
            pass
        try:
            db.execute("ALTER TABLE app_users ADD COLUMN rentman_crew_id INTEGER")
            db.commit()
        except Exception:
            pass
        try:
            db.execute("ALTER TABLE app_users ADD COLUMN current_project_code TEXT")
            db.commit()
        except Exception:
            pass
        try:
            db.execute("ALTER TABLE app_users ADD COLUMN current_project_name TEXT")
            db.commit()
        except Exception:
            pass


def ensure_session_override_table(db: DatabaseLike) -> None:
    if DB_VENDOR == "mysql":
        cursor = db.execute(SESSION_OVERRIDES_TABLE_MYSQL)
        try:
            cursor.close()
        except AttributeError:
            pass
        # Migrazione: aggiungi colonna project_code se non esiste
        try:
            db.execute("ALTER TABLE activity_session_overrides ADD COLUMN project_code VARCHAR(64) DEFAULT NULL")
            db.commit()
        except Exception:
            pass  # Colonna già esistente
        try:
            db.execute("CREATE INDEX idx_session_override_project ON activity_session_overrides(project_code)")
            db.commit()
        except Exception:
            pass  # Indice già esistente


def ensure_persistent_session_table(db: DatabaseLike) -> None:
    statement = (
        PERSISTENT_SESSIONS_TABLE_MYSQL if DB_VENDOR == "mysql" else PERSISTENT_SESSIONS_TABLE_SQLITE
    )
    for stmt in statement.strip().split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        cursor = db.execute(sql)
        try:
            cursor.close()
        except AttributeError:
            pass


def ensure_equipment_checks_table(db: DatabaseLike) -> None:
    statement = EQUIPMENT_CHECKS_TABLE_MYSQL if DB_VENDOR == "mysql" else EQUIPMENT_CHECKS_TABLE_SQLITE
    for stmt in statement.strip().split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        cursor = db.execute(sql)
        try:
            cursor.close()
        except AttributeError:
            pass


def ensure_project_materials_cache_table(db: DatabaseLike) -> None:
    statement = (
        PROJECT_MATERIALS_CACHE_TABLE_MYSQL if DB_VENDOR == "mysql" else PROJECT_MATERIALS_CACHE_TABLE_SQLITE
    )
    for stmt in statement.strip().split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        cursor = db.execute(sql)
        try:
            cursor.close()
        except AttributeError:
            pass


def ensure_local_equipment_table(db: DatabaseLike) -> None:
    statement = LOCAL_EQUIPMENT_TABLE_MYSQL if DB_VENDOR == "mysql" else LOCAL_EQUIPMENT_TABLE_SQLITE
    for stmt in statement.strip().split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        cursor = db.execute(sql)
        try:
            cursor.close()
        except AttributeError:
            pass


def ensure_project_photos_table(db: DatabaseLike) -> None:
    statement = PROJECT_PHOTOS_TABLE_MYSQL if DB_VENDOR == "mysql" else PROJECT_PHOTOS_TABLE_SQLITE
    for stmt in statement.strip().split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        cursor = db.execute(sql)
        try:
            cursor.close()
        except AttributeError:
            pass


# Cartella per salvare le foto del progetto
PHOTOS_UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads", "photos")
os.makedirs(PHOTOS_UPLOAD_FOLDER, exist_ok=True)

ALLOWED_PHOTO_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp", "heic", "heif"}
MAX_PHOTO_SIZE = 10 * 1024 * 1024  # 10 MB


def allowed_photo_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_PHOTO_EXTENSIONS


def _last_insert_id(db: DatabaseLike) -> Optional[int]:
    query = "SELECT LAST_INSERT_ID() AS lid" if DB_VENDOR == "mysql" else "SELECT last_insert_rowid() AS lid"
    row = db.execute(query).fetchone()
    if not row:
        return None
    value = row.get("lid") if isinstance(row, Mapping) else row[0]
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
        return

    for stmt in SESSION_OVERRIDES_TABLE_SQLITE.strip().split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        cursor = db.execute(sql)
        try:
            cursor.close()
        except AttributeError:
            pass


def init_db() -> None:
    if DB_VENDOR == "mysql":
        db = MySQLConnection(DATABASE_SETTINGS)
        try:
            for statement in MYSQL_SCHEMA_STATEMENTS:
                cursor = db.execute(statement)
                cursor.close()
            _ensure_entered_ts_column(db, "BIGINT")
            purge_legacy_seed(db)
            ensure_app_users_table(db)
            ensure_session_override_table(db)
            ensure_persistent_session_table(db)
            ensure_equipment_checks_table(db)
            ensure_project_materials_cache_table(db)
            bootstrap_user_store(db)
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
                activity_id TEXT NOT NULL,
                project_code TEXT NOT NULL DEFAULT '',
                label TEXT NOT NULL,
                sort_order INTEGER NOT NULL,
                plan_start TEXT,
                plan_end TEXT,
                planned_members INTEGER,
                planned_duration_ms INTEGER,
                notes TEXT,
                PRIMARY KEY (activity_id, project_code)
            );

            CREATE TABLE IF NOT EXISTS member_state (
                member_key TEXT NOT NULL,
                project_code TEXT NOT NULL DEFAULT '',
                member_name TEXT NOT NULL,
                activity_id TEXT,
                running INTEGER NOT NULL DEFAULT 0,
                start_ts INTEGER,
                elapsed_cached INTEGER NOT NULL DEFAULT 0,
                pause_start INTEGER,
                entered_ts INTEGER,
                PRIMARY KEY (member_key, project_code)
            );

            CREATE TABLE IF NOT EXISTS event_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_code TEXT NOT NULL DEFAULT '',
                ts INTEGER NOT NULL,
                kind TEXT NOT NULL,
                member_key TEXT,
                details TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_event_project ON event_log(project_code);

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

            CREATE TABLE IF NOT EXISTS app_users (
                username TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
        ensure_app_users_table(db)
                display_name TEXT NOT NULL,
                full_name TEXT,
                role TEXT NOT NULL DEFAULT 'user',
                is_active INTEGER NOT NULL DEFAULT 1,
                created_ts INTEGER NOT NULL,
                updated_ts INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS equipment_checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_code TEXT NOT NULL,
                item_key TEXT NOT NULL,
                checked_ts INTEGER NOT NULL,
                username TEXT,
                created_ts INTEGER NOT NULL,
                updated_ts INTEGER NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_equipment_project_item ON equipment_checks(project_code, item_key);
            CREATE INDEX IF NOT EXISTS idx_equipment_project ON equipment_checks(project_code);
            CREATE TABLE IF NOT EXISTS project_materials_cache (
                project_code TEXT PRIMARY KEY,
                project_name TEXT NOT NULL,
                data_json TEXT NOT NULL,
                created_ts INTEGER NOT NULL,
                updated_ts INTEGER NOT NULL
            );
            """
        )
        _ensure_entered_ts_column(db, "INTEGER")
        purge_legacy_seed(db)
        ensure_persistent_session_table(db)
        ensure_equipment_checks_table(db)
        ensure_project_materials_cache_table(db)
        bootstrap_user_store(db)
        db.commit()
    finally:
        db.close()


def _ensure_entered_ts_column(db: DatabaseLike, column_type: str) -> None:
    try:
        db.execute(f"ALTER TABLE member_state ADD COLUMN entered_ts {column_type}")
    except Exception:
        return


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


def clear_project_state(db: DatabaseLike, project_code: Optional[str] = None) -> None:
    """Rimuove il progetto e i relativi dati dal database."""
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    if project_code:
        # Cancella solo i dati del progetto specifico
        db.execute(f"DELETE FROM activities WHERE project_code = {placeholder}", (project_code,))
        db.execute(f"DELETE FROM member_state WHERE project_code = {placeholder}", (project_code,))
        db.execute(f"DELETE FROM event_log WHERE project_code = {placeholder}", (project_code,))
        delete_project_materials_cache(db, project_code)
    else:
        # Fallback: cancella tutto (per retrocompatibilità)
        db.execute("DELETE FROM activities")
        db.execute("DELETE FROM member_state")
        db.execute("DELETE FROM event_log")
        db.execute(
            f"DELETE FROM app_state WHERE {APP_STATE_KEY_COLUMN} IN ('project_code','project_name','activity_plan_meta','push_notified_activities','long_running_member_notifications')"
        )


def has_active_member_sessions(db: DatabaseLike) -> bool:
    """Restituisce True se esistono timer in corso o posti in pausa."""

    row = db.execute(
        """
        SELECT 1 FROM member_state
        WHERE running=? OR pause_start IS NOT NULL OR COALESCE(elapsed_cached, 0) > 0
        LIMIT 1
        """,
        (RUN_STATE_RUNNING,),
    ).fetchone()
    return row is not None


def apply_project_plan(db: DatabaseLike, plan: Dict[str, Any]) -> None:
    activities = list(plan.get("activities") or [])
    team = list(plan.get("team") or [])
    project_code = str(plan.get("project_code") or "UNKNOWN")
    project_name = str(plan.get("project_name") or project_code)
    
    # Cancella solo i dati del progetto specifico (non tocca altri progetti)
    clear_project_state(db, project_code)

    planned_counts: Dict[str, int] = {}
    for member in team:
        activity_id = (member.get("activity_id") or "").strip()
        if activity_id:
            planned_counts[activity_id] = planned_counts.get(activity_id, 0) + 1

    activity_rows: List[tuple] = []
    for index, activity in enumerate(activities, start=1):
        activity_id = activity.get("id")
        activity_key = (str(activity_id).strip() if activity_id is not None else "")
        plan_start = activity.get("plan_start")
        plan_end = activity.get("plan_end")
        planned_members = planned_counts.get(activity_key, 0)
        planned_duration_ms = compute_planned_duration_ms(
            plan_start,
            plan_end,
            planned_members,
        )
        activity_rows.append(
            (
                activity_id,
                project_code,
                activity.get("label"),
                index,
                plan_start,
                plan_end,
                planned_members,
                planned_duration_ms,
                activity.get("notes"),
            )
        )

    db.executemany(
        """
        INSERT INTO activities(
            activity_id, project_code, label, sort_order, plan_start, plan_end,
            planned_members, planned_duration_ms, notes
        ) VALUES(?,?,?,?,?,?,?,?,?)
        """,
        activity_rows,
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
        running = RUN_STATE_PAUSED
        start_ts = None
        member_rows.append(
            (key, project_code, name, activity_id, running, start_ts, 0, None, None)
        )

    if member_rows:
        db.executemany(
            """
            INSERT INTO member_state(
                member_key, project_code, member_name, activity_id, running, start_ts, elapsed_cached, pause_start, entered_ts
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            member_rows,
        )

    activity_meta = {}
    for activity in activities:
        activity_id = activity.get("id")
        if not activity_id:
            continue
        key = str(activity_id).strip()
        if not key:
            continue
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
        try:
            ensure_activity_schema(g.db)
            ensure_project_code_columns(g.db)
            ensure_app_users_table(g.db)
            ensure_session_override_table(g.db)
            ensure_persistent_session_table(g.db)
            ensure_equipment_checks_table(g.db)
            ensure_project_materials_cache_table(g.db)
            ensure_warehouse_manual_projects_table(g.db)
        except Exception:
            app.logger.exception("Impossibile aggiornare lo schema attività")
    return g.db


@app.teardown_appcontext
def close_db(_: BaseException | None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def compute_elapsed(row: Mapping[str, Any], reference: int) -> int:
    elapsed = row["elapsed_cached"] or 0
    if row["running"] == RUN_STATE_RUNNING:
        start_ts = row["start_ts"] or reference
        elapsed += max(0, reference - start_ts)
    return elapsed


def row_value(row: Mapping[str, Any], key: str) -> Optional[Any]:
    if hasattr(row, "get"):
        try:
            return row.get(key)  # type: ignore[call-arg]
        except Exception:
            pass
    try:
        return row[key]
    except Exception:
        return None


def fetch_equipment_checks(db: DatabaseLike, project_code: Optional[str]) -> Dict[str, int]:
    if not project_code:
        return {}
    rows = db.execute(
        "SELECT item_key, checked_ts FROM equipment_checks WHERE project_code=?",
        (project_code,),
    ).fetchall()
    result: Dict[str, int] = {}
    for row in rows:
        item_key = row_value(row, "item_key")
        if not item_key:
            continue
        timestamp_raw = row_value(row, "checked_ts")
        try:
            timestamp_int = int(timestamp_raw)
        except (TypeError, ValueError):
            continue
        result[str(item_key)] = timestamp_int
    return result


def persist_equipment_check(
    db: DatabaseLike,
    *,
    project_code: str,
    item_key: str,
    checked: bool,
    username: Optional[str] = None,
) -> Optional[int]:
    normalized_project = (project_code or "").strip()
    normalized_item = (item_key or "").strip()
    if not normalized_project or not normalized_item:
        return None
    if checked:
        now = now_ms()
        if DB_VENDOR == "mysql":
            db.execute(
                """
                INSERT INTO equipment_checks(project_code, item_key, checked_ts, username, created_ts, updated_ts)
                VALUES(?,?,?,?,?,?)
                ON DUPLICATE KEY UPDATE checked_ts=VALUES(checked_ts), username=VALUES(username), updated_ts=VALUES(updated_ts)
                """,
                (normalized_project, normalized_item, now, username, now, now),
            )
        else:
            db.execute(
                """
                INSERT INTO equipment_checks(project_code, item_key, checked_ts, username, created_ts, updated_ts)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(project_code, item_key) DO UPDATE SET
                    checked_ts=excluded.checked_ts,
                    username=excluded.username,
                    updated_ts=excluded.updated_ts
                """,
                (normalized_project, normalized_item, now, username, now, now),
            )
        return now

    db.execute(
        "DELETE FROM equipment_checks WHERE project_code=? AND item_key=?",
        (normalized_project, normalized_item),
    )
    return None


def load_project_materials_cache(db: DatabaseLike, project_code: Optional[str]) -> Optional[Dict[str, Any]]:
    if not project_code:
        return None
    row = db.execute(
        "SELECT project_name, data_json, updated_ts FROM project_materials_cache WHERE project_code=?",
        (project_code,),
    ).fetchone()
    if not row:
        return None
    raw_payload = row_value(row, "data_json")
    try:
        payload = json.loads(raw_payload) if raw_payload else {}
    except json.JSONDecodeError:
        payload = {}
    items = payload.get("items")
    if not isinstance(items, list):
        items = []
    folders = payload.get("folders")
    if not isinstance(folders, list):
        folders = []
    return {
        "project": {
            "code": project_code,
            "name": row_value(row, "project_name") or project_code,
        },
        "items": items,
        "folders": folders,
        "updated_ts": row_value(row, "updated_ts"),
    }


def save_project_materials_cache(
    db: DatabaseLike,
    project_code: Optional[str],
    project_name: Optional[str],
    *,
    items: Sequence[Mapping[str, Any]] | Sequence[Any],
    folders: Sequence[Mapping[str, Any]] | Sequence[Any],
) -> int:
    if not project_code:
        return 0
    normalized_name = project_name or project_code
    sanitized_items = list(items or [])
    sanitized_folders = list(folders or [])
    payload = json.dumps({"items": sanitized_items, "folders": sanitized_folders}, ensure_ascii=False)
    now = now_ms()
    if DB_VENDOR == "mysql":
        db.execute(
            """
            INSERT INTO project_materials_cache(project_code, project_name, data_json, created_ts, updated_ts)
            VALUES(?,?,?,?,?)
            ON DUPLICATE KEY UPDATE project_name=VALUES(project_name), data_json=VALUES(data_json), updated_ts=VALUES(updated_ts)
            """,
            (project_code, normalized_name, payload, now, now),
        )
    else:
        db.execute(
            """
            INSERT INTO project_materials_cache(project_code, project_name, data_json, created_ts, updated_ts)
            VALUES(?,?,?,?,?)
            ON CONFLICT(project_code) DO UPDATE SET
                project_name=excluded.project_name,
                data_json=excluded.data_json,
                updated_ts=excluded.updated_ts
            """,
            (project_code, normalized_name, payload, now, now),
        )
    return now


def delete_project_materials_cache(db: DatabaseLike, project_code: Optional[str]) -> None:
    if not project_code:
        return
    db.execute("DELETE FROM project_materials_cache WHERE project_code=?", (project_code,))


def find_last_move_ts(db: DatabaseLike, member_key: str, activity_id: str) -> Optional[int]:
    if not member_key or not activity_id:
        return None
    rows = db.execute(
        "SELECT ts, details FROM event_log WHERE member_key=? AND kind='move' ORDER BY ts DESC LIMIT 200",
        (member_key,),
    ).fetchall()
    for row in rows:
        try:
            details = json.loads(row["details"] or "{}")
        except Exception:
            continue
        if str(details.get("to")) == activity_id:
            return row["ts"]
    return None


def fetch_member(db: DatabaseLike, member_key: str, project_code: Optional[str] = None) -> Optional[Mapping[str, Any]]:
    if not member_key:
        return None
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    if project_code is not None:
        return db.execute(
            f"SELECT * FROM member_state WHERE member_key={placeholder} AND project_code={placeholder}",
            (member_key, project_code),
        ).fetchone()
    return db.execute(
        f"SELECT * FROM member_state WHERE member_key={placeholder}",
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
    
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    db.execute(
        f"""
        INSERT INTO push_notification_log(
            kind, activity_id, username, title, body, payload, sent_ts, created_ts
        ) VALUES({placeholder},{placeholder},{placeholder},{placeholder},{placeholder},{placeholder},{placeholder},{placeholder})
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
    db.commit()


def fetch_recent_push_notifications(
    db: DatabaseLike,
    *,
    username: str,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    sql = f"""
        SELECT id, kind, activity_id, username, title, body, payload, sent_ts, created_ts
        FROM push_notification_log
        WHERE username = {placeholder}
        ORDER BY sent_ts DESC, id DESC
    """
    params: List[Any] = [username]
    if limit is not None and limit > 0:
        safe_limit = max(1, min(limit, 1000))
        sql += f" LIMIT {placeholder}"
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


def parse_iso_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    try:
        return date.fromisoformat(candidate)
    except ValueError:
        return None


def _normalize_epoch_ms(value: Any) -> Optional[int]:
    ms = _coerce_int(value)
    if ms is None:
        return None
    return max(0, ms)


def _fetch_session_override_rows(
    db: DatabaseLike,
    *,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    member_filter: Optional[str] = None,
    activity_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    clauses: List[str] = []
    params: List[Any] = []

    if member_filter:
        clauses.append("LOWER(member_key)=LOWER(?)")
        params.append(member_filter.strip())
    if activity_filter:
        clauses.append("activity_id=?")
        params.append(activity_filter.strip())

    query = "SELECT * FROM activity_session_overrides"
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY start_ts DESC"

    rows = db.execute(query, tuple(params) if params else None).fetchall()
    results: List[Dict[str, Any]] = []
    for row in rows:
        record = dict(row)
        start_dt = datetime.fromtimestamp(record["start_ts"] / 1000, tz=timezone.utc).date()
        if start_date and start_dt < start_date:
            continue
        if end_date and start_dt > end_date:
            continue
        results.append(record)
    return results


def _override_row_to_session(row: Mapping[str, Any]) -> Dict[str, Any]:
    start_ts = int(row.get("start_ts") or 0)
    end_ts_value = row.get("end_ts")
    end_ts = int(end_ts_value) if end_ts_value is not None else start_ts
    net_ms = max(0, int(row.get("net_ms") or 0))
    pause_ms = max(0, int(row.get("pause_ms") or 0))
    pause_count = max(0, int(row.get("pause_count") or 0))
    manual_entry = bool(row.get("manual_entry"))
    note = row.get("note") or ""
    status = row.get("status") or "completed"

    payload = {
        "member_key": row.get("member_key"),
        "member_name": row.get("member_name"),
        "activity_id": row.get("activity_id"),
        "activity_label": row.get("activity_label") or row.get("activity_id"),
        "project_code": row.get("project_code") or "",
        "start_ts": start_ts,
        "end_ts": end_ts,
        "status": status if status in {"completed", "running"} else "completed",
        "net_ms": net_ms,
        "pause_ms": pause_ms,
        "pause_count": pause_count,
        "auto_closed": False,
        "override_id": row.get("id"),
        "manual_entry": manual_entry,
        "note": note,
        "source_member_key": row.get("source_member_key"),
        "source_activity_id": row.get("source_activity_id"),
        "source_start_ts": row.get("source_start_ts"),
    }
    return payload


def build_session_rows(
    db: DatabaseLike,
    *,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    member_filter: Optional[str] = None,
    activity_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    activity_rows = db.execute(
        "SELECT activity_id, label, planned_duration_ms FROM activities ORDER BY sort_order, label"
    ).fetchall()
    activity_map = {row["activity_id"]: row["label"] for row in activity_rows}
    activity_planned_map = {row["activity_id"]: row["planned_duration_ms"] for row in activity_rows}

    query = (
        "SELECT el.ts, el.kind, el.member_key, el.details, ms.member_name "
        "FROM event_log el "
        "LEFT JOIN member_state ms ON el.member_key = ms.member_key "
        "WHERE el.kind IN ('project_load', 'move', 'finish_activity', 'pause_member', 'resume_member') "
        "ORDER BY el.ts ASC"
    )
    event_rows = db.execute(query).fetchall()

    member_filter_norm = member_filter.strip().lower() if member_filter else None
    activity_filter_norm = activity_filter.strip() if activity_filter else None

    sessions: Dict[Tuple[str, str], Dict[str, Any]] = {}
    last_project_code: Optional[str] = None

    for row in event_rows:
        try:
            details = json.loads(row["details"]) if row["details"] else {}
        except json.JSONDecodeError:
            details = {}

        if row["kind"] == "project_load":
            candidate = details.get("project_code")
            if candidate:
                last_project_code = str(candidate).strip() or last_project_code
            continue

        if last_project_code and not details.get("project_code"):
            details["project_code"] = last_project_code

        member_key = row["member_key"]
        if not member_key:
            continue

        if member_filter_norm and member_key.lower() != member_filter_norm:
            continue

        if row["kind"] == "move":
            activity_id = details.get("to")
        else:
            activity_id = details.get("activity_id")

        if not activity_id:
            continue

        if activity_filter_norm and str(activity_id) != activity_filter_norm:
            continue

        ts = row["ts"]
        if ts is None:
            continue
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        event_date = dt.date()
        if start_date and event_date < start_date:
            continue
        if end_date and event_date > end_date:
            continue

        member_name = row["member_name"] or details.get("member_name") or "Operatore"

        session_key = (member_key, str(activity_id))
        if session_key not in sessions:
            sessions[session_key] = {
                "member_key": member_key,
                "member_name": member_name,
                "activity_id": str(activity_id),
                "events": [],
            }

        sessions[session_key]["events"].append(
            {
                "ts": ts,
                "dt": dt,
                "kind": row["kind"],
                "details": details,
            }
        )

    if not sessions:
        return []

    now_utc = datetime.now(tz=timezone.utc)
    results: List[Dict[str, Any]] = []

    for session_key, session in sessions.items():
        events = sorted(session["events"], key=lambda e: e["ts"])
        if not events:
            continue

        member_key, activity_id = session_key
        member_name = session["member_name"]
        activity_label = activity_map.get(activity_id, activity_id)

        start_event = None
        end_event = None
        pause_events: List[Dict[str, Any]] = []
        project_code = None

        for event in events:
            if event["kind"] == "move" and str(event["details"].get("to") or "") == activity_id:
                if not start_event:
                    start_event = event
                project_code = project_code or event["details"].get("project_code")
            elif event["kind"] == "pause_member":
                pause_events.append(event)
            elif event["kind"] == "finish_activity":
                end_event = event
                project_code = project_code or event["details"].get("project_code")

        if not start_event:
            start_event = events[0]
        if not start_event:
            continue

        start_dt = start_event["dt"]
        end_dt = end_event["dt"] if end_event else now_utc
        total_duration_ms = max(0, int((end_dt - start_dt).total_seconds() * 1000))

        net_duration_ms = total_duration_ms
        pause_duration_ms = 0
        if end_event:
            net_duration_ms = int(end_event["details"].get("duration_ms", total_duration_ms))
            pause_duration_ms = int(end_event["details"].get("pause_ms", max(0, total_duration_ms - net_duration_ms)))
        else:
            pause_duration_ms = max(0, total_duration_ms - net_duration_ms)

        pause_count = len(pause_events)
        status = "completed" if end_event else "running"

        end_ts_value = int(end_dt.timestamp() * 1000)
        
        # Ore preventivate per questa attività
        planned_ms = activity_planned_map.get(activity_id) or 0

        results.append(
            {
                "member_key": member_key,
                "member_name": member_name,
                "activity_id": activity_id,
                "activity_label": activity_label,
                "start_ts": int(start_dt.timestamp() * 1000),
                "end_ts": end_ts_value,
                "status": status,
                "net_ms": max(0, net_duration_ms),
                "pause_ms": max(0, pause_duration_ms),
                "pause_count": pause_count,
                "auto_closed": bool(end_event and end_event["details"].get("auto_close")),
                "project_code": project_code,
                "planned_ms": planned_ms,
                "override_id": None,
                "manual_entry": False,
                "note": "",
                "source_member_key": member_key,
                "source_activity_id": activity_id,
                "source_start_ts": int(start_dt.timestamp() * 1000),
            }
        )

    session_map: Dict[Tuple[str, str, int], Dict[str, Any]] = {
        (item["member_key"], item["activity_id"], item["start_ts"]): item
        for item in results
    }

    overrides = _fetch_session_override_rows(
        db,
        start_date=start_date,
        end_date=end_date,
        member_filter=member_filter,
        activity_filter=activity_filter,
    )

    merged: List[Dict[str, Any]] = []
    replaced_keys: Set[Tuple[str, str, int]] = set()

    for override_row in overrides:
        payload = _override_row_to_session(override_row)
        key = None
        if (
            payload.get("source_member_key")
            and payload.get("source_activity_id")
            and payload.get("source_start_ts")
        ):
            key = (
                str(payload["source_member_key"]),
                str(payload["source_activity_id"]),
                int(payload["source_start_ts"]),
            )
        if key and key in session_map:
            replaced_keys.add(key)
            session_map.pop(key, None)
        merged.append(payload)

    for key, item in session_map.items():
        if key in replaced_keys:
            continue
        merged.append(item)

    merged.sort(key=lambda item: item["start_ts"], reverse=True)
    return merged


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
        running_rows = [row for row in member_rows if row["running"] == RUN_STATE_RUNNING]
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
        WHERE ms.running=? AND ms.start_ts IS NOT NULL
        """,
        (RUN_STATE_RUNNING,),
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

    if kind == "create_activity":
        label = details.get("label") or label_for(details.get("activity_id"))
        return f"Nuova attività: {label or 'Attività'}"

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
    
    db = get_db()
    user_row = fetch_user_record(db, username_input)
    if not user_row:
        return jsonify({'success': False, 'error': 'Credenziali non valide'}), 401

    if not user_row.get('is_active'):
        return jsonify({'success': False, 'error': 'Account disabilitato'}), 403

    password_hash = user_row.get('password_hash') or ''
    if not verify_password(password, password_hash):
        return jsonify({'success': False, 'error': 'Credenziali non valide'}), 401

    username = user_row.get('username') or _normalize_username(username_input)
    _apply_user_session({**dict(user_row), 'username': username})
    response = jsonify({'success': True})
    try:
        token_value, expires_ts = _store_persistent_session(username)
    except Exception:
        app.logger.exception("Impossibile creare una sessione persistente per %s", username)
    else:
        _set_persistent_cookie(response, token_value, expires_ts)
    return response


@app.route("/logout")
def logout():
    """Clear session and redirect to login."""
    token = request.cookies.get(_persistent_cookie_name())
    if token:
        _delete_persistent_session(token)
    session.clear()
    response = redirect(url_for('login'))
    response.delete_cookie(_persistent_cookie_name(), path='/')
    return response


@app.route("/")
def home() -> ResponseReturnValue:
    if 'user' not in session:
        return redirect(url_for('login'))
    if session.get('is_admin'):
        return redirect(url_for('admin_dashboard_page'))
    if session.get('user_role') == ROLE_MAGAZZINO:
        return redirect(url_for('magazzino_home'))
    # Utenti "user" senza ruolo specifico - mostrano pagina minima con solo logout
    if session.get('user_role') == ROLE_USER:
        display_name = session.get('user_display') or session.get('user_name') or session.get('user')
        primary_name = session.get('user_name') or display_name or session.get('user')
        initials = session.get('user_initials') or compute_initials(primary_name or "")
        user_role = session.get('user_role', 'user')
        username = session.get('user')
        return render_template(
            "user_home.html",
            user_name=primary_name,
            user_display=display_name,
            user_initials=initials,
            user_role=user_role,
            username=username,
        )
    # Supervisor e altri ruoli - mostrano dashboard operativa completa
    db = get_db()
    display_name = session.get('user_display') or session.get('user_name') or session.get('user')
    primary_name = session.get('user_name') or display_name or session.get('user')
    initials = session.get('user_initials') or compute_initials(primary_name or "")
    is_admin = bool(session.get('is_admin'))
    project_code = get_app_state(db, "project_code")
    project_name = get_app_state(db, "project_name")
    initial_attachments: Dict[str, Any] = {"project": None, "items": []}
    initial_materials: Dict[str, Any] = {
        "project": None,
        "items": [],
        "folders": [],
        "equipment_checks": {},
        "updated_ts": None,
        "from_cache": False,
    }
    initial_project = None
    if project_code:
        initial_project = {
            "code": project_code,
            "name": project_name or project_code,
        }
        initial_attachments["project"] = initial_project
        initial_materials["project"] = initial_project
        initial_materials["equipment_checks"] = fetch_equipment_checks(db, project_code)
        cached_materials = load_project_materials_cache(db, project_code)
        if cached_materials:
            initial_materials["items"] = cached_materials.get("items", [])
            initial_materials["folders"] = cached_materials.get("folders", [])
            cached_project = cached_materials.get("project")
            if cached_project:
                initial_materials["project"] = cached_project
            initial_materials["updated_ts"] = cached_materials.get("updated_ts")
            initial_materials["from_cache"] = True
    header_clock = datetime.now().strftime("%d/%m/%Y | %H:%M")

    # Progetto salvato del supervisor (per auto-load se necessario)
    saved_supervisor_project = None
    supervisor_project_code = session.get('supervisor_project_code')
    if supervisor_project_code:
        saved_supervisor_project = {
            "code": supervisor_project_code,
            "name": session.get('supervisor_project_name') or supervisor_project_code,
        }

    return render_template(
        "index.html",
        user_name=primary_name,
        user_display=display_name,
        user_initials=initials,
        is_admin=is_admin,
        user_role=session.get('user_role', 'user'),
        initial_attachments=initial_attachments,
        initial_materials=initial_materials,
        initial_project=initial_project,
        saved_supervisor_project=saved_supervisor_project,
        header_clock=header_clock,
    )


# ============================================================
# API TIMBRATURA
# ============================================================

@app.get("/api/timbratura/oggi")
@login_required
def api_timbratura_oggi():
    """Restituisce le timbrature dell'utente per oggi."""
    username = session.get('user')
    if not username:
        return jsonify({"error": "Non autenticato"}), 401
    
    db = get_db()
    ensure_timbrature_table(db)
    
    today = datetime.now().strftime("%Y-%m-%d")
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    rows = db.execute(
        f"""
        SELECT tipo, ora, ora_mod FROM timbrature
        WHERE username = {placeholder} AND data = {placeholder}
        ORDER BY created_ts ASC
        """,
        (username, today)
    ).fetchall()
    
    timbrature = []
    for row in rows:
        ora_val = row['ora'] if isinstance(row, dict) else row[1]
        ora_mod_val = row['ora_mod'] if isinstance(row, dict) else row[2]
        # Gestisce sia TIME che stringa
        if hasattr(ora_val, 'strftime'):
            ora_str = ora_val.strftime("%H:%M")
        else:
            ora_str = str(ora_val)[:5] if ora_val else ""
        
        # Formatta ora_mod
        ora_mod_str = None
        if ora_mod_val:
            if hasattr(ora_mod_val, 'strftime'):
                ora_mod_str = ora_mod_val.strftime("%H:%M")
            else:
                ora_mod_str = str(ora_mod_val)[:5]
        
        timbrature.append({
            "tipo": row['tipo'] if isinstance(row, dict) else row[0],
            "ora": ora_str,
            "ora_mod": ora_mod_str
        })
    
    return jsonify({"timbrature": timbrature})


@app.post("/api/timbratura")
@login_required
def api_timbratura_registra():
    """Registra una nuova timbratura."""
    username = session.get('user')
    if not username:
        return jsonify({"error": "Non autenticato"}), 401
    
    data = request.get_json()
    if not data:
        return jsonify({"error": "Dati mancanti"}), 400
    
    tipo = data.get('tipo')
    if tipo not in ('inizio_giornata', 'fine_giornata', 'inizio_pausa', 'fine_pausa'):
        return jsonify({"error": "Tipo timbratura non valido"}), 400
    
    # Verifica bypass QR (solo per sviluppo)
    bypass_qr = data.get('bypass_qr', False)
    
    # Gestione dati offline (GPS o QR salvati quando offline)
    offline_timestamp = data.get('offline_timestamp')  # ISO timestamp da quando era offline
    offline_gps = data.get('offline_gps')  # {latitude, longitude, accuracy}
    offline_qr = data.get('offline_qr')  # QR code scansionato offline
    
    db = get_db()
    ensure_timbrature_table(db)
    
    # Se presente offline_gps, valida le coordinate
    if offline_gps and not bypass_qr:
        try:
            latitude = offline_gps.get('latitude')
            longitude = offline_gps.get('longitude')
            accuracy = offline_gps.get('accuracy', 9999)
            
            # Recupera configurazione GPS
            settings = get_company_settings(db)
            custom = settings.get('custom_settings', {})
            timbratura_config = custom.get('timbratura', {})
            gps_latitude = timbratura_config.get('gps_latitude')
            gps_longitude = timbratura_config.get('gps_longitude')
            gps_radius = timbratura_config.get('gps_radius', 100)
            gps_enabled = timbratura_config.get('gps_enabled', False)
            
            if gps_enabled and gps_latitude and gps_longitude:
                from math import radians, sin, cos, sqrt, atan2
                
                R = 6371000  # Raggio Terra in metri
                lat1, lon1 = radians(float(gps_latitude)), radians(float(gps_longitude))
                lat2, lon2 = radians(float(latitude)), radians(float(longitude))
                dlat = lat2 - lat1
                dlon = lon2 - lon1
                a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
                c = 2 * atan2(sqrt(a), sqrt(1-a))
                distance = R * c
                
                if distance > gps_radius:
                    app.logger.warning(f"Timbratura offline GPS rifiutata: {username} - distanza {distance:.0f}m > {gps_radius}m")
                    return jsonify({"error": f"Posizione GPS non valida (distanza: {distance:.0f}m)"}), 400
                
                app.logger.info(f"Timbratura offline GPS validata: {username} - distanza {distance:.0f}m")
                bypass_qr = True  # GPS valido, bypassa verifica QR
        except Exception as e:
            app.logger.error(f"Errore validazione GPS offline: {e}")
    
    # Se presente offline_qr, valida il QR code
    if offline_qr and not bypass_qr:
        try:
            settings = get_company_settings(db)
            custom = settings.get('custom_settings', {})
            timbratura_config = custom.get('timbratura', {})
            qr_code = timbratura_config.get('qr_code', '')
            if qr_code and offline_qr == qr_code:
                app.logger.info(f"Timbratura offline QR validata: {username}")
                bypass_qr = True  # QR valido
            else:
                app.logger.warning(f"Timbratura offline QR rifiutata: {username} - QR non corrispondente")
                return jsonify({"error": "QR Code non valido"}), 400
        except Exception as e:
            app.logger.error(f"Errore validazione QR offline: {e}")
    
    if not bypass_qr:
        # Verifica token di validazione QR
        token = data.get('token')
        session_token = session.get('timbratura_token')
        token_expires = session.get('timbratura_token_expires', 0)
        
        if not token or token != session_token:
            return jsonify({"error": "Devi prima scansionare il QR code", "need_qr": True}), 403
        
        if now_ms() > token_expires:
            # Pulisce token scaduto
            session.pop('timbratura_token', None)
            session.pop('timbratura_token_expires', None)
            return jsonify({"error": "Token scaduto, scansiona nuovamente il QR", "need_qr": True}), 403
    
    # Usa timestamp offline se presente, altrimenti ora attuale
    if offline_timestamp:
        try:
            # Parse ISO timestamp
            from dateutil import parser as dateparser
            offline_dt = dateparser.parse(offline_timestamp)
            now = offline_dt
            app.logger.info(f"Usando timestamp offline: {offline_timestamp}")
        except Exception as e:
            app.logger.warning(f"Errore parsing offline_timestamp: {e}, uso ora attuale")
            now = datetime.now()
    else:
        now = datetime.now()
    
    today = now.strftime("%Y-%m-%d")
    ora = now.strftime("%H:%M:%S")
    created_ts = now_ms()
    
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    # Verifica regole business
    existing = db.execute(
        f"SELECT tipo FROM timbrature WHERE username = {placeholder} AND data = {placeholder} ORDER BY created_ts ASC",
        (username, today)
    ).fetchall()
    existing_types = [r['tipo'] if isinstance(r, dict) else r[0] for r in existing]
    
    if tipo == 'inizio_giornata' and 'inizio_giornata' in existing_types:
        return jsonify({"error": "Hai già registrato l'inizio giornata oggi"}), 400
    
    if tipo == 'fine_giornata':
        if 'inizio_giornata' not in existing_types:
            return jsonify({"error": "Devi prima registrare l'inizio giornata"}), 400
        if 'fine_giornata' in existing_types:
            return jsonify({"error": "Hai già registrato la fine giornata oggi"}), 400
    
    if tipo == 'inizio_pausa':
        if 'inizio_giornata' not in existing_types:
            return jsonify({"error": "Devi prima registrare l'inizio giornata"}), 400
        if 'fine_giornata' in existing_types:
            return jsonify({"error": "Non puoi iniziare una pausa dopo la fine giornata"}), 400
        # Controlla se c'è già una pausa aperta
        pause_count = existing_types.count('inizio_pausa') - existing_types.count('fine_pausa')
        if pause_count > 0:
            return jsonify({"error": "Hai già una pausa in corso"}), 400
    
    if tipo == 'fine_pausa':
        pause_count = existing_types.count('inizio_pausa') - existing_types.count('fine_pausa')
        if pause_count <= 0:
            return jsonify({"error": "Non hai nessuna pausa in corso"}), 400
    
    # Calcola ora_mod in base alle regole
    ora_mod = None
    try:
        rules = get_timbratura_rules(db)
        
        # Ottieni turno per normalizzazione (solo per inizio_giornata)
        turno_start = None
        if tipo == 'inizio_giornata':
            # Trova il rentman_crew_id dell'utente
            user_row = db.execute(
                f"SELECT rentman_crew_id FROM app_users WHERE username = {placeholder}",
                (username,)
            ).fetchone()
            
            if user_row:
                crew_id = user_row['rentman_crew_id'] if isinstance(user_row, dict) else user_row[0]
                if crew_id:
                    # Cerca turno di oggi per l'utente dalla tabella rentman_plannings
                    turno_row = db.execute(
                        f"SELECT plan_start FROM rentman_plannings WHERE crew_id = {placeholder} AND planning_date = {placeholder} ORDER BY plan_start ASC LIMIT 1",
                        (crew_id, today)
                    ).fetchone()
                    if turno_row:
                        plan_start = turno_row['plan_start'] if isinstance(turno_row, dict) else turno_row[0]
                        if plan_start:
                            if hasattr(plan_start, 'strftime'):
                                turno_start = plan_start.strftime("%H:%M")
                            else:
                                # Formato datetime string: "2025-01-01 08:00:00"
                                plan_str = str(plan_start)
                                if len(plan_str) > 11:
                                    turno_start = plan_str[11:16]
                                else:
                                    turno_start = plan_str[:5]
        
        # Per fine_pausa, applica le regole sulla durata della pausa
        if tipo == 'fine_pausa':
            # Recupera l'ora di inizio pausa (l'ultima non chiusa)
            inizio_pausa_rows = db.execute(
                f"""SELECT ora, ora_mod FROM timbrature 
                   WHERE username = {placeholder} AND data = {placeholder} AND tipo = 'inizio_pausa'
                   ORDER BY created_ts DESC LIMIT 1""",
                (username, today)
            ).fetchone()
            
            if inizio_pausa_rows:
                inizio_ora_mod = inizio_pausa_rows['ora_mod'] if isinstance(inizio_pausa_rows, dict) else inizio_pausa_rows[1]
                if not inizio_ora_mod:
                    inizio_ora_mod = inizio_pausa_rows['ora'] if isinstance(inizio_pausa_rows, dict) else inizio_pausa_rows[0]
                
                # Formatta l'ora di inizio pausa
                if hasattr(inizio_ora_mod, 'strftime'):
                    inizio_str = inizio_ora_mod.strftime("%H:%M")
                else:
                    inizio_str = str(inizio_ora_mod)[:5]
                
                # Calcola durata modificata usando le regole pause
                durata_mod = calcola_pausa_mod(inizio_str, ora[:5], rules)
                
                # Calcola ora_mod di fine_pausa = inizio_pausa_mod + durata_mod
                inizio_parts = inizio_str.split(':')
                inizio_min = int(inizio_parts[0]) * 60 + int(inizio_parts[1])
                fine_mod_min = inizio_min + durata_mod
                
                h = fine_mod_min // 60
                m = fine_mod_min % 60
                ora_mod = f"{h:02d}:{m:02d}:00"
                
                app.logger.info(f"Pausa: {inizio_str} -> {ora[:5]} (durata effettiva {int(ora[:2])*60+int(ora[3:5]) - inizio_min} min, durata mod {durata_mod} min, fine mod {ora_mod})")
            else:
                ora_mod = calcola_ora_mod(ora, tipo, turno_start, rules)
        else:
            ora_mod = calcola_ora_mod(ora, tipo, turno_start, rules)
        
        app.logger.info(f"Ora mod calcolata: {ora} -> {ora_mod} (turno: {turno_start}, tipo: {tipo})")
    except Exception as e:
        app.logger.error(f"Errore calcolo ora_mod: {e}")
        ora_mod = ora  # Fallback: usa ora originale
    
    # Inserisce la timbratura
    db.execute(
        f"""
        INSERT INTO timbrature (username, tipo, data, ora, ora_mod, created_ts)
        VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})
        """,
        (username, tipo, today, ora, ora_mod, created_ts)
    )
    
    # CedolinoWeb: invia timbrata con ora originale e modificata
    # Mappa tipo timbratura -> timeframe CedolinoWeb
    TIPO_TO_TIMEFRAME = {
        'inizio_giornata': TIMEFRAME_INIZIO_GIORNATA,  # 1
        'inizio_pausa': TIMEFRAME_INIZIO_PAUSA,        # 4
        'fine_pausa': TIMEFRAME_FINE_PAUSA,            # 5
        'fine_giornata': TIMEFRAME_FINE_GIORNATA,      # 8
    }
    
    timeframe_id = TIPO_TO_TIMEFRAME.get(tipo)
    cedolino_url = None
    
    if timeframe_id:
        # Recupera il nome dell'utente per il log
        user_row = db.execute(
            f"SELECT display_name FROM app_users WHERE username = {placeholder}",
            (username,)
        ).fetchone()
        display_name = username
        if user_row:
            display_name = (user_row['display_name'] if isinstance(user_row, dict) else user_row[0]) or username
        
        # Genera sempre l'URL di debug (anche se CedolinoWeb è disabilitato)
        from urllib.parse import urlencode
        external_id_debug = get_external_id_for_username(db, username) or "N/A"
        external_group_id_debug = get_external_group_id_for_username(db, username) or "NULL"
        debug_params = {
            "data_riferimento": today,
            "data_originale": f"{today} {ora}",
            "data_modificata": f"{today} {ora_mod or ora}",
            "codice_utente": external_id_debug,
            "codice_terminale": CEDOLINO_CODICE_TERMINALE,
            "timeframe_id": str(timeframe_id),
            "assunzione_id": external_id_debug,
            "terminale_id": "NULL",
            "gruppo_id": external_group_id_debug,
            "turno_id": "NULL",
            "note": "",
            "validata": "true",
        }
        cedolino_url = f"{CEDOLINO_WEB_ENDPOINT}?{urlencode(debug_params)}"
        
        timbrata_ok, external_id, timbrata_error, _ = send_timbrata_utente(
            db,
            username=username,
            member_name=display_name,
            timeframe_id=timeframe_id,
            data_riferimento=today,
            ora_originale=ora,
            ora_modificata=ora_mod or ora,
        )
        
        if not timbrata_ok and external_id is None:
            # Utente senza ID esterno - blocca l'operazione (rollback implicito)
            return jsonify({
                "error": timbrata_error or "Utente senza ID esterno CedolinoWeb. Contattare l'amministratore.",
                "missing_external_id": True,
                "cedolino_url": cedolino_url  # Mostra comunque l'URL per debug
            }), 400
    
    db.commit()
    
    # Invalida il token dopo l'uso (ogni timbratura richiede nuova scansione)
    session.pop('timbratura_token', None)
    session.pop('timbratura_token_expires', None)
    
    app.logger.info(f"Timbratura registrata: {username} - {tipo} alle {ora}")
    
    # Restituisci anche l'URL CedolinoWeb per debug
    response_data = {"success": True, "tipo": tipo, "ora": ora[:5]}
    if cedolino_url:
        response_data["cedolino_url"] = cedolino_url
    
    return jsonify(response_data)


@app.post("/api/user/change-password")
@login_required
def api_user_change_password():
    """Permette all'utente di cambiare la propria password."""
    username = session.get('user')
    if not username:
        return jsonify({"error": "Non autenticato"}), 401
    
    data = request.get_json()
    if not data:
        return jsonify({"error": "Dati mancanti"}), 400
    
    current_password = data.get('current_password', '')
    new_password = data.get('new_password', '')
    
    if not current_password or not new_password:
        return jsonify({"error": "Password attuale e nuova password richieste"}), 400
    
    if len(new_password) < 4:
        return jsonify({"error": "La nuova password deve essere di almeno 4 caratteri"}), 400
    
    db = get_db()
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    # Verifica password attuale
    user = db.execute(
        f"SELECT password_hash FROM app_users WHERE username = {placeholder}",
        (username,)
    ).fetchone()
    
    if not user:
        return jsonify({"error": "Utente non trovato"}), 404
    
    stored_hash = user['password_hash'] if isinstance(user, dict) else user[0]
    
    # Verifica la password attuale
    if not check_password_hash(stored_hash, current_password):
        return jsonify({"error": "Password attuale non corretta"}), 400
    
    # Genera hash per la nuova password
    new_hash = generate_password_hash(new_password)
    
    # Aggiorna nel database
    db.execute(
        f"UPDATE app_users SET password_hash = {placeholder}, updated_ts = {placeholder} WHERE username = {placeholder}",
        (new_hash, now_ms(), username)
    )
    db.commit()
    
    app.logger.info(f"Utente {username} ha cambiato la propria password")
    
    return jsonify({"success": True, "message": "Password aggiornata con successo"})


@app.get("/api/user/turno-oggi")
@login_required
def api_user_turno_oggi():
    """Restituisce il turno dell'utente per oggi (da Rentman)."""
    username = session.get('user')
    if not username:
        return jsonify({"error": "Non autenticato"}), 401
    
    db = get_db()
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    # Trova il rentman_crew_id dell'utente
    user_row = db.execute(
        f"SELECT rentman_crew_id FROM app_users WHERE username = {placeholder}",
        (username,)
    ).fetchone()
    
    if not user_row:
        return jsonify({"turno": None, "message": "Utente non trovato"})
    
    crew_id = user_row['rentman_crew_id'] if isinstance(user_row, dict) else user_row[0]
    
    if not crew_id:
        return jsonify({"turno": None, "message": "Nessun operatore Rentman associato"})
    
    # Recupera il turno di oggi dalla tabella rentman_plannings (solo se inviato)
    today = datetime.now().strftime("%Y-%m-%d")
    
    ensure_rentman_plannings_table(db)
    
    planning = db.execute(
        f"""
        SELECT project_code, project_name, function_name, plan_start, plan_end,
               hours_planned, remark, is_leader, transport
        FROM rentman_plannings
        WHERE crew_id = {placeholder} AND planning_date = {placeholder} AND sent_to_webservice = 1
        ORDER BY plan_start ASC
        """,
        (crew_id, today)
    ).fetchall()
    
    if not planning:
        return jsonify({"turno": None, "message": "Nessun turno previsto per oggi"})
    
    turni = []
    for row in planning:
        if isinstance(row, dict):
            plan_start = row['plan_start']
            plan_end = row['plan_end']
        else:
            plan_start = row[3]
            plan_end = row[4]
        
        # Formatta orari
        start_str = ""
        end_str = ""
        if plan_start:
            if hasattr(plan_start, 'strftime'):
                start_str = plan_start.strftime("%H:%M")
            else:
                start_str = str(plan_start)[11:16] if len(str(plan_start)) > 11 else str(plan_start)[:5]
        if plan_end:
            if hasattr(plan_end, 'strftime'):
                end_str = plan_end.strftime("%H:%M")
            else:
                end_str = str(plan_end)[11:16] if len(str(plan_end)) > 11 else str(plan_end)[:5]
        
        turni.append({
            "project_code": row['project_code'] if isinstance(row, dict) else row[0],
            "project_name": row['project_name'] if isinstance(row, dict) else row[1],
            "function": row['function_name'] if isinstance(row, dict) else row[2],
            "start": start_str,
            "end": end_str,
            "hours": float(row['hours_planned'] if isinstance(row, dict) else row[5] or 0),
            "note": row['remark'] if isinstance(row, dict) else row[6],
            "is_leader": bool(row['is_leader'] if isinstance(row, dict) else row[7]),
            "transport": row['transport'] if isinstance(row, dict) else row[8],
        })
    
    return jsonify({"turno": turni[0] if len(turni) == 1 else None, "turni": turni})


# ============================================================
# I MIEI TURNI - Pagina Utente
# ============================================================

@app.get("/turni")
@login_required
def user_turni_page() -> ResponseReturnValue:
    """Pagina per visualizzare i turni dell'utente."""
    return render_template(
        "user_turni.html",
        username=session.get("user"),
        user_display=session.get("user_display", session.get("user")),
        user_initials=session.get("user_initials", "U"),
        user_role=session.get("user_role", "Utente"),
    )


@app.get("/api/user/turni")
@login_required
def api_user_turni() -> ResponseReturnValue:
    """API per recuperare i turni dell'utente (solo quelli pubblicati/inviati)."""
    username = session.get("user")
    
    db = get_db()
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    # Recupera il rentman_crew_id dell'utente
    user_row = db.execute(
        f"SELECT rentman_crew_id FROM app_users WHERE username = {placeholder}",
        (username,)
    ).fetchone()
    
    if not user_row:
        return jsonify({"turni": [], "message": "Utente non trovato"})
    
    crew_id = user_row['rentman_crew_id'] if isinstance(user_row, dict) else user_row[0]
    
    if not crew_id:
        return jsonify({"turni": [], "message": "Nessun operatore Rentman associato"})
    
    ensure_rentman_plannings_table(db)
    
    # Recupera tutti i turni pubblicati (sent_to_webservice = 1) degli ultimi 30 giorni e prossimi 60 giorni
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    sixty_days_future = (datetime.now() + timedelta(days=60)).strftime("%Y-%m-%d")
    
    if DB_VENDOR == "mysql":
        planning = db.execute(
            """
            SELECT planning_date, project_code, project_name, function_name, plan_start, plan_end,
                   hours_planned, remark, is_leader, transport, sent_ts
            FROM rentman_plannings
            WHERE crew_id = %s 
              AND sent_to_webservice = 1
              AND planning_date >= %s 
              AND planning_date <= %s
            ORDER BY planning_date ASC, plan_start ASC
            """,
            (crew_id, thirty_days_ago, sixty_days_future)
        ).fetchall()
    else:
        planning = db.execute(
            """
            SELECT planning_date, project_code, project_name, function_name, plan_start, plan_end,
                   hours_planned, remark, is_leader, transport, sent_ts
            FROM rentman_plannings
            WHERE crew_id = ? 
              AND sent_to_webservice = 1
              AND planning_date >= ? 
              AND planning_date <= ?
            ORDER BY planning_date ASC, plan_start ASC
            """,
            (crew_id, thirty_days_ago, sixty_days_future)
        ).fetchall()
    
    turni = []
    for row in planning:
        if isinstance(row, dict):
            planning_date = row['planning_date']
            plan_start = row['plan_start']
            plan_end = row['plan_end']
        else:
            planning_date = row[0]
            plan_start = row[4]
            plan_end = row[5]
        
        # Normalizza la data
        if hasattr(planning_date, 'isoformat'):
            date_str = planning_date.isoformat()
        else:
            date_str = str(planning_date)[:10]
        
        # Formatta orari
        start_str = ""
        end_str = ""
        if plan_start:
            if hasattr(plan_start, 'strftime'):
                start_str = plan_start.strftime("%H:%M")
            else:
                start_str = str(plan_start)[11:16] if len(str(plan_start)) > 11 else str(plan_start)[:5]
        if plan_end:
            if hasattr(plan_end, 'strftime'):
                end_str = plan_end.strftime("%H:%M")
            else:
                end_str = str(plan_end)[11:16] if len(str(plan_end)) > 11 else str(plan_end)[:5]
        
        turni.append({
            "date": date_str,
            "project_code": row['project_code'] if isinstance(row, dict) else row[1],
            "project_name": row['project_name'] if isinstance(row, dict) else row[2],
            "function": row['function_name'] if isinstance(row, dict) else row[3],
            "start": start_str,
            "end": end_str,
            "hours": float(row['hours_planned'] if isinstance(row, dict) else row[6] or 0),
            "note": row['remark'] if isinstance(row, dict) else row[7],
            "is_leader": bool(row['is_leader'] if isinstance(row, dict) else row[8]),
            "transport": row['transport'] if isinstance(row, dict) else row[9],
        })
    
    return jsonify({"turni": turni})


@app.get("/magazzino")
@login_required
def magazzino_home() -> ResponseReturnValue:
    guard = _magazzino_only()
    if guard is not None:
        return guard

    display_name = session.get('user_display') or session.get('user_name') or session.get('user')
    primary_name = session.get('user_name') or display_name or session.get('user')
    initials = session.get('user_initials') or compute_initials(primary_name or "")
    header_clock = datetime.now().strftime("%d/%m/%Y | %H:%M")

    return render_template(
        "magazzino.html",
        user_name=primary_name,
        user_display=display_name,
        user_initials=initials,
        header_clock=header_clock,
    )


WAREHOUSE_ACTIVITIES_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS warehouse_activities (
    id INT AUTO_INCREMENT PRIMARY KEY,
    project_code VARCHAR(128) NOT NULL,
    activity_label VARCHAR(255) NOT NULL,
    note TEXT,
    username VARCHAR(190) DEFAULT NULL,
    created_ts BIGINT NOT NULL,
    INDEX idx_warehouse_project (project_code),
    INDEX idx_warehouse_created (created_ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


WAREHOUSE_ACTIVITIES_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS warehouse_activities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_code TEXT NOT NULL,
    activity_label TEXT NOT NULL,
    note TEXT,
    username TEXT,
    created_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_warehouse_project ON warehouse_activities(project_code);
CREATE INDEX IF NOT EXISTS idx_warehouse_created ON warehouse_activities(created_ts);
"""


# Tabella per sessioni di lavoro magazzino (con timer)
WAREHOUSE_SESSIONS_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS warehouse_sessions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    project_code VARCHAR(128) NOT NULL,
    activity_label VARCHAR(255) NOT NULL,
    elapsed_ms BIGINT NOT NULL DEFAULT 0,
    note TEXT,
    username VARCHAR(190) DEFAULT NULL,
    created_ts BIGINT NOT NULL,
    INDEX idx_wh_sessions_project (project_code),
    INDEX idx_wh_sessions_created (created_ts),
    INDEX idx_wh_sessions_user (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


WAREHOUSE_SESSIONS_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS warehouse_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_code TEXT NOT NULL,
    activity_label TEXT NOT NULL,
    elapsed_ms INTEGER NOT NULL DEFAULT 0,
    note TEXT,
    username TEXT,
    created_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wh_sessions_project ON warehouse_sessions(project_code);
CREATE INDEX IF NOT EXISTS idx_wh_sessions_created ON warehouse_sessions(created_ts);
CREATE INDEX IF NOT EXISTS idx_wh_sessions_user ON warehouse_sessions(username);
"""


# Progetti manuali magazzino (persistenza cross-device per utente)
WAREHOUSE_MANUAL_PROJECTS_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS warehouse_manual_projects (
    id INT AUTO_INCREMENT PRIMARY KEY,
    project_code VARCHAR(128) NOT NULL,
    name VARCHAR(255) DEFAULT NULL,
    username VARCHAR(190) NOT NULL,
    created_ts BIGINT NOT NULL,
    UNIQUE KEY uniq_wh_manual_user_code (username, project_code),
    INDEX idx_wh_manual_user (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


WAREHOUSE_MANUAL_PROJECTS_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS warehouse_manual_projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_code TEXT NOT NULL,
    name TEXT,
    username TEXT NOT NULL,
    created_ts INTEGER NOT NULL,
    UNIQUE(username, project_code)
);
CREATE INDEX IF NOT EXISTS idx_wh_manual_user ON warehouse_manual_projects(username);
"""


# ============================================================
# TABELLE TIMBRATURA
# ============================================================

TIMBRATURE_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS timbrature (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(190) NOT NULL,
    tipo VARCHAR(50) NOT NULL,
    data DATE NOT NULL,
    ora TIME NOT NULL,
    created_ts BIGINT NOT NULL,
    INDEX idx_timbrature_user_date (username, data),
    INDEX idx_timbrature_date (data)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

TIMBRATURE_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS timbrature (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL,
    tipo TEXT NOT NULL,
    data TEXT NOT NULL,
    ora TEXT NOT NULL,
    created_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_timbrature_user_date ON timbrature(username, data);
CREATE INDEX IF NOT EXISTS idx_timbrature_date ON timbrature(data);
"""


def ensure_timbrature_table(db: DatabaseLike) -> None:
    statement = TIMBRATURE_TABLE_MYSQL if DB_VENDOR == "mysql" else TIMBRATURE_TABLE_SQLITE
    for stmt in statement.strip().split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        cursor = db.execute(sql)
        try:
            cursor.close()
        except AttributeError:
            pass
    
    # Migrazione: aggiungi colonna ora_mod se non esiste
    if DB_VENDOR == "mysql":
        try:
            db.execute("ALTER TABLE timbrature ADD COLUMN ora_mod TIME DEFAULT NULL")
            db.commit()
        except Exception:
            pass  # Colonna già esistente
    else:
        try:
            db.execute("ALTER TABLE timbrature ADD COLUMN ora_mod TEXT")
            db.commit()
        except Exception:
            pass


def ensure_warehouse_activities_table(db: DatabaseLike) -> None:
    statement = (
        WAREHOUSE_ACTIVITIES_TABLE_MYSQL if DB_VENDOR == "mysql" else WAREHOUSE_ACTIVITIES_TABLE_SQLITE
    )
    for stmt in statement.strip().split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        cursor = db.execute(sql)
        try:
            cursor.close()
        except AttributeError:
            pass


def ensure_warehouse_sessions_table(db: DatabaseLike) -> None:
    statement = (
        WAREHOUSE_SESSIONS_TABLE_MYSQL if DB_VENDOR == "mysql" else WAREHOUSE_SESSIONS_TABLE_SQLITE
    )
    for stmt in statement.strip().split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        cursor = db.execute(sql)
        try:
            cursor.close()
        except AttributeError:
            pass


def ensure_warehouse_manual_projects_table(db: DatabaseLike) -> None:
    statement = (
        WAREHOUSE_MANUAL_PROJECTS_TABLE_MYSQL
        if DB_VENDOR == "mysql"
        else WAREHOUSE_MANUAL_PROJECTS_TABLE_SQLITE
    )
    for stmt in statement.strip().split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        cursor = db.execute(sql)
        try:
            cursor.close()
        except AttributeError:
            pass


@app.get("/api/magazzino/projects/today")
@login_required
def api_magazzino_projects_today() -> ResponseReturnValue:
    """Recupera progetti attivi per la data specificata.

    Secondo la documentazione API Rentman (oas.json):
    - I progetti (projects) NON hanno un campo status diretto
    - Lo STATUS è nel SUBPROJECT (/subprojects) come riferimento "/statuses/ID"
    - Ogni progetto ha almeno un subproject

    Logica: recuperiamo i subprojects, filtriamo per status e data,
    poi raggruppiamo per progetto padre.
    """
    guard = _magazzino_only()
    if guard is not None:
        return guard

    requested = _normalize_date(request.args.get("date"))
    target_date = requested or datetime.now().date().isoformat()
    debug = _is_truthy(request.args.get("debug"))

    rentman_info: Dict[str, Any] = {"available": False, "used": False}

    # Keywords per filtrare gli status
    allowed_status_keywords: Tuple[str, ...] = (
        "confermat",
        "confirm",
        "in location",
        "locat",
        "pronto",
        "ready",
    )
    cancelled_keywords: Tuple[str, ...] = ("annull", "cancel")

    def normalize_status(value: str) -> str:
        cleaned = str(value or "").strip().lower()
        cleaned = cleaned.replace("_", " ")
        return " ".join(cleaned.split())

    def is_status_allowed(status_name: str) -> bool:
        normalized = normalize_status(status_name)
        if any(kw in normalized for kw in cancelled_keywords):
            return False
        if any(kw in normalized for kw in allowed_status_keywords):
            return True
        return False

    def subproject_matches_date(subproject: Mapping[str, Any], target: date) -> bool:
        date_fields = [
            ("equipment_period_from", "equipment_period_to"),
            ("usageperiod_start", "usageperiod_end"),
            ("planperiod_start", "planperiod_end"),
        ]
        for start_key, end_key in date_fields:
            start_val = _parse_date_any(subproject.get(start_key))
            end_val = _parse_date_any(subproject.get(end_key))
            if start_val is not None or end_val is not None:
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

    client = get_rentman_client()
    if client:
        rentman_info["available"] = True
        scan_limit = _coerce_int(os.environ.get("JOBLOG_RENTMAN_SUBPROJECT_SCAN_LIMIT")) or 15000
        scan_limit = max(1000, min(scan_limit, 30000))

        # 1) Recupera status map
        status_map: Dict[int, str] = {}
        try:
            for entry in client.get_project_statuses():
                status_id = entry.get("id")
                if isinstance(status_id, int):
                    status_name = entry.get("displayname") or entry.get("name") or str(status_id)
                    status_map[status_id] = str(status_name)
        except Exception:
            status_map = {}

        # 2) Recupera tutti i subprojects
        subprojects_all: List[Dict[str, Any]] = []
        rentman_error_type: Optional[str] = None
        rentman_error_detail: Optional[str] = None
        try:
            for sub in client.iter_collection("/subprojects", limit_total=scan_limit):
                subprojects_all.append(sub)
        except Exception as exc:
            app.logger.exception("Rentman: errore durante scansione subprojects")
            rentman_error_type = type(exc).__name__
            rentman_error_detail = str(exc)

        if rentman_error_type is None:
            target_date_obj = _parse_date_any(target_date)
            if target_date_obj is None:
                target_date_obj = datetime.now().date()

            # 3) Filtra subprojects per status e data
            valid_subprojects: List[Dict[str, Any]] = []
            projects_seen: Set[str] = set()

            for sub in subprojects_all:
                status_ref = sub.get("status")
                status_id = parse_reference(status_ref)
                status_name = status_map.get(status_id, "") if isinstance(status_id, int) else ""

                if not is_status_allowed(status_name):
                    continue

                if not subproject_matches_date(sub, target_date_obj):
                    continue

                project_ref = sub.get("project")
                if not project_ref:
                    continue

                valid_subprojects.append({
                    "subproject_id": sub.get("id"),
                    "project_ref": project_ref,
                    "status_id": status_id,
                    "status_name": status_name,
                })
                projects_seen.add(project_ref)

            # 4) Recupera dettagli progetti padre
            projects_rentman: List[Dict[str, Any]] = []
            seen_codes: Set[str] = set()

            for project_ref in projects_seen:
                project_id = parse_reference(project_ref)
                if project_id is None:
                    continue

                try:
                    payload = client._request("GET", f"/projects/{project_id}")
                    project_data = payload.get("data", {})

                    code = str(project_data.get("number") or project_data.get("reference") or project_id).strip().upper()
                    if not code or code in seen_codes:
                        continue
                    seen_codes.add(code)

                    name = project_data.get("displayname") or project_data.get("name") or code
                    projects_rentman.append({"code": code, "name": str(name)})
                except Exception:
                    continue

            rentman_info["used"] = True
            projects_rentman.sort(key=lambda item: (str(item.get("code") or ""), str(item.get("name") or "")))

            response: Dict[str, Any] = {
                "ok": True,
                "date": target_date,
                "projects": projects_rentman,
                "count": len(projects_rentman),
                "source": "rentman",
            }
            if debug:
                response["debug"] = {
                    "scan_limit": scan_limit,
                    "subprojects_total": len(subprojects_all),
                    "subprojects_valid": len(valid_subprojects),
                    "status_map_count": len(status_map),
                    "status_map": status_map,
                }
            return jsonify(response)

        if debug:
            if rentman_error_type:
                rentman_info["error"] = rentman_error_type
            if rentman_error_detail:
                rentman_info["error_detail"] = rentman_error_detail[:1200]
    else:
        rentman_info["available"] = False
        rentman_info["reason"] = "client_unavailable"

    # Fallback locale: projects.json (solo se Rentman non è disponibile o ha fallito)
    catalog = load_external_projects()
    projects: List[Dict[str, Any]] = []
    for entry in catalog.values():
        code = str(entry.get("project_code") or "").strip().upper()
        name = str(entry.get("project_name") or code).strip() or code
        activities_any = entry.get("activities")
        activities: List[Any] = activities_any if isinstance(activities_any, list) else []
        active = False
        for act in activities:
            if not isinstance(act, dict):
                continue
            if _activity_matches_date(act.get("plan_start"), act.get("plan_end"), target_date):
                active = True
                break
        if active:
            projects.append({"code": code, "name": name})

    projects.sort(key=lambda item: (str(item.get("code") or ""), str(item.get("name") or "")))
    response_local: Dict[str, Any] = {"ok": True, "date": target_date, "projects": projects, "count": len(projects), "source": "local"}
    if debug:
        response_local["debug"] = {"rentman": rentman_info}
    return jsonify(response_local)


@app.get("/api/magazzino/projects/manual")
@login_required
def api_magazzino_projects_manual_list() -> ResponseReturnValue:
    """Restituisce i progetti manuali salvati per l'utente (persistenti cross-device)."""
    guard = _magazzino_only()
    if guard is not None:
        return guard

    db = get_db()
    ensure_warehouse_manual_projects_table(db)
    username = session.get("user")
    rows = db.execute(
        """
        SELECT project_code AS code, name, created_ts
        FROM warehouse_manual_projects
        WHERE username = ?
        ORDER BY created_ts DESC
        LIMIT 500
        """,
        (username,),
    ).fetchall()
    items = [dict(row) for row in rows] if rows else []
    return jsonify({"ok": True, "items": items})


@app.post("/api/magazzino/projects/manual")
@login_required
def api_magazzino_projects_manual_upsert() -> ResponseReturnValue:
    """Salva o aggiorna un progetto manuale per l'utente corrente."""
    guard = _magazzino_only()
    if guard is not None:
        return guard

    data = request.get_json(silent=True) or {}
    code = _normalize_text(data.get("code"))
    name = _normalize_text(data.get("name"))
    if not code:
        return jsonify({"ok": False, "error": "missing_code"}), 400

    db = get_db()
    ensure_warehouse_manual_projects_table(db)
    username = session.get("user")
    now = now_ms()

    if DB_VENDOR == "mysql":
        db.execute(
            """
            INSERT INTO warehouse_manual_projects(project_code, name, username, created_ts)
            VALUES(%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE name=VALUES(name), created_ts=VALUES(created_ts)
            """,
            (code, name, username, now),
        )
    else:
        db.execute(
            """
            INSERT INTO warehouse_manual_projects(project_code, name, username, created_ts)
            VALUES(?,?,?,?)
            ON CONFLICT(username, project_code) DO UPDATE SET name=excluded.name, created_ts=excluded.created_ts
            """,
            (code, name, username, now),
        )
    try:
        db.commit()
    except Exception:
        pass
    return jsonify({"ok": True, "code": code, "name": name, "created_ts": now})


@app.get("/api/magazzino/projects/lookup")
@login_required
def api_magazzino_projects_lookup() -> ResponseReturnValue:
    """Recupera un progetto per codice (numero) anche fuori data odierna.

    Prova prima Rentman (se configurato), altrimenti il catalogo locale.
    """
    guard = _magazzino_only()
    if guard is not None:
        return guard

    code = _normalize_text(request.args.get("code")).upper()
    if not code:
        return jsonify({"ok": False, "error": "missing_code"}), 400

    project: Optional[Dict[str, str]] = None
    source = "local"
    debug = _is_truthy(request.args.get("debug"))
    rentman_debug: Dict[str, Any] = {}

    client = get_rentman_client()
    if client:
        try:
            # Riduci batch per evitare limiti dimensione risposta Rentman
            batch = 150  # < 300 e payload più piccolo
            max_total = 7500
            scanned = 0
            found = False
            offset = 0

            def norm(val: str) -> str:
                return str(val or "").strip().upper()

            while scanned < max_total:
                payload = client._request(
                    "GET",
                    "/projects",
                    params={
                        "limit": batch,
                        "offset": offset,
                        "fields": "number,reference,displayname,name",
                    },
                )
                data = payload.get("data") if isinstance(payload, dict) else None
                if debug:
                    rentman_debug.setdefault("pages", []).append(len(data) if isinstance(data, list) else None)
                if not isinstance(data, list) or not data:
                    break

                for entry in data:
                    num = norm(entry.get("number") or entry.get("reference"))
                    disp = norm(entry.get("displayname") or entry.get("name"))
                    if num == code or disp == code or code in num:
                        name = entry.get("displayname") or entry.get("name") or num
                        project = {"code": num or code, "name": str(name)}
                        source = "rentman"
                        found = True
                        break

                scanned += len(data)
                if found:
                    break
                if len(data) < batch:
                    break
                offset += batch

            if debug:
                rentman_debug["scanned"] = scanned
                rentman_debug["found"] = bool(project)

        except Exception as exc:
            project = None
            if debug:
                rentman_debug["error"] = True
                rentman_debug["error_type"] = type(exc).__name__
                rentman_debug["error_detail"] = str(exc)[:400]

    if project is None:
        catalog = load_external_projects()
        entry = catalog.get(code)
        if entry:
            name = str(entry.get("project_name") or code).strip() or code
            project = {"code": code, "name": name}
            source = "local"

    if project is None:
        resp: Dict[str, Any] = {"ok": False, "error": "not_found"}
        if debug:
            resp["rentman_debug"] = rentman_debug
        return jsonify(resp), 404

    resp: Dict[str, Any] = {"ok": True, "project": project, "source": source}
    if debug:
        resp["rentman_debug"] = rentman_debug
    return jsonify(resp)


@app.get("/api/magazzino/activities")
@login_required
def api_magazzino_activities_list() -> ResponseReturnValue:
    guard = _magazzino_only()
    if guard is not None:
        return guard

    project_code = _normalize_text(request.args.get("project_code")).upper()
    if not project_code:
        return jsonify({"ok": False, "error": "missing_project_code"}), 400

    db = get_db()
    ensure_warehouse_activities_table(db)
    rows = db.execute(
        """
        SELECT id, project_code, activity_label, note, username, created_ts
        FROM warehouse_activities
        WHERE project_code = ?
        ORDER BY created_ts DESC
        LIMIT 200
        """,
        (project_code,),
    ).fetchall()
    items = [dict(row) for row in rows] if rows else []
    return jsonify({"ok": True, "project_code": project_code, "items": items})


@app.post("/api/magazzino/activities")
@login_required
def api_magazzino_activities_create() -> ResponseReturnValue:
    guard = _magazzino_only()
    if guard is not None:
        return guard

    data = request.get_json(silent=True) or {}
    project_code = _normalize_text(data.get("project_code")).upper()
    activity_label = _normalize_text(data.get("activity_label"))
    note = _normalize_text(data.get("note"))
    if not project_code:
        return jsonify({"ok": False, "error": "missing_project_code"}), 400
    if not activity_label:
        return jsonify({"ok": False, "error": "missing_activity_label"}), 400

    db = get_db()
    ensure_warehouse_activities_table(db)
    now = now_ms()
    username = session.get("user")
    db.execute(
        """
        INSERT INTO warehouse_activities(project_code, activity_label, note, username, created_ts)
        VALUES(?,?,?,?,?)
        """,
        (project_code, activity_label, note or None, username, now),
    )
    try:
        db.commit()
    except Exception:
        pass
    return jsonify({"ok": True, "created_ts": now})


# ============== Sessioni Magazzino (Timer) ==============

@app.get("/api/magazzino/sessions")
@login_required
def api_magazzino_sessions_list() -> ResponseReturnValue:
    """Lista sessioni di lavoro magazzino per progetto."""
    guard = _magazzino_only()
    if guard is not None:
        return guard

    project_code = _normalize_text(request.args.get("project_code")).upper()
    if not project_code:
        return jsonify({"ok": False, "error": "missing_project_code"}), 400

    db = get_db()
    ensure_warehouse_sessions_table(db)

    # Filtra sessioni di oggi per default
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_start_ms = int(today_start.timestamp() * 1000)

    rows = db.execute(
        """
        SELECT id, project_code, activity_label, elapsed_ms, note, username, created_ts
        FROM warehouse_sessions
        WHERE project_code = ? AND created_ts >= ?
        ORDER BY created_ts DESC
        LIMIT 100
        """,
        (project_code, today_start_ms),
    ).fetchall()
    items = [dict(row) for row in rows] if rows else []
    return jsonify({"ok": True, "project_code": project_code, "items": items})


@app.post("/api/magazzino/sessions")
@login_required
def api_magazzino_sessions_create() -> ResponseReturnValue:
    """Salva una nuova sessione di lavoro magazzino."""
    guard = _magazzino_only()
    if guard is not None:
        return guard

    data = request.get_json(silent=True) or {}
    project_code = _normalize_text(data.get("project_code")).upper()
    activity_label = _normalize_text(data.get("activity_label"))
    elapsed_ms = _coerce_int(data.get("elapsed_ms"))
    note = _normalize_text(data.get("note"))

    if not project_code:
        return jsonify({"ok": False, "error": "missing_project_code"}), 400
    if not activity_label:
        return jsonify({"ok": False, "error": "missing_activity_label"}), 400
    if elapsed_ms is None or elapsed_ms < 0:
        elapsed_ms = 0

    db = get_db()
    ensure_warehouse_sessions_table(db)
    now = now_ms()
    username = session.get("user")

    db.execute(
        """
        INSERT INTO warehouse_sessions(project_code, activity_label, elapsed_ms, note, username, created_ts)
        VALUES(?,?,?,?,?,?)
        """,
        (project_code, activity_label, elapsed_ms, note or None, username, now),
    )
    try:
        db.commit()
    except Exception:
        pass
    return jsonify({"ok": True, "created_ts": now, "elapsed_ms": elapsed_ms})


@app.route("/api/activities", methods=["GET", "POST"])
@login_required
def api_activities():
    if request.method == "GET":
        db = get_db()
        rows = db.execute(
            "SELECT activity_id, label FROM activities ORDER BY sort_order, label"
        ).fetchall()
        return jsonify({"activities": [dict(row) for row in rows]})

    # POST - create new activity
    data = request.get_json(silent=True) or {}
    label = str(data.get("label") or "").strip()
    if not label:
        return jsonify({"ok": False, "error": "missing_label"}), 400

    raw_id = data.get("activity_id")
    requested_id = ""
    if isinstance(raw_id, str) and raw_id.strip():
        requested_id = _normalize_activity_id(raw_id)
        if not requested_id:
            return jsonify({"ok": False, "error": "invalid_activity_id"}), 400

    db = get_db()
    project_code = session.get('supervisor_project_code', '')
    if not project_code:
        return jsonify({"ok": False, "error": "no_active_project"}), 409
    project_name = session.get('supervisor_project_name') or project_code
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"

    if requested_id:
        existing = db.execute(
            f"SELECT 1 FROM activities WHERE activity_id={placeholder} AND project_code={placeholder}",
            (requested_id, project_code),
        ).fetchone()
        if existing is not None:
            return jsonify({"ok": False, "error": "activity_id_in_use"}), 409
        activity_id = requested_id
    else:
        activity_id = _generate_activity_id(db, label)

    order_row = db.execute(f"SELECT COALESCE(MAX(sort_order), 0) AS max_order FROM activities WHERE project_code={placeholder}", (project_code,)).fetchone()
    max_order = 0
    if order_row is not None:
        value = row_value(order_row, "max_order")
        if value is None:
            value = row_value(order_row, 0)
        if isinstance(value, (int, float)):
            max_order = int(value)
    sort_order = max_order + 1

    plan_start = _normalize_datetime(data.get("plan_start"))
    plan_end = _normalize_datetime(data.get("plan_end"))
    planned_members = _coerce_int(data.get("planned_members"))
    if planned_members is None or planned_members < 0:
        planned_members = 0
    notes_raw = data.get("notes")
    notes = str(notes_raw).strip() if isinstance(notes_raw, str) and notes_raw.strip() else None
    planned_duration_ms = compute_planned_duration_ms(plan_start, plan_end, planned_members)

    db.execute(
        f"""
        INSERT INTO activities(
            activity_id, project_code, label, sort_order, plan_start, plan_end,
            planned_members, planned_duration_ms, notes
        ) VALUES({placeholder},{placeholder},{placeholder},{placeholder},{placeholder},{placeholder},{placeholder},{placeholder},{placeholder})
        """,
        (
            activity_id,
            project_code,
            label,
            sort_order,
            plan_start,
            plan_end,
            planned_members,
            planned_duration_ms,
            notes,
        ),
    )

    meta = load_activity_meta(db)
    meta[str(activity_id)] = {
        "plan_start": plan_start,
        "plan_end": plan_end,
        "planned_members": planned_members,
        "planned_duration_ms": planned_duration_ms,
        "actual_runtime_ms": 0,
    }
    save_activity_meta(db, meta)

    now = now_ms()
    db.execute(
        f"INSERT INTO event_log(ts, kind, details, project_code) VALUES({placeholder},{placeholder},{placeholder},{placeholder})",
        (
            now,
            "create_activity",
            json.dumps({"activity_id": activity_id, "label": label}),
            project_code,
        ),
    )

    db.commit()

    payload = {
        "activity_id": activity_id,
        "label": label,
        "plan_start": plan_start,
        "plan_end": plan_end,
        "planned_members": planned_members,
        "planned_duration_ms": planned_duration_ms,
        "notes": notes,
        "sort_order": sort_order,
    }

    return (
        jsonify(
            {
                "ok": True,
                "activity": payload,
                "project": {"code": project_code, "name": project_name},
            }
        ),
        201,
    )


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
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"

    # Ogni supervisor vede il proprio progetto (dalla sessione)
    project_code = session.get('supervisor_project_code')
    project_name = session.get('supervisor_project_name') or project_code

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
        f"SELECT activity_id, label FROM activities WHERE project_code = {placeholder} ORDER BY sort_order, label",
        (project_code,)
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
        f"SELECT * FROM member_state WHERE project_code = {placeholder} ORDER BY member_name",
        (project_code,)
    ).fetchall()

    team: List[Dict[str, Any]] = []
    active_members: List[Dict[str, Any]] = []
    paused_keys = {
        row["member_key"]
        for row in db.execute(
            f"SELECT member_key FROM member_state WHERE project_code = {placeholder} AND running={placeholder} AND pause_start IS NOT NULL",
            (project_code, RUN_STATE_PAUSED,)
        ).fetchall()
    }
    for row in members:
        running_state = int(row["running"])
        last_start_ts = row["entered_ts"] or row["start_ts"]
        member = {
            "member_key": row["member_key"],
            "member_name": row["member_name"],
            "activity_id": row["activity_id"],
            "running": running_state == RUN_STATE_RUNNING,
            "running_state": running_state,
            "elapsed": compute_elapsed(row, now),
            "paused": row["member_key"] in paused_keys,
            "last_start_ts": last_start_ts,
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
    
    # Verifica se il progetto del supervisor è sincronizzato con quello globale
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
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    project_code = session.get('supervisor_project_code')
    if not project_code:
        return jsonify({"events": []})

    activity_labels = {
        row["activity_id"]: row["label"]
        for row in db.execute(
            f"SELECT activity_id, label FROM activities WHERE project_code = {placeholder}",
            (project_code,)
        )
    }

    rows = db.execute(
        f"SELECT id, ts, kind, member_key, details FROM event_log WHERE project_code = {placeholder} ORDER BY ts DESC LIMIT 25",
        (project_code,)
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


@app.get("/api/project/attachments")
@login_required
def api_project_attachments():
    db = get_db()
    project_code = get_app_state(db, "project_code")
    if not project_code:
        return jsonify({"project": None, "attachments": []})

    project_name = get_app_state(db, "project_name") or project_code
    exhaustive = request.args.get("mode") == "deep"
    attachments = fetch_project_attachments(project_code, exhaustive=exhaustive)
    return jsonify({"project": {"code": project_code, "name": project_name}, "attachments": attachments})


@app.get("/api/project/materials")
@login_required
def api_project_materials():
    db = get_db()
    project_code = get_app_state(db, "project_code")
    if not project_code:
        return jsonify({"project": None, "materials": [], "folders": [], "equipment_checks": {}, "from_cache": False})

    project_name = get_app_state(db, "project_name") or project_code
    mode = (request.args.get("mode") or "").strip().lower()
    refresh_requested = mode == "refresh"

    cached_payload: Optional[Dict[str, Any]] = None
    if not refresh_requested:
        cached_payload = load_project_materials_cache(db, project_code)

    if cached_payload:
        equipment_checks = fetch_equipment_checks(db, project_code)
        return jsonify(
            {
                "project": {"code": project_code, "name": project_name},
                "materials": cached_payload.get("items", []),
                "folders": cached_payload.get("folders", []),
                "equipment_checks": equipment_checks,
                "from_cache": True,
                "updated_ts": cached_payload.get("updated_ts"),
            }
        )

    materials_payload = fetch_project_materials(project_code)
    items = materials_payload.get("items", [])
    folders = materials_payload.get("folders", [])
    saved_ts = save_project_materials_cache(
        db,
        project_code,
        project_name,
        items=items,
        folders=folders,
    )
    try:
        db.commit()
    except Exception:
        app.logger.exception("Impossibile salvare la cache materiali per il progetto %s", project_code)
    equipment_checks = fetch_equipment_checks(db, project_code)
    return jsonify(
        {
            "project": {"code": project_code, "name": project_name},
            "materials": items,
            "folders": folders,
            "equipment_checks": equipment_checks,
            "from_cache": False,
            "updated_ts": saved_ts,
        }
    )


@app.get("/api/project/equipment/checks")
@login_required
def api_project_equipment_checks():
    db = get_db()
    project_code = get_app_state(db, "project_code")
    if not project_code:
        return jsonify({"project": None, "checks": {}})

    project_name = get_app_state(db, "project_name") or project_code
    checks = fetch_equipment_checks(db, project_code)
    return jsonify({"project": {"code": project_code, "name": project_name}, "checks": checks})


@app.post("/api/project/equipment/checks")
@login_required
def api_update_equipment_check():
    data = request.get_json(silent=True) or {}
    item_key = (data.get("item_key") or "").strip()
    if not item_key:
        return jsonify({"ok": False, "error": "missing_item_key"}), 400
    if "checked" not in data:
        return jsonify({"ok": False, "error": "missing_checked_flag"}), 400

    checked = bool(data.get("checked"))

    db = get_db()
    project_code = get_app_state(db, "project_code")
    if not project_code:
        return jsonify({"ok": False, "error": "no_active_project"}), 400

    username = session.get("user")
    timestamp = persist_equipment_check(
        db,
        project_code=project_code,
        item_key=item_key,
        checked=checked,
        username=username,
    )
    db.commit()

    return jsonify({"ok": True, "checked": checked, "timestamp": timestamp})


@app.route("/api/project/local-equipment", methods=["GET", "POST"])
@login_required
def api_local_equipment():
    db = get_db()
    project_code = get_app_state(db, "project_code")
    if not project_code:
        return jsonify({"ok": False, "error": "no_active_project"}), 400

    ensure_local_equipment_table(db)

    if request.method == "GET":
        rows = db.execute(
            "SELECT id, name, quantity, notes, group_name, created_ts, updated_ts "
            "FROM local_equipment WHERE project_code = ? ORDER BY created_ts DESC",
            (project_code,),
        ).fetchall()
        items = []
        for row in rows:
            if isinstance(row, Mapping):
                items.append(dict(row))
            elif isinstance(row, Sequence):
                items.append({
                    "id": row[0],
                    "name": row[1],
                    "quantity": row[2],
                    "notes": row[3],
                    "group_name": row[4],
                    "created_ts": row[5],
                    "updated_ts": row[6],
                })
        return jsonify({"ok": True, "items": items})

    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"ok": False, "error": "missing_name"}), 400

    quantity = int(data.get("quantity", 1))
    notes = (data.get("notes") or "").strip() or None
    group_name = (data.get("group_name") or "").strip() or "Attrezzature extra"
    now = int(time.time() * 1000)

    db.execute(
        "INSERT INTO local_equipment (project_code, name, quantity, notes, group_name, created_ts, updated_ts) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (project_code, name, quantity, notes, group_name, now, now),
    )
    new_id = _last_insert_id(db)
    db.commit()

    return jsonify({
        "ok": True,
        "item": {
            "id": new_id,
            "name": name,
            "quantity": quantity,
            "notes": notes,
            "group_name": group_name,
            "created_ts": now,
            "updated_ts": now,
        },
    })


@app.delete("/api/project/local-equipment/<int:item_id>")
@login_required
def api_delete_local_equipment(item_id: int):
    db = get_db()
    project_code = get_app_state(db, "project_code")
    if not project_code:
        return jsonify({"ok": False, "error": "no_active_project"}), 400

    ensure_local_equipment_table(db)
    db.execute(
        "DELETE FROM local_equipment WHERE id = ? AND project_code = ?",
        (item_id, project_code),
    )
    db.commit()
    return jsonify({"ok": True})


# ──────────────────────────────────────────────────────────────────────────────
# Foto progetto
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/api/project/photos", methods=["GET", "POST"])
@login_required
def api_project_photos():
    db = get_db()
    project_code = get_app_state(db, "project_code")
    if not project_code:
        return jsonify({"ok": False, "error": "no_active_project"}), 400

    ensure_project_photos_table(db)

    if request.method == "GET":
        rows = db.execute(
            "SELECT id, filename, original_name, mime_type, file_size, caption, created_ts "
            "FROM project_photos WHERE project_code = ? ORDER BY created_ts DESC",
            (project_code,),
        ).fetchall()
        items = []
        for row in rows:
            if isinstance(row, Mapping):
                items.append(dict(row))
            elif isinstance(row, Sequence):
                items.append({
                    "id": row[0],
                    "filename": row[1],
                    "original_name": row[2],
                    "mime_type": row[3],
                    "file_size": row[4],
                    "caption": row[5],
                    "created_ts": row[6],
                })
        return jsonify({"ok": True, "items": items, "project_code": project_code})

    # POST - Upload nuova foto
    if "photo" not in request.files:
        return jsonify({"ok": False, "error": "no_file"}), 400

    file = request.files["photo"]
    if file.filename == "":
        return jsonify({"ok": False, "error": "no_filename"}), 400

    if not allowed_photo_file(file.filename):
        return jsonify({"ok": False, "error": "invalid_file_type"}), 400

    # Leggi il file in memoria per verificare la dimensione
    file_data = file.read()
    if len(file_data) > MAX_PHOTO_SIZE:
        return jsonify({"ok": False, "error": "file_too_large"}), 400

    # Genera nome file univoco
    original_name = file.filename
    ext = original_name.rsplit(".", 1)[1].lower() if "." in original_name else "jpg"
    unique_filename = f"{project_code}_{int(time.time() * 1000)}_{os.urandom(4).hex()}.{ext}"

    # Salva il file
    file_path = os.path.join(PHOTOS_UPLOAD_FOLDER, unique_filename)
    with open(file_path, "wb") as f:
        f.write(file_data)

    # Salva nel database
    caption = request.form.get("caption", "").strip() or None
    mime_type = file.content_type or f"image/{ext}"
    now = int(time.time() * 1000)

    db.execute(
        "INSERT INTO project_photos (project_code, filename, original_name, mime_type, file_size, caption, created_ts) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (project_code, unique_filename, original_name, mime_type, len(file_data), caption, now),
    )
    new_id = _last_insert_id(db)
    db.commit()

    return jsonify({
        "ok": True,
        "item": {
            "id": new_id,
            "filename": unique_filename,
            "original_name": original_name,
            "mime_type": mime_type,
            "file_size": len(file_data),
            "caption": caption,
            "created_ts": now,
        },
    })


@app.get("/api/project/photos/<filename>")
@login_required
def api_get_photo(filename: str):
    """Serve una foto dal filesystem"""
    file_path = os.path.join(PHOTOS_UPLOAD_FOLDER, filename)
    if not os.path.exists(file_path):
        return jsonify({"ok": False, "error": "not_found"}), 404

    # Sicurezza: verifica che il file appartenga al progetto attivo
    db = get_db()
    project_code = get_app_state(db, "project_code")
    ensure_project_photos_table(db)

    row = db.execute(
        "SELECT id FROM project_photos WHERE filename = ? AND project_code = ?",
        (filename, project_code),
    ).fetchone()

    if not row:
        return jsonify({"ok": False, "error": "not_authorized"}), 403

    return send_from_directory(PHOTOS_UPLOAD_FOLDER, filename)


@app.delete("/api/project/photos/<int:photo_id>")
@login_required
def api_delete_photo(photo_id: int):
    db = get_db()
    project_code = get_app_state(db, "project_code")
    if not project_code:
        return jsonify({"ok": False, "error": "no_active_project"}), 400

    ensure_project_photos_table(db)

    # Recupera il filename per eliminare il file
    row = db.execute(
        "SELECT filename FROM project_photos WHERE id = ? AND project_code = ?",
        (photo_id, project_code),
    ).fetchone()

    if not row:
        return jsonify({"ok": False, "error": "not_found"}), 404

    filename = row["filename"] if isinstance(row, Mapping) else row[0]
    file_path = os.path.join(PHOTOS_UPLOAD_FOLDER, filename)

    # Elimina dal database
    db.execute(
        "DELETE FROM project_photos WHERE id = ? AND project_code = ?",
        (photo_id, project_code),
    )
    db.commit()

    # Elimina il file dal filesystem
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except OSError as e:
        app.logger.warning("Impossibile eliminare file foto %s: %s", file_path, e)

    return jsonify({"ok": True})


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

    db = get_db()
    if has_active_member_sessions(db):
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "active_sessions_present",
                    "message": "Sono presenti operatori con attività in corso o in pausa. Termina le sessioni prima di ricaricare il progetto.",
                }
            ),
            409,
        )

    plan = mock_fetch_project(project_code, project_date)
    if plan is None:
        clear_project_state(db)
        db.commit()
        # Pulisce anche la sessione del supervisor
        session.pop('supervisor_project_code', None)
        session.pop('supervisor_project_name', None)
        return jsonify({"ok": False, "error": "project_not_found"}), 404

    apply_project_plan(db, plan)
    db.commit()
    
    # Salva il progetto nella sessione del supervisor (individuale)
    session['supervisor_project_code'] = plan["project_code"]
    session['supervisor_project_name'] = plan.get("project_name")
    
    # Salva anche nel database per persistenza dopo logout/login
    username = session.get('user')
    if username:
        placeholder = "%s" if DB_VENDOR == "mysql" else "?"
        try:
            db.execute(
                f"UPDATE app_users SET current_project_code={placeholder}, current_project_name={placeholder} WHERE username={placeholder}",
                (plan["project_code"], plan.get("project_name"), username)
            )
            db.commit()
            app.logger.info("Progetto %s salvato per utente %s", plan["project_code"], username)
        except Exception as e:
            app.logger.warning("Impossibile salvare progetto corrente per %s: %s", username, e)

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

    project_code = session.get('supervisor_project_code')
    if not project_code:
        return jsonify({"ok": False, "error": "no_project_selected"}), 400

    db = get_db()
    now = now_ms()
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"

    if activity_id:
        exists = db.execute(
            f"SELECT 1 FROM activities WHERE activity_id={placeholder} AND project_code={placeholder}",
            (activity_id, project_code),
        ).fetchone()
        if exists is None:
            return jsonify({"ok": False, "error": "unknown_activity"}), 400

    existing = db.execute(
        f"SELECT * FROM member_state WHERE member_key={placeholder} AND project_code={placeholder}",
        (member_key, project_code),
    ).fetchone()

    if existing is None:
        db.execute(
            f"""
            INSERT INTO member_state(
                member_key, project_code, member_name, activity_id, running, start_ts, elapsed_cached, pause_start, entered_ts
            ) VALUES({placeholder},{placeholder},{placeholder},{placeholder},{placeholder},{placeholder},{placeholder},{placeholder},{placeholder})
            """,
            (member_key, project_code, member_name, None, 0, None, 0, None, None),
        )
        existing = db.execute(
            f"SELECT * FROM member_state WHERE member_key={placeholder} AND project_code={placeholder}",
            (member_key, project_code),
        ).fetchone()
    else:
        db.execute(
            f"UPDATE member_state SET member_name={placeholder} WHERE member_key={placeholder} AND project_code={placeholder}",
            (member_name, member_key, project_code),
        )

    if existing is None:
        app.logger.error("Member state insert fallita per %s", member_key)
        return jsonify({"ok": False, "error": "member_state_error"}), 500

    previous_activity = existing["activity_id"]
    previous_entered_ts = row_value(existing, "entered_ts")
    prev_elapsed = compute_elapsed(existing, now)
    normalized_previous = str(previous_activity) if previous_activity else None
    normalized_target = str(activity_id) if activity_id else None
    same_activity = normalized_previous is not None and normalized_previous == normalized_target

    running = RUN_STATE_RUNNING if activity_id else RUN_STATE_PAUSED
    start_ts = now if running else None
    reset_elapsed = bool(activity_id) and not same_activity
    elapsed_cached = 0 if reset_elapsed else prev_elapsed
    next_entered_ts = previous_entered_ts
    if activity_id and not same_activity:
        next_entered_ts = now
    if not activity_id:
        next_entered_ts = None

    activity_meta = load_activity_meta(db)
    meta_changed = False
    auto_closed_previous = False
    if normalized_previous and normalized_previous != normalized_target:
        if prev_elapsed > 0:
            meta_changed = increment_activity_runtime(activity_meta, normalized_previous, prev_elapsed)
        auto_closed_previous = True

    db.execute(
        f"""
        UPDATE member_state
        SET activity_id={placeholder}, running={placeholder}, start_ts={placeholder}, elapsed_cached={placeholder}, pause_start=NULL, entered_ts={placeholder}
        WHERE member_key={placeholder} AND project_code={placeholder}
        """,
        (activity_id, running, start_ts, elapsed_cached, next_entered_ts, member_key, project_code),
    )

    if meta_changed:
        save_activity_meta(db, activity_meta)

    if auto_closed_previous:
        # Calcola il tempo totale partendo dall'ultimo move verso questa attività
        activity_start_ts = find_last_move_ts(db, member_key, normalized_previous)
        if activity_start_ts is None:
            activity_start_ts = previous_entered_ts or existing["start_ts"]

        total_ms = 0
        if activity_start_ts:
            total_ms = max(0, now - activity_start_ts)
        
        pause_ms = max(0, total_ms - prev_elapsed)
        
        finish_payload = {
            "member_name": existing["member_name"],
            "activity_id": normalized_previous,
            "duration_ms": prev_elapsed,
            "total_ms": total_ms,
            "pause_ms": pause_ms,
            "auto_close": True,
            "project_code": project_code,
        }
        db.execute(
            f"INSERT INTO event_log(ts, kind, member_key, details, project_code) VALUES({placeholder},{placeholder},{placeholder},{placeholder},{placeholder})",
            (now, "finish_activity", member_key, json.dumps(finish_payload), project_code),
        )

    move_details = {
        "from": previous_activity,
        "to": activity_id,
        "member_name": member_name,
        "duration_ms": prev_elapsed,
        "project_code": project_code,
    }
    db.execute(
        f"INSERT INTO event_log(ts, kind, member_key, details, project_code) VALUES({placeholder},{placeholder},{placeholder},{placeholder},{placeholder})",
        (now, "move", member_key, json.dumps(move_details), project_code),
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
    project_code = session.get("supervisor_project_code", "")
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"

    # Verifica che l'attività esista per questo progetto
    activity_exists = db.execute(
        f"SELECT 1 FROM activities WHERE activity_id={placeholder} AND project_code={placeholder}",
        (activity_id, project_code),
    ).fetchone()
    
    if not activity_exists:
        return jsonify({"ok": False, "error": "activity_not_found"}), 404

    # Trova tutti i membri assegnati a questa attività con timer non avviato
    rows = db.execute(
        f"SELECT member_key FROM member_state WHERE activity_id={placeholder} AND running={placeholder} AND project_code={placeholder}",
        (activity_id, RUN_STATE_PAUSED, project_code),
    ).fetchall()

    if not rows:
        return jsonify({"ok": True, "affected": 0})

    affected = 0
    for row in rows:
        db.execute(
            f"UPDATE member_state SET running={placeholder}, start_ts={placeholder}, pause_start=NULL WHERE member_key={placeholder} AND project_code={placeholder}",
            (RUN_STATE_RUNNING, now, row["member_key"], project_code),
        )
        affected += 1

    db.execute(
        f"INSERT INTO event_log(ts, kind, details, project_code) VALUES({placeholder},{placeholder},{placeholder},{placeholder})",
        (now, "start_activity", json.dumps({"activity_id": activity_id, "affected": affected}), project_code),
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
    project_code = session.get("supervisor_project_code", "")
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"

    # Verifica che il membro esista e abbia un'attività assegnata
    member = db.execute(
        f"SELECT member_key, member_name, activity_id, running FROM member_state WHERE member_key={placeholder} AND project_code={placeholder}",
        (member_key, project_code),
    ).fetchone()
    
    if not member:
        return jsonify({"ok": False, "error": "member_not_found"}), 404
    
    if not member["activity_id"]:
        return jsonify({"ok": False, "error": "no_activity_assigned"}), 400
    
    if member["running"] == RUN_STATE_RUNNING:
        return jsonify({"ok": False, "error": "already_running"}), 400

    # Avvia il timer
    db.execute(
        f"UPDATE member_state SET running={placeholder}, start_ts={placeholder}, pause_start=NULL WHERE member_key={placeholder} AND project_code={placeholder}",
        (RUN_STATE_RUNNING, now, member_key, project_code),
    )

    db.execute(
        f"INSERT INTO event_log(ts, kind, details, project_code) VALUES({placeholder},{placeholder},{placeholder},{placeholder})",
        (now, "start_member", json.dumps({"member_key": member_key}), project_code),
    )

    db.commit()
    return jsonify({"ok": True})


@app.post("/api/start_all")
@login_required
def api_start_all():
    """Avvia i timer per tutti i membri che hanno un'attività assegnata."""
    now = now_ms()
    db = get_db()
    project_code = session.get("supervisor_project_code", "")
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"

    # Trova tutti i membri con activity_id assegnato ma non in esecuzione
    rows = db.execute(
        f"SELECT member_key FROM member_state WHERE activity_id IS NOT NULL AND running={placeholder} AND project_code={placeholder}",
        (RUN_STATE_PAUSED, project_code),
    ).fetchall()

    if not rows:
        return jsonify({"ok": True, "affected": 0})

    affected = 0
    for row in rows:
        db.execute(
            f"UPDATE member_state SET running={placeholder}, start_ts={placeholder}, pause_start=NULL WHERE member_key={placeholder} AND project_code={placeholder}",
            (RUN_STATE_RUNNING, now, row["member_key"], project_code),
        )
        affected += 1

    db.execute(
        f"INSERT INTO event_log(ts, kind, details, project_code) VALUES({placeholder},{placeholder},{placeholder},{placeholder})",
        (now, "start_all", json.dumps({"affected": affected}), project_code),
    )

    db.commit()
    return jsonify({"ok": True, "affected": affected})


@app.post("/api/pause_all")
@login_required
def api_pause_all():
    now = now_ms()
    db = get_db()
    project_code = session.get("supervisor_project_code", "")
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"

    rows = db.execute(
        f"SELECT member_key, start_ts, elapsed_cached FROM member_state WHERE running={placeholder} AND project_code={placeholder}",
        (RUN_STATE_RUNNING, project_code),
    ).fetchall()

    for row in rows:
        start_ts = row["start_ts"] or now
        elapsed = (row["elapsed_cached"] or 0) + max(0, now - start_ts)
        db.execute(
            f"""
            UPDATE member_state
            SET running={placeholder}, start_ts=NULL, elapsed_cached={placeholder}, pause_start={placeholder}
            WHERE member_key={placeholder} AND project_code={placeholder}
            """,
            (RUN_STATE_PAUSED, elapsed, now, row["member_key"], project_code),
        )

    if rows:
        db.execute(
            f"INSERT INTO event_log(ts, kind, details, project_code) VALUES({placeholder},{placeholder},{placeholder},{placeholder})",
            (now, "pause_all", json.dumps({"affected": len(rows)}), project_code),
        )

    db.commit()
    return jsonify({"ok": True})


@app.post("/api/resume_all")
@login_required
def api_resume_all():
    now = now_ms()
    db = get_db()
    project_code = session.get("supervisor_project_code", "")
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"

    rows = db.execute(
        f"SELECT member_key FROM member_state WHERE running={placeholder} AND pause_start IS NOT NULL AND project_code={placeholder}",
        (RUN_STATE_PAUSED, project_code),
    ).fetchall()

    for row in rows:
        db.execute(
            f"UPDATE member_state SET running={placeholder}, start_ts={placeholder}, pause_start=NULL WHERE member_key={placeholder} AND project_code={placeholder}",
            (RUN_STATE_RUNNING, now, row["member_key"], project_code),
        )

    if rows:
        db.execute(
            f"INSERT INTO event_log(ts, kind, details, project_code) VALUES({placeholder},{placeholder},{placeholder},{placeholder})",
            (now, "resume_all", json.dumps({"affected": len(rows)}), project_code),
        )

    db.commit()
    return jsonify({"ok": True})


@app.post("/api/finish_all")
@login_required
def api_finish_all():
    now = now_ms()
    db = get_db()
    project_code = session.get("supervisor_project_code", "")
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"

    rows = db.execute(
        f"SELECT * FROM member_state WHERE activity_id IS NOT NULL AND project_code={placeholder}",
        (project_code,),
    ).fetchall()

    activity_meta = load_activity_meta(db)
    meta_changed = False

    affected = 0
    for row in rows:
        elapsed = compute_elapsed(row, now)
        if row["activity_id"] and elapsed > 0:
            meta_changed |= increment_activity_runtime(activity_meta, str(row["activity_id"]), elapsed)
        
        activity_start_ts = find_last_move_ts(db, row["member_key"], str(row["activity_id"]))
        if activity_start_ts is None:
            activity_start_ts = row["entered_ts"] or row["start_ts"]

        total_ms = 0
        if activity_start_ts:
            total_ms = max(0, now - activity_start_ts)
        
        pause_ms = max(0, total_ms - elapsed)
        
        db.execute(
            f"""
            UPDATE member_state
            SET activity_id=NULL, running={placeholder}, start_ts=NULL, elapsed_cached=0, pause_start=NULL, entered_ts=NULL
            WHERE member_key={placeholder} AND project_code={placeholder}
            """,
            (RUN_STATE_FINISHED, row["member_key"], project_code),
        )
        db.execute(
            f"INSERT INTO event_log(ts, kind, member_key, details, project_code) VALUES({placeholder},{placeholder},{placeholder},{placeholder},{placeholder})",
            (
                now,
                "finish_activity",
                row["member_key"],
                json.dumps(
                    {
                        "member_name": row["member_name"],
                        "activity_id": row["activity_id"],
                        "duration_ms": elapsed,
                        "total_ms": total_ms,
                        "pause_ms": pause_ms,
                    }
                ),
                project_code,
            ),
        )
        affected += 1

    if affected:
        db.execute(
            f"INSERT INTO event_log(ts, kind, details, project_code) VALUES({placeholder},{placeholder},{placeholder},{placeholder})",
            (
                now,
                "finish_all",
                json.dumps({"affected": affected}),
                project_code,
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
    project_code = session.get("supervisor_project_code", "")
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    member = fetch_member(db, member_key, project_code)
    if member is None:
        return jsonify({"ok": False, "error": "member_not_found"}), 404

    if not member["activity_id"]:
        return jsonify({"ok": False, "error": "member_not_assigned"}), 400

    if member["pause_start"] is not None:
        return jsonify({"ok": True, "already_paused": True})

    if member["running"] != RUN_STATE_RUNNING:
        return jsonify({"ok": False, "error": "member_not_running"}), 400

    now = now_ms()
    elapsed = compute_elapsed(member, now)

    db.execute(
        f"""
        UPDATE member_state
        SET running={placeholder}, start_ts=NULL, elapsed_cached={placeholder}, pause_start={placeholder}
        WHERE member_key={placeholder} AND project_code={placeholder}
        """,
        (RUN_STATE_PAUSED, elapsed, now, member_key, project_code),
    )

    db.execute(
        f"INSERT INTO event_log(ts, kind, member_key, details, project_code) VALUES({placeholder},{placeholder},{placeholder},{placeholder},{placeholder})",
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
            project_code,
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
    project_code = session.get("supervisor_project_code", "")
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    member = fetch_member(db, member_key, project_code)
    if member is None:
        return jsonify({"ok": False, "error": "member_not_found"}), 404

    if not member["activity_id"]:
        return jsonify({"ok": False, "error": "member_not_assigned"}), 400

    if member["running"] == RUN_STATE_RUNNING:
        return jsonify({"ok": True, "already_running": True})

    if member["pause_start"] is None:
        return jsonify({"ok": False, "error": "member_not_paused"}), 400

    now = now_ms()

    db.execute(
        f"UPDATE member_state SET running={placeholder}, start_ts={placeholder}, pause_start=NULL WHERE member_key={placeholder} AND project_code={placeholder}",
        (RUN_STATE_RUNNING, now, member_key, project_code),
    )

    db.execute(
        f"INSERT INTO event_log(ts, kind, member_key, details, project_code) VALUES({placeholder},{placeholder},{placeholder},{placeholder},{placeholder})",
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
            project_code,
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
    project_code = session.get("supervisor_project_code", "")
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    member = fetch_member(db, member_key, project_code)
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

    activity_start_ts = find_last_move_ts(db, member_key, str(member["activity_id"]))
    if activity_start_ts is None:
        activity_start_ts = member["entered_ts"] or member["start_ts"]

    total_ms = 0
    if activity_start_ts:
        total_ms = max(0, now - activity_start_ts)
    pause_ms = max(0, total_ms - elapsed)

    db.execute(
        f"""
        UPDATE member_state
        SET activity_id=NULL, running={placeholder}, start_ts=NULL, elapsed_cached=0, pause_start=NULL, entered_ts=NULL
        WHERE member_key={placeholder} AND project_code={placeholder}
        """,
        (RUN_STATE_FINISHED, member_key, project_code),
    )

    db.execute(
        f"INSERT INTO event_log(ts, kind, member_key, details, project_code) VALUES({placeholder},{placeholder},{placeholder},{placeholder},{placeholder})",
        (
            now,
            "finish_activity",
            member_key,
            json.dumps(
                {
                    "member_name": member["member_name"],
                    "activity_id": member["activity_id"],
                    "duration_ms": elapsed,
                    "total_ms": total_ms,
                    "pause_ms": pause_ms,
                }
            ),
            project_code,
        ),
    )

    if meta_changed:
        save_activity_meta(db, activity_meta)

    db.commit()
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════════════════════
#  API - Gestione operatori nel progetto (capo squadra)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/project/available-operators")
@login_required
def api_project_available_operators():
    """Lista operatori disponibili (non già nel progetto) dalla tabella crew_members."""
    db = get_db()
    project_code = session.get("supervisor_project_code", "")
    
    if not project_code:
        return jsonify({"ok": False, "error": "no_project"}), 400
    
    ensure_crew_members_table(db)
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    # Ottieni gli operatori già nel progetto
    existing_keys = set()
    existing_rows = db.execute(
        f"SELECT member_key FROM member_state WHERE project_code = {placeholder}",
        (project_code,)
    ).fetchall()
    for row in existing_rows:
        key = row["member_key"] if isinstance(row, dict) else row[0]
        existing_keys.add(key)
    
    # Ottieni tutti gli operatori attivi da crew_members
    if DB_VENDOR == "mysql":
        crew_rows = db.execute(
            "SELECT rentman_id, name FROM crew_members WHERE is_active = 1 ORDER BY name"
        ).fetchall()
    else:
        crew_rows = db.execute(
            "SELECT rentman_id, name FROM crew_members WHERE is_active = 1 ORDER BY name"
        ).fetchall()
    
    available = []
    for row in crew_rows:
        rentman_id = row["rentman_id"] if isinstance(row, dict) else row[0]
        name = row["name"] if isinstance(row, dict) else row[1]
        member_key = f"rentman-crew-{rentman_id}"
        
        # Escludi operatori già nel progetto
        if member_key not in existing_keys:
            available.append({
                "key": member_key,
                "name": name,
                "rentman_id": rentman_id
            })
    
    return jsonify({"ok": True, "operators": available})


@app.post("/api/project/add-operator")
@login_required
def api_project_add_operator():
    """Aggiunge un operatore al progetto corrente."""
    data = request.get_json(silent=True) or {}
    
    # Supporta sia l'aggiunta da crew_members (rentman_id) che un nuovo operatore manuale (name)
    rentman_id = data.get("rentman_id")
    name = (data.get("name") or "").strip()
    
    if not rentman_id and not name:
        return jsonify({"ok": False, "error": "missing_data"}), 400
    
    db = get_db()
    project_code = session.get("supervisor_project_code", "")
    
    if not project_code:
        return jsonify({"ok": False, "error": "no_project"}), 400
    
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    now = now_ms()
    
    if rentman_id:
        # Aggiunta da crew_members
        member_key = f"rentman-crew-{rentman_id}"
        
        # Verifica che non esista già nel progetto
        existing = db.execute(
            f"SELECT 1 FROM member_state WHERE member_key = {placeholder} AND project_code = {placeholder}",
            (member_key, project_code)
        ).fetchone()
        
        if existing:
            return jsonify({"ok": False, "error": "already_in_project"}), 400
        
        # Ottieni il nome da crew_members
        if DB_VENDOR == "mysql":
            crew = db.execute(
                "SELECT name FROM crew_members WHERE rentman_id = %s", (rentman_id,)
            ).fetchone()
        else:
            crew = db.execute(
                "SELECT name FROM crew_members WHERE rentman_id = ?", (rentman_id,)
            ).fetchone()
        
        if not crew:
            return jsonify({"ok": False, "error": "operator_not_found"}), 404
        
        member_name = crew["name"] if isinstance(crew, dict) else crew[0]
    else:
        # Operatore manuale - genera una chiave unica
        base_key = f"local-{name.lower().replace(' ', '-')}"
        member_key = base_key
        counter = 1
        
        while True:
            existing = db.execute(
                f"SELECT 1 FROM member_state WHERE member_key = {placeholder} AND project_code = {placeholder}",
                (member_key, project_code)
            ).fetchone()
            if not existing:
                break
            member_key = f"{base_key}-{counter}"
            counter += 1
        
        member_name = name
    
    # Inserisci l'operatore nel progetto
    db.execute(
        f"""
        INSERT INTO member_state (member_key, project_code, member_name, activity_id, running, start_ts, elapsed_cached, pause_start, entered_ts)
        VALUES ({placeholder}, {placeholder}, {placeholder}, NULL, {placeholder}, NULL, 0, NULL, NULL)
        """,
        (member_key, project_code, member_name, RUN_STATE_PAUSED)
    )
    
    # Log evento
    db.execute(
        f"INSERT INTO event_log(ts, kind, details, project_code) VALUES({placeholder},{placeholder},{placeholder},{placeholder})",
        (now, "add_operator", json.dumps({"member_key": member_key, "member_name": member_name}), project_code)
    )
    
    db.commit()
    
    return jsonify({
        "ok": True,
        "member": {
            "key": member_key,
            "name": member_name
        }
    })


@app.post("/api/project/remove-operator")
@login_required
def api_project_remove_operator():
    """Rimuove un operatore dal progetto corrente."""
    data = request.get_json(silent=True) or {}
    member_key = (data.get("member_key") or "").strip()
    
    if not member_key:
        return jsonify({"ok": False, "error": "missing_member_key"}), 400
    
    db = get_db()
    project_code = session.get("supervisor_project_code", "")
    
    if not project_code:
        return jsonify({"ok": False, "error": "no_project"}), 400
    
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    now = now_ms()
    
    # Verifica che l'operatore esista e non sia in attività
    member = db.execute(
        f"SELECT member_name, activity_id, running FROM member_state WHERE member_key = {placeholder} AND project_code = {placeholder}",
        (member_key, project_code)
    ).fetchone()
    
    if not member:
        return jsonify({"ok": False, "error": "member_not_found"}), 404
    
    member_name = member["member_name"] if isinstance(member, dict) else member[0]
    activity_id = member["activity_id"] if isinstance(member, dict) else member[1]
    running = member["running"] if isinstance(member, dict) else member[2]
    
    # Non permettere la rimozione se l'operatore ha un timer attivo
    if running == RUN_STATE_RUNNING:
        return jsonify({"ok": False, "error": "member_running", "message": "Ferma il timer prima di rimuovere l'operatore"}), 400
    
    # Rimuovi l'operatore
    db.execute(
        f"DELETE FROM member_state WHERE member_key = {placeholder} AND project_code = {placeholder}",
        (member_key, project_code)
    )
    
    # Log evento
    db.execute(
        f"INSERT INTO event_log(ts, kind, details, project_code) VALUES({placeholder},{placeholder},{placeholder},{placeholder})",
        (now, "remove_operator", json.dumps({"member_key": member_key, "member_name": member_name}), project_code)
    )
    
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

    start_date_filter = parse_iso_date(start_date)
    end_date_filter = parse_iso_date(end_date)

    session_rows = build_session_rows(
        db,
        start_date=start_date_filter,
        end_date=end_date_filter,
    )

    export_data = []
    for session_row in session_rows:
        start_dt = datetime.fromtimestamp(session_row["start_ts"] / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(session_row["end_ts"] / 1000, tz=timezone.utc)
        status_label = "Completato" if session_row["status"] == "completed" else "In corso"
        export_data.append(
            {
                "operatore": session_row["member_name"],
                "attivita": session_row["activity_label"],
                "data_inizio": start_dt.strftime("%d/%m/%Y"),
                "ora_inizio": start_dt.strftime("%H:%M:%S"),
                "data_fine": end_dt.strftime("%d/%m/%Y") if session_row["status"] == "completed" else "In corso",
                "ora_fine": end_dt.strftime("%H:%M:%S") if session_row["status"] == "completed" else "-",
                "durata_netta": format_duration_ms(session_row["net_ms"]) or "00:00:00",
                "tempo_pausa": format_duration_ms(session_row["pause_ms"]) or "00:00:00",
                "num_pause": str(session_row["pause_count"]),
                "stato": status_label,
            }
        )
    
    app.logger.info("Export: generati %s record per l'export", len(export_data))

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


@app.get("/admin")
@app.get("/admin/dashboard")
@login_required
def admin_dashboard_page() -> ResponseReturnValue:
    if not is_admin_or_supervisor():
        abort(403)

    display_name = session.get("user_display") or session.get("user_name") or session.get("user")
    primary_name = session.get("user_name") or display_name or session.get("user")
    initials = session.get("user_initials") or compute_initials(primary_name or "")

    return render_template(
        "admin_dashboard_new.html",
        user_name=primary_name,
        user_display=display_name,
        user_initials=initials,
        is_admin=bool(session.get("is_admin")),
    )


@app.get("/admin/sessions")
@login_required
def admin_sessions_page() -> ResponseReturnValue:
    if not is_admin_or_supervisor():
        abort(403)

    display_name = session.get('user_display') or session.get('user_name') or session.get('user')
    primary_name = session.get('user_name') or display_name or session.get('user')
    initials = session.get('user_initials') or compute_initials(primary_name or "")

    return render_template(
        "admin_sessions.html",
        user_name=primary_name,
        user_display=display_name,
        user_initials=initials,
        is_admin=bool(session.get("is_admin")),
    )


@app.get("/api/admin/magazzino/summary")
@login_required
def api_admin_magazzino_summary() -> ResponseReturnValue:
    """Riepilogo ore magazzino per progetto (range date)."""
    if not is_admin_or_supervisor():
        return jsonify({"error": "forbidden"}), 403

    # Supporto range date: date_start/date_end oppure date singola (retrocompatibilità)
    date_start = parse_iso_date(request.args.get("date_start")) or parse_iso_date(request.args.get("date")) or datetime.now().date()
    date_end = parse_iso_date(request.args.get("date_end")) or date_start
    
    # Assicura che date_end >= date_start
    if date_end < date_start:
        date_start, date_end = date_end, date_start

    start_dt = datetime.combine(date_start, datetime.min.time())
    end_dt = datetime.combine(date_end, datetime.min.time()) + timedelta(days=1)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    db = get_db()
    ensure_warehouse_sessions_table(db)

    rows = db.execute(
        """
        SELECT project_code, COUNT(*) AS sessions, COALESCE(SUM(elapsed_ms), 0) AS total_ms
        FROM warehouse_sessions
        WHERE created_ts >= ? AND created_ts < ?
        GROUP BY project_code
        ORDER BY total_ms DESC, project_code ASC
        """,
        (start_ms, end_ms),
    ).fetchall()

    team_sessions = build_session_rows(
        db,
        start_date=date_start,
        end_date=date_end,
    )
    team_total_ms = sum(_coerce_int(s.get("net_ms")) or 0 for s in team_sessions)
    team_total_sessions = len(team_sessions)

    items: List[Dict[str, Any]] = []
    total_ms = 0
    total_sessions = 0
    for row in rows or []:
        ms_value = _coerce_int(row["total_ms"]) or 0
        sessions_count = _coerce_int(row["sessions"]) or 0
        total_ms += ms_value
        total_sessions += sessions_count
        items.append(
            {
                "project_code": row["project_code"],
                "sessions": sessions_count,
                "total_ms": ms_value,
            }
        )

    return jsonify(
        {
            "ok": True,
            "date": date_start.isoformat() if date_start == date_end else f"{date_start.isoformat()} - {date_end.isoformat()}",
            "items": items,
            "total_ms": total_ms,
            "total_sessions": total_sessions,
            "team_total_ms": team_total_ms,
            "team_total_sessions": team_total_sessions,
        }
    )


@app.route("/api/admin/day-sessions", methods=["GET"], endpoint="api_admin_day_sessions")
@login_required
def api_admin_day_sessions() -> ResponseReturnValue:
    """Restituisce sessioni di squadra e magazzino per il range di date indicato."""
    if not is_admin_or_supervisor():
        return jsonify({"error": "forbidden"}), 403

    # Supporto range date: date_start/date_end oppure date singola (retrocompatibilità)
    date_start = parse_iso_date(request.args.get("date_start")) or parse_iso_date(request.args.get("date")) or datetime.now().date()
    date_end = parse_iso_date(request.args.get("date_end")) or date_start
    
    # Assicura che date_end >= date_start
    if date_end < date_start:
        date_start, date_end = date_end, date_start

    start_dt = datetime.combine(date_start, datetime.min.time())
    end_dt = datetime.combine(date_end, datetime.min.time()) + timedelta(days=1)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    db = get_db()

    team_sessions = build_session_rows(
        db,
        start_date=date_start,
        end_date=date_end,
    )

    ensure_warehouse_sessions_table(db)
    wh_rows = db.execute(
        """
        SELECT project_code, activity_label, elapsed_ms, username, created_ts
        FROM warehouse_sessions
        WHERE created_ts >= ? AND created_ts < ?
        ORDER BY created_ts DESC
        LIMIT 500
        """,
        (start_ms, end_ms),
    ).fetchall()

    magazzino_sessions = [
        {
            "project_code": row["project_code"],
            "activity_label": row["activity_label"],
            "elapsed_ms": _coerce_int(row["elapsed_ms"]) or 0,
            "username": row["username"],
            "created_ts": _coerce_int(row["created_ts"]) or 0,
        }
        for row in wh_rows or []
    ]

    return jsonify(
        {
            "ok": True,
            "date": date_start.isoformat() if date_start == date_end else f"{date_start.isoformat()} - {date_end.isoformat()}",
            "team_sessions": team_sessions,
            "magazzino_sessions": magazzino_sessions,
        }
    )


@app.route(
    "/api/admin/day-sessions/export.xlsx",
    methods=["GET"],
    endpoint="api_admin_day_sessions_export_xlsx",
)
@login_required
def api_admin_day_sessions_export_xlsx() -> ResponseReturnValue:
    """Esporta in Excel le sessioni (Squadra + Magazzino) per il range di date indicato."""
    if not is_admin_or_supervisor():
        return jsonify({"error": "forbidden"}), 403

    # Supporto range date: date_start/date_end oppure date singola (retrocompatibilità)
    date_start = parse_iso_date(request.args.get("date_start")) or parse_iso_date(request.args.get("date")) or datetime.now().date()
    date_end = parse_iso_date(request.args.get("date_end")) or date_start
    
    # Assicura che date_end >= date_start
    if date_end < date_start:
        date_start, date_end = date_end, date_start

    start_dt = datetime.combine(date_start, datetime.min.time())
    end_dt = datetime.combine(date_end, datetime.min.time()) + timedelta(days=1)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    db = get_db()

    team_sessions = build_session_rows(
        db,
        start_date=date_start,
        end_date=date_end,
    )

    ensure_warehouse_sessions_table(db)
    wh_rows = db.execute(
        """
        SELECT project_code, activity_label, elapsed_ms, username, created_ts
        FROM warehouse_sessions
        WHERE created_ts >= ? AND created_ts < ?
        ORDER BY created_ts DESC
        LIMIT 2000
        """,
        (start_ms, end_ms),
    ).fetchall()

    merged_rows: List[Dict[str, Any]] = []
    for s in team_sessions or []:
        # Estrai data dalla sessione (usa timezone locale)
        start_ts = s.get("start_ts") or s.get("end_ts")
        if start_ts:
            try:
                dt = datetime.fromtimestamp(start_ts / 1000)  # Timezone locale
                date_str = dt.strftime("%d/%m/%Y")
            except Exception:
                date_str = ""
        else:
            date_str = ""
        merged_rows.append(
            {
                "date": date_str,
                "source": "Squadra",
                "project_code": s.get("project_code") or "",
                "user": s.get("member_name") or s.get("member_key") or "",
                "activity": s.get("activity_label") or s.get("activity_id") or "",
                "duration_ms": _coerce_int(s.get("net_ms")) or 0,
                "sort_ts": start_ts or 0,
            }
        )

    for row in wh_rows or []:
        # Estrai data dalla sessione magazzino (usa timezone locale)
        created_ts = _coerce_int(row.get("created_ts")) or 0
        if created_ts:
            try:
                dt = datetime.fromtimestamp(created_ts / 1000)  # Timezone locale
                date_str = dt.strftime("%d/%m/%Y")
            except Exception:
                date_str = ""
        else:
            date_str = ""
        merged_rows.append(
            {
                "date": date_str,
                "source": "Magazzino",
                "project_code": row.get("project_code") or "",
                "user": row.get("username") or "",
                "activity": row.get("activity_label") or "",
                "duration_ms": _coerce_int(row.get("elapsed_ms")) or 0,
                "sort_ts": created_ts,
            }
        )
    
    # Ordina per data/timestamp (più recenti prima)
    merged_rows.sort(key=lambda x: x.get("sort_ts", 0), reverse=True)

    wb = Workbook()
    ws_raw = wb.active
    if ws_raw is None:  # pragma: no cover
        ws_raw = wb.create_sheet(title="Sessioni")
    ws: Worksheet = cast(Worksheet, ws_raw)
    ws.title = "Sessioni"

    title_font = Font(name="Calibri", size=16, bold=True, color="1E293B")
    subtitle_font = Font(name="Calibri", size=11, color="64748B")
    header_font = Font(name="Calibri", size=12, bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="0EA5E9", end_color="0EA5E9", fill_type="solid")
    header_alignment = Alignment(horizontal="center", vertical="center")
    cell_font = Font(name="Calibri", size=11)
    cell_alignment = Alignment(horizontal="left", vertical="center")
    border_thin = Border(
        left=Side(style="thin", color="CBD5E1"),
        right=Side(style="thin", color="CBD5E1"),
        top=Side(style="thin", color="CBD5E1"),
        bottom=Side(style="thin", color="CBD5E1"),
    )

    ws["A1"] = "JobLog - Export sessioni"
    ws.merge_cells("A1:F1")
    ws["A1"].font = title_font
    ws["A1"].alignment = Alignment(horizontal="left", vertical="center")

    ws["A2"] = f"Data: {date_start.strftime('%d/%m/%Y')}" if date_start == date_end else f"Periodo: {date_start.strftime('%d/%m/%Y')} - {date_end.strftime('%d/%m/%Y')}"
    ws.merge_cells("A2:F2")
    ws["A2"].font = subtitle_font
    ws["A2"].alignment = Alignment(horizontal="left", vertical="center")

    ws.append([])

    headers = ["Data", "Fonte", "Progetto", "Utente", "Attività", "Ore"]
    ws.append(headers)
    header_row = ws.max_row
    for col_num, header in enumerate(headers, start=1):
        cell = ws.cell(row=header_row, column=col_num)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_alignment
        cell.border = border_thin

    for item in merged_rows:
        ws.append(
            [
                item["date"],
                item["source"],
                str(item["project_code"] or ""),
                str(item["user"] or ""),
                str(item["activity"] or ""),
                format_duration_ms(item["duration_ms"]) or "00:00:00",
            ]
        )
        row_num = ws.max_row
        for col_num in range(1, 7):
            cell = ws.cell(row=row_num, column=col_num)
            cell.font = cell_font
            cell.alignment = cell_alignment
            cell.border = border_thin
            if row_num % 2 == 0:
                cell.fill = PatternFill(start_color="F8FAFC", end_color="F8FAFC", fill_type="solid")

    ws.column_dimensions[get_column_letter(1)].width = 12
    ws.column_dimensions[get_column_letter(2)].width = 12
    ws.column_dimensions[get_column_letter(3)].width = 14
    ws.column_dimensions[get_column_letter(4)].width = 22
    ws.column_dimensions[get_column_letter(5)].width = 42
    ws.column_dimensions[get_column_letter(6)].width = 12

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"sessioni_{date_start.isoformat()}.xlsx" if date_start == date_end else f"sessioni_{date_start.isoformat()}_{date_end.isoformat()}.xlsx"
    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=filename,
    )


@app.get("/api/admin/sessions")
@login_required
def api_admin_sessions():
    if not is_admin_or_supervisor():
        return jsonify({"error": "forbidden"}), 403

    db = get_db()

    start_date = parse_iso_date(request.args.get("start_date", ""))
    end_date = parse_iso_date(request.args.get("end_date", ""))
    member_filter = request.args.get("member")
    activity_filter = request.args.get("activity_id")
    search_term = (request.args.get("search") or "").strip().lower()

    limit_param = request.args.get("limit")
    try:
        limit = int(limit_param) if limit_param else 200
    except (TypeError, ValueError):
        limit = 200
    limit = max(50, min(limit, 2000))

    # --- Sessioni squadra ---
    team_sessions = build_session_rows(
        db,
        start_date=start_date,
        end_date=end_date,
        member_filter=member_filter,
        activity_filter=activity_filter,
    )

    # --- Sessioni magazzino ---
    ensure_warehouse_sessions_table(db)
    wh_conditions: List[str] = []
    wh_params: List[Any] = []
    if start_date:
        start_dt = datetime.combine(start_date, datetime.min.time())
        wh_conditions.append("created_ts >= ?")
        wh_params.append(int(start_dt.timestamp() * 1000))
    if end_date:
        end_dt = datetime.combine(end_date, datetime.min.time()) + timedelta(days=1)
        wh_conditions.append("created_ts < ?")
        wh_params.append(int(end_dt.timestamp() * 1000))
    if member_filter:
        wh_conditions.append("username = ?")
        wh_params.append(member_filter)
    wh_where = (" WHERE " + " AND ".join(wh_conditions)) if wh_conditions else ""
    wh_rows = db.execute(
        f"""
        SELECT id, project_code, activity_label, elapsed_ms, username, created_ts
        FROM warehouse_sessions
        {wh_where}
        ORDER BY created_ts DESC
        LIMIT 1000
        """,
        tuple(wh_params),
    ).fetchall()

    # Unisci e filtra
    all_sessions: List[Dict[str, Any]] = []

    for item in team_sessions:
        all_sessions.append({**item, "_source": "Squadra", "_sort_ts": item.get("end_ts") or item.get("start_ts") or 0})

    for row in wh_rows or []:
        created_ts = _coerce_int(row["created_ts"]) or 0
        elapsed_ms = _coerce_int(row["elapsed_ms"]) or 0
        all_sessions.append({
            "_source": "Magazzino",
            "_sort_ts": created_ts,
            "member_key": row["username"] or "",
            "member_name": row["username"] or "",
            "activity_id": "",
            "activity_label": row["activity_label"] or "",
            "project_code": row["project_code"] or "",
            "start_ts": created_ts - elapsed_ms if elapsed_ms else created_ts,
            "end_ts": created_ts,
            "status": "completed",
            "net_ms": elapsed_ms,
            "pause_ms": 0,
            "pause_count": 0,
            "auto_closed": False,
            "override_id": None,
            "manual_entry": False,
            "note": "",
            "wh_id": row["id"],
        })

    # Filtra per search_term
    if search_term:
        filtered: List[Dict[str, Any]] = []
        for item in all_sessions:
            haystacks = [
                item.get("member_name", ""),
                item.get("member_key", ""),
                item.get("activity_label", ""),
                item.get("activity_id", ""),
                item.get("project_code", ""),
            ]
            if any(search_term in str(value).lower() for value in haystacks):
                filtered.append(item)
        all_sessions = filtered

    # Ordina per timestamp decrescente
    all_sessions.sort(key=lambda x: x.get("_sort_ts") or 0, reverse=True)
    all_sessions = all_sessions[:limit]

    payload = []
    for item in all_sessions:
        start_ts = item.get("start_ts") or 0
        end_ts = item.get("end_ts") or 0
        start_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc) if start_ts else None
        end_dt = datetime.fromtimestamp(end_ts / 1000, tz=timezone.utc) if end_ts else None
        payload.append(
            {
                "source": item.get("_source", "Squadra"),
                "member_key": item.get("member_key", ""),
                "member_name": item.get("member_name", ""),
                "activity_id": item.get("activity_id", ""),
                "activity_label": item.get("activity_label", ""),
                "project_code": item.get("project_code", ""),
                "start_ts": start_ts,
                "end_ts": end_ts,
                "start_iso": start_dt.isoformat() if start_dt else "",
                "end_iso": end_dt.isoformat() if end_dt else "",
                "status": item.get("status", "completed"),
                "net_ms": item.get("net_ms", 0),
                "pause_ms": item.get("pause_ms", 0),
                "pause_count": item.get("pause_count", 0),
                "net_hms": format_duration_ms(item.get("net_ms")) or "00:00:00",
                "pause_hms": format_duration_ms(item.get("pause_ms")) or "00:00:00",
                "auto_closed": item.get("auto_closed", False),
                "override_id": item.get("override_id"),
                "manual_entry": bool(item.get("manual_entry")),
                "note": item.get("note") or "",
                "source_member_key": item.get("source_member_key"),
                "source_activity_id": item.get("source_activity_id"),
                "source_start_ts": item.get("source_start_ts"),
                "editable": item.get("_source") == "Squadra",
                "wh_id": item.get("wh_id"),
            }
        )

    return jsonify(
        {
            "sessions": payload,
            "count": len(payload),
            "limit": limit,
            "generated_at": now_ms(),
        }
    )


def _admin_or_supervisor() -> Optional[ResponseReturnValue]:
    """Guard per endpoint accessibili a admin e supervisor."""
    if not is_admin_or_supervisor():
        return jsonify({"error": "forbidden"}), 403
    return None


def _admin_only() -> Optional[ResponseReturnValue]:
    """Guard per endpoint accessibili solo ad admin."""
    if not session.get("is_admin"):
        return jsonify({"error": "forbidden"}), 403
    return None


def _json_error(message: str, status: int = 400) -> ResponseReturnValue:
    return jsonify({"error": message}), status


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


@app.post("/api/admin/sessions/save")
@login_required
def api_admin_sessions_save() -> ResponseReturnValue:
    guard = _admin_or_supervisor()
    if guard is not None:
        return guard

    data = request.get_json(silent=True) or {}
    source = _normalize_text(data.get("source")) or "Squadra"
    project_code = _normalize_text(data.get("project_code")) or ""
    member_key = _normalize_text(data.get("member_key"))
    member_name = _normalize_text(data.get("member_name")) or member_key or "Operatore"
    activity_id = _normalize_text(data.get("activity_id"))
    activity_label = _normalize_text(data.get("activity_label")) or activity_id or "Attività"
    if not member_key:
        return _json_error("member_key (ID operatore / Username) è obbligatorio")
    if not activity_label:
        return _json_error("activity_label (Descrizione attività) è obbligatoria")

    start_ts = _normalize_epoch_ms(data.get("start_ts"))
    if start_ts is None:
        return _json_error("start_ts non valido")

    end_ts_raw = data.get("end_ts")
    end_ts_value = _normalize_epoch_ms(end_ts_raw) if end_ts_raw is not None else None
    if end_ts_value is not None and end_ts_value < start_ts:
        return _json_error("end_ts deve essere successivo a start_ts")

    net_ms = _normalize_epoch_ms(data.get("net_ms"))
    if net_ms is None:
        if end_ts_value is not None:
            net_ms = max(0, end_ts_value - start_ts)
        else:
            net_ms = 0

    pause_ms = _normalize_epoch_ms(data.get("pause_ms")) or 0
    pause_count = max(0, _coerce_int(data.get("pause_count")) or 0)
    note = _normalize_text(data.get("note"))
    override_id = _coerce_int(data.get("override_id"))

    db = get_db()
    user = session.get("user") or "admin"
    now = now_ms()

    # Sessione MAGAZZINO: salva in warehouse_sessions
    if source == "Magazzino":
        ensure_warehouse_sessions_table(db)
        created_ts = end_ts_value if end_ts_value else start_ts + net_ms
        db.execute(
            """
            INSERT INTO warehouse_sessions(project_code, activity_label, elapsed_ms, username, created_ts)
            VALUES(?,?,?,?,?)
            """,
            (project_code, activity_label, net_ms, member_key, created_ts),
        )
        db.commit()
        return jsonify({"ok": True, "source": "Magazzino"})

    # Sessione SQUADRA: salva in activity_session_overrides
    if not activity_id:
        activity_id = activity_label

    source_member_key = _normalize_text(data.get("source_member_key")) or None
    source_activity_id = _normalize_text(data.get("source_activity_id")) or None
    source_start_ts = data.get("source_start_ts")
    source_start_ms = (
        _normalize_epoch_ms(source_start_ts) if source_start_ts is not None else None
    )

    manual_entry = bool(data.get("manual_entry"))
    if not manual_entry and not (source_member_key and source_activity_id and source_start_ms):
        manual_entry = True

    status = "completed" if end_ts_value is not None else "running"
    end_ts_final = end_ts_value if end_ts_value is not None else start_ts

    ensure_session_override_table(db)

    params = (
        member_key,
        member_name,
        activity_id,
        activity_label,
        project_code,
        start_ts,
        end_ts_final,
        net_ms,
        pause_ms,
        pause_count,
        status,
        source_member_key,
        source_activity_id,
        source_start_ms,
        1 if manual_entry else 0,
        note or None,
        user,
        user,
        now,
        now,
    )

    if override_id:
        existing = db.execute(
            "SELECT id FROM activity_session_overrides WHERE id=?",
            (override_id,),
        ).fetchone()
        if not existing:
            return _json_error("override non trovato", 404)
        db.execute(
            """
            UPDATE activity_session_overrides
            SET member_key=?, member_name=?, activity_id=?, activity_label=?, project_code=?,
                start_ts=?, end_ts=?, net_ms=?, pause_ms=?, pause_count=?, status=?,
                source_member_key=?, source_activity_id=?, source_start_ts=?,
                manual_entry=?, note=?, updated_by=?, updated_ts=?
            WHERE id=?
            """,
            (
                member_key,
                member_name,
                activity_id,
                activity_label,
                project_code,
                start_ts,
                end_ts_final,
                net_ms,
                pause_ms,
                pause_count,
                status,
                source_member_key,
                source_activity_id,
                source_start_ms,
                1 if manual_entry else 0,
                note or None,
                user,
                now,
                override_id,
            ),
        )
    else:
        db.execute(
            """
            INSERT INTO activity_session_overrides(
                member_key, member_name, activity_id, activity_label, project_code,
                start_ts, end_ts, net_ms, pause_ms, pause_count, status,
                source_member_key, source_activity_id, source_start_ts,
                manual_entry, note, created_by, updated_by, created_ts, updated_ts
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            params,
        )
        override_id = _last_insert_id(db)

    db.commit()

    if not override_id:
        return jsonify({"ok": True})

    row = db.execute(
        "SELECT * FROM activity_session_overrides WHERE id=?",
        (override_id,),
    ).fetchone()
    if not row:
        return jsonify({"ok": True})

    payload = _override_row_to_session(dict(row))
    return jsonify({"ok": True, "session": payload})


@app.delete("/api/admin/sessions/<int:override_id>")
@login_required
def api_admin_sessions_delete(override_id: int) -> ResponseReturnValue:
    guard = _admin_only()
    if guard is not None:
        return guard

    db = get_db()
    ensure_session_override_table(db)
    existing = db.execute(
        "SELECT id FROM activity_session_overrides WHERE id=?",
        (override_id,),
    ).fetchone()
    if not existing:
        return _json_error("override non trovato", 404)

    db.execute("DELETE FROM activity_session_overrides WHERE id=?", (override_id,))
    db.commit()
    return jsonify({"ok": True})


@app.post("/api/_reset")
@login_required
def api_reset():
    db = get_db()
    seed_demo_data(db)
    db.commit()
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════════════════════
#  ANALISI ATTIVITÀ CROSS-PROGETTO
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/admin/activity-analysis")
@login_required
def admin_activity_analysis_page() -> ResponseReturnValue:
    """Pagina analisi attività cross-progetto."""
    if not is_admin_or_supervisor():
        abort(403)

    display_name = session.get("user_display") or session.get("user_name") or session.get("user")
    primary_name = session.get("user_name") or display_name or session.get("user")
    initials = session.get("user_initials") or compute_initials(primary_name or "")

    return render_template(
        "admin_activity_analysis.html",
        user_name=primary_name,
        user_display=display_name,
        user_initials=initials,
        is_admin=bool(session.get("is_admin")),
    )


@app.get("/api/admin/activity-analysis")
@login_required
def api_admin_activity_analysis() -> ResponseReturnValue:
    """API per analisi cross-progetto delle attività."""
    if not is_admin_or_supervisor():
        return jsonify({"error": "forbidden"}), 403

    mode = request.args.get("mode", "list")  # 'list' o 'analysis'
    date_start = parse_iso_date(request.args.get("date_start")) or (datetime.now().date() - timedelta(days=30))
    date_end = parse_iso_date(request.args.get("date_end")) or datetime.now().date()

    if date_end < date_start:
        date_start, date_end = date_end, date_start

    start_dt = datetime.combine(date_start, datetime.min.time())
    end_dt = datetime.combine(date_end, datetime.min.time()) + timedelta(days=1)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    db = get_db()

    # Raccogli sessioni da squadra
    team_sessions = build_session_rows(db, start_date=date_start, end_date=date_end)

    # Raccogli sessioni magazzino
    ensure_warehouse_sessions_table(db)
    wh_rows = db.execute(
        """
        SELECT project_code, activity_label, elapsed_ms, username, created_ts
        FROM warehouse_sessions
        WHERE created_ts >= ? AND created_ts < ?
        """,
        (start_ms, end_ms),
    ).fetchall()

    # Unisci tutte le sessioni
    all_sessions: List[Dict[str, Any]] = []

    for s in team_sessions:
        all_sessions.append({
            "project": s.get("project_code") or "N/A",
            "activity": s.get("activity_label") or s.get("activity_id") or "N/A",
            "duration_ms": _coerce_int(s.get("net_ms")) or 0,
            "source": "Squadra"
        })

    for row in wh_rows or []:
        all_sessions.append({
            "project": row["project_code"] or "N/A",
            "activity": row["activity_label"] or "N/A",
            "duration_ms": _coerce_int(row["elapsed_ms"]) or 0,
            "source": "Magazzino"
        })

    if mode == "list":
        # Restituisci lista attività con statistiche aggregate
        activity_stats: Dict[str, Dict[str, Any]] = {}
        for s in all_sessions:
            act = s["activity"]
            if act not in activity_stats:
                activity_stats[act] = {
                    "name": act,
                    "total_ms": 0,
                    "sessions": 0,
                    "projects": set()
                }
            activity_stats[act]["total_ms"] += s["duration_ms"]
            activity_stats[act]["sessions"] += 1
            activity_stats[act]["projects"].add(s["project"])

        activities = sorted(
            [
                {
                    "name": v["name"],
                    "total_ms": v["total_ms"],
                    "sessions": v["sessions"],
                    "projects": len(v["projects"])
                }
                for v in activity_stats.values()
            ],
            key=lambda x: x["total_ms"],
            reverse=True
        )

        return jsonify({
            "ok": True,
            "activities": activities,
            "date_start": date_start.isoformat(),
            "date_end": date_end.isoformat()
        })

    # mode == 'analysis': analisi dettagliata per attività selezionate
    selected_activities = request.args.getlist("activity")
    if not selected_activities:
        return jsonify({"error": "Nessuna attività selezionata"}), 400

    # Filtra sessioni per attività selezionate
    filtered = [s for s in all_sessions if s["activity"] in selected_activities]

    # Raggruppa per progetto e attività
    matrix: Dict[str, Dict[str, Dict[str, Any]]] = {}
    projects_set: Set[str] = set()
    details: List[Dict[str, Any]] = []

    # Raccogli dati per matrice
    grouped: Dict[str, Dict[str, List[int]]] = {}
    for s in filtered:
        proj = s["project"]
        act = s["activity"]
        projects_set.add(proj)

        if proj not in grouped:
            grouped[proj] = {}
        if act not in grouped[proj]:
            grouped[proj][act] = []
        grouped[proj][act].append(s["duration_ms"])

    # Calcola statistiche per ogni combinazione progetto/attività
    total_ms = 0
    total_sessions = 0

    for proj, acts in grouped.items():
        if proj not in matrix:
            matrix[proj] = {}
        for act, durations in acts.items():
            n = len(durations)
            tot = sum(durations)
            avg = tot / n if n > 0 else 0
            min_d = min(durations) if durations else 0
            max_d = max(durations) if durations else 0

            # Calcola varianza percentuale (coefficiente di variazione)
            if avg > 0 and n > 1:
                variance = sum((d - avg) ** 2 for d in durations) / n
                std_dev = variance ** 0.5
                cv_pct = (std_dev / avg) * 100
            else:
                cv_pct = 0

            matrix[proj][act] = {
                "total_ms": tot,
                "sessions": n,
                "avg_ms": int(avg),
                "min_ms": min_d,
                "max_ms": max_d
            }

            details.append({
                "project": proj,
                "activity": act,
                "sessions": n,
                "total_ms": tot,
                "avg_ms": int(avg),
                "min_ms": min_d,
                "max_ms": max_d,
                "variance_pct": round(cv_pct, 1)
            })

            total_ms += tot
            total_sessions += n

    # Ordina dettagli per ore totali decrescenti
    details.sort(key=lambda x: x["total_ms"], reverse=True)

    return jsonify({
        "ok": True,
        "projects": sorted(projects_set),
        "activities": selected_activities,
        "matrix": matrix,
        "details": details,
        "total_ms": total_ms,
        "total_sessions": total_sessions,
        "date_start": date_start.isoformat(),
        "date_end": date_end.isoformat()
    })


@app.get("/api/admin/activity-analysis/export.xlsx")
@login_required
def api_admin_activity_analysis_export() -> ResponseReturnValue:
    """Esporta analisi attività in Excel."""
    if not is_admin_or_supervisor():
        return jsonify({"error": "forbidden"}), 403

    date_start = parse_iso_date(request.args.get("date_start")) or (datetime.now().date() - timedelta(days=30))
    date_end = parse_iso_date(request.args.get("date_end")) or datetime.now().date()
    selected_activities = request.args.getlist("activity")

    if not selected_activities:
        return jsonify({"error": "Nessuna attività selezionata"}), 400

    # Riusa la logica dell'API
    if date_end < date_start:
        date_start, date_end = date_end, date_start

    start_dt = datetime.combine(date_start, datetime.min.time())
    end_dt = datetime.combine(date_end, datetime.min.time()) + timedelta(days=1)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    db = get_db()
    team_sessions = build_session_rows(db, start_date=date_start, end_date=date_end)

    ensure_warehouse_sessions_table(db)
    wh_rows = db.execute(
        """
        SELECT project_code, activity_label, elapsed_ms
        FROM warehouse_sessions
        WHERE created_ts >= ? AND created_ts < ?
        """,
        (start_ms, end_ms),
    ).fetchall()

    all_sessions: List[Dict[str, Any]] = []
    for s in team_sessions:
        all_sessions.append({
            "project": s.get("project_code") or "N/A",
            "activity": s.get("activity_label") or "N/A",
            "duration_ms": _coerce_int(s.get("net_ms")) or 0,
        })
    for row in wh_rows or []:
        all_sessions.append({
            "project": row["project_code"] or "N/A",
            "activity": row["activity_label"] or "N/A",
            "duration_ms": _coerce_int(row["elapsed_ms"]) or 0,
        })

    filtered = [s for s in all_sessions if s["activity"] in selected_activities]

    # Raggruppa e calcola statistiche
    grouped: Dict[str, Dict[str, List[int]]] = {}
    for s in filtered:
        proj, act = s["project"], s["activity"]
        if proj not in grouped:
            grouped[proj] = {}
        if act not in grouped[proj]:
            grouped[proj][act] = []
        grouped[proj][act].append(s["duration_ms"])

    rows_data: List[Dict[str, Any]] = []
    for proj, acts in grouped.items():
        for act, durations in acts.items():
            n = len(durations)
            tot = sum(durations)
            avg = tot / n if n > 0 else 0
            rows_data.append({
                "project": proj,
                "activity": act,
                "sessions": n,
                "total_ms": tot,
                "avg_ms": int(avg),
                "min_ms": min(durations) if durations else 0,
                "max_ms": max(durations) if durations else 0,
            })

    rows_data.sort(key=lambda x: x["total_ms"], reverse=True)

    # Genera Excel
    wb = Workbook()
    ws_raw = wb.active
    if ws_raw is None:
        ws_raw = wb.create_sheet(title="Analisi Attività")
    ws: Worksheet = cast(Worksheet, ws_raw)
    ws.title = "Analisi Attività"

    title_font = Font(name="Calibri", size=16, bold=True, color="1E293B")
    subtitle_font = Font(name="Calibri", size=11, color="64748B")
    header_font = Font(name="Calibri", size=12, bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="7C3AED", end_color="7C3AED", fill_type="solid")
    header_alignment = Alignment(horizontal="center", vertical="center")
    cell_font = Font(name="Calibri", size=11)
    cell_alignment = Alignment(horizontal="left", vertical="center")
    border_thin = Border(
        left=Side(style="thin", color="CBD5E1"),
        right=Side(style="thin", color="CBD5E1"),
        top=Side(style="thin", color="CBD5E1"),
        bottom=Side(style="thin", color="CBD5E1"),
    )

    ws["A1"] = "🔬 JobLog - Analisi Attività Cross-Progetto"
    ws.merge_cells("A1:G1")
    ws["A1"].font = title_font

    period_str = f"Periodo: {date_start.strftime('%d/%m/%Y')} - {date_end.strftime('%d/%m/%Y')}"
    ws["A2"] = period_str
    ws.merge_cells("A2:G2")
    ws["A2"].font = subtitle_font

    ws["A3"] = f"Attività analizzate: {', '.join(selected_activities)}"
    ws.merge_cells("A3:G3")
    ws["A3"].font = subtitle_font

    ws.append([])

    headers = ["Progetto", "Attività", "Sessioni", "Ore Totali", "Media", "Min", "Max"]
    ws.append(headers)
    header_row = ws.max_row
    for col_num, header in enumerate(headers, start=1):
        cell = ws.cell(row=header_row, column=col_num)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_alignment
        cell.border = border_thin

    for item in rows_data:
        ws.append([
            item["project"],
            item["activity"],
            item["sessions"],
            format_duration_ms(item["total_ms"]) or "00:00:00",
            format_duration_ms(item["avg_ms"]) or "00:00:00",
            format_duration_ms(item["min_ms"]) or "00:00:00",
            format_duration_ms(item["max_ms"]) or "00:00:00",
        ])
        row_num = ws.max_row
        for col_num in range(1, 8):
            cell = ws.cell(row=row_num, column=col_num)
            cell.font = cell_font
            cell.alignment = cell_alignment
            cell.border = border_thin
            if row_num % 2 == 0:
                cell.fill = PatternFill(start_color="F8FAFC", end_color="F8FAFC", fill_type="solid")

    for col_letter, width in {"A": 18, "B": 30, "C": 12, "D": 14, "E": 12, "F": 12, "G": 12}.items():
        ws.column_dimensions[col_letter].width = width

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"analisi_attivita_{date_start.isoformat()}_{date_end.isoformat()}.xlsx"
    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=filename,
    )


# Registrazione lazy del worker: si assicura che il thread sia attivo al primo accesso
@app.before_request
def _ensure_notification_worker() -> None:
    if _NOTIFICATION_THREAD is None or not _NOTIFICATION_THREAD.is_alive():
        start_notification_worker()


atexit.register(stop_notification_worker)


# Registrazione lazy del worker CedolinoWeb per retry timbrate
@app.before_request
def _ensure_cedolino_retry_worker() -> None:
    if _CEDOLINO_RETRY_THREAD is None or not _CEDOLINO_RETRY_THREAD.is_alive():
        start_cedolino_retry_worker()


# ═══════════════════════════════════════════════════════════════════════════════
#  CREW MEMBERS - DATABASE (Operatori Rentman)
# ═══════════════════════════════════════════════════════════════════════════════

CREW_MEMBERS_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS crew_members (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rentman_id INT NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    external_id VARCHAR(255) DEFAULT NULL,
    external_group_id VARCHAR(255) DEFAULT NULL,
    email VARCHAR(255) DEFAULT NULL,
    phone VARCHAR(50) DEFAULT NULL,
    is_active TINYINT(1) DEFAULT 1,
    created_ts BIGINT NOT NULL,
    updated_ts BIGINT NOT NULL,
    INDEX idx_crew_external (external_id),
    INDEX idx_crew_external_group (external_group_id),
    INDEX idx_crew_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

CREW_MEMBERS_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS crew_members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rentman_id INTEGER NOT NULL UNIQUE,
    name TEXT NOT NULL,
    external_id TEXT DEFAULT NULL,
    external_group_id TEXT DEFAULT NULL,
    email TEXT DEFAULT NULL,
    phone TEXT DEFAULT NULL,
    is_active INTEGER DEFAULT 1,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_crew_external ON crew_members(external_id);
CREATE INDEX IF NOT EXISTS idx_crew_external_group ON crew_members(external_group_id);
CREATE INDEX IF NOT EXISTS idx_crew_active ON crew_members(is_active);
"""


def ensure_crew_members_table(db: DatabaseLike) -> None:
    """Crea la tabella crew_members se non esiste."""
    statement = (
        CREW_MEMBERS_TABLE_MYSQL if DB_VENDOR == "mysql" else CREW_MEMBERS_TABLE_SQLITE
    )
    for stmt in statement.strip().split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        try:
            cursor = db.execute(sql)
            try:
                cursor.close()
            except AttributeError:
                pass
        except Exception:
            pass  # Tabella/indice già esistente
    
    # Migrazione: aggiunge colonna external_group_id se non esiste
    try:
        if DB_VENDOR == "mysql":
            # Verifica se la colonna esiste già
            check = db.execute(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_name='crew_members' AND column_name='external_group_id'"
            ).fetchone()
            col_exists = (check[0] if check else 0) > 0
            if not col_exists:
                db.execute("ALTER TABLE crew_members ADD COLUMN external_group_id VARCHAR(255) DEFAULT NULL AFTER external_id")
                db.commit()
                app.logger.info("Colonna external_group_id aggiunta a crew_members")
        else:
            # SQLite: verifica tramite PRAGMA
            cols = db.execute("PRAGMA table_info(crew_members)").fetchall()
            col_names = [c[1] for c in cols]
            if "external_group_id" not in col_names:
                db.execute("ALTER TABLE crew_members ADD COLUMN external_group_id TEXT DEFAULT NULL")
                db.commit()
                app.logger.info("Colonna external_group_id aggiunta a crew_members")
    except Exception as e:
        app.logger.warning("Migrazione external_group_id: %s", e)
    
    # Migrazione: aggiunge colonna timbratura_override per eccezioni per operatore
    try:
        if DB_VENDOR == "mysql":
            check = db.execute(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_name='crew_members' AND column_name='timbratura_override'"
            ).fetchone()
            col_exists = (check[0] if check else 0) > 0
            if not col_exists:
                db.execute("ALTER TABLE crew_members ADD COLUMN timbratura_override TEXT DEFAULT NULL")
                db.commit()
                app.logger.info("Colonna timbratura_override aggiunta a crew_members")
        else:
            cols = db.execute("PRAGMA table_info(crew_members)").fetchall()
            col_names = [c[1] for c in cols]
            if "timbratura_override" not in col_names:
                db.execute("ALTER TABLE crew_members ADD COLUMN timbratura_override TEXT DEFAULT NULL")
                db.commit()
                app.logger.info("Colonna timbratura_override aggiunta a crew_members")
    except Exception as e:
        app.logger.warning("Migrazione timbratura_override: %s", e)


def sync_crew_member_from_rentman(db: DatabaseLike, rentman_id: int, name: str) -> None:
    """Sincronizza un operatore da Rentman nel database locale (insert or update name)."""
    now = now_ms()
    if DB_VENDOR == "mysql":
        existing = db.execute(
            "SELECT id FROM crew_members WHERE rentman_id = %s", (rentman_id,)
        ).fetchone()
        if existing:
            db.execute(
                "UPDATE crew_members SET name = %s, updated_ts = %s WHERE rentman_id = %s",
                (name, now, rentman_id)
            )
        else:
            db.execute(
                "INSERT INTO crew_members (rentman_id, name, created_ts, updated_ts) VALUES (%s, %s, %s, %s)",
                (rentman_id, name, now, now)
            )
    else:
        existing = db.execute(
            "SELECT id FROM crew_members WHERE rentman_id = ?", (rentman_id,)
        ).fetchone()
        if existing:
            db.execute(
                "UPDATE crew_members SET name = ?, updated_ts = ? WHERE rentman_id = ?",
                (name, now, rentman_id)
            )
        else:
            db.execute(
                "INSERT INTO crew_members (rentman_id, name, created_ts, updated_ts) VALUES (?, ?, ?, ?)",
                (rentman_id, name, now, now)
            )


# ═══════════════════════════════════════════════════════════════════════════════
#  CEDOLINO WEB - Funzioni di integrazione
# ═══════════════════════════════════════════════════════════════════════════════

def ensure_cedolino_timbrature_table(db: DatabaseLike) -> None:
    """Crea la tabella cedolino_timbrature se non esiste."""
    statement = (
        CEDOLINO_TIMBRATURE_TABLE_MYSQL if DB_VENDOR == "mysql" else CEDOLINO_TIMBRATURE_TABLE_SQLITE
    )
    for stmt in statement.strip().split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        try:
            cursor = db.execute(sql)
            try:
                cursor.close()
            except AttributeError:
                pass
        except Exception:
            pass
    
    # Migrazioni: aggiungi colonne se non esistono
    if DB_VENDOR == "mysql":
        migrations = [
            "ALTER TABLE cedolino_timbrature ADD COLUMN username VARCHAR(190) DEFAULT NULL",
            "ALTER TABLE cedolino_timbrature ADD COLUMN ora_originale TIME DEFAULT NULL",
            "ALTER TABLE cedolino_timbrature ADD COLUMN ora_modificata TIME DEFAULT NULL",
            "ALTER TABLE cedolino_timbrature ADD COLUMN data_riferimento DATE DEFAULT NULL",
        ]
        for migration in migrations:
            try:
                db.execute(migration)
                db.commit()
            except Exception:
                pass  # Colonna già esistente
    else:
        migrations = [
            "ALTER TABLE cedolino_timbrature ADD COLUMN username TEXT",
            "ALTER TABLE cedolino_timbrature ADD COLUMN ora_originale TEXT",
            "ALTER TABLE cedolino_timbrature ADD COLUMN ora_modificata TEXT",
            "ALTER TABLE cedolino_timbrature ADD COLUMN data_riferimento TEXT",
        ]
        for migration in migrations:
            try:
                db.execute(migration)
                db.commit()
            except Exception:
                pass


def get_cedolino_settings() -> Optional[Dict[str, Any]]:
    """Restituisce le impostazioni CedolinoWeb dal config.json."""
    config = load_config()
    section = config.get("cedolino_web")
    if not section or not isinstance(section, dict):
        return None
    if not section.get("enabled"):
        return None
    return section


def get_external_id_for_member(db: DatabaseLike, member_key: str) -> Optional[str]:
    """
    Recupera l'external_id (ID CedolinoWeb) per un operatore dato il member_key.
    Il member_key ha formato 'rentman-crew-{rentman_id}'.
    """
    if not member_key:
        return None
    
    # Estrai rentman_id dal member_key
    if not member_key.startswith("rentman-crew-"):
        app.logger.warning("CedolinoWeb: member_key non valido (formato atteso: rentman-crew-ID): %s", member_key)
        return None
    
    try:
        rentman_id = int(member_key.replace("rentman-crew-", ""))
    except ValueError:
        app.logger.warning("CedolinoWeb: impossibile estrarre rentman_id da %s", member_key)
        return None
    
    # Cerca l'external_id nella tabella crew_members
    if DB_VENDOR == "mysql":
        row = db.execute(
            "SELECT external_id FROM crew_members WHERE rentman_id = %s",
            (rentman_id,)
        ).fetchone()
    else:
        row = db.execute(
            "SELECT external_id FROM crew_members WHERE rentman_id = ?",
            (rentman_id,)
        ).fetchone()
    
    if not row:
        app.logger.debug("CedolinoWeb: nessun operatore trovato per rentman_id %s", rentman_id)
        return None
    
    external_id = row["external_id"] if isinstance(row, dict) else row[0]
    return external_id if external_id else None


def get_external_id_for_username(db: DatabaseLike, username: str, return_reason: bool = False):
    """
    Recupera l'external_id (ID CedolinoWeb) per un utente dato il suo username.
    L'utente deve avere un rentman_crew_id associato in app_users,
    che viene poi cercato in crew_members per ottenere l'external_id.
    
    Args:
        db: connessione database
        username: username dell'utente
        return_reason: se True, ritorna anche il motivo in caso di errore
    
    Returns:
        Se return_reason=False: external_id o None
        Se return_reason=True: (external_id, reason) dove reason spiega il problema
    """
    if not username:
        if return_reason:
            return None, "Username non fornito"
        return None
    
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    # Recupera il rentman_crew_id dall'utente
    user_row = db.execute(
        f"SELECT rentman_crew_id FROM app_users WHERE username = {placeholder}",
        (username,)
    ).fetchone()
    
    if not user_row:
        app.logger.debug("CedolinoWeb: utente %s non trovato", username)
        if return_reason:
            return None, "Utente non trovato nel database"
        return None
    
    crew_id = user_row['rentman_crew_id'] if isinstance(user_row, dict) else user_row[0]
    if not crew_id:
        app.logger.debug("CedolinoWeb: utente %s non ha rentman_crew_id", username)
        if return_reason:
            return None, "Utente non ha un Operatore associato. Vai in Gestione Utenti e associa un operatore."
        return None
    
    # Cerca l'external_id nella tabella crew_members
    if DB_VENDOR == "mysql":
        row = db.execute(
            "SELECT external_id FROM crew_members WHERE rentman_id = %s",
            (crew_id,)
        ).fetchone()
    else:
        row = db.execute(
            "SELECT external_id FROM crew_members WHERE rentman_id = ?",
            (crew_id,)
        ).fetchone()
    
    if not row:
        app.logger.debug("CedolinoWeb: nessun crew_member trovato per rentman_id %s", crew_id)
        if return_reason:
            return None, f"Operatore (ID {crew_id}) non trovato nella tabella operatori"
        return None
    
    external_id = row["external_id"] if isinstance(row, dict) else row[0]
    if not external_id:
        if return_reason:
            return None, "L'operatore associato non ha l'ID Esterno CedolinoWeb configurato. Vai in Gestione Operatori."
        return None
    
    if return_reason:
        return external_id, ""
    return external_id


def get_external_group_id_for_username(db: DatabaseLike, username: str) -> Optional[str]:
    """
    Recupera l'external_group_id (Gruppo ID CedolinoWeb) per un utente dato il suo username.
    L'utente deve avere un rentman_crew_id associato in app_users,
    che viene poi cercato in crew_members per ottenere l'external_group_id.
    """
    if not username:
        return None
    
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    # Recupera il rentman_crew_id dall'utente
    user_row = db.execute(
        f"SELECT rentman_crew_id FROM app_users WHERE username = {placeholder}",
        (username,)
    ).fetchone()
    
    if not user_row:
        return None
    
    crew_id = user_row['rentman_crew_id'] if isinstance(user_row, dict) else user_row[0]
    if not crew_id:
        return None
    
    # Cerca l'external_group_id nella tabella crew_members
    if DB_VENDOR == "mysql":
        row = db.execute(
            "SELECT external_group_id FROM crew_members WHERE rentman_id = %s",
            (crew_id,)
        ).fetchone()
    else:
        row = db.execute(
            "SELECT external_group_id FROM crew_members WHERE rentman_id = ?",
            (crew_id,)
        ).fetchone()
    
    if not row:
        return None
    
    external_group_id = row["external_group_id"] if isinstance(row, dict) else row[0]
    return external_group_id if external_group_id else None


def call_cedolino_webservice(
    external_id: str,
    timeframe_id: int,
    data_riferimento: str,
    data_originale: str,
    data_modificata: str,
    endpoint: Optional[str] = None,
    gruppo_id: Optional[str] = None
) -> Tuple[bool, Optional[str], str]:
    """
    Chiama il webservice CedolinoWeb per registrare una timbrata.
    
    Args:
        external_id: ID esterno dell'operatore (codice_utente e assunzione_id)
        timeframe_id: tipo di timbrata (1=inizio, 3=pausa, 4=fine pausa, 8=fine)
        data_riferimento: data di riferimento (formato YYYY-MM-DD)
        data_originale: data/ora originale (formato YYYY-MM-DD HH:MM:SS)
        data_modificata: data/ora modificata (formato YYYY-MM-DD HH:MM:SS)
        endpoint: URL del webservice (default: CEDOLINO_WEB_ENDPOINT)
        gruppo_id: ID del gruppo esterno (default: NULL)
    
    Returns:
        Tuple (success: bool, error_message: Optional[str], request_url: str)
    """
    import requests
    
    if not endpoint:
        endpoint = CEDOLINO_WEB_ENDPOINT
    
    params = {
        "data_riferimento": data_riferimento,
        "data_originale": data_originale,
        "data_modificata": data_modificata,
        "codice_utente": external_id,
        "codice_terminale": CEDOLINO_CODICE_TERMINALE,
        "timeframe_id": str(timeframe_id),
        "assunzione_id": external_id,
        "terminale_id": "NULL",
        "gruppo_id": gruppo_id if gruppo_id else "NULL",
        "turno_id": "NULL",
        "note": "",
        "validata": "true",
    }
    
    # Costruisci URL completo per debug
    from urllib.parse import urlencode
    full_url = f"{endpoint}?{urlencode(params)}"
    app.logger.info("=" * 80)
    app.logger.info("CedolinoWeb REQUEST URL:")
    app.logger.info(full_url)
    app.logger.info("=" * 80)
    
    try:
        app.logger.info(
            "CedolinoWeb: invio timbrata per %s, timeframe=%s, originale=%s, modificata=%s",
            external_id, timeframe_id, data_originale, data_modificata
        )
        response = requests.get(endpoint, params=params, timeout=30)
        
        # Log della risposta
        app.logger.info("CedolinoWeb RESPONSE: status=%s, body=%s", response.status_code, response.text[:500] if response.text else "(vuoto)")
        
        if response.status_code == 200:
            app.logger.info("CedolinoWeb: timbrata registrata con successo per %s", external_id)
            return True, None, full_url
        else:
            error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
            app.logger.warning("CedolinoWeb: errore HTTP - %s", error_msg)
            return False, error_msg, full_url
            
    except requests.Timeout:
        error_msg = "Timeout durante la connessione"
        app.logger.warning("CedolinoWeb: %s", error_msg)
        return False, error_msg, full_url
    except requests.RequestException as e:
        error_msg = f"Errore di rete: {str(e)}"
        app.logger.warning("CedolinoWeb: %s", error_msg)
        return False, error_msg, full_url
    except Exception as e:
        error_msg = f"Errore imprevisto: {str(e)}"
        app.logger.exception("CedolinoWeb: errore imprevisto")
        return False, error_msg, full_url


def send_timbrata(
    db: DatabaseLike,
    member_key: str,
    member_name: str,
    timeframe_id: int,
    timestamp_ms: Optional[int] = None,
    project_code: Optional[str] = None,
    activity_id: Optional[str] = None,
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Registra una timbrata operatore e tenta l'invio a CedolinoWeb.
    Per le timbrature operatori usa lo stesso orario per originale e modificato.
    
    Args:
        db: connessione database
        member_key: chiave operatore (es. rentman-crew-1656)
        member_name: nome operatore
        timeframe_id: tipo timbrata (1, 3, 4, 8)
        timestamp_ms: timestamp (default: now)
        project_code: codice progetto corrente
        activity_id: ID attività corrente
    
    Returns:
        Tuple (success: bool, external_id: Optional[str], error: Optional[str])
        - success=True se timbrata inviata o CedolinoWeb disabilitato
        - external_id=None se operatore non ha ID esterno
        - error=messaggio se fallito
    """
    settings = get_cedolino_settings()
    
    # Se CedolinoWeb non è configurato/abilitato, ritorna OK silenziosamente
    if not settings:
        return True, None, None
    
    if timestamp_ms is None:
        timestamp_ms = now_ms()
    
    # Recupera external_id
    external_id = get_external_id_for_member(db, member_key)
    if not external_id:
        # Operatore senza ID esterno - blocca l'operazione
        return False, None, "Operatore senza ID esterno CedolinoWeb"
    
    # Calcola data_riferimento e ora
    dt = datetime.fromtimestamp(timestamp_ms / 1000.0)
    data_riferimento = dt.strftime("%Y-%m-%d")
    ora = dt.strftime("%H:%M:%S")
    data_ora_completa = f"{data_riferimento} {ora}"
    
    # Assicurati che la tabella esista
    ensure_cedolino_timbrature_table(db)
    
    now = now_ms()
    
    # Salva la timbrata nel database
    if DB_VENDOR == "mysql":
        db.execute(
            """
            INSERT INTO cedolino_timbrature 
            (member_key, member_name, username, external_id, timeframe_id, timestamp_ms, 
             data_riferimento, ora_originale, ora_modificata, project_code, activity_id, created_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (member_key, member_name, None, external_id, timeframe_id, timestamp_ms,
             data_riferimento, ora, ora, project_code, activity_id, now)
        )
    else:
        db.execute(
            """
            INSERT INTO cedolino_timbrature 
            (member_key, member_name, username, external_id, timeframe_id, timestamp_ms, 
             data_riferimento, ora_originale, ora_modificata, project_code, activity_id, created_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (member_key, member_name, None, external_id, timeframe_id, timestamp_ms,
             data_riferimento, ora, ora, project_code, activity_id, now)
        )
    
    # Recupera l'ID appena inserito
    timbrata_id = _last_insert_id(db)
    
    # Tenta l'invio al webservice
    endpoint = settings.get("endpoint") or CEDOLINO_WEB_ENDPOINT
    success, error, _url = call_cedolino_webservice(
        external_id, timeframe_id, data_riferimento, data_ora_completa, data_ora_completa, endpoint
    )
    
    # Aggiorna lo stato di sincronizzazione
    if success:
        if DB_VENDOR == "mysql":
            db.execute(
                "UPDATE cedolino_timbrature SET synced_ts = %s WHERE id = %s",
                (now_ms(), timbrata_id)
            )
        else:
            db.execute(
                "UPDATE cedolino_timbrature SET synced_ts = ? WHERE id = ?",
                (now_ms(), timbrata_id)
            )
    else:
        if DB_VENDOR == "mysql":
            db.execute(
                "UPDATE cedolino_timbrature SET sync_error = %s, sync_attempts = 1 WHERE id = %s",
                (error, timbrata_id)
            )
        else:
            db.execute(
                "UPDATE cedolino_timbrature SET sync_error = ?, sync_attempts = 1 WHERE id = ?",
                (error, timbrata_id)
            )
    
    return success, external_id, error


def send_timbrata_utente(
    db: DatabaseLike,
    username: str,
    member_name: str,
    timeframe_id: int,
    data_riferimento: str,
    ora_originale: str,
    ora_modificata: str,
) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
    """
    Registra una timbrata utente e tenta l'invio a CedolinoWeb.
    Usa ora_originale e ora_modificata dalla tabella timbrature.
    
    Args:
        db: connessione database
        username: username dell'utente
        member_name: nome operatore/utente
        timeframe_id: tipo timbrata (1, 3, 4, 8)
        data_riferimento: data della timbrata (YYYY-MM-DD)
        ora_originale: orario reale della timbrata (HH:MM:SS)
        ora_modificata: orario modificato/arrotondato (HH:MM:SS)
    
    Returns:
        Tuple (success: bool, external_id: Optional[str], error: Optional[str], request_url: Optional[str])
        - success=True se timbrata inviata o CedolinoWeb disabilitato
        - external_id=None se operatore non ha ID esterno
        - error=messaggio se fallito
        - request_url=URL completo della chiamata (per debug)
    """
    settings = get_cedolino_settings()
    
    # Se CedolinoWeb non è configurato/abilitato, ritorna OK silenziosamente
    if not settings:
        return True, None, None, None
    
    # Recupera external_id dall'username con motivo dettagliato
    external_id, reason = get_external_id_for_username(db, username, return_reason=True)
    if not external_id:
        # Utente senza ID esterno - blocca l'operazione
        return False, None, reason or "Utente senza ID esterno CedolinoWeb", None
    
    # Recupera external_group_id dall'username
    external_group_id = get_external_group_id_for_username(db, username)
    
    # Assicurati che ora_modificata abbia valore (fallback a ora_originale)
    if not ora_modificata:
        ora_modificata = ora_originale
    
    # Componi data_originale e data_modificata
    data_originale = f"{data_riferimento} {ora_originale}"
    data_modificata = f"{data_riferimento} {ora_modificata}"
    
    # Timestamp in millisecondi
    try:
        dt = datetime.strptime(f"{data_riferimento} {ora_originale}", "%Y-%m-%d %H:%M:%S")
        timestamp_ms = int(dt.timestamp() * 1000)
    except ValueError:
        # Prova con formato senza secondi
        try:
            dt = datetime.strptime(f"{data_riferimento} {ora_originale}", "%Y-%m-%d %H:%M")
            timestamp_ms = int(dt.timestamp() * 1000)
        except ValueError:
            timestamp_ms = now_ms()
    
    # Assicurati che la tabella esista
    ensure_cedolino_timbrature_table(db)
    
    now = now_ms()
    
    # Salva la timbrata nel database
    if DB_VENDOR == "mysql":
        db.execute(
            """
            INSERT INTO cedolino_timbrature 
            (member_key, member_name, username, external_id, timeframe_id, timestamp_ms, 
             data_riferimento, ora_originale, ora_modificata, project_code, activity_id, created_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (None, member_name, username, external_id, timeframe_id, timestamp_ms,
             data_riferimento, ora_originale, ora_modificata, None, None, now)
        )
    else:
        db.execute(
            """
            INSERT INTO cedolino_timbrature 
            (member_key, member_name, username, external_id, timeframe_id, timestamp_ms, 
             data_riferimento, ora_originale, ora_modificata, project_code, activity_id, created_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (None, member_name, username, external_id, timeframe_id, timestamp_ms,
             data_riferimento, ora_originale, ora_modificata, None, None, now)
        )
    
    # Recupera l'ID appena inserito
    timbrata_id = _last_insert_id(db)
    
    # Tenta l'invio al webservice
    endpoint = settings.get("endpoint") or CEDOLINO_WEB_ENDPOINT
    success, error, request_url = call_cedolino_webservice(
        external_id, timeframe_id, data_riferimento, data_originale, data_modificata, endpoint, external_group_id
    )
    
    # Aggiorna lo stato di sincronizzazione
    if success:
        if DB_VENDOR == "mysql":
            db.execute(
                "UPDATE cedolino_timbrature SET synced_ts = %s WHERE id = %s",
                (now_ms(), timbrata_id)
            )
        else:
            db.execute(
                "UPDATE cedolino_timbrature SET synced_ts = ? WHERE id = ?",
                (now_ms(), timbrata_id)
            )
    else:
        if DB_VENDOR == "mysql":
            db.execute(
                "UPDATE cedolino_timbrature SET sync_error = %s, sync_attempts = 1 WHERE id = %s",
                (error, timbrata_id)
            )
        else:
            db.execute(
                "UPDATE cedolino_timbrature SET sync_error = ?, sync_attempts = 1 WHERE id = ?",
                (error, timbrata_id)
            )
    
    return success, external_id, error, request_url


def retry_pending_timbrature(db: DatabaseLike, max_attempts: int = 5) -> int:
    """
    Ritenta l'invio delle timbrate non sincronizzate.
    
    Args:
        db: connessione database
        max_attempts: numero massimo di tentativi
    
    Returns:
        Numero di timbrate sincronizzate con successo
    """
    settings = get_cedolino_settings()
    if not settings:
        return 0
    
    endpoint = settings.get("endpoint") or CEDOLINO_WEB_ENDPOINT
    
    # Recupera timbrate non sincronizzate con tentativi < max
    if DB_VENDOR == "mysql":
        rows = db.execute(
            """
            SELECT id, external_id, timeframe_id, data_riferimento, ora_originale, ora_modificata, sync_attempts
            FROM cedolino_timbrature
            WHERE synced_ts IS NULL AND sync_attempts < %s
            ORDER BY created_ts ASC
            LIMIT 50
            """,
            (max_attempts,)
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT id, external_id, timeframe_id, data_riferimento, ora_originale, ora_modificata, sync_attempts
            FROM cedolino_timbrature
            WHERE synced_ts IS NULL AND sync_attempts < ?
            ORDER BY created_ts ASC
            LIMIT 50
            """,
            (max_attempts,)
        ).fetchall()
    
    synced_count = 0
    for row in rows:
        timbrata_id = row["id"] if isinstance(row, dict) else row[0]
        external_id = row["external_id"] if isinstance(row, dict) else row[1]
        timeframe_id = row["timeframe_id"] if isinstance(row, dict) else row[2]
        data_rif = row["data_riferimento"] if isinstance(row, dict) else row[3]
        ora_orig = row["ora_originale"] if isinstance(row, dict) else row[4]
        ora_mod = row["ora_modificata"] if isinstance(row, dict) else row[5]
        attempts = row["sync_attempts"] if isinstance(row, dict) else row[6]
        
        # Formatta data_riferimento come stringa se necessario
        if hasattr(data_rif, 'strftime'):
            data_riferimento = data_rif.strftime("%Y-%m-%d")
        else:
            data_riferimento = str(data_rif)
        
        # Formatta ora come stringa se necessario
        if hasattr(ora_orig, 'strftime'):
            ora_originale = ora_orig.strftime("%H:%M:%S")
        else:
            ora_originale = str(ora_orig)
        
        if hasattr(ora_mod, 'strftime'):
            ora_modificata = ora_mod.strftime("%H:%M:%S")
        else:
            ora_modificata = str(ora_mod) if ora_mod else ora_originale
        
        # Componi data_originale e data_modificata
        data_originale = f"{data_riferimento} {ora_originale}"
        data_modificata = f"{data_riferimento} {ora_modificata}"
        
        success, error, _url = call_cedolino_webservice(
            external_id, timeframe_id, data_riferimento, data_originale, data_modificata, endpoint
        )
        
        if success:
            if DB_VENDOR == "mysql":
                db.execute(
                    "UPDATE cedolino_timbrature SET synced_ts = %s, sync_error = NULL WHERE id = %s",
                    (now_ms(), timbrata_id)
                )
            else:
                db.execute(
                    "UPDATE cedolino_timbrature SET synced_ts = ?, sync_error = NULL WHERE id = ?",
                    (now_ms(), timbrata_id)
                )
            synced_count += 1
        else:
            if DB_VENDOR == "mysql":
                db.execute(
                    "UPDATE cedolino_timbrature SET sync_error = %s, sync_attempts = %s WHERE id = %s",
                    (error, attempts + 1, timbrata_id)
                )
            else:
                db.execute(
                    "UPDATE cedolino_timbrature SET sync_error = ?, sync_attempts = ? WHERE id = ?",
                    (error, attempts + 1, timbrata_id)
                )
    
    if rows:
        db.commit()
    
    return synced_count


def _cedolino_retry_worker(stop_event: Event) -> None:
    """Worker thread per ritentare l'invio delle timbrate CedolinoWeb non sincronizzate."""
    app.logger.info(
        "CedolinoWeb retry worker: avviato (intervallo %ss)", CEDOLINO_RETRY_INTERVAL_SECONDS
    )
    
    while not stop_event.is_set():
        try:
            with app.app_context():
                settings = get_cedolino_settings()
                if not settings:
                    app.logger.debug("CedolinoWeb retry worker: integrazione disabilitata")
                else:
                    max_attempts = settings.get("max_retry_attempts", 10)
                    db = get_db()
                    synced = retry_pending_timbrature(db, max_attempts)
                    if synced > 0:
                        app.logger.info("CedolinoWeb retry worker: sincronizzate %s timbrate", synced)
        except Exception as exc:
            app.logger.exception("CedolinoWeb retry worker: errore", exc_info=exc)
        finally:
            stop_event.wait(CEDOLINO_RETRY_INTERVAL_SECONDS)


def start_cedolino_retry_worker() -> None:
    """Avvia il worker per i retry CedolinoWeb."""
    global _CEDOLINO_RETRY_THREAD, _CEDOLINO_RETRY_STOP
    
    if _CEDOLINO_RETRY_THREAD and _CEDOLINO_RETRY_THREAD.is_alive():
        return
    
    _CEDOLINO_RETRY_STOP = Event()
    _CEDOLINO_RETRY_THREAD = Thread(
        target=_cedolino_retry_worker,
        args=(_CEDOLINO_RETRY_STOP,),
        name="joblog-cedolino-retry",
        daemon=True
    )
    _CEDOLINO_RETRY_THREAD.start()
    app.logger.info("CedolinoWeb retry worker: thread avviato")


def stop_cedolino_retry_worker() -> None:
    """Ferma il worker per i retry CedolinoWeb."""
    global _CEDOLINO_RETRY_THREAD, _CEDOLINO_RETRY_STOP
    
    stop_event = _CEDOLINO_RETRY_STOP
    thread = _CEDOLINO_RETRY_THREAD
    
    if stop_event is not None:
        stop_event.set()
    
    if thread and thread.is_alive():
        thread.join(timeout=5)
    
    _CEDOLINO_RETRY_THREAD = None
    _CEDOLINO_RETRY_STOP = None


atexit.register(stop_cedolino_retry_worker)


# ═══════════════════════════════════════════════════════════════════════════════
#  PIANIFICAZIONI RENTMAN - DATABASE
# ═══════════════════════════════════════════════════════════════════════════════

RENTMAN_PLANNINGS_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS rentman_plannings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rentman_id INT NOT NULL,
    planning_date DATE NOT NULL,
    crew_id INT,
    crew_name VARCHAR(255),
    function_id INT,
    function_name VARCHAR(255),
    project_id INT,
    project_name VARCHAR(500),
    project_code VARCHAR(128),
    plan_start DATETIME,
    plan_end DATETIME,
    hours_planned DECIMAL(10,2),
    hours_registered DECIMAL(10,2),
    remark TEXT,
    is_leader TINYINT(1) DEFAULT 0,
    transport VARCHAR(50),
    sent_to_webservice TINYINT(1) DEFAULT 0,
    sent_ts BIGINT DEFAULT NULL,
    webservice_response TEXT,
    created_ts BIGINT NOT NULL,
    updated_ts BIGINT NOT NULL,
    UNIQUE KEY uniq_rentman_planning (rentman_id, planning_date),
    INDEX idx_planning_date (planning_date),
    INDEX idx_planning_crew (crew_id),
    INDEX idx_planning_project (project_code),
    INDEX idx_planning_sent (sent_to_webservice)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

RENTMAN_PLANNINGS_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS rentman_plannings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rentman_id INTEGER NOT NULL,
    planning_date TEXT NOT NULL,
    crew_id INTEGER,
    crew_name TEXT,
    function_id INTEGER,
    function_name TEXT,
    project_id INTEGER,
    project_name TEXT,
    project_code TEXT,
    plan_start TEXT,
    plan_end TEXT,
    hours_planned REAL,
    hours_registered REAL,
    remark TEXT,
    is_leader INTEGER DEFAULT 0,
    transport TEXT,
    sent_to_webservice INTEGER DEFAULT 0,
    sent_ts INTEGER DEFAULT NULL,
    webservice_response TEXT,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL,
    UNIQUE(rentman_id, planning_date)
);
CREATE INDEX IF NOT EXISTS idx_planning_date ON rentman_plannings(planning_date);
CREATE INDEX IF NOT EXISTS idx_planning_crew ON rentman_plannings(crew_id);
CREATE INDEX IF NOT EXISTS idx_planning_project ON rentman_plannings(project_code);
CREATE INDEX IF NOT EXISTS idx_planning_sent ON rentman_plannings(sent_to_webservice);
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  REQUEST TYPES - TIPOLOGIE RICHIESTE (ferie, permessi, rimborsi, ecc.)
# ═══════════════════════════════════════════════════════════════════════════════

REQUEST_TYPES_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS request_types (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    value_type ENUM('hours', 'days', 'amount', 'km') NOT NULL,
    external_id VARCHAR(100),
    description TEXT,
    active TINYINT(1) DEFAULT 1,
    sort_order INT DEFAULT 0,
    created_ts BIGINT NOT NULL,
    updated_ts BIGINT NOT NULL,
    INDEX idx_request_type_active (active),
    INDEX idx_request_type_value (value_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

REQUEST_TYPES_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS request_types (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    value_type TEXT NOT NULL CHECK(value_type IN ('hours', 'days', 'amount', 'km')),
    external_id TEXT,
    description TEXT,
    active INTEGER DEFAULT 1,
    sort_order INTEGER DEFAULT 0,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_request_type_active ON request_types(active);
CREATE INDEX IF NOT EXISTS idx_request_type_value ON request_types(value_type);
"""

# Tabella per le richieste degli utenti
USER_REQUESTS_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS user_requests (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    username VARCHAR(100) NOT NULL,
    request_type_id INT NOT NULL,
    date_from DATE NOT NULL,
    date_to DATE,
    value_amount DECIMAL(10,2) NOT NULL,
    notes TEXT,
    cdc VARCHAR(100),
    attachment_path VARCHAR(500),
    status ENUM('pending', 'approved', 'rejected') DEFAULT 'pending',
    reviewed_by VARCHAR(100),
    reviewed_ts BIGINT,
    review_notes TEXT,
    created_ts BIGINT NOT NULL,
    updated_ts BIGINT NOT NULL,
    INDEX idx_request_user (user_id),
    INDEX idx_request_username (username),
    INDEX idx_request_status (status),
    INDEX idx_request_date (date_from),
    INDEX idx_request_type (request_type_id),
    FOREIGN KEY (request_type_id) REFERENCES request_types(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

USER_REQUESTS_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS user_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    username TEXT NOT NULL,
    request_type_id INTEGER NOT NULL,
    date_from TEXT NOT NULL,
    date_to TEXT,
    value_amount REAL NOT NULL,
    notes TEXT,
    cdc TEXT,
    attachment_path TEXT,
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'approved', 'rejected')),
    reviewed_by TEXT,
    reviewed_ts INTEGER,
    review_notes TEXT,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL,
    FOREIGN KEY (request_type_id) REFERENCES request_types(id)
);
CREATE INDEX IF NOT EXISTS idx_request_user ON user_requests(user_id);
CREATE INDEX IF NOT EXISTS idx_request_username ON user_requests(username);
CREATE INDEX IF NOT EXISTS idx_request_status ON user_requests(status);
CREATE INDEX IF NOT EXISTS idx_request_date ON user_requests(date_from);
CREATE INDEX IF NOT EXISTS idx_request_type ON user_requests(request_type_id);
"""


def ensure_request_types_table(db: DatabaseLike) -> None:
    """Crea la tabella request_types se non esiste."""
    statement = (
        REQUEST_TYPES_TABLE_MYSQL if DB_VENDOR == "mysql" else REQUEST_TYPES_TABLE_SQLITE
    )
    for stmt in statement.strip().split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        cursor = db.execute(sql)
        try:
            cursor.close()
        except AttributeError:
            pass


def ensure_user_requests_table(db: DatabaseLike) -> None:
    """Crea la tabella user_requests se non esiste e aggiunge colonne mancanti."""
    statement = (
        USER_REQUESTS_TABLE_MYSQL if DB_VENDOR == "mysql" else USER_REQUESTS_TABLE_SQLITE
    )
    for stmt in statement.strip().split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        cursor = db.execute(sql)
        try:
            cursor.close()
        except AttributeError:
            pass
    
    # Aggiungi colonne mancanti se la tabella esisteva già
    try:
        if DB_VENDOR == "mysql":
            db.execute("ALTER TABLE user_requests ADD COLUMN cdc VARCHAR(100)")
            db.commit()
    except Exception:
        pass  # Colonna già esiste
    
    try:
        if DB_VENDOR == "mysql":
            db.execute("ALTER TABLE user_requests ADD COLUMN attachment_path VARCHAR(500)")
            db.commit()
    except Exception:
        pass  # Colonna già esiste


def ensure_rentman_plannings_table(db: DatabaseLike) -> None:
    """Crea la tabella rentman_plannings se non esiste."""
    statement = (
        RENTMAN_PLANNINGS_TABLE_MYSQL if DB_VENDOR == "mysql" else RENTMAN_PLANNINGS_TABLE_SQLITE
    )
    for stmt in statement.strip().split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        cursor = db.execute(sql)
        try:
            cursor.close()
        except AttributeError:
            pass


# ═══════════════════════════════════════════════════════════════════════════════
#  PIANIFICAZIONI RENTMAN - ROUTES
# ═══════════════════════════════════════════════════════════════════════════════


def get_joblog_hours_for_date(db: DatabaseLike, target_date: str) -> Dict[str, float]:
    """
    Calcola le ore registrate in JobLog per ogni operatore in una data specifica.
    
    Returns:
        Dict con member_name.lower() come chiave e ore totali come valore.
    """
    try:
        from datetime import datetime as dt_parse
        # Parse target date
        target_dt = dt_parse.strptime(target_date, "%Y-%m-%d").date()
    except ValueError:
        app.logger.warning(f"Data non valida per JobLog hours: {target_date}")
        return {}
    
    # Calcola timestamp inizio e fine giornata
    start_of_day = dt_parse.combine(target_dt, dt_parse.min.time())
    end_of_day = dt_parse.combine(target_dt, dt_parse.max.time())
    start_ts = int(start_of_day.timestamp() * 1000)
    end_ts = int(end_of_day.timestamp() * 1000)
    
    # Query per eventi finish_activity con duration_ms
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    query = f"""
        SELECT el.ts, el.member_key, el.details, ms.member_name
        FROM event_log el
        LEFT JOIN member_state ms ON el.member_key = ms.member_key AND el.project_code = ms.project_code
        WHERE el.kind = 'finish_activity'
        AND el.ts >= {placeholder} AND el.ts <= {placeholder}
    """
    
    try:
        rows = db.execute(query, (start_ts, end_ts)).fetchall()
    except Exception as exc:
        app.logger.error(f"Errore query JobLog hours: {exc}")
        return {}
    
    # Accumula ore per operatore
    hours_by_member: Dict[str, float] = {}
    
    for row in rows:
        try:
            details = json.loads(row["details"]) if row["details"] else {}
        except json.JSONDecodeError:
            continue
        
        duration_ms = details.get("duration_ms", 0)
        if not duration_ms:
            continue
        
        # Usa member_name dalla tabella member_state o dai dettagli evento
        member_name = row["member_name"] or details.get("member_name") or row["member_key"]
        if not member_name:
            continue
        
        # Normalizza il nome (lowercase) per il matching
        member_name_lower = member_name.strip().lower()
        
        # Converti ms in ore
        hours = duration_ms / (1000 * 60 * 60)
        
        if member_name_lower in hours_by_member:
            hours_by_member[member_name_lower] += hours
        else:
            hours_by_member[member_name_lower] = hours
    
    return hours_by_member


def match_crew_name_to_joblog(crew_name: str, joblog_hours: Dict[str, float]) -> Optional[float]:
    """
    Cerca di matchare un nome operatore Rentman con i dati JobLog.
    Usa matching esatto e fuzzy sul nome.
    """
    if not crew_name:
        return None
    
    crew_lower = crew_name.strip().lower()
    
    # 1. Match esatto
    if crew_lower in joblog_hours:
        return joblog_hours[crew_lower]
    
    # 2. Prova a confrontare parti del nome
    crew_parts = set(crew_lower.split())
    
    for member_name, hours in joblog_hours.items():
        member_parts = set(member_name.split())
        
        # Se almeno 2 parole coincidono, o se una parola è contenuta nell'altra
        common = crew_parts & member_parts
        if len(common) >= 2:
            return hours
        
        # Se il nome completo è contenuto
        if crew_lower in member_name or member_name in crew_lower:
            return hours
        
        # Match su cognome (tipicamente la seconda parola)
        crew_words = crew_lower.split()
        member_words = member_name.split()
        
        # Se hanno almeno 2 parole e la seconda (cognome) coincide
        if len(crew_words) >= 2 and len(member_words) >= 2:
            if crew_words[1] == member_words[1]:
                return hours
    
    return None


@app.get("/admin/rentman-planning")
@login_required
def admin_rentman_planning_page() -> ResponseReturnValue:
    """Pagina pianificazioni Rentman."""
    if not is_admin_or_supervisor():
        abort(403)

    display_name = session.get("user_display") or session.get("user_name") or session.get("user")
    primary_name = session.get("user_name") or display_name or session.get("user")
    initials = session.get("user_initials") or compute_initials(primary_name or "")

    return render_template(
        "admin_rentman_planning.html",
        user_name=primary_name,
        user_display=display_name,
        user_initials=initials,
        is_admin=bool(session.get("is_admin")),
    )


@app.get("/api/admin/rentman-planning")
@login_required
def api_admin_rentman_planning() -> ResponseReturnValue:
    """API per recuperare pianificazioni Rentman per una data."""
    if not is_admin_or_supervisor():
        return jsonify({"error": "forbidden"}), 403

    target_date = request.args.get("date")
    if not target_date:
        target_date = datetime.now().date().isoformat()

    try:
        from rentman_client import RentmanClient, RentmanError
        client = RentmanClient()
    except Exception as exc:
        app.logger.warning("Rentman client non disponibile: %s", exc)
        return jsonify({"error": "Rentman non configurato", "details": str(exc)}), 500

    try:
        plannings = client.get_crew_plannings_by_date(target_date)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except RentmanError as exc:
        return jsonify({"error": "Errore Rentman", "details": str(exc)}), 500

    # Calcola ore JobLog per questa data
    db = get_db()
    joblog_hours = get_joblog_hours_for_date(db, target_date)

    # Arricchisci i dati con info su crew e progetto
    # Cache per evitare chiamate duplicate
    crew_cache: Dict[int, Dict[str, Any]] = {}
    function_cache: Dict[int, Dict[str, Any]] = {}
    project_cache: Dict[int, Dict[str, Any]] = {}

    results = []
    for planning in plannings:
        # Estrai ID dal riferimento (es. "/crew/123" -> 123)
        crew_ref = planning.get("crewmember", "")
        crew_id = None
        if crew_ref and "/" in crew_ref:
            try:
                crew_id = int(crew_ref.split("/")[-1])
            except (ValueError, IndexError):
                pass

        function_ref = planning.get("function", "")
        function_id = None
        if function_ref and "/" in function_ref:
            try:
                function_id = int(function_ref.split("/")[-1])
            except (ValueError, IndexError):
                pass

        # Recupera dettagli crew
        crew_name = planning.get("displayname", "")
        if crew_id and crew_id not in crew_cache:
            crew_data = client.get_crew_member(crew_id)
            if crew_data:
                crew_cache[crew_id] = crew_data
        if crew_id and crew_id in crew_cache:
            cd = crew_cache[crew_id]
            crew_name = cd.get("displayname") or f"{cd.get('firstname', '')} {cd.get('lastname', '')}".strip() or crew_name

        # Recupera dettagli funzione e progetto
        project_name = ""
        project_code = ""
        function_name = ""
        if function_id and function_id not in function_cache:
            func_data = client.get_project_function(function_id)
            if func_data:
                function_cache[function_id] = func_data
        if function_id and function_id in function_cache:
            fd = function_cache[function_id]
            function_name = fd.get("name") or fd.get("displayname") or ""
            # Estrai progetto dalla funzione
            project_ref = fd.get("project", "")
            if project_ref and "/" in project_ref:
                try:
                    project_id = int(project_ref.split("/")[-1])
                    if project_id not in project_cache:
                        proj_data = client.get_project(project_id)
                        if proj_data:
                            project_cache[project_id] = proj_data
                    if project_id in project_cache:
                        pd = project_cache[project_id]
                        project_name = pd.get("name") or pd.get("displayname") or ""
                        project_code = pd.get("number") or pd.get("reference") or ""
                except (ValueError, IndexError):
                    pass

        # Calcola ore JobLog per questo operatore
        joblog_registered = match_crew_name_to_joblog(crew_name, joblog_hours)
        
        results.append({
            "id": planning.get("id"),
            "crew_id": crew_id,
            "crew_name": crew_name,
            "function_name": function_name,
            "project_name": project_name,
            "project_code": project_code,
            "start": planning.get("planperiod_start"),
            "end": planning.get("planperiod_end"),
            "hours_planned": planning.get("hours_planned"),
            "hours_registered": round(joblog_registered, 2) if joblog_registered is not None else 0,
            "hours_rentman": planning.get("hours_registered"),  # Mantieni valore originale Rentman per riferimento
            "remark": planning.get("remark", ""),
            "is_leader": planning.get("project_leader", False),
            "transport": planning.get("transport", ""),
        })

    # Ordina per orario di inizio e poi per nome crew
    results.sort(key=lambda x: (x.get("start") or "", x.get("crew_name") or ""))

    # Merge con dati salvati nel DB per preservare sent_to_webservice e rilevare modifiche
    db = get_db()
    ensure_rentman_plannings_table(db)
    
    if DB_VENDOR == "mysql":
        saved_rows = db.execute(
            "SELECT rentman_id, sent_to_webservice, plan_start, plan_end, project_name, sent_ts FROM rentman_plannings WHERE planning_date = %s",
            (target_date,)
        ).fetchall()
    else:
        saved_rows = db.execute(
            "SELECT rentman_id, sent_to_webservice, plan_start, plan_end, project_name, sent_ts FROM rentman_plannings WHERE planning_date = ?",
            (target_date,)
        ).fetchall()
    
    # Crea mappa rentman_id -> {sent, old_start, old_end, old_project, sent_ts}
    saved_map = {}
    for row in saved_rows:
        if isinstance(row, Mapping):
            saved_map[row["rentman_id"]] = {
                "sent": bool(row["sent_to_webservice"]),
                "old_start": row["plan_start"],
                "old_end": row["plan_end"],
                "old_project": row["project_name"],
                "sent_ts": row["sent_ts"]
            }
        else:
            saved_map[row[0]] = {
                "sent": bool(row[1]),
                "old_start": row[2],
                "old_end": row[3],
                "old_project": row[4],
                "sent_ts": row[5]
            }
    
    # Arricchisci i risultati con info invio e modifiche
    for r in results:
        rentman_id = r.get("id")
        if rentman_id in saved_map:
            saved = saved_map[rentman_id]
            r["sent_to_webservice"] = saved["sent"]
            
            # Aggiungi timestamp invio formattato
            if saved["sent_ts"]:
                try:
                    ts_val = saved["sent_ts"]
                    if isinstance(ts_val, (int, float)):
                        # sent_ts è in millisecondi (es: 1735634845125)
                        ts_sec = ts_val / 1000 if ts_val > 1e12 else ts_val
                        r["sent_ts"] = datetime.fromtimestamp(ts_sec).strftime("%d/%m/%Y %H:%M:%S")
                    elif isinstance(ts_val, str):
                        # Potrebbe essere ISO string (es: 2025-12-31T08:47:25.125Z)
                        ts_clean = ts_val.replace("Z", "+00:00")
                        dt = datetime.fromisoformat(ts_clean)
                        r["sent_ts"] = dt.strftime("%d/%m/%Y %H:%M:%S")
                    elif hasattr(ts_val, 'strftime'):
                        # È già un datetime
                        r["sent_ts"] = ts_val.strftime("%d/%m/%Y %H:%M:%S")
                    else:
                        r["sent_ts"] = str(ts_val)  # Fallback: converti a stringa
                except Exception:
                    r["sent_ts"] = None
            else:
                r["sent_ts"] = None
            
            # Rileva se è stato modificato rispetto all'ultimo invio
            if saved["sent"]:
                # Confronta orari e progetto
                # Gestisci sia stringhe che datetime objects
                new_start_raw = r.get("start", "")
                new_start = str(new_start_raw)[:16] if new_start_raw else ""
                old_start_raw = saved["old_start"]
                old_start = str(old_start_raw)[:16] if old_start_raw else ""
                new_end_raw = r.get("end", "")
                new_end = str(new_end_raw)[:16] if new_end_raw else ""
                old_end_raw = saved["old_end"]
                old_end = str(old_end_raw)[:16] if old_end_raw else ""
                
                is_modified = (
                    new_start != old_start or 
                    new_end != old_end or 
                    r.get("project_name", "") != (saved["old_project"] or "")
                )
                r["is_modified"] = is_modified
            else:
                r["is_modified"] = False
        else:
            r["sent_to_webservice"] = False
            r["is_modified"] = False
            r["sent_ts"] = None

    return jsonify({
        "ok": True,
        "date": target_date,
        "count": len(results),
        "plannings": results,
    })


@app.post("/api/admin/rentman-planning/save")
@login_required
def api_admin_rentman_planning_save() -> ResponseReturnValue:
    """Salva le pianificazioni Rentman nel database locale."""
    if not is_admin_or_supervisor():
        return jsonify({"error": "forbidden"}), 403

    data = request.get_json() or {}
    target_date = data.get("date")
    plannings = data.get("plannings", [])

    if not target_date:
        return jsonify({"error": "Data richiesta"}), 400

    if not plannings:
        return jsonify({"error": "Nessuna pianificazione da salvare"}), 400

    db = get_db()
    ensure_rentman_plannings_table(db)
    ensure_crew_members_table(db)  # Assicura che la tabella operatori esista

    now_ms = int(time.time() * 1000)
    saved = 0
    updated = 0
    synced_crews = set()  # Track già sincronizzati per evitare duplicati

    def parse_iso_datetime(dt_str: str | None) -> str | None:
        """Converte datetime ISO in formato MySQL compatibile."""
        if not dt_str:
            return None
        try:
            # Prova a parsare datetime ISO con timezone
            from datetime import datetime as dt_parse
            # Rimuovi timezone se presente e converti
            if '+' in dt_str:
                dt_str = dt_str.split('+')[0]
            elif dt_str.endswith('Z'):
                dt_str = dt_str[:-1]
            # Converti in formato MySQL
            parsed = dt_parse.fromisoformat(dt_str)
            return parsed.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            return dt_str

    for p in plannings:
        rentman_id = p.get("id")
        if not rentman_id:
            continue

        # Sincronizza operatore nel database locale (una volta sola per crew_id)
        crew_id = p.get("crew_id")
        crew_name = p.get("crew_name")
        if crew_id and crew_id not in synced_crews:
            sync_crew_member_from_rentman(db, crew_id, crew_name or f"Crew {crew_id}")
            synced_crews.add(crew_id)
            # NON fare continue qui! Devo comunque salvare la pianificazione

        # Converti hours da secondi a ore se necessario
        hours_planned = p.get("hours_planned")
        if hours_planned and float(hours_planned) > 100:
            hours_planned = float(hours_planned) / 3600

        hours_registered = p.get("hours_registered")
        if hours_registered and float(hours_registered) > 100:
            hours_registered = float(hours_registered) / 3600

        # Converti datetime in formato MySQL compatibile
        plan_start = parse_iso_datetime(p.get("start"))
        plan_end = parse_iso_datetime(p.get("end"))

        # Estrai project_id dalla funzione (se disponibile)
        project_id = p.get("project_id")
        function_id = p.get("function_id")

        # Check if exists
        if DB_VENDOR == "mysql":
            existing = db.execute(
                "SELECT id, sent_to_webservice FROM rentman_plannings WHERE rentman_id = %s AND planning_date = %s",
                (rentman_id, target_date)
            ).fetchone()
        else:
            existing = db.execute(
                "SELECT id, sent_to_webservice FROM rentman_plannings WHERE rentman_id = ? AND planning_date = ?",
                (rentman_id, target_date)
            ).fetchone()

        if existing:
            # Update existing record (but preserve sent status)
            if DB_VENDOR == "mysql":
                db.execute("""
                    UPDATE rentman_plannings SET
                        crew_id = %s, crew_name = %s, function_id = %s, function_name = %s,
                        project_id = %s, project_name = %s, project_code = %s,
                        plan_start = %s, plan_end = %s, hours_planned = %s, hours_registered = %s,
                        remark = %s, is_leader = %s, transport = %s, updated_ts = %s
                    WHERE rentman_id = %s AND planning_date = %s
                """, (
                    p.get("crew_id"), p.get("crew_name"), function_id, p.get("function_name"),
                    project_id, p.get("project_name"), p.get("project_code"),
                    plan_start, plan_end, hours_planned, hours_registered,
                    p.get("remark"), 1 if p.get("is_leader") else 0, p.get("transport"), now_ms,
                    rentman_id, target_date
                ))
            else:
                db.execute("""
                    UPDATE rentman_plannings SET
                        crew_id = ?, crew_name = ?, function_id = ?, function_name = ?,
                        project_id = ?, project_name = ?, project_code = ?,
                        plan_start = ?, plan_end = ?, hours_planned = ?, hours_registered = ?,
                        remark = ?, is_leader = ?, transport = ?, updated_ts = ?
                    WHERE rentman_id = ? AND planning_date = ?
                """, (
                    p.get("crew_id"), p.get("crew_name"), function_id, p.get("function_name"),
                    project_id, p.get("project_name"), p.get("project_code"),
                    plan_start, plan_end, hours_planned, hours_registered,
                    p.get("remark"), 1 if p.get("is_leader") else 0, p.get("transport"), now_ms,
                    rentman_id, target_date
                ))
            updated += 1
        else:
            # Insert new record
            if DB_VENDOR == "mysql":
                db.execute("""
                    INSERT INTO rentman_plannings (
                        rentman_id, planning_date, crew_id, crew_name, function_id, function_name,
                        project_id, project_name, project_code, plan_start, plan_end,
                        hours_planned, hours_registered, remark, is_leader, transport,
                        sent_to_webservice, created_ts, updated_ts
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s, %s)
                """, (
                    rentman_id, target_date, p.get("crew_id"), p.get("crew_name"),
                    function_id, p.get("function_name"), project_id, p.get("project_name"),
                    p.get("project_code"), plan_start, plan_end,
                    hours_planned, hours_registered, p.get("remark"),
                    1 if p.get("is_leader") else 0, p.get("transport"), now_ms, now_ms
                ))
            else:
                db.execute("""
                    INSERT INTO rentman_plannings (
                        rentman_id, planning_date, crew_id, crew_name, function_id, function_name,
                        project_id, project_name, project_code, plan_start, plan_end,
                        hours_planned, hours_registered, remark, is_leader, transport,
                        sent_to_webservice, created_ts, updated_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                """, (
                    rentman_id, target_date, p.get("crew_id"), p.get("crew_name"),
                    function_id, p.get("function_name"), project_id, p.get("project_name"),
                    p.get("project_code"), plan_start, plan_end,
                    hours_planned, hours_registered, p.get("remark"),
                    1 if p.get("is_leader") else 0, p.get("transport"), now_ms, now_ms
                ))
            saved += 1

    db.commit()

    return jsonify({
        "ok": True,
        "saved": saved,
        "updated": updated,
        "total": saved + updated,
    })


@app.get("/api/admin/rentman-planning/saved")
@login_required
def api_admin_rentman_planning_saved() -> ResponseReturnValue:
    """Recupera le pianificazioni salvate dal database locale."""
    if not is_admin_or_supervisor():
        return jsonify({"error": "forbidden"}), 403

    target_date = request.args.get("date")
    if not target_date:
        target_date = datetime.now().date().isoformat()

    db = get_db()
    ensure_rentman_plannings_table(db)

    if DB_VENDOR == "mysql":
        rows = db.execute(
            "SELECT * FROM rentman_plannings WHERE planning_date = %s ORDER BY plan_start, crew_name",
            (target_date,)
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT * FROM rentman_plannings WHERE planning_date = ? ORDER BY plan_start, crew_name",
            (target_date,)
        ).fetchall()

    plannings = []
    for row in rows:
        if isinstance(row, Mapping):
            plannings.append(dict(row))
        else:
            # SQLite row to dict
            cols = ["id", "rentman_id", "planning_date", "crew_id", "crew_name", "function_id",
                    "function_name", "project_id", "project_name", "project_code", "plan_start",
                    "plan_end", "hours_planned", "hours_registered", "remark", "is_leader",
                    "transport", "sent_to_webservice", "sent_ts", "webservice_response",
                    "created_ts", "updated_ts"]
            plannings.append(dict(zip(cols, row)))

    return jsonify({
        "ok": True,
        "date": target_date,
        "count": len(plannings),
        "plannings": plannings,
    })


@app.post("/api/admin/rentman-planning/send")
@login_required
def api_admin_rentman_planning_send() -> ResponseReturnValue:
    """Invia le pianificazioni al webservice esterno e notifica gli utenti."""
    if not is_admin_or_supervisor():
        return jsonify({"error": "forbidden"}), 403

    data = request.get_json() or {}
    planning_ids = data.get("ids", [])  # Lista di ID locali da inviare
    plannings = data.get("plannings", [])  # O lista di pianificazioni con rentman_id
    force_resend = data.get("force_resend", False)  # Se True, invia notifiche anche per già inviati

    db = get_db()
    ensure_rentman_plannings_table(db)

    rows = []
    
    if planning_ids:
        # Recupera le pianificazioni per ID locale
        placeholders = ",".join(["%s" if DB_VENDOR == "mysql" else "?"] * len(planning_ids))
        query = f"SELECT * FROM rentman_plannings WHERE id IN ({placeholders})"
        rows = db.execute(query, planning_ids).fetchall()
    elif plannings:
        # Recupera per rentman_id (se esistono nel DB)
        rentman_ids = [p.get("rentman_id") or p.get("id") for p in plannings if p.get("rentman_id") or p.get("id")]
        if rentman_ids:
            placeholders = ",".join(["%s" if DB_VENDOR == "mysql" else "?"] * len(rentman_ids))
            query = f"SELECT * FROM rentman_plannings WHERE rentman_id IN ({placeholders})"
            rows = db.execute(query, rentman_ids).fetchall()

    if not rows and not plannings:
        return jsonify({"error": "Nessuna pianificazione selezionata"}), 400

    # TODO: Implementare l'invio al webservice
    # Per ora segna come inviate
    now_ms_val = int(time.time() * 1000)
    sent_count = 0
    errors = []
    
    # Raccogli info per notifiche push
    users_to_notify: Dict[int, List[Dict[str, Any]]] = {}  # crew_id -> list of turni

    # Se abbiamo righe dal DB, processa quelle
    if rows:
        for row in rows:
            local_id = row["id"] if isinstance(row, Mapping) else row[0]
            crew_id = row["crew_id"] if isinstance(row, Mapping) else row[3]
            planning_date = row["planning_date"] if isinstance(row, Mapping) else row[2]
            project_name = row["project_name"] if isinstance(row, Mapping) else row[8]
            
            # Controlla se era già stato inviato
            was_sent = row["sent_to_webservice"] if isinstance(row, Mapping) else row[17]
            
            try:
                # TODO: Chiamata al webservice qui
                # response = requests.post(webservice_url, json=payload)
                # webservice_response = response.text
                
                webservice_response = "OK - Placeholder (webservice non configurato)"
                
                # Aggiorna il record come inviato
                if DB_VENDOR == "mysql":
                    db.execute("""
                        UPDATE rentman_plannings 
                        SET sent_to_webservice = 1, sent_ts = %s, webservice_response = %s, updated_ts = %s
                        WHERE id = %s
                    """, (now_ms_val, webservice_response, now_ms_val, local_id))
                else:
                    db.execute("""
                        UPDATE rentman_plannings 
                        SET sent_to_webservice = 1, sent_ts = ?, webservice_response = ?, updated_ts = ?
                        WHERE id = ?
                    """, (now_ms_val, webservice_response, now_ms_val, local_id))
                
                sent_count += 1
                
                # Aggiungi alla lista per notifica:
                # - Se non era già inviato (primo invio)
                # - OPPURE se force_resend=True (reinvio manuale o turno modificato)
                should_notify = not was_sent or force_resend
                if should_notify and crew_id:
                    if crew_id not in users_to_notify:
                        users_to_notify[crew_id] = []
                    users_to_notify[crew_id].append({
                        "date": str(planning_date)[:10] if planning_date else "",
                        "project": project_name or "Progetto",
                        "is_update": was_sent  # Per differenziare il messaggio notifica
                    })
                
            except Exception as exc:
                app.logger.error("Errore invio pianificazione %s: %s", local_id, exc)
                errors.append({"id": local_id, "error": str(exc)})
    else:
        # Se non abbiamo righe dal DB ma abbiamo pianificazioni raw, segna come "inviate" ma non salvate
        # Questo è un caso limite: l'utente vuole inviare senza salvare nel DB locale
        for p in plannings:
            try:
                # TODO: Chiamata al webservice qui
                webservice_response = "OK - Placeholder (webservice non configurato, non salvato nel DB)"
                sent_count += 1
            except Exception as exc:
                errors.append({"id": p.get("id"), "error": str(exc)})

    db.commit()
    
    # Invia notifiche push agli utenti
    notifications_sent = 0
    if users_to_notify:
        notifications_sent = _send_turni_notifications(db, users_to_notify)

    return jsonify({
        "ok": True,
        "sent": sent_count,
        "notifications_sent": notifications_sent,
        "errors": errors,
    })


def _send_turni_notifications(db: DatabaseLike, users_to_notify: Dict[int, List[Dict[str, Any]]]) -> int:
    """Invia notifiche push agli utenti per i nuovi turni pubblicati."""
    app.logger.info("_send_turni_notifications chiamata con %d crew_id", len(users_to_notify))
    
    settings = get_webpush_settings()
    if not settings:
        app.logger.info("Notifiche push non configurate, skip invio turni")
        return 0
    
    notifications_sent = 0
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    for crew_id, turni in users_to_notify.items():
        app.logger.info("Cerco utente per crew_id=%s", crew_id)
        
        # Trova l'utente associato a questo crew_id
        user_row = db.execute(
            f"SELECT username FROM app_users WHERE rentman_crew_id = {placeholder}",
            (crew_id,)
        ).fetchone()
        
        if not user_row:
            app.logger.warning("Nessun utente trovato per crew_id=%s", crew_id)
            continue
        
        username = user_row['username'] if isinstance(user_row, dict) else user_row[0]
        app.logger.info("Trovato utente %s per crew_id=%s", username, crew_id)
        
        # Recupera le subscription push dell'utente
        subscriptions = db.execute(
            f"SELECT endpoint, p256dh, auth FROM push_subscriptions WHERE username = {placeholder}",
            (username,)
        ).fetchall()
        
        if not subscriptions:
            app.logger.warning("Nessuna subscription push per utente %s", username)
            continue
        
        app.logger.info("Trovate %d subscription per %s", len(subscriptions), username)
        
        # Prepara il messaggio
        if len(turni) == 1:
            title = "📅 Nuovo turno pubblicato"
            body = f"{turni[0]['date']} - {turni[0]['project']}"
        else:
            title = f"📅 {len(turni)} nuovi turni pubblicati"
            body = f"Hai {len(turni)} nuovi turni. Apri l'app per i dettagli."
        
        payload = {
            "title": title,
            "body": body,
            "icon": "/static/icons/icon-192.png",
            "badge": "/static/icons/badge-72.png",
            "tag": "turni-update",
            "data": {
                "url": "/turni",
                "type": "turni_published"
            }
        }
        
        # Invia a tutte le subscription dell'utente
        for sub in subscriptions:
            endpoint = sub['endpoint'] if isinstance(sub, dict) else sub[0]
            p256dh = sub['p256dh'] if isinstance(sub, dict) else sub[1]
            auth = sub['auth'] if isinstance(sub, dict) else sub[2]
            
            subscription_info = {
                "endpoint": endpoint,
                "keys": {
                    "p256dh": p256dh,
                    "auth": auth
                }
            }
            
            try:
                webpush(
                    subscription_info=subscription_info,
                    data=json.dumps(payload),
                    vapid_private_key=settings["vapid_private"],
                    vapid_claims={"sub": settings["subject"]},
                    ttl=86400,  # 24 ore
                )
                notifications_sent += 1
                app.logger.info("Notifica turno inviata a %s", username)
                    
            except WebPushException as e:
                app.logger.warning("Errore invio notifica turno a %s: %s", username, e)
                # Rimuovi subscription se non valida
                if e.response and e.response.status_code in {404, 410}:
                    remove_push_subscription(db, endpoint)
            except Exception as e:
                app.logger.error("Errore generico invio notifica turno: %s", e)
        
        # Salva la notifica nel log (una volta per utente, dopo aver provato tutte le subscription)
        if notifications_sent > 0:
            try:
                record_push_notification(
                    db,
                    kind="turni_published",
                    title=title,
                    body=body,
                    payload=payload,
                    username=username,
                )
                app.logger.info("Notifica turno salvata nel log per %s", username)
            except Exception as e:
                app.logger.error("Errore salvataggio notifica nel log: %s", e)
    
    return notifications_sent


# ═══════════════════════════════════════════════════════════════════════════════
#  QR CODE TIMBRATURA - GiQR LEVEL 4 LITE
# ═══════════════════════════════════════════════════════════════════════════════

QR_DEVICE_ID = os.environ.get("QR_DEVICE_ID", "WebApp-001")
QR_REFRESH_SECONDS = int(os.environ.get("QR_REFRESH_SECONDS", "10"))


def _generate_giqr_payload() -> dict:
    """Genera payload GiQR Level 4 Lite con firma."""
    now = datetime.now()
    
    dt_str = now.strftime("%Y%m%d%H%M%S")
    nonce = random.randint(0, 65535)
    
    # Firma semplice (algoritmo dal codice originale)
    sig = (
        now.hour * 97 +
        now.minute * 59 +
        now.second * 31 +
        now.year +
        now.month +
        now.day +
        sum(QR_DEVICE_ID.encode())
    ) % 100000
    
    return {
        "v": 1,
        "dt": dt_str,
        "dev": QR_DEVICE_ID,
        "n": nonce,
        "sig": sig
    }


@app.route("/api/qr-timbratura", methods=["GET"])
@login_required
def api_qr_timbratura():
    """Genera QR code dinamico per timbratura in formato base64 PNG."""
    payload = _generate_giqr_payload()
    
    # JSON → bytes → Base64
    raw_json = json.dumps(payload).encode("utf-8")
    b64_payload = base64.b64encode(raw_json).decode("utf-8")
    
    # Genera QR code
    qr = qrcode.QRCode(box_size=8, border=2)
    qr.add_data(b64_payload)
    qr.make(fit=True)
    
    img = qr.make_image(fill_color="black", back_color="white")
    
    # Converti in base64 per invio al frontend
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)
    img_base64 = base64.b64encode(buffer.getvalue()).decode("utf-8")
    
    return jsonify({
        "ok": True,
        "image": f"data:image/png;base64,{img_base64}",
        "payload": payload,
        "refresh_seconds": QR_REFRESH_SECONDS,
        "timestamp": datetime.now().isoformat()
    })


@app.route("/qr-timbratura")
@login_required
def qr_timbratura_page():
    """Pagina standalone per QR timbratura (schermo intero)."""
    return render_template(
        "qr_timbratura.html",
        refresh_seconds=QR_REFRESH_SECONDS,
        device_id=QR_DEVICE_ID
    )


def _validate_giqr_payload(payload: dict) -> Tuple[bool, str]:
    """
    Valida un payload GiQR.
    Ritorna (is_valid, error_message).
    """
    if not payload:
        return False, "Payload vuoto"
    
    required_fields = ["v", "dt", "dev", "n", "sig"]
    for field in required_fields:
        if field not in payload:
            return False, f"Campo mancante: {field}"
    
    # Verifica versione
    if payload.get("v") != 1:
        return False, "Versione QR non supportata"
    
    # Verifica timestamp (non più vecchio di 60 secondi)
    dt_str = payload.get("dt", "")
    try:
        qr_time = datetime.strptime(dt_str, "%Y%m%d%H%M%S")
        now = datetime.now()
        age_seconds = abs((now - qr_time).total_seconds())
        if age_seconds > 60:
            return False, f"QR scaduto ({int(age_seconds)} secondi)"
    except ValueError:
        return False, "Timestamp non valido"
    
    # Verifica firma
    dev = payload.get("dev", "")
    sig_expected = (
        qr_time.hour * 97 +
        qr_time.minute * 59 +
        qr_time.second * 31 +
        qr_time.year +
        qr_time.month +
        qr_time.day +
        sum(dev.encode())
    ) % 100000
    
    if payload.get("sig") != sig_expected:
        return False, "Firma QR non valida"
    
    return True, ""


@app.post("/api/timbratura/validate-qr")
@login_required
def api_timbratura_validate_qr():
    """Valida un QR code scansionato per la timbratura."""
    data = request.get_json()
    if not data:
        return jsonify({"valid": False, "error": "Dati mancanti"}), 400
    
    qr_data = data.get("qr_data", "")
    
    # Decodifica Base64 → JSON
    try:
        decoded = base64.b64decode(qr_data).decode("utf-8")
        payload = json.loads(decoded)
    except Exception:
        return jsonify({"valid": False, "error": "QR non valido"}), 400
    
    # Valida payload
    is_valid, error = _validate_giqr_payload(payload)
    
    if not is_valid:
        return jsonify({"valid": False, "error": error}), 400
    
    # Genera un token temporaneo di validazione (valido 5 minuti)
    validation_token = secrets.token_urlsafe(32)
    expires = now_ms() + (5 * 60 * 1000)  # 5 minuti
    
    # Salva in sessione
    session["timbratura_token"] = validation_token
    session["timbratura_token_expires"] = expires
    
    return jsonify({
        "valid": True,
        "token": validation_token,
        "expires_in": 300,
        "device": payload.get("dev", "")
    })


# ═══════════════════════════════════════════════════════════════════════════════
#  TIMBRATURA GPS - Geolocalizzazione
# ═══════════════════════════════════════════════════════════════════════════════

def get_timbratura_config() -> Dict[str, Any]:
    """Restituisce la configurazione timbratura.
    
    Priorità:
    1. Database (company_settings.custom_settings.timbratura)
    2. config.json (fallback per retrocompatibilità)
    3. Valori di default
    """
    default_config = {
        "qr_enabled": True,
        "gps_enabled": False,
        "gps_locations": [],
        "gps_max_accuracy_meters": 50
    }
    
    # Prova prima dal database
    try:
        db = get_db()
        company_settings = get_company_settings(db)
        custom_settings = company_settings.get("custom_settings", {})
        
        if isinstance(custom_settings, str):
            custom_settings = json.loads(custom_settings) if custom_settings else {}
        
        if "timbratura" in custom_settings:
            db_config = custom_settings["timbratura"]
            # Merge con i default
            return {
                "qr_enabled": db_config.get("qr_enabled", True),
                "gps_enabled": db_config.get("gps_enabled", False),
                "gps_locations": db_config.get("gps_locations", []),
                "gps_max_accuracy_meters": db_config.get("gps_max_accuracy_meters", 50)
            }
    except Exception as e:
        app.logger.warning(f"Errore lettura config timbratura da DB: {e}")
    
    # Fallback: config.json (per retrocompatibilità)
    config = load_config()
    if "timbratura" in config:
        file_config = config["timbratura"]
        return {
            "qr_enabled": file_config.get("qr_enabled", True),
            "gps_enabled": file_config.get("gps_enabled", False),
            "gps_locations": file_config.get("gps_locations", []),
            "gps_max_accuracy_meters": file_config.get("gps_max_accuracy_meters", 50)
        }
    
    return default_config


def get_user_timbratura_config(member_key: str = None) -> Dict[str, Any]:
    """
    Restituisce la configurazione timbratura effettiva per un utente.
    
    Se l'utente ha delle eccezioni configurate (timbratura_override),
    queste DISABILITANO i metodi per quell'operatore.
    
    La logica è:
    - Config aziendale definisce i metodi disponibili per tutti
    - Le eccezioni operatore DISABILITANO metodi specifici
    - Se tutti i metodi sono disabilitati = timbratura diretta
    """
    base_config = get_timbratura_config()
    
    if not member_key:
        return base_config
    
    # Cerca le eccezioni per questo operatore
    try:
        db = get_db()
        ensure_crew_members_table(db)
        
        # Cerca prima per corrispondenza esatta, poi per corrispondenza parziale (nome inizia con)
        row = None
        member_key_lower = member_key.lower().strip()
        
        if DB_VENDOR == "mysql":
            # Prima cerca corrispondenza esatta
            row = db.execute(
                "SELECT timbratura_override FROM crew_members WHERE rentman_id = %s OR LOWER(name) = %s LIMIT 1",
                (member_key, member_key_lower)
            ).fetchone()
            
            # Se non trovato, cerca per nome che inizia con member_key (es. "angelo" -> "Angelo Ruggieri")
            if not row:
                row = db.execute(
                    "SELECT timbratura_override FROM crew_members WHERE LOWER(name) LIKE %s AND timbratura_override IS NOT NULL LIMIT 1",
                    (member_key_lower + '%',)
                ).fetchone()
        else:
            row = db.execute(
                "SELECT timbratura_override FROM crew_members WHERE rentman_id = ? OR LOWER(name) = ? LIMIT 1",
                (member_key, member_key_lower)
            ).fetchone()
            
            if not row:
                row = db.execute(
                    "SELECT timbratura_override FROM crew_members WHERE LOWER(name) LIKE ? AND timbratura_override IS NOT NULL LIMIT 1",
                    (member_key_lower + '%',)
                ).fetchone()
        
        if row and row[0]:
            override = row[0]
            if isinstance(override, str):
                override = json.loads(override)
            
            app.logger.info(f"Trovata eccezione timbratura per '{member_key}': {override}")
            
            # Le eccezioni DISABILITANO i metodi (override con False)
            if "qr_disabled" in override and override.get("qr_disabled"):
                base_config["qr_enabled"] = False
            if "gps_disabled" in override and override.get("gps_disabled"):
                base_config["gps_enabled"] = False
                
    except Exception as e:
        app.logger.warning(f"Errore lettura eccezioni timbratura per {member_key}: {e}")
    
    return base_config


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calcola la distanza in metri tra due coordinate usando la formula di Haversine.
    """
    import math
    R = 6371000  # Raggio della Terra in metri
    
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    
    a = math.sin(delta_phi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c


@app.get("/api/timbratura/config")
@login_required
def api_timbratura_config():
    """Restituisce le opzioni di timbratura disponibili per l'utente corrente."""
    # Recupera il nome utente dalla sessione per verificare eventuali eccezioni
    # user_name contiene il nome completo che dovrebbe corrispondere a crew_members.name
    user_full_name = session.get("user_name") or session.get("user_display") or session.get("user")
    timb_config = get_user_timbratura_config(user_full_name)
    
    app.logger.debug(f"Timbratura config per utente '{user_full_name}': qr={timb_config.get('qr_enabled')}, gps={timb_config.get('gps_enabled')}")
    
    # Non esporre le coordinate esatte, solo i nomi delle locations
    locations = []
    for loc in timb_config.get("gps_locations", []):
        locations.append({
            "name": loc.get("name", "Sede"),
            "radius_meters": loc.get("radius_meters", 100)
        })
    
    return jsonify({
        "qr_enabled": timb_config.get("qr_enabled", True),
        "gps_enabled": timb_config.get("gps_enabled", False),
        "gps_locations": locations,
        "gps_max_accuracy_meters": timb_config.get("gps_max_accuracy_meters", 50)
    })


@app.post("/api/timbratura/validate-gps")
@login_required
def api_timbratura_validate_gps():
    """
    Valida la posizione GPS dell'utente per la timbratura.
    Verifica che l'utente si trovi entro il raggio di una delle sedi configurate.
    """
    data = request.get_json()
    if not data:
        return jsonify({"valid": False, "error": "Dati mancanti"}), 400
    
    latitude = data.get("latitude")
    longitude = data.get("longitude")
    accuracy = data.get("accuracy", 999999)  # Accuratezza in metri
    
    if latitude is None or longitude is None:
        return jsonify({"valid": False, "error": "Coordinate GPS mancanti"}), 400
    
    try:
        latitude = float(latitude)
        longitude = float(longitude)
        accuracy = float(accuracy)
    except (TypeError, ValueError):
        return jsonify({"valid": False, "error": "Coordinate non valide"}), 400
    
    # Recupera il nome utente dalla sessione per verificare eventuali eccezioni
    user_full_name = session.get("user_name") or session.get("user_display") or session.get("user")
    timb_config = get_user_timbratura_config(user_full_name)
    
    # Verifica se GPS è abilitato (considerando eccezioni utente)
    if not timb_config.get("gps_enabled", False):
        return jsonify({"valid": False, "error": "Timbratura GPS non abilitata"}), 400
    
    # Verifica accuratezza del GPS
    max_accuracy = timb_config.get("gps_max_accuracy_meters", 50)
    if accuracy > max_accuracy:
        return jsonify({
            "valid": False, 
            "error": f"Precisione GPS insufficiente ({int(accuracy)}m). Richiesta: max {max_accuracy}m. Prova all'aperto o attendi qualche secondo."
        }), 400
    
    locations = timb_config.get("gps_locations", [])
    if not locations:
        return jsonify({"valid": False, "error": "Nessuna sede configurata per timbratura GPS"}), 400
    
    # Verifica se l'utente è entro il raggio di una delle sedi
    matched_location = None
    min_distance = float('inf')
    
    for loc in locations:
        loc_lat = loc.get("latitude")
        loc_lon = loc.get("longitude")
        loc_radius = loc.get("radius_meters", 100)
        
        if loc_lat is None or loc_lon is None:
            continue
        
        distance = haversine_distance(latitude, longitude, loc_lat, loc_lon)
        
        if distance < min_distance:
            min_distance = distance
        
        # Considera anche l'accuratezza del GPS nell'errore
        effective_distance = distance - accuracy  # Distanza minima possibile
        
        if effective_distance <= loc_radius:
            matched_location = loc
            break
    
    if not matched_location:
        return jsonify({
            "valid": False, 
            "error": f"Non sei in una sede autorizzata. Distanza minima: {int(min_distance)}m",
            "distance": int(min_distance)
        }), 400
    
    # GPS valido! Genera token come per QR
    validation_token = secrets.token_urlsafe(32)
    expires = now_ms() + (5 * 60 * 1000)  # 5 minuti
    
    # Salva in sessione (stesso meccanismo del QR)
    session["timbratura_token"] = validation_token
    session["timbratura_token_expires"] = expires
    session["timbratura_method"] = "gps"
    session["timbratura_location"] = matched_location.get("name", "Sede")
    
    app.logger.info(f"GPS validato per {session.get('user')}: {matched_location.get('name')} (distanza: {int(min_distance)}m, accuracy: {int(accuracy)}m)")
    
    return jsonify({
        "valid": True,
        "token": validation_token,
        "expires_in": 300,
        "location": matched_location.get("name", "Sede"),
        "distance": int(min_distance),
        "accuracy": int(accuracy)
    })


# ═══════════════════════════════════════════════════════════════════════════════
#  GESTIONE UTENTI - ADMIN UI
# ═══════════════════════════════════════════════════════════════════════════════

VALID_USER_ROLES = {"user", "supervisor", "admin", "magazzino"}


@app.get("/admin/users")
@login_required
def admin_users_page() -> ResponseReturnValue:
    """Pagina gestione utenti (solo admin)."""
    if not session.get("is_admin"):
        abort(403)

    display_name = session.get("user_display") or session.get("user_name") or session.get("user")
    primary_name = session.get("user_name") or display_name or session.get("user")
    initials = session.get("user_initials") or compute_initials(primary_name or "")

    return render_template(
        "admin_users.html",
        user_name=primary_name,
        user_display=display_name,
        user_initials=initials,
        is_admin=True,
    )


@app.get("/api/admin/users")
@login_required
def api_admin_users_list() -> ResponseReturnValue:
    """Lista tutti gli utenti."""
    if not session.get("is_admin"):
        return jsonify({"error": "forbidden"}), 403

    db = get_db()
    rows = db.execute("""
        SELECT u.username, u.display_name, u.full_name, u.role, u.is_active, 
               u.created_ts, u.updated_ts, u.rentman_crew_id, c.name as crew_name
        FROM app_users u
        LEFT JOIN crew_members c ON u.rentman_crew_id = c.rentman_id
        ORDER BY u.username ASC
    """).fetchall()

    users = []
    for row in rows:
        users.append({
            "username": row["username"] if isinstance(row, dict) else row[0],
            "display_name": row["display_name"] if isinstance(row, dict) else row[1],
            "full_name": row["full_name"] if isinstance(row, dict) else row[2],
            "role": row["role"] if isinstance(row, dict) else row[3],
            "is_active": bool(row["is_active"] if isinstance(row, dict) else row[4]),
            "created_ts": row["created_ts"] if isinstance(row, dict) else row[5],
            "updated_ts": row["updated_ts"] if isinstance(row, dict) else row[6],
            "rentman_crew_id": row["rentman_crew_id"] if isinstance(row, dict) else row[7],
            "crew_name": row["crew_name"] if isinstance(row, dict) else row[8],
        })

    return jsonify({"users": users})


@app.post("/api/admin/users")
@login_required
def api_admin_users_create() -> ResponseReturnValue:
    """Crea un nuovo utente."""
    if not session.get("is_admin"):
        return jsonify({"error": "forbidden"}), 403

    data = request.get_json()
    if not data:
        return jsonify({"error": "Dati non validi"}), 400

    username = (data.get("username") or "").strip().lower()
    password = data.get("password", "")
    display_name = (data.get("display_name") or "").strip()
    full_name = (data.get("full_name") or "").strip() or None
    role = (data.get("role") or "user").strip().lower()
    is_active = data.get("is_active", True)
    rentman_crew_id = data.get("rentman_crew_id")
    if rentman_crew_id is not None:
        rentman_crew_id = int(rentman_crew_id) if rentman_crew_id else None

    if not username:
        return jsonify({"error": "Username richiesto"}), 400
    if not password:
        return jsonify({"error": "Password richiesta"}), 400
    if not display_name:
        display_name = username
    if role not in VALID_USER_ROLES:
        return jsonify({"error": f"Ruolo non valido. Valori ammessi: {', '.join(sorted(VALID_USER_ROLES))}"}), 400

    db = get_db()

    # Verifica se esiste già
    if DB_VENDOR == "mysql":
        existing = db.execute("SELECT username FROM app_users WHERE username = %s", (username,)).fetchone()
    else:
        existing = db.execute("SELECT username FROM app_users WHERE username = ?", (username,)).fetchone()

    if existing:
        return jsonify({"error": f"L'utente '{username}' esiste già"}), 409

    now = now_ms()
    password_hashed = hash_password(password)

    if DB_VENDOR == "mysql":
        db.execute("""
            INSERT INTO app_users (username, password_hash, display_name, full_name, role, is_active, rentman_crew_id, created_ts, updated_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (username, password_hashed, display_name, full_name, role, 1 if is_active else 0, rentman_crew_id, now, now))
    else:
        db.execute("""
            INSERT INTO app_users (username, password_hash, display_name, full_name, role, is_active, rentman_crew_id, created_ts, updated_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (username, password_hashed, display_name, full_name, role, 1 if is_active else 0, rentman_crew_id, now, now))
    db.commit()

    app.logger.info("Admin %s ha creato utente %s con ruolo %s", session.get("user"), username, role)
    return jsonify({"ok": True, "username": username}), 201


@app.put("/api/admin/users/<username>")
@login_required
def api_admin_users_update(username: str) -> ResponseReturnValue:
    """Modifica un utente esistente."""
    if not session.get("is_admin"):
        return jsonify({"error": "forbidden"}), 403

    username = username.strip().lower()
    data = request.get_json()
    app.logger.info("PUT /api/admin/users/%s - payload: %s", username, data)
    if not data:
        return jsonify({"error": "Dati non validi"}), 400

    db = get_db()

    # Verifica che l'utente esista
    if DB_VENDOR == "mysql":
        existing = db.execute("SELECT username FROM app_users WHERE username = %s", (username,)).fetchone()
    else:
        existing = db.execute("SELECT username FROM app_users WHERE username = ?", (username,)).fetchone()

    if not existing:
        return jsonify({"error": f"Utente '{username}' non trovato"}), 404

    # Prepara i campi da aggiornare
    updates = []
    params = []

    if "display_name" in data:
        display_name = (data["display_name"] or "").strip()
        if display_name:
            updates.append("display_name = " + ("%s" if DB_VENDOR == "mysql" else "?"))
            params.append(display_name)

    if "full_name" in data:
        full_name = (data["full_name"] or "").strip() or None
        updates.append("full_name = " + ("%s" if DB_VENDOR == "mysql" else "?"))
        params.append(full_name)

    if "role" in data:
        role = (data["role"] or "").strip().lower()
        if role not in VALID_USER_ROLES:
            return jsonify({"error": f"Ruolo non valido. Valori ammessi: {', '.join(sorted(VALID_USER_ROLES))}"}), 400
        # Impedisci di rimuovere il ruolo admin a se stessi
        current_user = session.get("user", "").lower()
        if username == current_user and role != "admin":
            return jsonify({"error": "Non puoi rimuovere il ruolo admin a te stesso"}), 400
        updates.append("role = " + ("%s" if DB_VENDOR == "mysql" else "?"))
        params.append(role)

    if "is_active" in data:
        is_active = data["is_active"]
        # Impedisci di disattivare se stessi
        current_user = session.get("user", "").lower()
        if username == current_user and not is_active:
            return jsonify({"error": "Non puoi disattivare il tuo account"}), 400
        updates.append("is_active = " + ("%s" if DB_VENDOR == "mysql" else "?"))
        params.append(1 if is_active else 0)

    if "password" in data and data["password"]:
        password_hashed = hash_password(data["password"])
        updates.append("password_hash = " + ("%s" if DB_VENDOR == "mysql" else "?"))
        params.append(password_hashed)

    if "rentman_crew_id" in data:
        rentman_crew_id = data.get("rentman_crew_id")
        # Può essere None per rimuovere l'associazione, o un intero
        if rentman_crew_id is not None and rentman_crew_id != "":
            rentman_crew_id = int(rentman_crew_id)
        else:
            rentman_crew_id = None
        updates.append("rentman_crew_id = " + ("%s" if DB_VENDOR == "mysql" else "?"))
        params.append(rentman_crew_id)

    if not updates:
        return jsonify({"error": "Nessun campo da aggiornare"}), 400

    updates.append("updated_ts = " + ("%s" if DB_VENDOR == "mysql" else "?"))
    params.append(now_ms())
    params.append(username)

    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    sql = f"UPDATE app_users SET {', '.join(updates)} WHERE username = {placeholder}"
    app.logger.info("SQL UPDATE: %s con params: %s", sql, params)
    
    cursor = db.execute(sql, tuple(params))
    app.logger.info("Rows affected: %s", cursor.rowcount if hasattr(cursor, 'rowcount') else 'N/A')
    
    db.commit()
    app.logger.info("COMMIT eseguito per utente %s", username)
    
    # Verifica immediata
    if DB_VENDOR == "mysql":
        verify = db.execute("SELECT rentman_crew_id FROM app_users WHERE username = %s", (username,)).fetchone()
        app.logger.info("Verifica dopo commit - rentman_crew_id: %s", verify)

    app.logger.info("Admin %s ha modificato utente %s", session.get("user"), username)
    return jsonify({"ok": True, "username": username})


@app.delete("/api/admin/users/<username>")
@login_required
def api_admin_users_delete(username: str) -> ResponseReturnValue:
    """Elimina un utente."""
    if not session.get("is_admin"):
        return jsonify({"error": "forbidden"}), 403

    username = username.strip().lower()

    # Impedisci di eliminare se stessi
    current_user = session.get("user", "").lower()
    if username == current_user:
        return jsonify({"error": "Non puoi eliminare il tuo account"}), 400

    db = get_db()

    # Verifica che l'utente esista
    if DB_VENDOR == "mysql":
        existing = db.execute("SELECT username FROM app_users WHERE username = %s", (username,)).fetchone()
    else:
        existing = db.execute("SELECT username FROM app_users WHERE username = ?", (username,)).fetchone()

    if not existing:
        return jsonify({"error": f"Utente '{username}' non trovato"}), 404

    if DB_VENDOR == "mysql":
        db.execute("DELETE FROM app_users WHERE username = %s", (username,))
    else:
        db.execute("DELETE FROM app_users WHERE username = ?", (username,))
    db.commit()

    app.logger.info("Admin %s ha eliminato utente %s", session.get("user"), username)
    return jsonify({"ok": True, "deleted": username})


# ═══════════════════════════════════════════════════════════════════════════════
#  GESTIONE OPERATORI (Admin) - API
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/admin/operators")
@login_required
def admin_operators() -> ResponseReturnValue:
    """Pagina gestione operatori."""
    if not session.get("is_admin"):
        flash("Accesso non autorizzato", "danger")
        return redirect(url_for("index"))
    return render_template("admin_operators.html", is_admin=True)


@app.get("/api/admin/operators")
@login_required
def api_admin_operators_list() -> ResponseReturnValue:
    """Lista tutti gli operatori dal database."""
    if not session.get("is_admin"):
        return jsonify({"error": "forbidden"}), 403

    db = get_db()
    ensure_crew_members_table(db)

    if DB_VENDOR == "mysql":
        cursor = db.execute(
            "SELECT id, rentman_id, name, external_id, external_group_id, email, phone, is_active, created_ts, updated_ts, timbratura_override "
            "FROM crew_members ORDER BY name"
        )
    else:
        cursor = db.execute(
            "SELECT id, rentman_id, name, external_id, external_group_id, email, phone, is_active, created_ts, updated_ts, timbratura_override "
            "FROM crew_members ORDER BY name"
        )
    rows = cursor.fetchall()

    operators = []
    for row in rows:
        timbratura_override = row[10] if len(row) > 10 else None
        if timbratura_override and isinstance(timbratura_override, str):
            try:
                timbratura_override = json.loads(timbratura_override)
            except:
                timbratura_override = None
        
        operators.append({
            "id": row[0],
            "rentman_id": row[1],
            "name": row[2],
            "external_id": row[3],
            "external_group_id": row[4],
            "email": row[5],
            "phone": row[6],
            "is_active": bool(row[7]),
            "created_ts": row[8],
            "updated_ts": row[9],
            "timbratura_override": timbratura_override
        })

    return jsonify({"ok": True, "operators": operators})


@app.put("/api/admin/operators/<int:operator_id>")
@login_required
def api_admin_operators_update(operator_id: int) -> ResponseReturnValue:
    """Aggiorna un operatore (principalmente external_id)."""
    if not session.get("is_admin"):
        return jsonify({"error": "forbidden"}), 403

    data = request.get_json(silent=True) or {}

    db = get_db()
    ensure_crew_members_table(db)

    # Verifica che l'operatore esista
    if DB_VENDOR == "mysql":
        existing = db.execute("SELECT id, name FROM crew_members WHERE id = %s", (operator_id,)).fetchone()
    else:
        existing = db.execute("SELECT id, name FROM crew_members WHERE id = ?", (operator_id,)).fetchone()

    if not existing:
        return jsonify({"error": "Operatore non trovato"}), 404

    # Prepara i campi da aggiornare
    external_id = data.get("external_id")
    if external_id is not None:
        external_id = external_id.strip() if external_id else None

    external_group_id = data.get("external_group_id")
    if external_group_id is not None:
        external_group_id = external_group_id.strip() if external_group_id else None

    email = data.get("email")
    if email is not None:
        email = email.strip() if email else None

    phone = data.get("phone")
    if phone is not None:
        phone = phone.strip() if phone else None

    is_active = data.get("is_active")
    if is_active is not None:
        is_active = 1 if is_active else 0

    now = now_ms()

    # Costruisci query di aggiornamento dinamica
    updates = []
    params = []

    if "external_id" in data:
        updates.append("external_id = " + ("%s" if DB_VENDOR == "mysql" else "?"))
        params.append(external_id)
    if "external_group_id" in data:
        updates.append("external_group_id = " + ("%s" if DB_VENDOR == "mysql" else "?"))
        params.append(external_group_id)
    if "email" in data:
        updates.append("email = " + ("%s" if DB_VENDOR == "mysql" else "?"))
        params.append(email)
    if "phone" in data:
        updates.append("phone = " + ("%s" if DB_VENDOR == "mysql" else "?"))
        params.append(phone)
    if "is_active" in data:
        updates.append("is_active = " + ("%s" if DB_VENDOR == "mysql" else "?"))
        params.append(is_active)
    
    # Gestione eccezioni timbratura (qr_disabled, gps_disabled)
    if "timbratura_override" in data:
        timbratura_override = data.get("timbratura_override")
        if timbratura_override and isinstance(timbratura_override, dict):
            # Verifica se c'è almeno una disabilitazione attiva
            has_override = timbratura_override.get("qr_disabled") or timbratura_override.get("gps_disabled")
            if has_override:
                timbratura_override = json.dumps(timbratura_override)
            else:
                timbratura_override = None
        else:
            timbratura_override = None
        updates.append("timbratura_override = " + ("%s" if DB_VENDOR == "mysql" else "?"))
        params.append(timbratura_override)

    if not updates:
        return jsonify({"error": "Nessun campo da aggiornare"}), 400

    updates.append("updated_ts = " + ("%s" if DB_VENDOR == "mysql" else "?"))
    params.append(now)
    params.append(operator_id)

    sql = f"UPDATE crew_members SET {', '.join(updates)} WHERE id = " + ("%s" if DB_VENDOR == "mysql" else "?")

    db.execute(sql, tuple(params))
    db.commit()

    app.logger.info("Admin %s ha modificato operatore %s (id=%d)", session.get("user"), existing[1], operator_id)
    return jsonify({"ok": True, "id": operator_id})


@app.post("/api/admin/operators/sync")
@login_required
def api_admin_operators_sync() -> ResponseReturnValue:
    """Forza la sincronizzazione degli operatori da Rentman."""
    if not session.get("is_admin"):
        return jsonify({"error": "forbidden"}), 403

    try:
        from rentman_client import rentman_request
    except ImportError:
        return jsonify({"error": "Client Rentman non disponibile"}), 500

    # Recupera tutti i crew members da Rentman
    try:
        response = rentman_request("GET", "/crew")
        if not response:
            return jsonify({"error": "Nessuna risposta da Rentman"}), 502
    except Exception as e:
        app.logger.error("Errore sync operatori Rentman: %s", e)
        return jsonify({"error": str(e)}), 500

    db = get_db()
    ensure_crew_members_table(db)

    synced = 0
    crew_data = response.get("data", [])
    for crew in crew_data:
        rentman_id = crew.get("id")
        name = crew.get("displayname") or crew.get("name") or f"Crew {rentman_id}"
        if rentman_id:
            sync_crew_member_from_rentman(db, rentman_id, name)
            synced += 1

    db.commit()

    app.logger.info("Admin %s ha sincronizzato %d operatori da Rentman", session.get("user"), synced)
    return jsonify({"ok": True, "synced": synced})


# ═══════════════════════════════════════════════════════════════════════════════
#  REGOLE TIMBRATURE - ADMIN
# ═══════════════════════════════════════════════════════════════════════════════

def ensure_timbratura_rules_table(db):
    """Crea la tabella timbratura_rules se non esiste."""
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    if DB_VENDOR == "mysql":
        db.execute("""
            CREATE TABLE IF NOT EXISTS timbratura_rules (
                id INT PRIMARY KEY DEFAULT 1,
                anticipo_max_minuti INT DEFAULT 30,
                tolleranza_ritardo_minuti INT DEFAULT 5,
                arrotondamento_ingresso_minuti INT DEFAULT 15,
                arrotondamento_ingresso_tipo VARCHAR(1) DEFAULT '+',
                arrotondamento_uscita_minuti INT DEFAULT 15,
                arrotondamento_uscita_tipo VARCHAR(1) DEFAULT '-',
                pausa_blocco_minimo_minuti INT DEFAULT 30,
                pausa_incremento_minuti INT DEFAULT 15,
                pausa_tolleranza_minuti INT DEFAULT 5,
                updated_ts BIGINT,
                updated_by VARCHAR(100)
            )
        """)
        # Aggiunge colonne se mancanti (per upgrade)
        try:
            db.execute("ALTER TABLE timbratura_rules ADD COLUMN arrotondamento_ingresso_tipo VARCHAR(1) DEFAULT '+'")
            db.commit()
        except:
            pass
        try:
            db.execute("ALTER TABLE timbratura_rules ADD COLUMN arrotondamento_uscita_tipo VARCHAR(1) DEFAULT '-'")
            db.commit()
        except:
            pass
    else:
        db.execute("""
            CREATE TABLE IF NOT EXISTS timbratura_rules (
                id INTEGER PRIMARY KEY DEFAULT 1,
                anticipo_max_minuti INTEGER DEFAULT 30,
                tolleranza_ritardo_minuti INTEGER DEFAULT 5,
                arrotondamento_ingresso_minuti INTEGER DEFAULT 15,
                arrotondamento_ingresso_tipo TEXT DEFAULT '+',
                arrotondamento_uscita_minuti INTEGER DEFAULT 15,
                arrotondamento_uscita_tipo TEXT DEFAULT '-',
                pausa_blocco_minimo_minuti INTEGER DEFAULT 30,
                pausa_incremento_minuti INTEGER DEFAULT 15,
                pausa_tolleranza_minuti INTEGER DEFAULT 5,
                updated_ts INTEGER,
                updated_by TEXT
            )
        """)
        # Aggiunge colonne se mancanti (per upgrade)
        try:
            db.execute("ALTER TABLE timbratura_rules ADD COLUMN arrotondamento_ingresso_tipo TEXT DEFAULT '+'")
            db.commit()
        except:
            pass
        try:
            db.execute("ALTER TABLE timbratura_rules ADD COLUMN arrotondamento_uscita_tipo TEXT DEFAULT '-'")
            db.commit()
        except:
            pass
    db.commit()


def get_timbratura_rules(db) -> dict:
    """Ottiene le regole timbrature dal database."""
    ensure_timbratura_rules_table(db)
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    row = db.execute("SELECT * FROM timbratura_rules WHERE id = 1").fetchone()
    
    if not row:
        # Inserisce valori di default
        db.execute("""
            INSERT INTO timbratura_rules (id, anticipo_max_minuti, tolleranza_ritardo_minuti,
                arrotondamento_ingresso_minuti, arrotondamento_uscita_minuti,
                pausa_blocco_minimo_minuti, pausa_incremento_minuti, pausa_tolleranza_minuti)
            VALUES (1, 30, 5, 15, 15, 30, 15, 5)
        """)
        db.commit()
        row = db.execute("SELECT * FROM timbratura_rules WHERE id = 1").fetchone()
    
    if isinstance(row, dict):
        return row
    
    # Converte tuple a dict
    columns = ['id', 'anticipo_max_minuti', 'tolleranza_ritardo_minuti',
               'arrotondamento_ingresso_minuti', 'arrotondamento_uscita_minuti',
               'pausa_blocco_minimo_minuti', 'pausa_incremento_minuti', 
               'pausa_tolleranza_minuti', 'updated_ts', 'updated_by']
    return dict(zip(columns, row))


@app.get("/admin/timbratura-rules")
@login_required
def admin_timbratura_rules_page() -> ResponseReturnValue:
    """Pagina configurazione regole timbrature (solo admin)."""
    if not session.get("is_admin"):
        abort(403)
    return render_template("admin_timbratura_rules.html", is_admin=True)


@app.get("/api/admin/timbratura-rules")
@login_required
def api_get_timbratura_rules():
    """Restituisce le regole timbrature correnti."""
    if not session.get("is_admin"):
        return jsonify({"error": "Non autorizzato"}), 403
    
    db = get_db()
    rules = get_timbratura_rules(db)
    return jsonify(rules)


@app.post("/api/admin/timbratura-rules")
@login_required
def api_save_timbratura_rules():
    """Salva le regole timbrature."""
    if not session.get("is_admin"):
        return jsonify({"error": "Non autorizzato"}), 403
    
    data = request.get_json()
    if not data:
        return jsonify({"error": "Dati mancanti"}), 400
    
    db = get_db()
    ensure_timbratura_rules_table(db)
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    # Valida i dati numerici
    fields = {
        'anticipo_max_minuti': (0, 120),
        'tolleranza_ritardo_minuti': (0, 60),
        'arrotondamento_ingresso_minuti': (1, 60),
        'arrotondamento_uscita_minuti': (1, 60),
        'pausa_blocco_minimo_minuti': (5, 120),
        'pausa_incremento_minuti': (5, 60),
        'pausa_tolleranza_minuti': (0, 30)
    }
    
    # Campi di tipo arrotondamento (+ - ~)
    tipo_fields = ['arrotondamento_ingresso_tipo', 'arrotondamento_uscita_tipo']
    valid_tipos = ['+', '-', '~']
    
    values = {}
    for field, (min_val, max_val) in fields.items():
        val = data.get(field)
        if val is not None:
            val = int(val)
            if val < min_val or val > max_val:
                return jsonify({"error": f"{field} deve essere tra {min_val} e {max_val}"}), 400
            values[field] = val
    
    # Valida e aggiungi campi tipo
    for field in tipo_fields:
        val = data.get(field)
        if val is not None:
            if val not in valid_tipos:
                return jsonify({"error": f"{field} deve essere uno di: +, -, ~"}), 400
            values[field] = val
    
    if not values:
        return jsonify({"error": "Nessun campo da aggiornare"}), 400
    
    # Costruisce query di update
    set_clause = ", ".join([f"{k} = {placeholder}" for k in values.keys()])
    params = list(values.values()) + [now_ms(), session.get('user')]
    
    db.execute(
        f"UPDATE timbratura_rules SET {set_clause}, updated_ts = {placeholder}, updated_by = {placeholder} WHERE id = 1",
        params
    )
    db.commit()
    
    app.logger.info("Admin %s ha aggiornato le regole timbrature: %s", session.get('user'), values)
    return jsonify({"success": True})


# ═══════════════════════════════════════════════════════════════════════════════
#  CALCOLO ORA MODIFICATA (ora_mod)
# ═══════════════════════════════════════════════════════════════════════════════

def calcola_ora_mod(ora_originale: str, tipo: str, turno_start: str = None, rules: dict = None) -> str:
    """
    Calcola l'ora modificata in base alle regole.
    
    Args:
        ora_originale: orario originale (HH:MM:SS o HH:MM)
        tipo: tipo timbratura (inizio_giornata, fine_giornata, inizio_pausa, fine_pausa)
        turno_start: orario inizio turno se presente (HH:MM)
        rules: dizionario con le regole (se None, usa default)
    
    Returns:
        ora modificata (HH:MM:SS)
    """
    if rules is None:
        rules = {
            'anticipo_max_minuti': 30,
            'tolleranza_ritardo_minuti': 5,
            'arrotondamento_ingresso_minuti': 15,
            'arrotondamento_uscita_minuti': 15
        }
    
    # Converte ora originale in minuti
    parts = ora_originale.split(':')
    ora_min = int(parts[0]) * 60 + int(parts[1])
    
    # Se c'è un turno e siamo in ingresso, prova normalizzazione
    if turno_start and tipo == 'inizio_giornata':
        turno_parts = turno_start.split(':')
        turno_min = int(turno_parts[0]) * 60 + int(turno_parts[1])
        
        anticipo = rules.get('anticipo_max_minuti', 30)
        tolleranza = rules.get('tolleranza_ritardo_minuti', 5)
        
        # Finestra di normalizzazione: [turno - anticipo, turno + tolleranza]
        min_window = turno_min - anticipo
        max_window = turno_min + tolleranza
        
        if min_window <= ora_min <= max_window:
            # Normalizza all'orario del turno
            h = turno_min // 60
            m = turno_min % 60
            return f"{h:02d}:{m:02d}:00"
    
    # Arrotondamento
    if tipo == 'inizio_giornata':
        # Arrotonda in eccesso (sfavorevole al dipendente per ingresso)
        blocco = rules.get('arrotondamento_ingresso_minuti', 15)
        ora_mod_min = ((ora_min + blocco - 1) // blocco) * blocco
    elif tipo == 'fine_giornata':
        # Arrotonda in difetto (sfavorevole al dipendente per uscita)
        blocco = rules.get('arrotondamento_uscita_minuti', 15)
        ora_mod_min = (ora_min // blocco) * blocco
    else:
        # Per inizio_pausa e fine_pausa, nessun arrotondamento sull'ora singola
        # Le pause vengono gestite sulla durata totale
        return ora_originale if ':' in ora_originale and ora_originale.count(':') == 2 else f"{ora_originale}:00"
    
    h = ora_mod_min // 60
    m = ora_mod_min % 60
    return f"{h:02d}:{m:02d}:00"


def calcola_pausa_mod(inizio_pausa: str, fine_pausa: str, rules: dict = None) -> int:
    """
    Calcola la durata della pausa modificata in minuti.
    
    Args:
        inizio_pausa: orario inizio pausa (HH:MM:SS o HH:MM)
        fine_pausa: orario fine pausa (HH:MM:SS o HH:MM)
        rules: dizionario con le regole (se None, usa default)
    
    Returns:
        durata pausa in minuti (arrotondata secondo le regole)
    """
    if rules is None:
        rules = {
            'pausa_blocco_minimo_minuti': 30,
            'pausa_incremento_minuti': 15,
            'pausa_tolleranza_minuti': 5
        }
    
    # Converte in minuti
    ip = inizio_pausa.split(':')
    fp = fine_pausa.split(':')
    inizio_min = int(ip[0]) * 60 + int(ip[1])
    fine_min = int(fp[0]) * 60 + int(fp[1])
    
    durata_effettiva = fine_min - inizio_min
    
    blocco_min = rules.get('pausa_blocco_minimo_minuti', 30)
    incremento = rules.get('pausa_incremento_minuti', 15)
    tolleranza = rules.get('pausa_tolleranza_minuti', 5)
    
    # Se durata < blocco minimo, usa blocco minimo
    if durata_effettiva <= blocco_min:
        return blocco_min
    
    # Calcola eccesso rispetto al blocco minimo
    eccesso = durata_effettiva - blocco_min
    
    # Quanti blocchi di incremento sono necessari?
    blocchi_extra = eccesso // incremento
    resto = eccesso % incremento
    
    # Se il resto è > tolleranza, aggiungi un blocco
    if resto > tolleranza:
        blocchi_extra += 1
    
    return blocco_min + (blocchi_extra * incremento)


# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIGURAZIONE AZIENDA - COMPANY SETTINGS
# ═══════════════════════════════════════════════════════════════════════════════

COMPANY_SETTINGS_TABLE_MYSQL = """
CREATE TABLE IF NOT EXISTS company_settings (
    id INT PRIMARY KEY DEFAULT 1,
    company_name VARCHAR(200) NOT NULL DEFAULT 'La Mia Azienda',
    external_id VARCHAR(100),
    logo_path VARCHAR(500),
    address TEXT,
    phone VARCHAR(50),
    email VARCHAR(200),
    website VARCHAR(200),
    vat_number VARCHAR(50),
    fiscal_code VARCHAR(50),
    modules_enabled JSON,
    custom_settings JSON,
    created_ts BIGINT NOT NULL,
    updated_ts BIGINT NOT NULL,
    updated_by VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

COMPANY_SETTINGS_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS company_settings (
    id INTEGER PRIMARY KEY DEFAULT 1,
    company_name TEXT NOT NULL DEFAULT 'La Mia Azienda',
    external_id TEXT,
    logo_path TEXT,
    address TEXT,
    phone TEXT,
    email TEXT,
    website TEXT,
    vat_number TEXT,
    fiscal_code TEXT,
    modules_enabled TEXT,
    custom_settings TEXT,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL,
    updated_by TEXT
)
"""


def ensure_company_settings_table(db: DatabaseLike) -> None:
    """Crea la tabella company_settings se non esiste."""
    statement = (
        COMPANY_SETTINGS_TABLE_MYSQL if DB_VENDOR == "mysql" else COMPANY_SETTINGS_TABLE_SQLITE
    )
    for stmt in statement.strip().split(";"):
        sql = stmt.strip()
        if not sql:
            continue
        cursor = db.execute(sql)
        try:
            cursor.close()
        except AttributeError:
            pass
    db.commit()


def get_company_settings(db: DatabaseLike) -> dict:
    """Ottiene le impostazioni azienda dal database."""
    ensure_company_settings_table(db)
    
    cursor = db.execute("SELECT * FROM company_settings WHERE id = 1")
    row = cursor.fetchone()
    
    if not row:
        # Inserisci valori di default
        now_ts = int(time.time() * 1000)
        if DB_VENDOR == "mysql":
            db.execute(
                """INSERT INTO company_settings 
                   (id, company_name, modules_enabled, custom_settings, created_ts, updated_ts) 
                   VALUES (1, 'La Mia Azienda', '{}', '{}', %s, %s)""",
                (now_ts, now_ts)
            )
        else:
            db.execute(
                """INSERT INTO company_settings 
                   (id, company_name, modules_enabled, custom_settings, created_ts, updated_ts) 
                   VALUES (1, 'La Mia Azienda', '{}', '{}', ?, ?)""",
                (now_ts, now_ts)
            )
        db.commit()
        cursor = db.execute("SELECT * FROM company_settings WHERE id = 1")
        row = cursor.fetchone()
    
    # Converti in dizionario - gestisce sia tuple che dict
    if isinstance(row, dict):
        settings = dict(row)
    else:
        columns = [desc[0] for desc in cursor.description]
        settings = dict(zip(columns, row))
    
    # Parse JSON fields
    for json_field in ['modules_enabled', 'custom_settings']:
        if settings.get(json_field):
            try:
                if isinstance(settings[json_field], str):
                    settings[json_field] = json.loads(settings[json_field])
            except (json.JSONDecodeError, TypeError):
                settings[json_field] = {}
        else:
            settings[json_field] = {}
    
    return settings


def save_company_settings(db: DatabaseLike, data: dict, updated_by: str) -> bool:
    """Salva le impostazioni azienda (INSERT o UPDATE)."""
    ensure_company_settings_table(db)
    now_ts = int(time.time() * 1000)
    
    # Prepara JSON fields
    modules_enabled = json.dumps(data.get('modules_enabled', {}))
    custom_settings = json.dumps(data.get('custom_settings', {}))
    
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    # Verifica se esiste già un record
    cursor = db.execute("SELECT id FROM company_settings WHERE id = 1")
    exists = cursor.fetchone() is not None
    
    if exists:
        # UPDATE
        db.execute(f"""
            UPDATE company_settings SET
                company_name = {placeholder},
                external_id = {placeholder},
                logo_path = {placeholder},
                address = {placeholder},
                phone = {placeholder},
                email = {placeholder},
                website = {placeholder},
                vat_number = {placeholder},
                fiscal_code = {placeholder},
                modules_enabled = {placeholder},
                custom_settings = {placeholder},
                updated_ts = {placeholder},
                updated_by = {placeholder}
            WHERE id = 1
        """, (
            data.get('company_name', 'La Mia Azienda'),
            data.get('external_id'),
            data.get('logo_path'),
            data.get('address'),
            data.get('phone'),
            data.get('email'),
            data.get('website'),
            data.get('vat_number'),
            data.get('fiscal_code'),
            modules_enabled,
            custom_settings,
            now_ts,
            updated_by
        ))
    else:
        # INSERT
        db.execute(f"""
            INSERT INTO company_settings (
                id, company_name, external_id, logo_path, address, phone, email, 
                website, vat_number, fiscal_code, modules_enabled, custom_settings, 
                created_ts, updated_ts, updated_by
            ) VALUES (
                1, {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, 
                {placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, 
                {placeholder}, {placeholder}, {placeholder}, {placeholder}
            )
        """, (
            data.get('company_name', 'La Mia Azienda'),
            data.get('external_id'),
            data.get('logo_path'),
            data.get('address'),
            data.get('phone'),
            data.get('email'),
            data.get('website'),
            data.get('vat_number'),
            data.get('fiscal_code'),
            modules_enabled,
            custom_settings,
            now_ts,
            now_ts,
            updated_by
        ))
    
    db.commit()
    return True


@app.get("/admin/company-settings")
@login_required
def admin_company_settings_page() -> ResponseReturnValue:
    """Pagina configurazione azienda (solo admin)."""
    if not session.get("is_admin"):
        abort(403)

    display_name = session.get("user_display") or session.get("user_name") or session.get("user")
    primary_name = session.get("user_name") or display_name or session.get("user")
    initials = session.get("user_initials") or compute_initials(primary_name or "")

    return render_template(
        "admin_company_settings.html",
        user_name=primary_name,
        user_display=display_name,
        user_initials=initials,
        is_admin=True,
    )


@app.get("/api/admin/company-settings")
@login_required
def api_get_company_settings() -> ResponseReturnValue:
    """API per ottenere le impostazioni azienda."""
    if not session.get("is_admin"):
        return jsonify({"error": "Non autorizzato"}), 403
    
    db = get_db()
    settings = get_company_settings(db)
    return jsonify(settings)


@app.post("/api/admin/company-settings")
@login_required
def api_save_company_settings() -> ResponseReturnValue:
    """API per salvare le impostazioni azienda."""
    if not session.get("is_admin"):
        return jsonify({"error": "Non autorizzato"}), 403
    
    data = request.get_json()
    if not data:
        return jsonify({"error": "Dati mancanti"}), 400
    
    db = get_db()
    username = session.get("user") or session.get("username") or "admin"
    
    try:
        save_company_settings(db, data, username)
        return jsonify({"ok": True, "message": "Impostazioni salvate"})
    except Exception as e:
        app.logger.error(f"Errore salvataggio company settings: {e}")
        return jsonify({"error": str(e)}), 500


@app.post("/api/admin/company-settings/logo")
@login_required
def api_upload_company_logo() -> ResponseReturnValue:
    """API per caricare il logo aziendale."""
    if not session.get("is_admin"):
        return jsonify({"error": "Non autorizzato"}), 403
    
    if 'logo' not in request.files:
        return jsonify({"error": "Nessun file caricato"}), 400
    
    file = request.files['logo']
    if file.filename == '':
        return jsonify({"error": "Nessun file selezionato"}), 400
    
    # Verifica estensione
    allowed_extensions = {'png', 'jpg', 'jpeg', 'gif', 'svg', 'webp'}
    ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else ''
    if ext not in allowed_extensions:
        return jsonify({"error": f"Formato non supportato. Usa: {', '.join(allowed_extensions)}"}), 400
    
    # Salva il file
    logo_dir = os.path.join(app.root_path, 'static', 'uploads', 'logo')
    os.makedirs(logo_dir, exist_ok=True)
    
    # Nome file univoco
    filename = f"company_logo_{int(time.time())}.{ext}"
    filepath = os.path.join(logo_dir, filename)
    file.save(filepath)
    
    # Path relativo per il database
    logo_path = f"/static/uploads/logo/{filename}"
    
    # Aggiorna database
    db = get_db()
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    now_ts = int(time.time() * 1000)
    username = session.get("user") or session.get("username") or "admin"
    
    ensure_company_settings_table(db)
    db.execute(f"""
        UPDATE company_settings SET 
            logo_path = {placeholder}, 
            updated_ts = {placeholder},
            updated_by = {placeholder}
        WHERE id = 1
    """, (logo_path, now_ts, username))
    db.commit()
    
    return jsonify({"ok": True, "logo_path": logo_path})


@app.delete("/api/admin/company-settings/logo")
@login_required
def api_delete_company_logo() -> ResponseReturnValue:
    """API per eliminare il logo aziendale."""
    if not session.get("is_admin"):
        return jsonify({"error": "Non autorizzato"}), 403
    
    db = get_db()
    settings = get_company_settings(db)
    
    # Elimina file se esiste
    if settings.get('logo_path'):
        old_path = os.path.join(app.root_path, settings['logo_path'].lstrip('/'))
        if os.path.exists(old_path):
            try:
                os.remove(old_path)
            except Exception as e:
                app.logger.warning(f"Errore eliminazione logo: {e}")
    
    # Aggiorna database
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    now_ts = int(time.time() * 1000)
    username = session.get("user") or session.get("username") or "admin"
    
    db.execute(f"""
        UPDATE company_settings SET 
            logo_path = NULL, 
            updated_ts = {placeholder},
            updated_by = {placeholder}
        WHERE id = 1
    """, (now_ts, username))
    db.commit()
    
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════════════════════
#  GESTIONE TIPOLOGIE RICHIESTE - ADMIN UI
# ═══════════════════════════════════════════════════════════════════════════════

VALUE_TYPE_LABELS = {
    "hours": "Ore",
    "days": "Giorni",
    "amount": "Importo €",
    "km": "Chilometri"
}


@app.get("/admin/request-types")
@login_required
def admin_request_types_page() -> ResponseReturnValue:
    """Pagina gestione tipologie richieste (solo admin)."""
    if not session.get("is_admin"):
        abort(403)

    display_name = session.get("user_display") or session.get("user_name") or session.get("user")
    primary_name = session.get("user_name") or display_name or session.get("user")
    initials = session.get("user_initials") or compute_initials(primary_name or "")

    return render_template(
        "admin_request_types.html",
        user_name=primary_name,
        user_display=display_name,
        user_initials=initials,
        is_admin=True,
    )


@app.get("/api/admin/request-types")
@login_required
def api_admin_request_types_list() -> ResponseReturnValue:
    """Lista tutte le tipologie di richiesta."""
    if not is_admin_or_supervisor():
        return jsonify({"error": "forbidden"}), 403

    db = get_db()
    ensure_request_types_table(db)
    
    rows = db.execute("""
        SELECT id, name, value_type, external_id, description, active, sort_order, created_ts, updated_ts
        FROM request_types
        ORDER BY sort_order ASC, name ASC
    """).fetchall()

    types = []
    for row in rows:
        if isinstance(row, Mapping):
            types.append({
                "id": row["id"],
                "name": row["name"],
                "value_type": row["value_type"],
                "value_type_label": VALUE_TYPE_LABELS.get(row["value_type"], row["value_type"]),
                "external_id": row["external_id"],
                "description": row["description"],
                "active": bool(row["active"]),
                "sort_order": row["sort_order"],
                "created_ts": row["created_ts"],
                "updated_ts": row["updated_ts"],
            })
        else:
            types.append({
                "id": row[0],
                "name": row[1],
                "value_type": row[2],
                "value_type_label": VALUE_TYPE_LABELS.get(row[2], row[2]),
                "external_id": row[3],
                "description": row[4],
                "active": bool(row[5]),
                "sort_order": row[6],
                "created_ts": row[7],
                "updated_ts": row[8],
            })

    return jsonify({"types": types, "value_types": VALUE_TYPE_LABELS})


@app.post("/api/admin/request-types")
@login_required
def api_admin_request_types_create() -> ResponseReturnValue:
    """Crea una nuova tipologia di richiesta."""
    if not session.get("is_admin"):
        return jsonify({"error": "forbidden"}), 403

    data = request.get_json()
    if not data:
        return jsonify({"error": "Dati non validi"}), 400

    name = (data.get("name") or "").strip()
    value_type = data.get("value_type", "hours")
    external_id = (data.get("external_id") or "").strip() or None
    description = (data.get("description") or "").strip() or None
    active = data.get("active", True)
    sort_order = data.get("sort_order", 0)

    if not name:
        return jsonify({"error": "Il nome è obbligatorio"}), 400

    if value_type not in VALUE_TYPE_LABELS:
        return jsonify({"error": f"Tipo valore non valido. Valori ammessi: {list(VALUE_TYPE_LABELS.keys())}"}), 400

    db = get_db()
    ensure_request_types_table(db)
    now_ms = int(time.time() * 1000)

    if DB_VENDOR == "mysql":
        db.execute("""
            INSERT INTO request_types (name, value_type, external_id, description, active, sort_order, created_ts, updated_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (name, value_type, external_id, description, 1 if active else 0, sort_order, now_ms, now_ms))
    else:
        db.execute("""
            INSERT INTO request_types (name, value_type, external_id, description, active, sort_order, created_ts, updated_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (name, value_type, external_id, description, 1 if active else 0, sort_order, now_ms, now_ms))
    
    db.commit()

    return jsonify({"ok": True, "message": f"Tipologia '{name}' creata con successo"})


@app.put("/api/admin/request-types/<int:type_id>")
@login_required
def api_admin_request_types_update(type_id: int) -> ResponseReturnValue:
    """Aggiorna una tipologia di richiesta."""
    if not session.get("is_admin"):
        return jsonify({"error": "forbidden"}), 403

    data = request.get_json()
    if not data:
        return jsonify({"error": "Dati non validi"}), 400

    name = (data.get("name") or "").strip()
    value_type = data.get("value_type", "hours")
    external_id = (data.get("external_id") or "").strip() or None
    description = (data.get("description") or "").strip() or None
    active = data.get("active", True)
    sort_order = data.get("sort_order", 0)

    if not name:
        return jsonify({"error": "Il nome è obbligatorio"}), 400

    if value_type not in VALUE_TYPE_LABELS:
        return jsonify({"error": f"Tipo valore non valido"}), 400

    db = get_db()
    ensure_request_types_table(db)
    now_ms = int(time.time() * 1000)

    if DB_VENDOR == "mysql":
        db.execute("""
            UPDATE request_types
            SET name = %s, value_type = %s, external_id = %s, description = %s, 
                active = %s, sort_order = %s, updated_ts = %s
            WHERE id = %s
        """, (name, value_type, external_id, description, 1 if active else 0, sort_order, now_ms, type_id))
    else:
        db.execute("""
            UPDATE request_types
            SET name = ?, value_type = ?, external_id = ?, description = ?, 
                active = ?, sort_order = ?, updated_ts = ?
            WHERE id = ?
        """, (name, value_type, external_id, description, 1 if active else 0, sort_order, now_ms, type_id))
    
    db.commit()

    return jsonify({"ok": True, "message": f"Tipologia '{name}' aggiornata"})


@app.delete("/api/admin/request-types/<int:type_id>")
@login_required
def api_admin_request_types_delete(type_id: int) -> ResponseReturnValue:
    """Elimina una tipologia di richiesta."""
    if not session.get("is_admin"):
        return jsonify({"error": "forbidden"}), 403

    db = get_db()
    ensure_request_types_table(db)
    ensure_user_requests_table(db)

    # Verifica che non ci siano richieste collegate
    if DB_VENDOR == "mysql":
        count = db.execute("SELECT COUNT(*) as cnt FROM user_requests WHERE request_type_id = %s", (type_id,)).fetchone()
    else:
        count = db.execute("SELECT COUNT(*) as cnt FROM user_requests WHERE request_type_id = ?", (type_id,)).fetchone()
    
    cnt = count["cnt"] if isinstance(count, Mapping) else count[0]
    if cnt > 0:
        return jsonify({"error": f"Impossibile eliminare: ci sono {cnt} richieste collegate a questa tipologia"}), 400

    if DB_VENDOR == "mysql":
        db.execute("DELETE FROM request_types WHERE id = %s", (type_id,))
    else:
        db.execute("DELETE FROM request_types WHERE id = ?", (type_id,))
    
    db.commit()

    return jsonify({"ok": True, "message": "Tipologia eliminata"})


def _send_request_review_notification(db: DatabaseLike, username: str, type_name: str, status: str, review_notes: str) -> None:
    """Invia notifica push all'utente quando la sua richiesta viene revisionata."""
    settings = get_webpush_settings()
    if not settings:
        app.logger.info("Notifiche push non configurate, skip notifica revisione richiesta")
        return
    
    placeholder = "%s" if DB_VENDOR == "mysql" else "?"
    
    # Recupera le subscription push dell'utente
    subscriptions = db.execute(
        f"SELECT endpoint, p256dh, auth FROM push_subscriptions WHERE username = {placeholder}",
        (username,)
    ).fetchall()
    
    if not subscriptions:
        app.logger.info("Nessuna subscription push per utente %s", username)
        return
    
    # Prepara il messaggio
    if status == "approved":
        title = "✅ Richiesta approvata"
        body = f"La tua richiesta di {type_name} è stata approvata"
    else:
        title = "❌ Richiesta respinta"
        body = f"La tua richiesta di {type_name} è stata respinta"
    
    if review_notes:
        body += f"\n📝 Note: {review_notes}"
    
    payload = {
        "title": title,
        "body": body,
        "icon": "/static/icons/icon-192x192.png",
        "badge": "/static/icons/icon-72x72.png",
        "tag": f"request-review-{status}",
        "data": {
            "url": "/user/requests",
            "type": "request_reviewed"
        }
    }
    
    # Invia a tutte le subscription dell'utente
    sent_ok = False
    for sub in subscriptions:
        endpoint = sub['endpoint'] if isinstance(sub, dict) else sub[0]
        p256dh = sub['p256dh'] if isinstance(sub, dict) else sub[1]
        auth = sub['auth'] if isinstance(sub, dict) else sub[2]
        
        subscription_info = {
            "endpoint": endpoint,
            "keys": {
                "p256dh": p256dh,
                "auth": auth
            }
        }
        
        try:
            webpush(
                subscription_info=subscription_info,
                data=json.dumps(payload),
                vapid_private_key=settings["vapid_private"],
                vapid_claims={"sub": settings["subject"]},
                ttl=86400,  # 24 ore
            )
            app.logger.info("Notifica revisione richiesta inviata a %s", username)
            sent_ok = True
                
        except WebPushException as e:
            app.logger.warning("Errore invio notifica revisione a %s: %s", username, e)
            if e.response and e.response.status_code in {404, 410}:
                remove_push_subscription(db, endpoint)
        except Exception as e:
            app.logger.error("Errore generico invio notifica revisione: %s", e)
    
    # Salva la notifica nel log (una volta per utente)
    if sent_ok:
        try:
            record_push_notification(
                db,
                kind="request_reviewed",
                title=title,
                body=body,
                payload=payload,
                username=username,
            )
            app.logger.info("Notifica revisione salvata nel log per %s", username)
        except Exception as e:
            app.logger.error("Errore salvataggio notifica revisione nel log: %s", e)


# =====================================================
# ADMIN USER REQUESTS - Gestione richieste utenti
# =====================================================

@app.route("/admin/user-requests")
@login_required
def admin_user_requests_page() -> ResponseReturnValue:
    """Pagina admin per gestire le richieste degli utenti."""
    if not session.get("is_admin"):
        return ("Forbidden", 403)
    
    username = session.get("username", "Admin")
    initials = "".join([p[0].upper() for p in username.split()[:2]]) if username else "?"
    
    return render_template(
        "admin_user_requests.html",
        is_admin=True,
        user_name=username,
        user_initials=initials
    )


@app.get("/api/admin/user-requests")
@login_required
def api_admin_user_requests_list() -> ResponseReturnValue:
    """Restituisce tutte le richieste degli utenti per l'admin."""
    if not session.get("is_admin"):
        return jsonify({"error": "Accesso negato"}), 403
    
    db = get_db()
    ensure_user_requests_table(db)
    
    if DB_VENDOR == "mysql":
        rows = db.execute("""
            SELECT ur.id, ur.username, ur.request_type_id, rt.name as type_name, rt.value_type,
                   ur.date_from, ur.date_to, ur.value_amount, ur.notes, ur.status,
                   ur.reviewed_by, ur.reviewed_ts, ur.review_notes, ur.created_ts, ur.updated_ts,
                   ur.cdc, ur.attachment_path
            FROM user_requests ur
            JOIN request_types rt ON ur.request_type_id = rt.id
            ORDER BY 
                CASE ur.status WHEN 'pending' THEN 0 ELSE 1 END,
                ur.created_ts DESC
        """).fetchall()
    else:
        rows = db.execute("""
            SELECT ur.id, ur.username, ur.request_type_id, rt.name as type_name, rt.value_type,
                   ur.date_from, ur.date_to, ur.value_amount, ur.notes, ur.status,
                   ur.reviewed_by, ur.reviewed_ts, ur.review_notes, ur.created_ts, ur.updated_ts,
                   ur.cdc, ur.attachment_path
            FROM user_requests ur
            JOIN request_types rt ON ur.request_type_id = rt.id
            ORDER BY 
                CASE ur.status WHEN 'pending' THEN 0 ELSE 1 END,
                ur.created_ts DESC
        """).fetchall()

    requests = []
    for row in rows:
        if isinstance(row, Mapping):
            requests.append({
                "id": row["id"],
                "username": row["username"],
                "request_type_id": row["request_type_id"],
                "type_name": row["type_name"],
                "value_type": row["value_type"],
                "date_from": row["date_from"],
                "date_to": row["date_to"],
                "value": float(row["value_amount"]) if row["value_amount"] else None,
                "notes": row["notes"],
                "status": row["status"],
                "reviewed_by": row["reviewed_by"],
                "reviewed_ts": row["reviewed_ts"],
                "review_notes": row["review_notes"],
                "created_ts": row["created_ts"],
                "updated_ts": row["updated_ts"],
                "cdc": row["cdc"],
                "attachment_path": row["attachment_path"],
            })
        else:
            requests.append({
                "id": row[0],
                "username": row[1],
                "request_type_id": row[2],
                "type_name": row[3],
                "value_type": row[4],
                "date_from": row[5],
                "date_to": row[6],
                "value": float(row[7]) if row[7] else None,
                "notes": row[8],
                "status": row[9],
                "reviewed_by": row[10],
                "reviewed_ts": row[11],
                "review_notes": row[12],
                "created_ts": row[13],
                "updated_ts": row[14],
                "cdc": row[15] if len(row) > 15 else None,
                "attachment_path": row[16] if len(row) > 16 else None,
            })

    return jsonify({"requests": requests})


@app.put("/api/admin/user-requests/<int:request_id>")
@login_required
def api_admin_user_request_review(request_id: int) -> ResponseReturnValue:
    """Approva o respinge una richiesta utente."""
    if not session.get("is_admin"):
        return jsonify({"error": "Accesso negato"}), 403
    
    data = request.get_json() or {}
    status = data.get("status")
    review_notes = data.get("review_notes", "").strip()
    
    if status not in ("approved", "rejected"):
        return jsonify({"error": "Stato non valido. Usa 'approved' o 'rejected'"}), 400
    
    db = get_db()
    ensure_user_requests_table(db)
    
    # Verifica che la richiesta esista e sia pending
    if DB_VENDOR == "mysql":
        existing = db.execute("""
            SELECT ur.id, ur.status, ur.username, rt.name as type_name
            FROM user_requests ur
            JOIN request_types rt ON ur.request_type_id = rt.id
            WHERE ur.id = %s
        """, (request_id,)).fetchone()
    else:
        existing = db.execute("""
            SELECT ur.id, ur.status, ur.username, rt.name as type_name
            FROM user_requests ur
            JOIN request_types rt ON ur.request_type_id = rt.id
            WHERE ur.id = ?
        """, (request_id,)).fetchone()
    
    if not existing:
        return jsonify({"error": "Richiesta non trovata"}), 404
    
    if isinstance(existing, Mapping):
        current_status = existing["status"]
        target_username = existing["username"]
        type_name = existing["type_name"]
    else:
        current_status = existing[1]
        target_username = existing[2]
        type_name = existing[3]
    
    if current_status != "pending":
        return jsonify({"error": "La richiesta è già stata revisionata"}), 400
    
    reviewed_by = session.get("user", "")
    now = int(datetime.now().timestamp() * 1000)
    
    if DB_VENDOR == "mysql":
        db.execute("""
            UPDATE user_requests 
            SET status = %s, reviewed_by = %s, reviewed_ts = %s, review_notes = %s, updated_ts = %s
            WHERE id = %s
        """, (status, reviewed_by, now, review_notes, now, request_id))
    else:
        db.execute("""
            UPDATE user_requests 
            SET status = ?, reviewed_by = ?, reviewed_ts = ?, review_notes = ?, updated_ts = ?
            WHERE id = ?
        """, (status, reviewed_by, now, review_notes, now, request_id))
    
    db.commit()
    
    # Invia notifica push all'utente
    _send_request_review_notification(db, target_username, type_name, status, review_notes)
    
    status_label = "approvata" if status == "approved" else "respinta"
    return jsonify({"ok": True, "message": f"Richiesta {status_label} con successo"})


# =====================================================
# USER REQUESTS - Pagina e API per richieste utente
# =====================================================

@app.route("/user/requests")
@login_required
def user_requests_page() -> ResponseReturnValue:
    """Pagina utente per inviare richieste (ferie, permessi, rimborsi, ecc.)."""
    return render_template(
        "user_requests.html",
        username=session.get("user"),
        user_display=session.get("user_display", session.get("user")),
        user_initials=session.get("user_initials", "U"),
        user_role=session.get("user_role", "Utente"),
    )


@app.route("/user/notifications")
@login_required
def user_notifications_page() -> ResponseReturnValue:
    """Pagina utente per visualizzare lo storico delle notifiche push."""
    return render_template(
        "user_notifications.html",
        username=session.get("user"),
        user_display=session.get("user_display", session.get("user")),
        user_initials=session.get("user_initials", "U"),
        user_role=session.get("user_role", "Utente"),
    )


@app.get("/api/user/request-types")
@login_required
def api_user_request_types_list() -> ResponseReturnValue:
    """Restituisce le tipologie di richiesta attive per l'utente."""
    db = get_db()
    ensure_request_types_table(db)
    
    rows = db.execute("""
        SELECT id, name, value_type, description
        FROM request_types
        WHERE active = 1
        ORDER BY sort_order ASC, name ASC
    """).fetchall()

    types = []
    for row in rows:
        if isinstance(row, Mapping):
            types.append({
                "id": row["id"],
                "name": row["name"],
                "value_type": row["value_type"],
                "description": row["description"],
            })
        else:
            types.append({
                "id": row[0],
                "name": row[1],
                "value_type": row[2],
                "description": row[3],
            })

    return jsonify({"types": types})


@app.get("/api/user/residuals")
@login_required
def api_user_residuals() -> ResponseReturnValue:
    """Restituisce le ore residue di ferie e permessi per l'utente loggato."""
    # Per ora ritorna valori placeholder - in futuro si potrà integrare con sistema HR
    username = session.get("user", "")
    
    # TODO: Implementare calcolo reale basato su:
    # - Ore totali annuali assegnate
    # - Ore già godute/approvate
    
    return jsonify({
        "ferie_hours": 0,
        "permessi_hours": 0
    })


@app.get("/api/user/requests")
@login_required
def api_user_requests_list() -> ResponseReturnValue:
    """Restituisce lo storico delle richieste dell'utente loggato."""
    username = session.get("user", "")
    
    db = get_db()
    ensure_user_requests_table(db)
    
    if DB_VENDOR == "mysql":
        rows = db.execute("""
            SELECT ur.id, ur.request_type_id, rt.name as type_name, rt.value_type,
                   ur.date_from, ur.date_to, ur.value_amount, ur.notes, ur.status,
                   ur.review_notes, ur.created_ts, ur.updated_ts, ur.cdc, ur.attachment_path
            FROM user_requests ur
            JOIN request_types rt ON ur.request_type_id = rt.id
            WHERE ur.username = %s
            ORDER BY ur.created_ts DESC
        """, (username,)).fetchall()
    else:
        rows = db.execute("""
            SELECT ur.id, ur.request_type_id, rt.name as type_name, rt.value_type,
                   ur.date_from, ur.date_to, ur.value_amount, ur.notes, ur.status,
                   ur.review_notes, ur.created_ts, ur.updated_ts, ur.cdc, ur.attachment_path
            FROM user_requests ur
            JOIN request_types rt ON ur.request_type_id = rt.id
            WHERE ur.username = ?
            ORDER BY ur.created_ts DESC
        """, (username,)).fetchall()

    value_units = {"hours": "ore", "days": "giorni", "amount": "€", "km": "km"}
    
    requests = []
    for row in rows:
        if isinstance(row, Mapping):
            requests.append({
                "id": row["id"],
                "request_type_id": row["request_type_id"],
                "type_name": row["type_name"],
                "value_type": row["value_type"],
                "value_unit": value_units.get(row["value_type"], ""),
                "date_start": row["date_from"],
                "date_end": row["date_to"],
                "value": float(row["value_amount"]) if row["value_amount"] else None,
                "notes": row["notes"],
                "status": row["status"],
                "admin_notes": row["review_notes"],
                "created_ts": row["created_ts"],
                "updated_ts": row["updated_ts"],
                "cdc": row["cdc"],
                "attachment_path": row["attachment_path"],
            })
        else:
            requests.append({
                "id": row[0],
                "request_type_id": row[1],
                "type_name": row[2],
                "value_type": row[3],
                "value_unit": value_units.get(row[3], ""),
                "date_start": row[4],
                "date_end": row[5],
                "value": float(row[6]) if row[6] else None,
                "notes": row[7],
                "status": row[8],
                "admin_notes": row[9],
                "created_ts": row[10],
                "updated_ts": row[11],
                "cdc": row[12] if len(row) > 12 else None,
                "attachment_path": row[13] if len(row) > 13 else None,
            })

    return jsonify({"requests": requests})


@app.post("/api/user/requests")
@login_required
def api_user_requests_create() -> ResponseReturnValue:
    """Crea una nuova richiesta utente. Supporta JSON o multipart/form-data per upload file."""
    username = session.get("user", "")
    
    # Determina se è multipart/form-data o JSON
    content_type = request.content_type or ""
    
    if 'multipart/form-data' in content_type:
        # Form con allegati (supporta multipli)
        request_type_id = request.form.get("request_type_id")
        date_start = request.form.get("date_start") or request.form.get("start_date")
        date_end = request.form.get("date_end") or request.form.get("end_date")
        value = request.form.get("value")
        notes = (request.form.get("notes") or "").strip()
        cdc = (request.form.get("cdc") or "").strip() or None
        # Supporta sia 'attachment' singolo che 'attachments' multipli
        attachment_files = request.files.getlist("attachments") or []
        single_attachment = request.files.get("attachment")
        if single_attachment and single_attachment.filename:
            attachment_files.append(single_attachment)
    else:
        # JSON (senza allegato) - force=True per evitare errore 415
        data = request.get_json(force=True, silent=True) or {}
        request_type_id = data.get("request_type_id")
        date_start = data.get("date_start") or data.get("start_date")
        date_end = data.get("date_end") or data.get("end_date")
        value = data.get("value")
        notes = (data.get("notes") or "").strip()
        cdc = (data.get("cdc") or "").strip() or None
        attachment_files = []

    if not request_type_id or not date_start:
        return jsonify({"error": "Tipo richiesta e data inizio sono obbligatori"}), 400

    db = get_db()
    ensure_user_requests_table(db)
    
    # Verifica che il tipo richiesta esista e sia attivo
    if DB_VENDOR == "mysql":
        rt = db.execute("SELECT id, value_type FROM request_types WHERE id = %s AND active = 1", (request_type_id,)).fetchone()
    else:
        rt = db.execute("SELECT id, value_type FROM request_types WHERE id = ? AND active = 1", (request_type_id,)).fetchone()
    
    if not rt:
        return jsonify({"error": "Tipologia richiesta non valida o non attiva"}), 400

    value_type = rt["value_type"] if isinstance(rt, dict) else rt[1]
    
    # Validazioni specifiche per value_type
    if value_type == "amount":
        if not cdc:
            return jsonify({"error": "Centro di costo (CDC) è obbligatorio per i rimborsi"}), 400
        if not attachment_files or len(attachment_files) == 0:
            return jsonify({"error": "Almeno un allegato (foto ricevuta) è obbligatorio per i rimborsi"}), 400

    # Gestione upload file multipli
    attachment_paths = []
    allowed_extensions = {'png', 'jpg', 'jpeg', 'gif', 'webp', 'pdf'}
    
    for idx, attachment_file in enumerate(attachment_files):
        if attachment_file and attachment_file.filename:
            ext = attachment_file.filename.rsplit('.', 1)[-1].lower() if '.' in attachment_file.filename else ''
            if ext not in allowed_extensions:
                return jsonify({"error": f"Formato file non supportato: {attachment_file.filename}. Usa: {', '.join(allowed_extensions)}"}), 400
            
            # Crea directory se non esiste
            upload_dir = os.path.join(app.root_path, 'uploads', 'requests', username)
            os.makedirs(upload_dir, exist_ok=True)
            
            # Nome file univoco (con indice per multipli)
            timestamp = int(time.time() * 1000)  # millisecondi per unicità
            filename = f"request_{timestamp}_{idx}.{ext}"
            filepath = os.path.join(upload_dir, filename)
            attachment_file.save(filepath)
            attachment_paths.append(f"/uploads/requests/{username}/{filename}")
    
    # Salva come JSON array se ci sono più file, altrimenti stringa singola per retrocompatibilità
    if len(attachment_paths) == 0:
        attachment_path = None
    elif len(attachment_paths) == 1:
        attachment_path = attachment_paths[0]
    else:
        attachment_path = json.dumps(attachment_paths)

    now = int(datetime.now().timestamp() * 1000)  # timestamp in millisecondi
    value_amount = float(value) if value else 0.0
    
    if DB_VENDOR == "mysql":
        db.execute("""
            INSERT INTO user_requests (user_id, username, request_type_id, date_from, date_to, value_amount, notes, cdc, attachment_path, status, created_ts, updated_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s, %s)
        """, (0, username, request_type_id, date_start, date_end, value_amount, notes, cdc, attachment_path, now, now))
    else:
        db.execute("""
            INSERT INTO user_requests (user_id, username, request_type_id, date_from, date_to, value_amount, notes, cdc, attachment_path, status, created_ts, updated_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
        """, (0, username, request_type_id, date_start, date_end, value_amount, notes, cdc, attachment_path, now, now))
    
    db.commit()

    return jsonify({"ok": True, "message": "Richiesta inviata con successo"})


# Route per servire gli allegati delle richieste
@app.route('/uploads/requests/<path:filename>')
@login_required
def serve_request_attachment(filename):
    """Serve gli allegati delle richieste (solo per l'utente proprietario o admin)."""
    # Verifica accesso: l'utente può vedere solo i propri file, admin può vedere tutti
    username = session.get("user", "")
    is_admin = session.get("is_admin", False)
    
    # Estrai username dal path
    parts = filename.split('/')
    if len(parts) >= 1:
        file_owner = parts[0]
        if not is_admin and file_owner != username:
            return jsonify({"error": "Accesso negato"}), 403
    
    return send_from_directory(
        os.path.join(app.root_path, 'uploads', 'requests'),
        filename
    )


if __name__ == "__main__":
    init_db()
    
    # HTTPS per permettere accesso camera da mobile in rete locale
    # Usa: python app.py --https
    import sys
    if "--https" in sys.argv:
        print("🔐 Avvio in modalità HTTPS (per accesso camera da mobile)")
        print("⚠️  Il browser mostrerà un avviso di sicurezza - clicca 'Avanzate' → 'Procedi'")
        app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False, ssl_context='adhoc')
    else:
        app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
