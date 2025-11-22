from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

try:
    import pymysql  # type: ignore[import]
    from pymysql.cursors import DictCursor  # type: ignore[import]
except ImportError:  # pragma: no cover - PyMySQL non installato
    pymysql = None
    DictCursor = None


DATABASE_FILE = Path(__file__).with_name("joblog.db")
CONFIG_FILE = Path(__file__).with_name("config.json")

ROLE_USER = "user"
ROLE_SUPERVISOR = "supervisor"
ROLE_ADMIN = "admin"
VALID_ROLES = {ROLE_USER, ROLE_SUPERVISOR, ROLE_ADMIN}


def hash_password(password: str) -> str:
    """Restituisce l'hash SHA-256 della password."""
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def now_ms() -> int:
    return int(time.time() * 1000)


def normalize_username(value: str) -> str:
    return value.strip().lower()


def normalize_role(value: Optional[str]) -> str:
    if not value:
        return ROLE_USER
    candidate = value.strip().lower()
    if candidate not in VALID_ROLES:
        raise SystemExit(
            f"Ruolo non valido '{value}'. Valori ammessi: {', '.join(sorted(VALID_ROLES))}"
        )
    return candidate


def load_config() -> Dict[str, Any]:
    if not CONFIG_FILE.exists():
        return {}
    try:
        with CONFIG_FILE.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
            return data if isinstance(data, dict) else {}
    except json.JSONDecodeError as exc:
        raise SystemExit(f"config.json non valido: {exc}")


def get_database_settings() -> Dict[str, Any]:
    config = load_config()
    database_section = config.get("database")
    raw_db: Dict[str, Any] = database_section if isinstance(database_section, dict) else {}

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
    except (TypeError, ValueError):  # pragma: no cover
        port = 3306

    return {
        "vendor": vendor,
        "host": read("host", "JOBLOG_DB_HOST", "localhost"),
        "port": port,
        "user": read("user", "JOBLOG_DB_USER", "root"),
        "password": read("password", "JOBLOG_DB_PASSWORD", ""),
        "name": read("name", "JOBLOG_DB_NAME", "joblog"),
    }


class DatabaseAdapter:
    def __init__(self, settings: Mapping[str, Any]):
        self.vendor = str(settings.get("vendor", "sqlite")).lower()
        if self.vendor == "mysql":
            if pymysql is None or DictCursor is None:
                raise SystemExit(
                    "PyMySQL non è installato. Esegui 'pip install PyMySQL' per usare il backend MySQL."
                )
            self._conn = pymysql.connect(  # type: ignore[union-attr]
                host=settings.get("host", "localhost"),
                port=int(settings.get("port", 3306) or 3306),
                user=settings.get("user"),
                password=settings.get("password"),
                database=settings.get("name"),
                charset="utf8mb4",
                cursorclass=DictCursor,
                autocommit=False,
            )
        else:
            self._conn = sqlite3.connect(DATABASE_FILE)
            self._conn.row_factory = sqlite3.Row

    def __enter__(self) -> "DatabaseAdapter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc:
            self._conn.rollback()
        else:
            self._conn.commit()
        self._conn.close()

    def _prepare_sql(self, sql: str) -> str:
        if self.vendor == "mysql" and "%s" not in sql and "%(" not in sql:
            return sql.replace("?", "%s")
        return sql

    @staticmethod
    def _prepare_params(params: Optional[Iterable[Any]]) -> Sequence[Any]:
        if params is None:
            return ()
        if isinstance(params, (list, tuple)):
            return tuple(params)
        return (params,)

    def run(self, sql: str, params: Optional[Iterable[Any]] = None) -> int:
        cursor = self._conn.cursor()
        try:
            cursor.execute(self._prepare_sql(sql), self._prepare_params(params))
            return cursor.rowcount or 0
        finally:
            cursor.close()

    def fetchone(self, sql: str, params: Optional[Iterable[Any]] = None) -> Optional[Dict[str, Any]]:
        cursor = self._conn.cursor()
        try:
            cursor.execute(self._prepare_sql(sql), self._prepare_params(params))
            row = cursor.fetchone()
            if row is None:
                return None
            if isinstance(row, Mapping):
                return dict(row)
            if isinstance(row, sqlite3.Row):
                return dict(row)
            if isinstance(row, Sequence):
                columns = [col[0] for col in cursor.description] if cursor.description else []
                return {col: row[idx] for idx, col in enumerate(columns)}
            return None
        finally:
            cursor.close()

    def fetchall(self, sql: str, params: Optional[Iterable[Any]] = None) -> Sequence[Dict[str, Any]]:
        cursor = self._conn.cursor()
        try:
            cursor.execute(self._prepare_sql(sql), self._prepare_params(params))
            rows = cursor.fetchall()
            result: list[Dict[str, Any]] = []
            if not rows:
                return result
            if isinstance(rows[0], Mapping):
                return [dict(row) for row in rows]
            if isinstance(rows[0], sqlite3.Row):
                return [dict(row) for row in rows]
            columns = [col[0] for col in cursor.description] if cursor.description else []
            for row in rows:
                result.append({col: row[idx] for idx, col in enumerate(columns)})
            return result
        finally:
            cursor.close()


def ensure_users_table(db: DatabaseAdapter) -> None:
    if db.vendor == "mysql":
        db.run(
            """
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
        )
    else:
        db.run(
            """
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
        )


def prompt_password(confirm: bool = False) -> str:
    password = getpass.getpass("Password: ")
    if not confirm:
        return password
    confirmation = getpass.getpass("Conferma password: ")
    if password != confirmation:
        raise SystemExit("Le password non coincidono")
    return password


def format_timestamp(value: Optional[int]) -> str:
    if not value:
        return "-"
    try:
        return datetime.fromtimestamp(int(value) / 1000).strftime("%Y-%m-%d %H:%M")
    except (ValueError, OSError):  # pragma: no cover - timestamp invalido
        return str(value)


def command_list(_: argparse.Namespace) -> None:
    with DatabaseAdapter(get_database_settings()) as db:
        ensure_users_table(db)
        rows = db.fetchall(
            """
            SELECT username, display_name, full_name, role, is_active, created_ts
            FROM app_users
            ORDER BY username
            """
        )
        if not rows:
            print("Nessun utente registrato")
            return
        print("Utenti registrati:")
        for row in rows:
            username = row.get("username") or "(sconosciuto)"
            display = row.get("display_name") or username
            full_name = row.get("full_name") or "-"
            role = (row.get("role") or ROLE_USER).lower()
            status = "attivo" if row.get("is_active") else "disabilitato"
            created = format_timestamp(row.get("created_ts"))
            print(f"- {display} ({username}) — ruolo: {role}, stato: {status}, creato: {created}, nome completo: {full_name}")


def command_create(args: argparse.Namespace) -> None:
    username = args.username.strip()
    if not username:
        raise SystemExit("Username obbligatorio")
    role = normalize_role(args.role)
    full_name = args.full_name.strip() if args.full_name else None
    display_name = args.display_name.strip() if args.display_name else username
    if not full_name:
        full_name = display_name
    if args.password:
        password = args.password
    else:
        password = prompt_password(confirm=True)
    if not password:
        raise SystemExit("Password obbligatoria")

    with DatabaseAdapter(get_database_settings()) as db:
        ensure_users_table(db)
        existing = db.fetchone(
            "SELECT username FROM app_users WHERE LOWER(username)=LOWER(?)",
            (username,),
        )
        if existing:
            raise SystemExit("Utente già esistente")

        timestamp = now_ms()
        db.run(
            """
            INSERT INTO app_users (
                username, password_hash, display_name, full_name, role, is_active, created_ts, updated_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalize_username(username),
                hash_password(password),
                display_name,
                full_name,
                role,
                0 if args.inactive else 1,
                timestamp,
                timestamp,
            ),
        )
        print(f"Utente '{username}' creato con ruolo '{role}'")


def command_set_password(args: argparse.Namespace) -> None:
    username = args.username.strip()
    if not username:
        raise SystemExit("Username obbligatorio")
    if args.password:
        password = args.password
    else:
        password = prompt_password(confirm=True)
    if not password:
        raise SystemExit("Password obbligatoria")

    with DatabaseAdapter(get_database_settings()) as db:
        ensure_users_table(db)
        updated = db.run(
            """
            UPDATE app_users
            SET password_hash=?, updated_ts=?
            WHERE LOWER(username)=LOWER(?)
            """,
            (hash_password(password), now_ms(), username),
        )
        if not updated:
            raise SystemExit("Utente non trovato")
        print(f"Password aggiornata per '{username}'")


def command_delete(args: argparse.Namespace) -> None:
    username = args.username.strip()
    if not username:
        raise SystemExit("Username obbligatorio")

    if not args.force:
        answer = input(f"Confermi l'eliminazione di '{username}'? [y/N] ")
        if answer.lower() not in {"y", "yes"}:
            print("Operazione annullata")
            return

    with DatabaseAdapter(get_database_settings()) as db:
        ensure_users_table(db)
        deleted = db.run("DELETE FROM app_users WHERE LOWER(username)=LOWER(?)", (username,))
        if not deleted:
            raise SystemExit("Utente non trovato")
        print(f"Utente '{username}' eliminato")


def command_set_role(args: argparse.Namespace) -> None:
    username = args.username.strip()
    if not username:
        raise SystemExit("Username obbligatorio")
    role = normalize_role(args.role)

    with DatabaseAdapter(get_database_settings()) as db:
        ensure_users_table(db)
        updated = db.run(
            """
            UPDATE app_users
            SET role=?, updated_ts=?
            WHERE LOWER(username)=LOWER(?)
            """,
            (role, now_ms(), username),
        )
        if not updated:
            raise SystemExit("Utente non trovato")
        print(f"Ruolo aggiornato per '{username}': {role}")


def command_toggle_active(args: argparse.Namespace, active: bool) -> None:
    username = args.username.strip()
    if not username:
        raise SystemExit("Username obbligatorio")

    with DatabaseAdapter(get_database_settings()) as db:
        ensure_users_table(db)
        updated = db.run(
            """
            UPDATE app_users
            SET is_active=?, updated_ts=?
            WHERE LOWER(username)=LOWER(?)
            """,
            (1 if active else 0, now_ms(), username),
        )
        if not updated:
            raise SystemExit("Utente non trovato")
        state_label = "attivato" if active else "disabilitato"
        print(f"Utente '{username}' {state_label}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gestione utenti JobLog (tabella app_users)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="Elenca gli utenti registrati")
    list_parser.set_defaults(func=command_list)

    create_parser = subparsers.add_parser("create", help="Crea un nuovo utente")
    create_parser.add_argument("username", help="Username per l'accesso")
    create_parser.add_argument("--display-name", help="Nome visualizzato (default: username)")
    create_parser.add_argument(
        "--name",
        "--full-name",
        dest="full_name",
        help="Nome completo (default: display name)",
    )
    create_parser.add_argument(
        "--role",
        choices=sorted(VALID_ROLES),
        default=ROLE_USER,
        help="Ruolo applicativo",
    )
    create_parser.add_argument("--inactive", action="store_true", help="Crea l'utente disabilitato")
    create_parser.add_argument("--password", help="Password in chiaro (usa con cautela)")
    create_parser.set_defaults(func=command_create)

    password_parser = subparsers.add_parser("set-password", help="Aggiorna la password di un utente")
    password_parser.add_argument("username", help="Username esistente")
    password_parser.add_argument("--password", help="Nuova password in chiaro (usa con cautela)")
    password_parser.set_defaults(func=command_set_password)

    role_parser = subparsers.add_parser("set-role", help="Aggiorna il ruolo di un utente")
    role_parser.add_argument("username", help="Username esistente")
    role_parser.add_argument("role", choices=sorted(VALID_ROLES), help="Nuovo ruolo")
    role_parser.set_defaults(func=command_set_role)

    activate_parser = subparsers.add_parser("activate", help="Riattiva un utente disabilitato")
    activate_parser.add_argument("username", help="Username esistente")
    activate_parser.set_defaults(func=lambda args: command_toggle_active(args, True))

    deactivate_parser = subparsers.add_parser("deactivate", help="Disabilita un utente")
    deactivate_parser.add_argument("username", help="Username esistente")
    deactivate_parser.set_defaults(func=lambda args: command_toggle_active(args, False))

    delete_parser = subparsers.add_parser("delete", help="Elimina un utente")
    delete_parser.add_argument("username", help="Username da eliminare")
    delete_parser.add_argument("--force", action="store_true", help="Non chiedere conferma")
    delete_parser.set_defaults(func=command_delete)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
