from __future__ import annotations

import argparse
import getpass
import hashlib
import json
from pathlib import Path
from typing import Dict

USERS_FILE = Path(__file__).with_name("users.json")


def hash_password(password: str) -> str:
    """Return the SHA-256 hash of the given password."""
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def load_users() -> Dict[str, Dict[str, str]]:
    if not USERS_FILE.exists():
        return {}
    try:
        with USERS_FILE.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
            if isinstance(data, dict):
                return data
    except json.JSONDecodeError as exc:
        raise SystemExit(f"File utenti non valido: {exc}")
    raise SystemExit("File utenti non valido: atteso oggetto JSON")


def save_users(users: Dict[str, Dict[str, str]]) -> None:
    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = USERS_FILE.with_suffix(USERS_FILE.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(users, handle, indent=2, sort_keys=True)
        handle.write("\n")
    tmp_path.replace(USERS_FILE)


def find_user_key(users: Dict[str, Dict[str, str]], username: str) -> str | None:
    target = username.strip().lower()
    for key in users:
        if key.lower() == target:
            return key
    return None


def prompt_password(confirm: bool = False) -> str:
    password = getpass.getpass("Password: ")
    if not confirm:
        return password
    confirm_value = getpass.getpass("Conferma password: ")
    if password != confirm_value:
        raise SystemExit("Le password non coincidono")
    return password


def command_list(_: argparse.Namespace) -> None:
    users = load_users()
    if not users:
        print("Nessun utente registrato")
        return
    print("Utenti registrati:")
    for key in sorted(users):
        info = users[key]
        display = info.get("display") or key
        name = info.get("name") or "(nessun nome)"
        print(f"- {display} [{name}]")


def command_create(args: argparse.Namespace) -> None:
    users = load_users()
    username = args.username.strip()
    if not username:
        raise SystemExit("Username obbligatorio")
    if find_user_key(users, username):
        raise SystemExit("Utente gia esistente")
    name = args.name.strip() if args.name else username
    if args.password:
        password = args.password
    else:
        password = prompt_password(confirm=True)
    if not password:
        raise SystemExit("Password obbligatoria")
    key = username.lower()
    users[key] = {
        "password": hash_password(password),
        "name": name,
        "display": username,
    }
    save_users(users)
    print(f"Utente '{username}' creato")


def command_set_password(args: argparse.Namespace) -> None:
    users = load_users()
    username = args.username.strip()
    user_key = find_user_key(users, username)
    if not user_key:
        raise SystemExit("Utente non trovato")
    if args.password:
        password = args.password
    else:
        password = prompt_password(confirm=True)
    if not password:
        raise SystemExit("Password obbligatoria")
    users[user_key]["password"] = hash_password(password)
    save_users(users)
    print(f"Password aggiornata per '{users[user_key].get('display', user_key)}'")


def command_delete(args: argparse.Namespace) -> None:
    users = load_users()
    username = args.username.strip()
    user_key = find_user_key(users, username)
    if not user_key:
        raise SystemExit("Utente non trovato")
    if not args.force:
        answer = input(f"Confermi l'eliminazione di '{users[user_key].get('display', user_key)}'? [y/N] ")
        if answer.lower() not in {"y", "yes"}:
            print("Operazione annullata")
            return
    users.pop(user_key)
    save_users(users)
    print(f"Utente '{username}' eliminato")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gestione utenti JobLog")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="Elenca gli utenti registrati")
    list_parser.set_defaults(func=command_list)

    create_parser = subparsers.add_parser("create", help="Crea un nuovo utente")
    create_parser.add_argument("username", help="Username per l'accesso")
    create_parser.add_argument("--name", help="Nome completo da mostrare")
    create_parser.add_argument("--password", help="Password in chiaro (sconsigliato)")
    create_parser.set_defaults(func=command_create)

    password_parser = subparsers.add_parser("set-password", help="Aggiorna la password di un utente")
    password_parser.add_argument("username", help="Username esistente")
    password_parser.add_argument("--password", help="Nuova password in chiaro (sconsigliato)")
    password_parser.set_defaults(func=command_set_password)

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
