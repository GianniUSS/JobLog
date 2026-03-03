"""Utility script per azzerare il database JobLOG e ricaricare i dati demo.

Esempio di utilizzo:

    E:/Progetti/JOBLogApp/.venv-1/Scripts/python.exe reset_db.py

Lo script sfrutta le stesse routine dell'app Flask, così il comportamento è
identico al reload effettuato dall'interfaccia.
"""

from __future__ import annotations

import argparse
import sqlite3
from contextlib import contextmanager
from typing import Iterator

from app import (
    DB_VENDOR,
    DATABASE,
    DATABASE_SETTINGS,
    MySQLConnection,
    init_db,
    seed_demo_data,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reset del database JobLOG")
    parser.add_argument(
        "--skip-seed",
        action="store_true",
        help="Limita il reset alla sola ricreazione dello schema senza caricare i dati demo",
    )
    return parser.parse_args()


@contextmanager
def open_database() -> Iterator:
    if DB_VENDOR == "mysql":
        db = MySQLConnection(DATABASE_SETTINGS)
    else:
        db = sqlite3.connect(DATABASE)
    try:
        yield db
    finally:
        db.close()


def reset_database(skip_seed: bool = False) -> None:
    print("[JobLOG] Inizializzo lo schema...")
    init_db()

    if skip_seed:
        print("[JobLOG] Schema aggiornato. Seed demo saltato per richiesta utente.")
        return

    print("[JobLOG] Carico il progetto demo...")
    with open_database() as db:
        seed_demo_data(db)
        db.commit()
    print("[JobLOG] Database ripristinato correttamente.")


if __name__ == "__main__":
    args = parse_args()
    reset_database(skip_seed=args.skip_seed)
