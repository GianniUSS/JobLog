"""Client helper for interacting with the Rentman REST API."""
from __future__ import annotations

import os
import logging
from typing import Any, Dict, Iterable, List, Optional

import requests

logger = logging.getLogger(__name__)

API_BASE_URL = "https://api.rentman.net"
DEFAULT_TIMEOUT = 20
MAX_LIMIT = 300
_CHUNK_SIZE = 40


class RentmanError(Exception):
    """Base error for Rentman client issues."""


class RentmanAuthError(RentmanError):
    """Raised when authentication or configuration fails."""


class RentmanNotFound(RentmanError):
    """Raised when a requested resource does not exist."""


class RentmanAPIError(RentmanError):
    """Raised for non-success responses from the Rentman API."""


def _chunked(source: Iterable[Any], size: int = _CHUNK_SIZE) -> Iterable[List[Any]]:
    """Yield successive chunks from *source* of length *size*."""

    batch: List[Any] = []
    for item in source:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


class RentmanClient:
    """Lightweight wrapper around the Rentman REST API."""

    def __init__(
        self,
        token: Optional[str] = None,
        *,
        base_url: str = API_BASE_URL,
        timeout: int = DEFAULT_TIMEOUT,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.token = (token or os.environ.get("RENTMAN_API_TOKEN", "")).strip()
        if not self.token:
            raise RentmanAuthError("Rentman API token non configurato")

        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    # ------------------------------------------------------------------
    # Low level HTTP helpers
    # ------------------------------------------------------------------
    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        response = self.session.request(
            method,
            url,
            params=params,
            json=json,
            timeout=self.timeout,
        )

        if response.status_code in (401, 403):
            raise RentmanAuthError("Autenticazione Rentman fallita")
        if response.status_code == 404:
            raise RentmanNotFound(f"Risorsa non trovata: {url}")
        if response.status_code >= 400:
            raise RentmanAPIError(
                f"Errore Rentman {response.status_code}: {response.text.strip()}"
            )

        if response.status_code == 204:
            return {}

        try:
            return response.json()
        except ValueError as exc:  # pragma: no cover - difesa da risposte inattese
            raise RentmanAPIError("Risposta JSON non valida da Rentman") from exc

    def _get_all(self, path: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch an entire collection handling pagination automatically."""

        base_params = dict(params or {})
        limit = int(base_params.get("limit", MAX_LIMIT)) or MAX_LIMIT
        limit = min(limit, MAX_LIMIT)

        items: List[Dict[str, Any]] = []
        offset = int(base_params.get("offset", 0))

        while True:
            page_params = dict(base_params)
            page_params["limit"] = limit
            page_params["offset"] = offset
            payload = self._request("GET", path, params=page_params)
            data = payload.get("data") or []
            items.extend(data)

            if len(data) < limit:
                break
            offset += limit

        return items

    # ------------------------------------------------------------------
    # High level helpers
    # ------------------------------------------------------------------
    def _find_project(self, field: str, value: str) -> Optional[Dict[str, Any]]:
        if not value:
            return None

        payload = self._request(
            "GET",
            "/projects",
            params={field: value, "limit": 1},
        )
        data = payload.get("data") or []
        if data:
            logger.info("Rentman: progetto trovato con %s=%s", field, value)
            return data[0]

        logger.info("Rentman: nessun match %s=%s", field, value)
        return None

    def get_project_by_number(self, number: str) -> Optional[Dict[str, Any]]:
        return self._find_project("number", number)

    def get_project_by_reference(self, reference: str) -> Optional[Dict[str, Any]]:
        return self._find_project("reference", reference)

    def find_project(self, code: str) -> Optional[Dict[str, Any]]:
        slug = (code or "").strip()
        if not slug:
            return None

        candidates = []
        candidates.append(("number", slug))
        if slug.isdigit():
            normalized = str(int(slug))
            if normalized != slug:
                candidates.append(("number", normalized))
        candidates.append(("reference", slug))

        for field, value in candidates:
            try:
                project = self._find_project(field, value)
            except RentmanNotFound:
                logger.info("Rentman: risorsa non trovata per %s=%s", field, value)
                continue
            if project:
                return project
        return None

    def get_project_functions(self, project_id: int) -> List[Dict[str, Any]]:
        reference = f"/projects/{project_id}"
        return self._get_all("/projectfunctions", {"project": reference})

    def get_project_subprojects(self, project_id: int) -> List[Dict[str, Any]]:
        reference = f"/projects/{project_id}"
        return self._get_all("/subprojects", {"project": reference})

    def get_project_crew_by_function_ids(self, function_ids: Iterable[int]) -> List[Dict[str, Any]]:
        refs = [f"/projectfunctions/{fid}" for fid in function_ids if fid is not None]
        if not refs:
            return []

        records: List[Dict[str, Any]] = []
        for reference in refs:
            records.extend(
                self._get_all(
                    "/projectcrew",
                    {"function": reference},
                )
            )
        return records

    def get_crew_members_by_ids(self, crew_ids: Iterable[int]) -> List[Dict[str, Any]]:
        ids = [str(cid) for cid in crew_ids if cid is not None]
        if not ids:
            return []

        records: List[Dict[str, Any]] = []
        for crew_id in ids:
            records.extend(
                self._get_all(
                    "/crew",
                    {"id": crew_id},
                )
            )
        return records