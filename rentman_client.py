"""Client helper for interacting with the Rentman REST API."""
from __future__ import annotations

import json
import logging
import os
from datetime import date as date_cls, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

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
            config_path = Path(__file__).with_name("config.json")
            if config_path.exists():
                try:
                    with config_path.open("r", encoding="utf-8") as handle:
                        payload = json.load(handle)
                    if isinstance(payload, dict):
                        raw_token = payload.get("rentman_api_token")
                        if isinstance(raw_token, str):
                            self.token = raw_token.strip()
                except (json.JSONDecodeError, OSError):
                    logger.debug("Rentman: impossibile leggere config.json per il token", exc_info=True)
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

    def iter_collection(
        self,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        limit_total: int = 2000,
    ) -> Iterator[Dict[str, Any]]:
        """Iterate a collection endpoint, paging until *limit_total* items are yielded."""

        base_params = dict(params or {})
        limit = int(base_params.get("limit", MAX_LIMIT)) or MAX_LIMIT
        limit = min(limit, MAX_LIMIT)

        offset = int(base_params.get("offset", 0))
        yielded = 0

        while True:
            if yielded >= limit_total:
                break

            page_params = dict(base_params)
            page_params["limit"] = limit
            page_params["offset"] = offset
            payload = self._request("GET", path, params=page_params)
            data = payload.get("data") or []
            if not data:
                break

            for entry in data:
                if not isinstance(entry, dict):
                    continue
                yield entry
                yielded += 1
                if yielded >= limit_total:
                    break

            if len(data) < limit:
                break
            offset += limit

    def iter_projects(self, *, limit_total: int = 2000, params: Optional[Dict[str, Any]] = None) -> Iterator[Dict[str, Any]]:
        return self.iter_collection("/projects", params=params, limit_total=limit_total)

    def get_project_statuses(self) -> List[Dict[str, Any]]:
        """Best-effort fetch of project statuses."""
        try:
            return self._get_all("/projectstatuses")
        except RentmanNotFound:
            pass
        except RentmanAuthError as exc:
            logger.debug("Rentman: accesso projectstatuses negato (%s)", exc)
        except RentmanAPIError as exc:
            logger.debug("Rentman: projectstatuses non disponibili (%s)", exc)

        try:
            return self._get_all("/statuses")
        except RentmanNotFound:
            return []
        except RentmanAuthError as exc:
            logger.debug("Rentman: accesso statuses negato (%s)", exc)
            return []
        except RentmanAPIError as exc:
            logger.debug("Rentman: statuses non disponibili (%s)", exc)
            return []

    def get_projects(self, *, limit_total: int = 2000, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        return list(self.iter_projects(limit_total=limit_total, params=params))

    def fetch_active_projects(
        self,
        *,
        date: str,
        statuses: Optional[Iterable[str]] = None,
        limit_total: int = 500,
    ) -> List[Dict[str, Any]]:
        """Fetch a reduced project selection around *date* and optionally filter by custom status codes."""

        date_iso = (date or "").strip()
        if not date_iso:
            raise ValueError("date richiesto")

        target_date: Optional[datetime] = None
        try:
            target_date = datetime.fromisoformat(date_iso)
        except ValueError:
            target_date = None

        status_codes = {
            str(code).strip()
            for code in (statuses or [])
            if code is not None and str(code).strip()
        }

        def parse_date(value: Any) -> Optional[date_cls]:
            if isinstance(value, str):
                candidate = value.strip()
                if len(candidate) < 8:
                    return None
                normalized = candidate.replace(" ", "T").replace("Z", "+00:00")
                # Gestione rapida dei separatori tipo "YYYY/MM/DD"
                if "T" not in normalized and "+" not in normalized and "-" not in normalized and ":" not in normalized:
                    normalized = candidate.replace("/", "-")
                try:
                    dt_value = datetime.fromisoformat(normalized)
                except ValueError:
                    try:
                        dt_value = datetime.fromisoformat(f"{normalized}T00:00:00")
                    except ValueError:
                        try:
                            dt_value = datetime.strptime(candidate, "%d/%m/%Y")
                        except ValueError:
                            try:
                                dt_value = datetime.strptime(candidate, "%Y/%m/%d")
                            except ValueError:
                                return None
                return dt_value.date()
            return None

        target_day: Optional[date_cls] = target_date.date() if target_date is not None else None

        attempts: List[Dict[str, Any]] = []
        if target_date is not None:
            window_start = (target_date - timedelta(days=365)).date().isoformat()
            attempts.append({"limit": MAX_LIMIT, "modified[gte]": window_start})
        attempts.append({"limit": MAX_LIMIT})

        collected: List[Dict[str, Any]] = []
        seen_ids: set[Any] = set()

        for attempt_params in attempts:
            remaining = limit_total - len(collected)
            if remaining <= 0:
                break

            params = dict(attempt_params)
            base_limit = int(params.get("limit", MAX_LIMIT)) or MAX_LIMIT
            params["limit"] = max(20, min(base_limit, remaining, 120))

            for project in self.iter_projects(limit_total=remaining, params=params):
                if not isinstance(project, dict):
                    continue

                project_id = project.get("id")
                if project_id in seen_ids:
                    continue
                seen_ids.add(project_id)

                if status_codes:
                    custom_payload = project.get("custom")
                    status_value: Optional[str] = None
                    if isinstance(custom_payload, dict):
                        raw_custom = custom_payload.get("custom_6")
                        if isinstance(raw_custom, str):
                            status_value = raw_custom.strip()
                        elif isinstance(raw_custom, (int, float)):
                            status_value = str(int(raw_custom))
                    if status_value is None:
                        current_value = project.get("current")
                        if isinstance(current_value, (int, float)):
                            status_value = str(int(current_value))
                        elif isinstance(current_value, str) and current_value.strip():
                            status_value = current_value.strip()
                    if status_value is None or status_value not in status_codes:
                        continue

                if target_day is not None:
                    start_dates: List[date_cls] = []
                    end_dates: List[date_cls] = []

                    for key in (
                        "equipment_period_from",
                        "usageperiod_start",
                        "planperiod_start",
                        "projectperiod_start",
                        "period_start",
                        "start",
                    ):
                        parsed = parse_date(project.get(key))
                        if parsed is not None:
                            start_dates.append(parsed)

                    for key in (
                        "equipment_period_to",
                        "usageperiod_end",
                        "planperiod_end",
                        "projectperiod_end",
                        "period_end",
                        "end",
                    ):
                        parsed = parse_date(project.get(key))
                        if parsed is not None:
                            end_dates.append(parsed)

                    custom_payload = project.get("custom") if isinstance(project.get("custom"), dict) else None
                    if custom_payload:
                        custom_single = custom_payload.get("custom_13") or custom_payload.get("custom_41")
                        single_date = parse_date(custom_single)
                        if single_date is not None and not start_dates and not end_dates:
                            start_dates.append(single_date)
                            end_dates.append(single_date)

                        custom_range = custom_payload.get("custom_38") or custom_payload.get("custom_40")
                        if isinstance(custom_range, str) and custom_range.strip() and not start_dates and not end_dates:
                            tokens = [token.strip() for token in custom_range.replace("\\", "/").split("/") if token.strip()]
                            if tokens:
                                first = parse_date(tokens[0])
                                last = parse_date(tokens[-1])
                                if first is not None:
                                    start_dates.append(first)
                                if last is not None:
                                    end_dates.append(last)

                    start_day = min(start_dates) if start_dates else None
                    end_day = max(end_dates) if end_dates else None

                    if start_day is None and end_day is None:
                        continue
                    if start_day is None:
                        start_day = end_day
                    if end_day is None:
                        end_day = start_day
                    if start_day is None or end_day is None:
                        continue
                    if end_day < start_day:
                        start_day, end_day = end_day, start_day
                    if not (start_day <= target_day <= end_day):
                        continue

                collected.append(project)

        return collected

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

    def get_project_planned_equipment(self, project_id: int) -> List[Dict[str, Any]]:
        """Return the equipment planned on a project (materials list)."""

        logger.info("Rentman: recupero materiali pianificati per progetto %s", project_id)
        try:
            items = self._get_all(f"/projects/{project_id}/projectequipment")
            logger.info("Rentman: projectequipment custom link -> %s righe", len(items))
            return items
        except RentmanNotFound:
            logger.info(
                "Rentman: endpoint /projects/%s/projectequipment non disponibile, provo fallback /projectequipment",
                project_id,
            )
        except RentmanAPIError as exc:
            logger.warning(
                "Rentman: errore %s leggendo /projects/%s/projectequipment",
                exc,
                project_id,
            )

        project_ref = f"/projects/{project_id}"
        try:
            fallback_items = self._get_all("/projectequipment", {"project": project_ref})
            logger.info("Rentman: fallback /projectequipment -> %s righe", len(fallback_items))
            return fallback_items
        except RentmanNotFound:
            logger.info("Rentman: nessun materiale per progetto %s", project_id)
        except RentmanAPIError as exc:
            logger.warning(
                "Rentman: errore %s leggendo /projectequipment?project=%s",
                exc,
                project_ref,
            )
        return []

    def get_project_files(self, project_id: int, *, exhaustive: bool = True) -> List[Dict[str, Any]]:
        collected: List[Dict[str, Any]] = []
        seen_ids: set[int | str] = set()

        def collect(items: Optional[List[Dict[str, Any]]], origin: str) -> None:
            if not items:
                logger.info("Rentman: nessun file da %s", origin)
                return
            added = 0
            for entry in items:
                if not isinstance(entry, dict):
                    continue
                file_id = entry.get("id")
                if file_id in seen_ids:
                    continue
                seen_ids.add(file_id)
                collected.append(entry)
                added += 1
            logger.info("Rentman: %s -> aggiunti %s nuovi file (tot=%s)", origin, added, len(collected))

        logger.info("Rentman: recupero allegati via /projects/%s/files", project_id)
        try:
            collect(self._get_all(f"/projects/{project_id}/files"), "/projects/{id}/files")
        except RentmanNotFound:
            logger.info("Rentman: endpoint /projects/%s/files non disponibile (404)", project_id)
        except RentmanAPIError as exc:
            logger.warning("Rentman: errore %s leggendo /projects/%s/files", exc, project_id)

        if not exhaustive:
            if collected:
                logger.info("Rentman: allegati base raccolti=%s (modalità light)", len(collected))
            else:
                logger.info("Rentman: nessun allegato in modalità light per progetto %s", project_id)
            return collected

        def fetch_files(params: Dict[str, Any], tag: str) -> None:
            try:
                items = self._get_all("/files", params)
            except RentmanNotFound:
                logger.info("Rentman: /files%s non disponibile", tag)
                return
            except RentmanAPIError as exc:
                logger.warning("Rentman: errore %s leggendo /files%s", exc, tag)
                return
            collect(items, f"/files{tag}")

        project_ref = f"/projects/{project_id}"
        fetch_files({"itemtype": "project", "itemid": project_id}, "?itemtype=project&itemid")
        fetch_files({"item[eq]": project_ref}, "?item[eq]=project")

        # Cartelle specifiche del progetto
        folders = self.get_project_file_folders(project_id)
        for folder in folders:
            folder_id = folder.get("id")
            if not folder_id:
                continue
            fetch_files({"folder[eq]": folder_id}, f"?folder={folder_id}")

        # Allegati collegati ai sottoprogetti
        try:
            subprojects = self.get_project_subprojects(project_id)
        except RentmanError:
            subprojects = []
        for sub in subprojects:
            sub_id = sub.get("id")
            if not sub_id:
                continue
            fetch_files({"item[eq]": f"/subprojects/{sub_id}"}, f"?item=subproject-{sub_id}")

        if collected:
            return collected

        logger.info("Rentman: nessun allegato trovato per progetto %s", project_id)
        return []

    def get_project_file_folders(self, project_id: int) -> List[Dict[str, Any]]:
        logger.info("Rentman: recupero cartelle file per progetto %s", project_id)
        try:
            folders = self._get_all(f"/projects/{project_id}/file_folders")
            logger.info("Rentman: ottenute %s cartelle", len(folders))
            return folders
        except RentmanNotFound:
            logger.info("Rentman: nessuna cartella per progetto %s", project_id)
        except RentmanAPIError as exc:
            logger.warning("Rentman: errore %s leggendo file_folders per %s", exc, project_id)
        return []

    def get_project_equipment_groups(self, project_id: int) -> List[Dict[str, Any]]:
        logger.info("Rentman: recupero gruppi materiali per progetto %s", project_id)
        try:
            groups = self._get_all(f"/projects/{project_id}/projectequipmentgroup")
            logger.info("Rentman: /projects/%s/projectequipmentgroup -> %s gruppi", project_id, len(groups))
            return groups
        except RentmanNotFound:
            logger.info("Rentman: endpoint /projects/%s/projectequipmentgroup non disponibile", project_id)
        except RentmanAPIError as exc:
            logger.warning("Rentman: errore %s leggendo projectequipmentgroup per progetto %s", exc, project_id)

        project_ref = f"/projects/{project_id}"
        try:
            fallback = self._get_all("/projectequipmentgroup", {"project": project_ref})
            logger.info("Rentman: fallback /projectequipmentgroup -> %s gruppi", len(fallback))
            return fallback
        except RentmanNotFound:
            logger.info("Rentman: nessun gruppo materiali per progetto %s", project_id)
        except RentmanAPIError as exc:
            logger.warning("Rentman: errore %s leggendo /projectequipmentgroup?project=%s", exc, project_ref)
        return []

    def get_equipment(self, equipment_id: int) -> Optional[Dict[str, Any]]:
        logger.info("Rentman: recupero dettaglio equipment %s", equipment_id)
        try:
            payload = self._request("GET", f"/equipment/{equipment_id}")
        except RentmanNotFound:
            logger.info("Rentman: equipment %s non trovato", equipment_id)
            return None
        except RentmanAPIError as exc:
            logger.warning("Rentman: errore %s leggendo equipment %s", exc, equipment_id)
            return None

        data = payload.get("data") if isinstance(payload, dict) else None
        if data:
            return data
        logger.info("Rentman: equipment %s senza payload dati", equipment_id)
        return None

    def get_file(self, file_id: int) -> Optional[Dict[str, Any]]:
        logger.info("Rentman: recupero dettaglio file %s", file_id)
        try:
            payload = self._request("GET", f"/files/{file_id}")
        except RentmanNotFound:
            logger.info("Rentman: file %s non trovato", file_id)
            return None
        except RentmanAPIError as exc:
            logger.warning("Rentman: errore %s leggendo file %s", exc, file_id)
            return None

        data = payload.get("data") if isinstance(payload, dict) else None
        if data:
            return data
        logger.info("Rentman: file %s senza payload dati", file_id)
        return None

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

    def get_project_function_groups(self, project_id: int) -> List[Dict[str, Any]]:
        """Recupera i gruppi di funzioni (fasi) del progetto via /projects/{id}/projectfunctiongroups."""
        logger.info("Rentman: recupero function groups (fasi) per progetto %s", project_id)
        try:
            items = self._get_all(f"/projects/{project_id}/projectfunctiongroups")
            logger.info("Rentman: /projects/%s/projectfunctiongroups -> %s gruppi", project_id, len(items))
            return items
        except RentmanNotFound:
            logger.info("Rentman: nessun function group per progetto %s", project_id)
            return []
        except RentmanAPIError as exc:
            logger.warning("Rentman: errore %s leggendo /projects/%s/projectfunctiongroups", exc, project_id)
            return []

    def get_project_crew(self, project_id: int) -> List[Dict[str, Any]]:
        """Recupera TUTTA la crew pianificata del progetto via /projects/{id}/projectcrew."""
        logger.info("Rentman: recupero crew pianificata per progetto %s", project_id)
        try:
            items = self._get_all(f"/projects/{project_id}/projectcrew")
            logger.info("Rentman: /projects/%s/projectcrew -> %s record", project_id, len(items))
            return items
        except RentmanNotFound:
            logger.info("Rentman: nessuna crew pianificata per progetto %s", project_id)
            return []
        except RentmanAPIError as exc:
            logger.warning("Rentman: errore %s leggendo /projects/%s/projectcrew", exc, project_id)
            return []

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

    def get_crew_plannings_by_date(self, target_date: str) -> List[Dict[str, Any]]:
        """
        Recupera le pianificazioni crew per una data specifica.
        Restituisce i record di projectcrew che hanno pianificazione attiva in quella data.
        """
        from datetime import datetime, timedelta

        date_iso = (target_date or "").strip()
        if not date_iso:
            raise ValueError("date richiesto")

        try:
            dt = datetime.fromisoformat(date_iso)
        except ValueError:
            raise ValueError(f"Formato data non valido: {date_iso}")

        day_start = dt.replace(hour=0, minute=0, second=0).isoformat()
        day_end = dt.replace(hour=23, minute=59, second=59).isoformat()

        logger.info("Rentman: recupero pianificazioni crew per data %s", date_iso)

        # Recupera projectcrew con planperiod che include la data
        try:
            all_crew = self._get_all(
                "/projectcrew",
                {
                    "planperiod_start[lte]": day_end,
                    "planperiod_end[gte]": day_start,
                }
            )
            logger.info("Rentman: trovate %s pianificazioni crew", len(all_crew))
            return all_crew
        except RentmanNotFound:
            logger.info("Rentman: nessuna pianificazione trovata per %s", date_iso)
            return []
        except RentmanAPIError as exc:
            logger.warning("Rentman: errore %s recuperando pianificazioni", exc)
            return []

    def get_crew_member(self, crew_id: int) -> Optional[Dict[str, Any]]:
        """Recupera i dettagli di un singolo crew member."""
        logger.info("Rentman: recupero dettaglio crew %s", crew_id)
        try:
            payload = self._request("GET", f"/crew/{crew_id}")
        except RentmanNotFound:
            logger.info("Rentman: crew %s non trovato", crew_id)
            return None
        except RentmanAPIError as exc:
            logger.warning("Rentman: errore %s leggendo crew %s", exc, crew_id)
            return None

        data = payload.get("data") if isinstance(payload, dict) else None
        return data

    def get_project_function(self, function_id: int) -> Optional[Dict[str, Any]]:
        """Recupera i dettagli di una funzione progetto."""
        logger.info("Rentman: recupero dettaglio projectfunction %s", function_id)
        try:
            payload = self._request("GET", f"/projectfunctions/{function_id}")
        except RentmanNotFound:
            logger.info("Rentman: projectfunction %s non trovata", function_id)
            return None
        except RentmanAPIError as exc:
            logger.warning("Rentman: errore %s leggendo projectfunction %s", exc, function_id)
            return None

        data = payload.get("data") if isinstance(payload, dict) else None
        return data

    def get_project(self, project_id: int) -> Optional[Dict[str, Any]]:
        """Recupera i dettagli di un progetto."""
        logger.info("Rentman: recupero dettaglio project %s", project_id)
        try:
            payload = self._request("GET", f"/projects/{project_id}")
        except RentmanNotFound:
            logger.info("Rentman: project %s non trovato", project_id)
            return None
        except RentmanAPIError as exc:
            logger.warning("Rentman: errore %s leggendo project %s", exc, project_id)
            return None

        data = payload.get("data") if isinstance(payload, dict) else None
        return data

    def get_subproject(self, subproject_id: int) -> Optional[Dict[str, Any]]:
        """Recupera i dettagli di un subproject."""
        logger.info("Rentman: recupero dettaglio subproject %s", subproject_id)
        try:
            payload = self._request("GET", f"/subprojects/{subproject_id}")
        except RentmanNotFound:
            logger.info("Rentman: subproject %s non trovato", subproject_id)
            return None
        except RentmanAPIError as exc:
            logger.warning("Rentman: errore %s leggendo subproject %s", exc, subproject_id)
            return None

        data = payload.get("data") if isinstance(payload, dict) else None
        return data

    def get_contact(self, contact_id: int) -> Optional[Dict[str, Any]]:
        """Recupera i dettagli di un contatto (location)."""
        logger.info("Rentman: recupero dettaglio contact %s", contact_id)
        try:
            payload = self._request("GET", f"/contacts/{contact_id}")
        except RentmanNotFound:
            logger.info("Rentman: contact %s non trovato", contact_id)
            return None
        except RentmanAPIError as exc:
            logger.warning("Rentman: errore %s leggendo contact %s", exc, contact_id)
            return None

        data = payload.get("data") if isinstance(payload, dict) else None
        return data

    def get_project_vehicles(self, project_id: int) -> List[Dict[str, Any]]:
        """Recupera i veicoli assegnati a un progetto."""
        logger.info("Rentman: recupero veicoli per progetto %s", project_id)
        try:
            items = self._get_all(f"/projects/{project_id}/projectvehicles")
            logger.info("Rentman: trovati %s veicoli per progetto %s", len(items), project_id)
            return items
        except RentmanNotFound:
            logger.info("Rentman: nessun veicolo per progetto %s", project_id)
            return []
        except RentmanAPIError as exc:
            logger.warning("Rentman: errore %s leggendo veicoli progetto %s", exc, project_id)
            return []

    def get_vehicle(self, vehicle_id: int) -> Optional[Dict[str, Any]]:
        """Recupera i dettagli di un veicolo."""
        logger.info("Rentman: recupero dettaglio vehicle %s", vehicle_id)
        try:
            payload = self._request("GET", f"/vehicles/{vehicle_id}")
        except RentmanNotFound:
            logger.info("Rentman: vehicle %s non trovato", vehicle_id)
            return None
        except RentmanAPIError as exc:
            logger.warning("Rentman: errore %s leggendo vehicle %s", exc, vehicle_id)
            return None

        data = payload.get("data") if isinstance(payload, dict) else None
        return data