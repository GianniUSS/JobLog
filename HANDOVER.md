# JOBLogApp — Handover Document

> **Ultimo aggiornamento:** 23 febbraio 2026  
> **Stack:** Flask 3.0 · Python 3.11 · MySQL 8 · PWA · Vanilla JS  
> **File principale:** `app.py` (~25.180 righe, ~395 funzioni, ~163 route)

---

## 1. Panoramica Progetto

**JOBLogApp** è una PWA aziendale per la gestione delle attività lavorative, integrata con **Rentman** (piattaforma noleggio attrezzature) e **CedolinoWeb** (gestione buste paga).

### Funzionalità principali
- **Timbrature** — Clock-in/out con QR code, GPS o manuale, arrotondamento configurabile
- **Pianificazione Rentman** — Sincronizzazione turni da Rentman, assegnazione veicoli/autisti
- **Gestione richieste** — Ferie, permessi, straordinari, ritardi, rimborsi
- **Timer produzione** — Tracciamento attività in tempo reale per operatori produzione
- **Push notifications** — Notifiche real-time via Web Push (VAPID)
- **Cedolino** — Sincronizzazione timbrature con CedolinoWeb per elaborazione buste paga
- **Documenti** — Distribuzione circolari, comunicazioni, buste paga con conferma lettura
- **Report** — Presenze mensili, analisi attività, export Excel

---

## 2. Struttura Progetto

```
JOBLogApp/
├── app.py                          # Backend monolitico Flask (~24.750 righe)
├── rentman_client.py               # Client API Rentman (~787 righe)
├── config.json                     # Configurazione (DB, VAPID, Cedolino, GPS)
├── requirements.txt                # Dipendenze Python
├── users.json / projects.json      # Dati legacy/demo
├── vapid.json                      # Chiavi VAPID per push
├── templates/                      # 31 template Jinja2
│   ├── admin_*.html                # 19 pagine admin
│   ├── user_*.html                 # 7 pagine utente
│   ├── login.html / index.html     # Auth e homepage
│   └── partials/admin_menu.html    # Menu laterale admin
├── static/
│   ├── sw.js                       # Service Worker PWA (~510 righe)
│   ├── manifest.json               # PWA manifest
│   ├── js/                         # JS modulari (dashboard, sessioni, ecc.)
│   ├── icons/                      # Icone PWA (72→512px)
│   └── uploads/                    # File caricati
├── scripts/                        # Script di supporto
└── .flask_session/                 # Sessioni server-side
```

---

## 3. Configurazione (`config.json`)

```json
{
  "rentman_api_token": "JWT_TOKEN",
  "database": {
    "vendor": "mysql",        // "mysql" o "sqlite"
    "host": "localhost",
    "port": 3306,
    "user": "tim_root",
    "password": "gianni225524",
    "name": "joblog"
  },
  "webpush": {
    "vapid_public": "...",
    "vapid_private": "...",
    "subject": "mailto:ops@example.com"
  },
  "cedolino_web": {
    "enabled": true,
    "endpoint": "http://80.211.18.30/WebServices/crea_timbrata_elaborata",
    "retry_interval_minutes": 5,
    "max_retry_attempts": 10
  },
  "timbratura": {
    "qr_enabled": true,
    "gps_enabled": true,
    "gps_locations": [
      { "name": "Sede Principale", "latitude": 45.4642, "longitude": 9.1900, "radius_meters": 100 },
      { "name": "Magazzino di fasano", "latitude": 40.8575, "longitude": 17.3497, "radius_meters": 100 }
    ],
    "gps_max_accuracy_meters": 50
  }
}
```

---

## 4. Database — 36 Tabelle

### Tabelle Core

| Tabella | Scopo |
|---------|-------|
| `app_users` | Utenti (username, password hash, ruolo, gruppo, `cedolino_group_id`) |
| `user_groups` | Gruppi utenti (Produzione, Impiegati, ecc.) |
| `timbrature` | Timbrature registrate (tipo, ora, ora_mod, data, username) |
| `user_requests` | Richieste utenti (ferie, permessi, ritardi, extra turno) |
| `request_types` | Tipologie richieste configurabili (value_type, external_id) |

### Tabelle Rentman

| Tabella | Scopo |
|---------|-------|
| `rentman_plannings` | Pianificazioni turni da Rentman (37+ colonne incl. GPS, pause, `gestione_squadra`) |
| `crew_members` | Membri crew sincronizzati da Rentman |
| `vehicle_driver_assignments` | Assegnazione veicoli ad autisti per progetto/data |
| `location_cache` | Cache coordinate GPS per location Rentman |

### Tabelle Attività/Timer

| Tabella | Scopo |
|---------|-------|
| `activities` | Attività di progetto (label, durata pianificata) |
| `event_log` | Log eventi (move, start, pause, resume, stop) |
| `member_state` | Stato corrente operatori (running, paused, activity) |
| `warehouse_active_timers` | Timer produzione attivi |
| `warehouse_sessions` | Sessioni di lavoro completate |
| `warehouse_activities` | Attività produzione configurate |
| `activity_session_overrides` | Override manuali per sessioni |

### Tabelle Cedolino/Documenti

| Tabella | Scopo |
|---------|-------|
| `cedolino_timbrature` | Timbrature da sincronizzare con CedolinoWeb |
| `user_documents` | Documenti aziendali (circolari, buste paga) |
| `user_documents_read` | Tracciamento lettura documenti |

### Tabelle Push/Sessioni

| Tabella | Scopo |
|---------|-------|
| `push_subscriptions` | Subscription push browser |
| `push_notification_log` | Log notifiche inviate |
| `persistent_sessions` | Cookie "ricordami" |
| `app_state` | Stato globale app (key/value) |

### Tabelle Regole

| Tabella | Scopo |
|---------|-------|
| `group_timbratura_rules` | Regole timbratura per gruppo |
| `timbratura_rules` | Regole timbratura globali (fallback) |
| `company_settings` | Impostazioni azienda |

### Altre

| Tabella | Scopo |
|---------|-------|
| `employee_shifts` | Turni settimanali impiegati non-Rentman |
| `overtime_requests` | Richieste straordinario (legacy) |
| `equipment_checks` | Checklist attrezzature progetto |
| `local_equipment` | Attrezzatura locale |
| `project_materials_cache` | Cache materiali da Rentman |
| `project_photos` | Foto progetto |
| `company_phones` | Registro telefoni aziendali (phone_code PK, label, active) |
| `phone_assignments` | Assegnazioni telefono→operatore→progetto (phone_code, project_code, activity_id, assigned_to) |
| `project_phases_state` | Stato fasi per progetto/funzione (completed, completed_by, date) |

---

## 5. Autenticazione e Ruoli

| Ruolo | Permessi |
|-------|----------|
| `user` | Timbrature, richieste personali, visualizzazione turni/documenti |
| `supervisor` | Come admin ma senza gestione utenti/sistema. Assegnato automaticamente via telefono aziendale (`/login?phone=XXX`) — vedi Sezione 29 |
| `admin` | Tutto: gestione utenti, gruppi, regole, planning, review richieste |

- **Sessioni:** Flask-Session filesystem, 24h lifetime
- **Cookie persistente:** `joblog_auth` (30 giorni), salvato in `persistent_sessions`
- **Helper:** `is_admin_or_supervisor()` per check permessi nelle API

---

## 6. Endpoints API — Categorie

| Categoria | ~Count | Prefisso | Descrizione |
|-----------|--------|----------|-------------|
| Admin API | 66 | `/api/admin/...` | CRUD utenti/gruppi, sessioni, presenze, turni, documenti, richieste |
| Admin Pages | 20 | `/admin/...` | Pagine HTML admin |
| User API | 15 | `/api/user/...` | Turni, timbrature, richieste, documenti, notifiche |
| User Pages | 6 | `/user/...` | Pagine HTML utente |
| Push | 7 | `/api/push/...` | Subscribe, notifiche, status |
| Timbratura | 7 | `/api/timbratura/...` | Registrazione, validazione QR/GPS |
| Production | 5 | `/api/production/...` | Timer attività produzione, lookup progetto, timer attivi |

---

## 7. Sistema Timbrature

### Flusso Completo

```
Utente apre app → Scelta modalità (QR/GPS/Manuale)
    ↓
POST /api/timbratura → Backend determina tipo (inizio_giornata, inizio_pausa, fine_pausa, fine_giornata)
    ↓
calcola_ora_mod() → Arrotondamento in base a regole gruppo
    ↓
Verifica flessibilità → verifica_flessibilita_timbrata()
    ↓
Verifica ritardo → _detect_late_arrival() → Se ritardo: _create_late_arrival_request()
    ↓
Verifica extra turno (blocco inline in POST /api/timbratura) → Se extra: _create_auto_extra_turno_request()
    ↓
Gestione timer produzione → Start/Pause/Resume/Stop automatico
    ↓
Sincronizzazione Cedolino → INSERT in cedolino_timbrature
    ↓
Response al frontend con: timbratura salvata + late_arrival? + production_activity?
```

### Regole Timbratura per Gruppo (`group_timbratura_rules`)

| Campo | Default | Descrizione |
|-------|---------|-------------|
| `rounding_mode` | `single` | `single` = arrotonda singola timbrata, `daily` = arrotonda totale giornaliero |
| `flessibilita_ingresso_minuti` | 30 | Finestra flessibilità ingresso |
| `flessibilita_uscita_minuti` | 30 | Finestra flessibilità uscita |
| `arrotondamento_giornaliero_minuti` | 15 | Blocco arrotondamento (daily mode) |
| `arrotondamento_giornaliero_tipo` | `floor` | floor / ceil / nearest |
| `oltre_flessibilita_action` | `allow` | allow / warn / block |
| `late_threshold_minutes` | 15 | Soglia ritardo (0 = disabilitato) |
| `usa_regole_pausa_standard` | 1 | Usa regole pausa globali |

### Regole Globali (`timbratura_rules`) — Fallback

- `anticipo_max_minuti` — Massimo anticipo consentito
- `tolleranza_ritardo_minuti` — Tolleranza ritardo
- `arrotondamento_ingresso/uscita_minuti/tipo` — Blocchi arrotondamento
- `pausa_blocco_minimo_minuti` — Durata minima pausa
- `pausa_incremento_minuti` — Incremento arrotondamento pausa
- `pausa_tolleranza_minuti` — Tolleranza pausa

---

## 8. Sistema Controllo Ritardi

### Implementazione

1. **Configurazione:** `late_threshold_minutes` nella tabella `group_timbratura_rules` (per gruppo, default 15 min)
2. **Rilevamento:** `_detect_late_arrival()` confronta `ora_timbrata` con `turno_start` + soglia
3. **Creazione automatica:** `_create_late_arrival_request()` inserisce in `user_requests` (request_type_id = 19 "Giustificazione Ritardo")
4. **Notifiche push:** Admin (`_send_late_arrival_notification_to_admins()`) e utente (`_send_late_arrival_notification_to_user()`)
5. **Frontend popup:** `user_home.html` mostra modal per inserire motivazione ritardo
6. **Coda attività:** Se c'è un popup attività produzione, viene accodato (`_pendingProductionActivity`) e mostrato dopo la chiusura del popup ritardo
7. **Review admin:** In `admin_user_requests.html` con dual actions: "Accetta giustificazione" / "Registra ritardo"
8. **Notifica review:** `_build_late_arrival_details()` costruisce dettagli per notifica review

### Request Type ID 19 — Giustificazione Ritardo

```
extra_data JSON: {
  "turno_start": "08:00",
  "ora_timbrata": "08:25",
  "late_minutes": 25,
  "threshold": 15,
  "auto_detected": true
}
```

---

## 9. Extra Turno / Fuori Flessibilità

### Extra Turno (Request Type: "Extra Turno")
- Rilevato nel blocco inline di `POST /api/timbratura` (fine_giornata) quando:
  - Ingresso prima di `turno_start - anticipo_max_minuti`
  - Uscita dopo `turno_end + flessibilità` (in daily mode)
- Creato automaticamente: `_create_auto_extra_turno_request()`
- Value type: `minutes`

> Nota: `_detect_extra_turno()` è mantenuta nel codice per retrocompatibilità/refactor futuro, ma il flusso attuale usa il blocco inline.

### Formula ora_mod (daily mode) — `_calcola_ora_fine_daily()`
```
ore_nette = uscita_reale - ingresso - pausa_pianificata
ore_arrotondate = floor(ore_nette / blocco) * blocco    # es. floor(546/30)*30 = 540
differenza = ore_nette - ore_arrotondate                 # es. 546 - 540 = 6 min
ora_mod = uscita_reale - differenza                      # es. 19:01 - 6 = 18:55
```
- **Pausa**: sempre pianificata (da `employee_shifts`), mai effettiva
- **Blocco straordinario**: `arrotondamento_giornaliero_minuti` (default 30 min)
- **Tipo arrotondamento**: `arrotondamento_giornaliero_tipo` (floor/ceil/nearest)

### Dati Extra Turno (`extra_data` JSON)
```json
{
  "planned_start": "09:00", "planned_end": "18:00",
  "actual_start": "19:01", "ora_mod": "18:55",
  "worked_minutes": 546, "planned_minutes": 480,
  "extra_minutes_lordo": 66, "blocco_straordinario_minuti": 30,
  "tipo_arrotondamento": "floor", "differenza_minuti": 6,
  "pausa_effettiva_minuti": 54, "pausa_pianificata_minuti": 60,
  "break_confirmed": true, "break_skipped": false, "break_skip_reason": null
}
```

### Visualizzazione Extra Turno

| Dove | Cosa mostra | Logica status |
|------|------------|---------------|
| **Home storico** (Fine Giornata) | Ora + nota stato | Pending: "⏳ Sarà 18:55 se approvato"; Approved: ora barrata + ora_mod; Rejected: ora originale |
| **Storico timbrature** (Uscita) | Ora condizionale | Pending/rejected: ora originale; Approved: ora_mod |
| **Storico timbrature** (Calendario) | ⏳ icona | Pending: clessidra, ore ridotte |
| **Storico timbrature** (TOTALE GIORNATA) | Ore senza extra | Pending/rejected: extra escluso dal totale |
| **Admin richieste** | Griglia box dettagliata | Straordinario lordo, blocco, conteggiato, differenza, pausa |

### Fuori Flessibilità (Request Type ID 17)
- Hardcoded type_id = 17
- Creato da `_create_flex_request()` quando la timbrata è fuori flessibilità e `oltre_flessibilita_action = 'allow'`

---

## 10. Gestione Squadra (`gestione_squadra`)

Sistema per distinguere tra gestione attività a squadre e attività individuale, **indipendente** dalla presenza di un leader.

### Logica

| `gestione_squadra` | Comportamento |
|----|------|
| `0` (default) | **Attività individuale**: ogni operatore sceglie la propria attività al clock-in (popup produzione) |
| `1` | **Gestione squadra**: il capo squadra gestisce le attività per tutti → skip popup individuale |

### Implementazione

- **DB:** Colonna `gestione_squadra TINYINT(1) DEFAULT 0` in `rentman_plannings`
- **API:** `POST /api/admin/rentman/planning/toggle-gestione-squadra` — aggiorna TUTTE le righe dello stesso progetto+data
- **UI:** Toggle badge nel project header in `admin_rentman_planning.html`
  - `👥 Gestione squadra` (blu, attivo) / `👤 Attività individuale` (grigio)
- **Backend:** La logica in `POST /api/timbratura` legge `gestione_squadra` dal turno dell'utente:
  - Se `gestione_squadra = 1` → non mostra popup attività
  - Se `gestione_squadra = 0` → mostra popup attività individuale

### Nota importante
Il campo `is_leader` nella tabella `rentman_plannings` è **read-only** (viene da Rentman) e indica solo chi è il capo squadra. NON implica automaticamente che ci sia una gestione a squadre delle attività — per questo c'è il campo separato `gestione_squadra`.

---

## 11. Timer Produzione

### Architettura
- **Tabella:** `warehouse_active_timers` — un timer attivo per utente
- **Campi:** `project_code`, `project_name`, `activity_label`, `running`, `paused`, `start_ts`, `elapsed_ms`, `pause_start_ts`
- **Integrazione timbratura:** automatico start/pause/resume/stop al clock-in/out

### Flow
```
Timbratura inizio_giornata → popup attività (se gestione_squadra=0 e utente non è leader)
    ↓
Utente sceglie attività → POST /api/production/timer/start
    ↓
Timer running → tracking elapsed_ms in tempo reale
    ↓
Timbratura inizio_pausa → _pause_production_timer()
    ↓
Timbratura fine_pausa → _resume_production_timer()
    ↓
Timbratura fine_giornata → _stop_production_timer() → sessione salvata in warehouse_sessions
```

### API
- `POST /api/production/timer/start` — Avvia timer con project_code + activity_label
- `POST /api/production/timer/switch` — Cambia attività (ferma precedente, avvia nuova)
- `GET /api/production/timer` — Stato corrente timer

---

## 12. Integrazione Rentman

### Client (`rentman_client.py`)
Wrapper per l'API REST Rentman (`https://api.rentman.net`).

**Metodi principali:**
- `fetch_active_projects()` — Progetti attivi per range date
- `get_crew_plannings_by_date()` — Pianificazioni crew per data
- `get_crew_member/members_by_ids()` — Dettagli membri crew
- `get_project_functions/subprojects/equipment()` — Risorse progetto
- `iter_projects()` — Iteratore con paginazione

### Sincronizzazione Turni
- Admin page: `/admin/rentman-planning` → click "Sync"
- `GET /api/admin/rentman-planning?date=YYYY-MM-DD` — Fetch turni da Rentman API
- `POST /api/admin/rentman-planning/save` — Salva nel DB locale (merge dati)
- `POST /api/admin/rentman-planning/send` — Invia turni agli operatori + notifica push
- **Merge logic:** Dati Rentman + campi custom DB (GPS mode, pause, gestione_squadra)

### Tabella `rentman_plannings` — Colonne principali

| Colonna | Fonte | Descrizione |
|---------|-------|-------------|
| `rentman_id` | Rentman | ID univoco pianificazione |
| `crew_id/name` | Rentman | Operatore assegnato |
| `project_id/name/code` | Rentman | Progetto |
| `function_name` | Rentman | Funzione/ruolo operatore |
| `plan_start/end` | Rentman | Orario pianificato |
| `is_leader` | Rentman | Capo squadra (read-only) |
| `location_*` | Rentman + DB | Coordinate location |
| `timbratura_gps_mode` | DB | `group` o `location` |
| `break_start/end/minutes` | DB | Pausa configurata admin |
| `gestione_squadra` | DB | Gestione attività a squadre |
| `vehicle_data` | Rentman + DB | Veicoli + assegnazione autisti |
| `sent_to_webservice` | DB | Turno inviato agli operatori |
| `is_obsolete` | DB | Turno rimosso da Rentman |

---

## 13. Integrazione CedolinoWeb

### Flusso
```
Timbratura registrata → INSERT in cedolino_timbrature
    ↓
Sync immediata → POST a CedolinoWeb endpoint
    ↓
Se fallisce → retry automatico (_cedolino_retry_worker, ogni 5 min, max 10 tentativi)
```

### Timeframe IDs
| ID | Tipo |
|----|------|
| 1 | Inizio giornata |
| 4 | Inizio pausa |
| 5 | Fine pausa |
| 8 | Fine giornata |

### Config
- Endpoint: `http://80.211.18.30/WebServices/crea_timbrata_elaborata`
- Codice terminale: `musa_mobile`
- Collegamento con `overtime_request_id` per bloccare sync fino a review admin

---

## 14. Push Notifications

### Setup
- **Libreria:** `pywebpush` con protocollo VAPID
- **Service Worker:** `static/sw.js` (~510 righe)
- **Chiavi VAPID:** configurate in `config.json` sezione `webpush`

### API
| Endpoint | Metodo | Scopo |
|----------|--------|-------|
| `/api/push/subscribe` | POST | Registra subscription browser |
| `/api/push/unsubscribe` | POST | Rimuovi subscription |
| `/api/push/status` | GET | Stato subscription utente |
| `/api/push/test` | POST | Invia notifica test |
| `/api/push/notifications` | GET | Lista notifiche utente |
| `/api/push/notifications/read` | POST | Marca letta |
| `/api/push/notifications/read-all` | POST | Marca tutte lette |

### Tipi Notifica
- `overdue_activity` — Attività scaduta
- `long_running_member` — Operatore attivo da lungo tempo
- `late_arrival_request` — Ritardo rilevato
- `flex_request` — Fuori flessibilità
- `overtime_review` — Review straordinario
- `document_notification` — Nuovo documento
- `planning_notification` — Turno assegnato

### Background Worker
`_notification_worker()` — Thread che gira ogni 60 secondi:
- Controlla attività scadute
- Verifica operatori attivi da troppo tempo
- Invia push notification automatiche

---

## 15. PWA

### Service Worker (`static/sw.js`)
- **Cache strategy:** Network-first con fallback a cache
- **Offline queue:** IndexedDB + Background Sync per POST offline
- **Cache separate:** static, dynamic, API
- **Auto-update:** Gestione SW update con skip-waiting

### Manifest
- Display: `standalone`
- Orientamento: `portrait`
- Tema: `#0ea5e9` (sky blue)
- Background: `#1e293b` (dark)
- Icone: 8 dimensioni (72→512px) PNG + SVG

---

## 16. GPS e Location

### Modalità GPS per timbratura
| Modalità | Descrizione |
|----------|-------------|
| `group` | Valida posizione rispetto alla sede del gruppo (da `config.json`) |
| `location` | Valida posizione rispetto alla location del progetto Rentman |

### Validazione
- `POST /api/timbratura/validate-gps` — Verifica posizione utente
- Calcolo distanza Haversine rispetto al punto configurato
- Soglia: `gps_max_accuracy_meters` (default 50m) + `radius_meters` della location

### Geocoding
- `geocode_address()` usa Nominatim (OpenStreetMap)
- Rate limiting + cache in-memory
- Cache persistente in tabella `location_cache`

### Admin Locations
- `/admin/locations` — CRUD sedi GPS
- Ogni location: nome, latitudine, longitudine, raggio

---

## 17. Templates

### Admin (19 pagine)

| Template | Scopo |
|----------|-------|
| `admin_dashboard.html` | Dashboard principale con overview |
| `admin_sessions.html` | Report sessioni attività |
| `admin_presenze.html` | Report presenze mensili + export |
| `admin_activity_analysis.html` | Analisi attività con grafici |
| `admin_rentman_planning.html` | Pianificazione turni Rentman (~4.350 righe) |
| `admin_user_requests.html` | Revisione richieste utenti (~2.000 righe) |
| `admin_users.html` | Gestione utenti |
| `admin_groups.html` | Gestione gruppi |
| `admin_operators.html` | Operatori (sync Rentman) |
| `admin_employee_shifts.html` | Turni impiegati |
| `admin_locations.html` | Sedi GPS |
| `admin_timbratura_rules.html` | Regole timbratura globali |
| `admin_group_timbratura_rules.html` | Regole timbratura per gruppo |
| `admin_request_types.html` | Tipologie richieste |
| `admin_overtime.html` | Gestione straordinari |
| `admin_documents.html` | Documenti aziendali |
| `admin_group_planning.html` | Pianificazione per gruppo |
| `admin_company_settings.html` | Impostazioni azienda |
| `admin.html` | Pagina admin legacy |

### Utente (7 pagine)

| Template | Scopo |
|----------|-------|
| `user_home.html` | Homepage + timbrature (~8.090 righe) |
| `user_requests.html` | Richieste personali |
| `user_turni.html` | Visualizzazione turni |
| `user_notifications.html` | Notifiche push |
| `user_storico_timbrature.html` | Storico timbrature |
| `user_documents.html` | Documenti ricevuti |
| `user_overtime.html` | Straordinari |

### Altro

| Template | Scopo |
|----------|-------|
| `login.html` | Pagina login |
| `index.html` | Redirect/landing |
| `qr_timbratura.html` | Visualizzazione QR code |

---

## 18. Funzioni Helper Critiche

### Timbratura e Arrotondamento
| Funzione | Scopo |
|----------|-------|
| `calcola_ora_mod()` | Calcola ora arrotondata (single mode: a blocchi, daily mode: solo verifica flex) |
| `calcola_pausa_mod()` | Calcola durata pausa arrotondata (blocco minimo + incrementi) |
| `_calcola_ora_fine_daily()` | Calcola ora fine in daily mode (ore lorde - pausa → arrotondamento) |
| `calcola_ore_giornaliere_arrotondate()` | Calcola ore giornaliere arrotondate |
| `verifica_flessibilita_timbrata()` | Verifica se timbrata è dentro flessibilità |
| `get_user_timbratura_rules()` | Recupera regole gruppo → fallback globali |

### Rilevamento Automatico
| Funzione | Scopo |
|----------|-------|
| `_detect_late_arrival()` | Rileva ritardo rispetto a `turno_start + late_threshold` |
| `_create_late_arrival_request()` | Crea richiesta Giustificazione Ritardo + notifiche |
| `_detect_extra_turno()` | Helper legacy (non usata nel flusso principale attuale) |
| `_create_auto_extra_turno_request()` | Crea richiesta Extra Turno automatica |
| `_create_flex_request()` | Crea richiesta Fuori Flessibilità (type_id=17) |

### Notifiche
| Funzione | Scopo |
|----------|-------|
| `_send_late_arrival_notification_to_admins()` | Push agli admin per ritardo |
| `_send_late_arrival_notification_to_user()` | Push all'utente per ritardo |
| `_build_late_arrival_details()` | Costruisce dettagli ritardo per la review |
| `_send_request_review_notification()` | Notifica utente su esito review |

### Timer Produzione
| Funzione | Scopo |
|----------|-------|
| `_start_production_timer()` | Avvia timer (INSERT/UPDATE `warehouse_active_timers`) |
| `_pause_production_timer()` | Mette in pausa timer |
| `_resume_production_timer()` | Riprende timer |
| `_stop_production_timer()` | Ferma timer e salva sessione |

---

## 19. Background Threads

| Thread | Funzione | Intervallo | Scopo |
|--------|----------|------------|-------|
| Notification Worker | `_notification_worker()` | 60s | Controlla attività scadute, operatori attivi, invio push |
| Cedolino Retry | `_cedolino_retry_worker()` | 300s (5min) | Ritenta sync timbrature fallite con CedolinoWeb |

---

## 20. Data di Simulazione

Il sistema supporta una data simulata per test:
- `SIMULATED_DATE` — Variabile globale
- `get_simulated_now()` / `get_simulated_today()` — Funzioni helper
- API: `POST/GET/DELETE /api/admin/simulated-date`

---

## 21. Script di Utilità

| Script | Scopo |
|--------|-------|
| `manage_users.py` | Gestione utenti da CLI |
| `rentman_client.py` | Client Rentman standalone |
| `check_*.py` | ~40 script di debug/verifica per vari aspetti |
| `fix_*.py` | ~10 script di fix per dati corrotti |
| `debug_*.py` | ~10 script di debug specifici |
| `cleanup_*.py` | Script pulizia dati |
| `migrate_cedolino_timbrature.py` | Migrazione tabella cedolino |

---

## 22. Dipendenze (`requirements.txt`)

| Pacchetto | Versione | Scopo |
|-----------|----------|-------|
| Flask | 3.0.3 | Web framework |
| requests | 2.31.0 | HTTP client (Rentman API) |
| openpyxl | 3.1.2 | Export Excel |
| PyMySQL | 1.1.0 | MySQL connector |
| pywebpush | 2.1.2 | Push notifications VAPID |
| Flask-Session | 0.5.0 | Server-side sessions |
| qrcode[pil] | 8.0 | Generazione QR codes |
| Pillow | >=10.0.0 | Image processing |
| python-dateutil | >=2.8.2 | Date parsing |

---

## 23. Note Operative

### Avvio Applicazione
```powershell
cd E:\Progetti\JOBLogApp
python app.py
```
Flask serve su `http://localhost:5000` (default).

### Problema Noto: Processi Multipli
Flask con reloader può generare processi duplicati. Se le modifiche non si applicano:
```powershell
taskkill /F /IM python.exe
python app.py
```

### Database — Migrazioni Automatiche
Le migrazioni sono integrate in `ensure_*_table()`. Alla prima richiesta che tocca una tabella, le colonne mancanti vengono aggiunte automaticamente via `ALTER TABLE`.

### Cache PWA
Il Service Worker può cachare versioni vecchie del frontend. Per forzare il refresh:
1. DevTools → Application → Service Workers → Update
2. Oppure: incrementare la versione nel manifest/SW

### Utenti di Test
- **donato** — Operatore produzione (crew_id=1923, group_id=7 Produzione)
- **admin** — Amministratore sistema

---

## 24. Architettura Dati — Flusso Request Types

```
Richiesta Automatica (ritardo/extra turno/flex)
    ↓
user_requests.status = 'pending'
    ↓
Push notification → Admin
    ↓
Admin review in admin_user_requests.html
    ↓
Azione: approve/reject (per ritardo: "Accetta giustificazione" / "Registra ritardo")
    ↓
Push notification → Utente con esito
    ↓
(Se approvato) → Eventuale sync con CedolinoWeb
```

### Request IDs Hardcoded nel Codice
| ID | Nome | Note |
|----|------|------|
| 17 | Fuori Flessibilità | Hardcoded in `_create_flex_request()` |
| 19 | Giustificazione Ritardo | Creato da `_ensure_late_arrival_request_type()` |
| — | Extra Turno | ID dinamico, trovato via query `name = 'Extra Turno'` |

---

## 25. Modifiche Recenti (Febbraio 2026)

### Modale Cambia Attività Fullscreen — COMPLETATO (15/02)
- Modale a schermo intero per cambio attività produzione
- **Numpad cambio progetto**: tastierino numerico per inserire codice progetto manuale
- **Lookup Rentman**: `GET /api/production/project-lookup?code=XXXX` cerca progetto via API Rentman
- **Note obbligatorie**: attività "Altro" richiede descrizione obbligatoria (popup note)
- **Card IN CORSO**: mostra progetto e note attività corrente
- **Card Progetto Pianificato**: nascosta automaticamente se uguale all'attività in corso
- **Card Progetto Manuale**: appare dopo selezione da numpad con proprio pulsante "Cambia Attività"

### Storico Timbrature Accordion — COMPLETATO (15/02)
- Barra fissa in basso alla home timbrature (blu, `position: fixed`)
- Al tocco si espande verso l'alto mostrando le timbrature del giorno
- Animazione CSS smooth con `max-height` transition

### Fix Navigazione PWA — COMPLETATO (15/02)
- Ricarica automatica `timbraturaConfig` + dati su:
  - `pageshow` (bfcache restore)
  - `visibilitychange` (tab focus)
- Ordine caricamento: config PRIMA, poi timbrature (per `is_production_group`)

### Sistema Controllo Ritardi — COMPLETATO
- Rilevamento automatico alla timbratura
- Popup frontend con campo motivazione
- Notifiche push dettagliate (admin + utente)
- Review admin con dual actions
- Home page con stato ritardo (3 colori: pending/approved/rejected)
- Coda attività produzione dopo popup ritardo (`_pendingProductionActivity`)

### Gestione Squadra — COMPLETATO
- Campo `gestione_squadra` in `rentman_plannings`
- Toggle UI in pianificazione admin
- Logica backend: `gestione_squadra=1` → skip popup attività individuale
- Decoupled da `is_leader` (che è read-only da Rentman)

### Extra Turno: Formula e Visualizzazione — COMPLETATO (17/02)
- **Formula ora_mod corretta**: `ora_mod = uscita_reale - differenza` (dove `differenza = ore_nette - ore_arrotondate`)
- **Pausa pianificata**: il calcolo usa sempre la pausa pianificata (da `employee_shifts`), non quella effettiva
- **Admin griglia box**: straordinario lordo, blocco (30 min per difetto), conteggiato, differenza detratta, pausa — in box separati (non inline)
- **Admin dettagli pausa**: mostra se confermata, timbrata, o non effettuata con motivo
- **Storico TOTALE GIORNATA**: esclude Extra Turno se pending o rifiutato
- **Storico Calendario**: icona ⏳ per giorni con Extra Turno in attesa
- **Storico Uscita**: mostra ora originale se pending/rifiutato, ora_mod se approvato, con nota "Sarà X:XX se approvato"
- **Home storico Fine Giornata**: ora di uscita condizionale allo stato Extra Turno:
  - Pending → ora originale + nota arancione "⏳ Sarà 18:55 se approvato"
  - Approvato → ora barrata + ora_mod + badge verde "✅ Confermato"
  - Rifiutato → ora originale + badge rosso "❌ Extra rifiutato"
- **Backend API `/api/timbratura/oggi`**: restituisce Extra Turno con qualsiasi stato (non solo pending), include `ora_mod` da `extra_data`
- **Retrocompatibilità**: richieste vecchie senza nuovi campi mostrano solo "Extra: +Xh Xm"
- Commits: `5ace3d4`, `40e9886`, `f8c3f8c`, `160a43b`, `75d908b`, `7d79039`

### Fix Critici Timbratura (17/02) — COMPLETATO
- **Fix bloccante Extra Turno**: risolto `NameError: name 'user_rules' is not defined` nel blocco `POST /api/timbratura` durante `fine_giornata`.
  - Fix: uso di `rules_for_extra` al posto di `user_rules` per `arrotondamento_giornaliero_minuti/tipo`.
  - Impatto: prima il calcolo extra risultava positivo nei log ma la richiesta non veniva creata (`extra_turno_data=None` in risposta).
- **Produzione senza flessibilità (hardening)**:
  - In `get_user_timbratura_rules()` per gruppi `is_production=1` la flessibilità viene forzata a `0` (ingresso/uscita), indipendentemente dai valori salvati.
  - I trigger flex ingresso/uscita restano comunque skippati per produzione (difesa su più livelli).
- **Notifica push Flex coerente**:
  - Titolo push differenziato per `inizio_giornata` vs `fine_giornata` (non più sempre "Richiesta anticipo ingresso").
- **Anti-duplicato richieste automatiche**:
  - Aggiunto controllo su `user_requests` (stato `pending`, stesso utente/data/tipo) in:
    - `_create_flex_request()`
    - `_create_auto_extra_turno_request()`
    - `_create_late_arrival_request()`

### Nota Operativa (Debug locale)
- Gli script di debug che importano `mysql.connector` richiedono `mysql-connector-python` installato nel venv locale (`.venv-1`).

---

## 26. Modifiche Recenti (19 febbraio 2026)

### Deroga Pausa Ridotta — Fix completo pipeline daily mode — COMPLETATO (19/02)

#### Problema
Con la `rounding_mode = 'daily'` e una Deroga Pausa Ridotta approvata (pausa timbrata 29 min invece dei 60 pianificati), il sistema:
- Calcolava `fine_giornata.ora_mod` usando `effective_break_minutes=29` invece di `rounded_break_minutes=30`
- Il frontend mostrava l'uscita raw (18:14) al posto dell'`ora_mod` (18:00) in tutti i widget
- Lo storico calcolava ore nette con la pausa pianificata (60 min) invece di quella approvata (30 min)
- Il turno del giorno sbagliato (sempre Lunedì) veniva usato per la pausa pianificata in storico

#### Fix 1 — `_process_break_reduction_review()` (`app.py` ~riga 19947)
```python
rounded_break = int(extra.get("rounded_break_minutes", 0) or 0)
# Se approved: usa pausa arrotondata (rounded_break) → ora_mod fine_giornata corretta
# Fallback su effective_break se rounded_break non disponibile (record vecchi)
forced_break = (rounded_break or effective_break) if status == "approved" else planned_break
```
- **Prima**: con `effective_break=29` → ore_nette=555 → extra=75 → blocco→60 → diff=15 → `ora_mod=17:59`
- **Dopo**: con `rounded_break=30` → ore_nette=554 → extra=74 → blocco→60 → diff=14 → `ora_mod=18:00` ✓

#### Fix 2 — Storico per-day shift (`app.py` ~riga 23067)
- Sostituita la query `ORDER BY day_of_week ASC LIMIT 1` (prendeva sempre Lunedì) con un dizionario `shifts_by_dow = {day_of_week: shift}` popolato da `SELECT ... ORDER BY day_of_week ASC`
- Per ogni giorno del mese: `day_of_week = datetime.strptime(data, '%Y-%m-%d').weekday()` → `day_shift = shifts_by_dow.get(day_of_week)`
- Corregge il calcolo di `pausa_turno_minuti` e `turno_inizio/fine` per ogni giorno

#### Fix 3 — Storico calcolo ore con deroga approvata (`app.py` ~riga 23198)
```python
br_day = break_reduction_by_date.get(data)
if br_day and br_day.get('status') == 'approved' and pausa_minuti > 0:
    pausa_per_calcolo = pausa_minuti          # 30 min (da ora_mod fine_pausa)
else:
    pausa_per_calcolo = pausa_turno_minuti if pausa_minuti > 0 else 0  # 60 min pianificata
```

#### Fix 4 — Template `user_storico_timbrature.html` — Uscita in TIMBRATURE CONFERMATE
- `brApproved` ora definito in cima (prima del blocco `fineT`), condiviso da tutti i sotto-blocchi
- Uscita usa `fineT.ora_mod` quando `hasExtraApproved || brApproved` (non solo extra turno)
- Ore lorde calcolate con `uscita = fineT.ora_mod` quando `hasExtraApproved || brApproved`
- Pausa: label `'Pausa (deroga approvata)'` + nota `pausa pianificata: 1:00` se deroga attiva
- Ore nette: usa `calcRiepilogo.ore_nette_minuti` dal backend (coerente con il TOTALE)

#### Fix 5 — Template `user_home.html` — Widget "Storico Timbrature Oggi"
- Fine Giornata in `daily mode` ora mostra `ora` barrata + `ora_mod` verde anche se non c'è `pendingOvertime`
- Aggiunto check `brApprovedToday`: cerca `allTimbrature.find(x => x.tipo === 'fine_pausa' && x.break_reduction_request?.status === 'approved')`
- Se `brApprovedToday` → uscita = `18:14` (barrata) + `18:00` (verde)

#### Fix 6 — Script DB una-tantum `_fix_fg_oramod.py`
- Script eseguito per aggiornare i record già approvati con la vecchia logica
- Legge `effective_break_minutes` da `extra_data`, calcola `rounded_break = ceil(effective / blocco)`
- Ricalcola `ora_mod` per `fine_giornata` e aggiorna DB
- Esito: `giannipi 2026-02-19` → `fine_giornata.ora_mod = 18:00:00` ✓

### Extra Turno = Anticipo — Nessuna richiesta ET creata — COMPLETATO (19/02)

#### Problema
Quando `anticipo == extra_turno_minutes` (ingresso anticipato copre esattamente lo straordinario), veniva comunque creata una richiesta Extra Turno e compariva il badge "Extra Turno in attesa" nel widget storico.

#### Fix (`app.py` ~riga 6213)
Spostato il check anticipo **PRIMA** della chiamata a `_create_auto_extra_turno_request()`:
```python
# Legge flessibilità dal DB
flex_row_pre = db.execute(...)  # anticipo approvato per utente/data
if flex_row_pre and abs(flex_val_pre - extra_minutes) < 1:
    extra_turno_skip_motivation = True
    app.logger.info("Extra Turno SKIP: anticipo == extra → nessuna richiesta ET creata")

# Crea ET solo se NON già coperta dall'anticipo
if not extra_turno_skip_motivation:
    extra_turno_request_id = _create_auto_extra_turno_request(...)
```

### Formula `ora_mod` (daily mode) — Sezione aggiornata

> Integra quanto in Sezione 9 (Extra Turno)

```
# Con deroga pausa ridotta approvata:
ore_nette = uscita_reale - ingresso - rounded_break    # es. 18:14 - 08:30 - 30 = 554
ore_arrotondate = turno_base + floor((ore_nette - turno_base) / blocco) * blocco
                = 480 + floor(74/30)*30 = 480+60 = 540
differenza = ore_nette - ore_arrotondate = 554 - 540 = 14
ora_mod = uscita_reale - differenza = 18:14 - 14 = 18:00 ✓

# Senza deroga (pausa standard):
ore_nette = 18:14 - 08:30 - 60 = 524   → ora_mod = 17:59 (o simile)
```

### Note Operative Aggiunte
- **Processi multipli Flask** (porta 5000): verificare con `netstat -ano | findstr "LISTENING" | findstr ":5000"`, killare i PIDs duplicati prima di riavviare
- **Service Worker**: il SW aggiornato rimane in `waiting` finché non si fa `skipWaiting` o si chiude il tab. Usare DevTools → Application → Service Workers → `skipWaiting` + hard refresh
- **Record DB esistenti**: il fix `_fix_fg_oramod.py` ricalcola `fine_giornata.ora_mod` per deroghe già approvate. Da eseguire una-tantum dopo ogni deploy che modifica la logica `_process_break_reduction_review`

---

## 27. Modifiche Recenti (20 febbraio 2026)

### Mancata Timbratura per Gruppi Produzione — COMPLETATO (20/02)

#### Descrizione
Operatori dei gruppi produzione (`is_production=1`) possono dichiarare una "mancata timbratura" quando non hanno timbrato in tempo. La funzionalità sblocca immediatamente il timeframe e mostra il popup di selezione attività produzione.

#### Flusso
```
Utente clicca "Mancata Timbratura" nel menu timbratura
    ↓
Popup con: tipo (ingresso/pausa/fine_pausa/uscita), ora, motivazione obbligatoria
    ↓
POST /api/user/requests → request_type_id=13, value_type='timbratura'
    ↓
Backend: INSERT in timbrature (status='pending_review', method='manual_request')
    ↓
calcola_ora_mod() applicata → arrotondamento (es. 07:13 → 07:15)
    ↓
Sblocco immediato timeframe (la timbratura è registrata subito)
    ↓
Se inizio_giornata e is_production_group → redirect con popup attività produzione
    ↓
Timbratura_ts per attività produzione usa ora_mod (non ora reale)
    ↓
Badge "In attesa di revisione" nello storico timbrature
    ↓
Admin approva/rifiuta → _process_approved_mancata_timbratura()
```

#### Arrotondamento
L'ora dichiarata viene arrotondata tramite `calcola_ora_mod()`:
- **Turno source**: `employee_shifts` (day_of_week) → fallback `rentman_plannings` (crew_id, planning_date)
- **Regole**: `get_user_timbratura_rules(db, username)` → anticipo_max, tolleranza_ritardo, arrotondamento 15min
- Applicato sia alla registrazione iniziale sia all'approvazione (`_process_approved_mancata_timbratura()`)
- CedolinoWeb riceve `ora_mod` calcolata (non l'ora dichiarata dall'utente)

#### Request Type ID 13 — Mancata Timbratura
```json
extra_data: {
  "tipo_timbratura": "ingresso",
  "ora_timbratura": "07:13",
  "motivazione": "Dimenticato di timbrare"
}
```

#### Dettagli tecnici
- **Status timbratura**: `pending_review` finché admin non approva/rifiuta
- **Badge storico**: giallo "⏳ In attesa di revisione" / verde "✅ Approvata" / rosso "❌ Rifiutata"
- **Approvazione**: `_process_approved_mancata_timbratura()` aggiorna `timbrature.status='approved'` e sincronizza con CedolinoWeb
- **Rifiuto**: elimina la timbratura registrata
- **Redirect dopo invio**: `/?from_mancata_timbratura=1` + sessionStorage per popup attività

### Rimozione Pagina Magazzino — COMPLETATO (20/02)

#### Cosa è stato rimosso
- **File eliminati**: `templates/magazzino.html`, `static/js/magazzino.js`, `MAGAZZINO_IMPROVEMENTS.md`
- **Route**: `/magazzino` (pagina) + tutti gli endpoint `/api/magazzino/*` (~726 righe)
- **Endpoint admin**: `/api/admin/magazzino/summary`
- **Costanti**: `ROLE_MAGAZZINO`, `WAREHOUSE_MANUAL_PROJECTS_TABLE_*`
- **Funzioni**: `_magazzino_only()` (guard), `ensure_warehouse_manual_projects_table()`
- **Ruolo**: "magazzino" rimosso da `VALID_USER_ROLES`, login session, role parsing
- **Template**: `magazzino_enabled` rimosso da tutti i `render_template`, nav link rimosso da 6 template utente
- **Admin UI**: opzione ruolo "Magazzino" rimossa da admin_users, filtro sorgente da admin_dashboard/sessions, modulo da admin_company_settings

#### Cosa è stato MANTENUTO (usato dai gruppi produzione)
- **Tabelle**: `warehouse_active_timers`, `warehouse_sessions`, `warehouse_activities`
- **Helper**: `_start_production_timer()`, `_pause_production_timer()`, `_resume_production_timer()`, `_stop_production_timer()`
- **API**: `/api/production/timer/start`, `/api/production/timer/switch`, `/api/production/timer`, `/api/production/project-lookup`
- **Admin views**: Sessioni admin mostrano dati produzione (etichetta rinominata "Magazzino" → "Produzione")

### Fasi Funzione (Function Phases) — COMPLETATO (20/02)

#### Descrizione
Sistema di fasi lavorative configurabili per funzione (es. "Montaggio" ha fasi: Carico, Scarico, Montaggio struttura...). Le fasi appaiono nel popup di selezione attività sulla home operatore e sono visibili nella pagina pianificazione admin.

#### Architettura
- **Storage**: `company_settings.custom_settings.function_phases` (JSON in MySQL)
- **Matching**: EXACT only (case-insensitive) — es. "montaggio Teli Masseria Eccellenza" NON matcha "Montaggio"
- **Stato fasi progetto**: tabella `project_phases_state` (project_key, function_key, phase_name, completed, completed_by, date)

#### Configurazione fasi
- **Dove**: pagina pianificazione admin (`admin_rentman_planning.html`) — click sul nome funzione nella colonna FUNZIONE
- **Modal**: mostra la singola funzione cliccata, con fasi riordinabili (▲/▼) e aggiungibili/rimuovibili
- **Salvataggio**: `POST /api/admin/function-phases` — merge con config esistente (non sovrascrive altre funzioni)
- **API lettura**: `GET /api/admin/function-phases` — restituisce tutte le configurazioni

#### Funzioni configurate
| Funzione | Fasi |
|----------|------|
| Montaggio | Carico mezzo, Scarico mezzo in location, Montaggio struttura, Finalizzazione montaggio, Trasporto verso location |
| Allestimento | Carico Mezzo, Trasporto in location, Scarico Mezzo, Allestimento Fase 1, Allestimento Fase 2, Rientro in magazzino |
| Servizio di Pulizie | Trasporto in location, Pulizia Bagni, Pulizie Uffici, Rientro in sede |
| montaggio Teli Masseria Eccellenza | Carico, Trasporto in location, Scarico, Montaggio teli, Rientro in magazzino |

#### Home operatore — Popup selezione fase
- **Trigger**: "Inizio Giornata" e "Cambia Attività" (se la funzione del turno ha fasi configurate)
- **Funzione**: `showPhaseSelectionPopup()` (async) — carica fasi + stato timer corrente
- **Fase attiva**: mostrata come "IN CORSO" con pallino pulsante, bloccata (non cliccabile)
- **"Extra attività"**: pulsante nel popup per inserire attività libera con testo personalizzato
- **Chiusura fase precedente**: quando si cambia attività, la fase precedente viene chiusa automaticamente via `POST /api/production/timer/toggle`
- **Race condition fix**: `_loadTurnoPhasesPromise` traccia il caricamento asincrono, `showProductionActivityPopup` aspetta il completamento
- **Fasi NON mostrate** nella box home (solo nei popup)

#### Pianificazione admin — Visualizzazione fasi
- **Badge fasi**: mostrati sotto ogni header progetto, raggruppati per funzione
- **Stato**: icone ✅ (completata), 🔵 (operatori attivi), ⬜ (non iniziata)
- **Non cliccabili**: i badge sono `<span>` read-only, senza `onclick`
- **Conteggio operatori attivi**: quando un operatore ha un timer attivo su una fase, appare un badge blu con pallino pulsante e il numero di operatori (tooltip mostra i nomi)
- **Barra progresso**: mostra percentuale fasi completate per funzione

#### API timer attivi
- **Endpoint**: `GET /api/production/active-timers` — restituisce tutti i timer attivi (`warehouse_active_timers WHERE running=1`) raggruppati per `project_code` → `activity_label` → `[username, ...]`
- **Auto-refresh**: ogni 30 secondi la pagina planning richiama l'API e aggiorna i badge se i dati sono cambiati
- **Frontend**: `loadActiveTimers()` salva in `activeTimersData`, `getOperatorsOnPhase(projectKey, phaseName)` cerca operatori per fase (con fallback case-insensitive)

#### File modificati
- `app.py`: endpoint `GET /api/production/active-timers`, `get_phases_for_function()` (exact-only)
- `admin_rentman_planning.html`: `buildPhasesHTML()`, `loadActiveTimers()`, `getOperatorsOnPhase()`, CSS `.phase-operators`, `.phase-active`, `.phase-active-dot`, `@keyframes pulseDot`, auto-refresh 30s
- `user_home.html`: `showPhaseSelectionPopup()`, `selectPhaseForActivity()`, `confirmExtraActivity()`, `loadTurnoPhases()`, `_loadTurnoPhasesPromise`

---

## 28. Modifiche Recenti (23 febbraio 2026)

### Flusso Modale "Cambia Attività" — Revisione completa UX — COMPLETATO (23/02)

#### Descrizione
Rivisitazione completa del flusso di cambio attività/progetto nella modale fullscreen. L'ordine è invertito: prima si sceglie l'attività, poi il progetto (e non viceversa). Dopo aver selezionato un progetto, si vedono **solo** le attività senza card superflue.

#### Modifiche al flusso

1. **Attività prima del progetto**: `openSwitchActivityModal()` mostra direttamente la griglia attività (`showSwitchActivityGrid('planned')`) invece di aprire il numpad
2. **Numpad rimosso dalla griglia**: `gridActivityClick()` non apre più il numpad — usa direttamente il progetto già disponibile (`_switchOverrideProject || _switchGridProject || _switchCurrentProject`) e chiama `confirmSwitchActivity()` immediatamente. Numpad solo come fallback se nessun progetto è disponibile
3. **Attività subito dopo progetto**: `numpadConfirm()` Caso 3 chiama `showSwitchActivityGrid('manual')` direttamente (senza mostrare la card progetto manuale)
4. **Sblocco attività su cambio progetto**: `_rebuildActivityGrid(unlockAll)` rigenera la griglia dinamicamente sbloccando tutte le attività quando il progetto è diverso da quello in corso
5. **Vista pulita**: dopo selezione progetto da numpad, le card superiori (IN CORSO, Pianificato, Cambia Progetto) vengono nascoste — visibile solo header progetto + griglia attività

#### Funzioni modificate (`user_home.html`)

| Funzione | Modifica |
|----------|----------|
| `openSwitchActivityModal()` | Mostra griglia attività direttamente, non numpad |
| `gridActivityClick()` | Usa progetto disponibile → `confirmSwitchActivity()` senza numpad |
| `showSwitchActivityGrid(source)` | Nasconde `switchTopCards` per source='manual', chiama `_rebuildActivityGrid()` |
| `_rebuildActivityGrid(unlockAll)` | **NUOVA** — rigenera HTML griglia, sblocca tutte le attività se `unlockAll=true`, mostra header "PROGETTO SELEZIONATO P. XXXX" |
| `hideSwitchActivityGrid()` | Ripristina visibilità `switchTopCards` |
| `numpadConfirm()` Caso 3 | Va diretto a `showSwitchActivityGrid('manual')` + toast |
| `confirmStartProductionActivity()` | Se `projectCode` esiste → avvia direttamente, numpad solo se manca progetto |

#### Elementi HTML aggiunti
- `<div id="switchTopCards">`: wrapper per pulsante "Cambia Progetto" + card IN CORSO + card Pianificato
- Header "PROGETTO SELEZIONATO" in `_rebuildActivityGrid()` con codice e nome progetto

#### Variabili di stato
| Variabile | Scopo |
|-----------|-------|
| `_switchOverrideProject` | Progetto selezionato manualmente da numpad `{code, name}` |
| `_switchCurrentProject` | Progetto del timer attivo `{code, name}` |
| `_switchGridProject` | Progetto contestuale per la griglia `{code, name}` |
| `_pendingGridActivity` | Attività in attesa di progetto (usata solo come fallback) |

### Pulsante "Cambia Progetto" ingrandito — COMPLETATO (23/02)
- Padding: `16px 20px`, font-size: `16px`, border-radius: `12px`
- Box-shadow più pronunciato: `0 3px 10px rgba(59,130,246,0.35)`

### Chip "Sedi" e "Info turno" — Redesign omogeneo — COMPLETATO (23/02)

#### Problema
I chip "Sedi" e "Info turno" nella card turno avevano uno stile diverso (sfondo più scuro, bordi più spessi, font-weight diverso) rispetto alle righe orario/funzione/location sopra.

#### Fix
- **CSS `.action-chips`**: layout cambiato da `flex` a `grid` con `grid-template-columns: 1fr 1fr`, `margin-top: 0`
- **CSS `.action-chip`**: allineato allo stile di `.turno-detail` — stessi `background: rgba(255,255,255,0.15)`, `backdrop-filter: blur(10px)`, `border-radius: 6px`, `border: 1px solid rgba(255,255,255,0.25)`, `font-size: 14px`, `font-weight: 600`
- **Aggiunto**: stato `:active` con `background: rgba(255,255,255,0.25)`, icone in `<span class="icon">`
- **HTML**: `turno-details` div ora ha `id="turnoDetailsGrid"`, i chip vengono appendati all'interno della griglia dei dettagli (non più come elemento separato sotto `timbraturaEl`)
- **Risultato**: chip visivamente identici alle righe informative sopra, integrati nella stessa griglia

### Fix `plannedBreakMinutes` non definita — COMPLETATO (23/02)

#### Problema
Premendo "Fine Giornata" compariva `ReferenceError: plannedBreakMinutes is not defined` nella funzione `checkBreakBeforeEndDay()` (riga ~3341 di `user_home.html`), bloccando il flusso.

#### Causa
La variabile `plannedBreakMinutes` era definita solo all'interno di `checkBreakReduction()` (scope locale), ma veniva usata anche in `checkBreakBeforeEndDay()` senza essere dichiarata lì.

#### Fix (`user_home.html` ~riga 3296)
Aggiunto calcolo locale di `plannedBreakMinutes` dentro `checkBreakBeforeEndDay()`:
```javascript
let plannedBreakMinutes = 0;
if (turnoConPausa.break_start && turnoConPausa.break_end) {
    const bs = parseTimeToMinutes(turnoConPausa.break_start);
    const be = parseTimeToMinutes(turnoConPausa.break_end);
    if (bs != null && be != null && be > bs) plannedBreakMinutes = be - bs;
} else {
    plannedBreakMinutes = Number(turnoConPausa.break_minutes || 0) || 0;
}
```

### Active Timers: Matching fasi individuali nella pianificazione — COMPLETATO (23/02)

#### Problema
Con gestione singola (non squadra), quando un operatore avviava un timer su una fase (es. "Carico Mezzo"), nella pagina pianificazione admin il chip della fase non mostrava il pallino blu/operatori attivi.

#### Causa
I timer individuali (`warehouse_active_timers`) salvano solo `activity_label` (nome fase, es. "Carico Mezzo"), senza la funzione. La pagina planning cercava la chiave composta `"Allestimento::Carico Mezzo"` in `activeTimersData`, che conteneva solo la chiave semplice `"Carico Mezzo"` → nessun match → nessun badge attivo.

#### Fix 1 — Backend (`app.py` — API `/api/production/active-timers`)
Aggiunta generazione automatica di chiavi composte `funzione::fase` per i timer individuali:
```python
fn_phases_cfg = get_function_phases_config(db)
for pc, labels in list(result.items()):
    for al, usernames in list(labels.items()):
        if '::' in al: continue  # già compound
        for func_key, tmpl in fn_phases_cfg.items():
            for ph in tmpl.get('phases', []):
                if ph.get('name','').lower().strip() == al.lower().strip():
                    compound_key = f"{func_key}::{al}"
                    # aggiungi compound key
```

#### Fix 2 — Frontend (`admin_rentman_planning.html` — `getOperatorsOnPhase()`)
Aggiunto fallback sulla chiave semplice (solo `phaseName`) quando la chiave composta non trova match:
```javascript
// Fallback: prova chiave semplice anche con functionKey
const simpleOps = projectTimers[phaseName];
if (simpleOps && simpleOps.length) return simpleOps;
```

### Auto-completamento fase su Fine Giornata — COMPLETATO (24/02)

#### Problema
Quando un operatore faceva "Fine Giornata" con un timer attivo su una fase, il timer veniva fermato e la sessione salvata, ma la fase non veniva mai marcata come completata nella tabella `project_phase_progress`. Questo causava che nella pagina pianificazione la fase restasse ⬜ (non completata) anche se l'operatore l'aveva effettivamente svolta.

#### Causa
Il completamento fase avveniva solo nel frontend `selectPhaseForActivity()` quando si **cambiava** attività (switch), non quando si fermava il timer (fine giornata).

#### Fix — Backend (`app.py` — funzione `_auto_complete_phase_on_stop()`)
Nuova funzione helper chiamata automaticamente da `_stop_production_timer()`:
- Cerca `activity_label` nelle fasi configurate (`get_function_phases_config`)
- Se trovata, fa UPSERT in `project_phase_progress` con `completed=1`
- Logica: match case-insensitive tra `activity_label` e nomi fasi di tutte le funzioni
- Chiamata in un try/except per non bloccare lo stop del timer in caso di errore

```python
# In _stop_production_timer(), dopo il salvataggio sessione:
try:
    _auto_complete_phase_on_stop(db, username, proj_code, activity)
except Exception as phase_err:
    app.logger.warning(f"Errore auto-completamento fase: {phase_err}")
```

### Gestione Squadra: Skip popup attività per operatori — COMPLETATO (24/02)

#### Problema
Operatori con turno in `gestione_squadra=1` vedevano comunque il popup di selezione attività/fasi dopo "Inizio Giornata", quando l'attività dovrebbe essere gestita esclusivamente dal capo squadra.

#### Fix — Backend (`app.py` ~riga 7089)
- Il loop sui turni odierni salta i turni con `gestione_squadra=1` (e utente non-leader) per la selezione di `turno_per_popup`
- Aggiunto flag `all_gestione_squadra`: se tutti i turni sono gestione_squadra, non genera `production_activity` nella risposta
- L'`else` finale (nessun `turno_per_popup`) genera il popup manuale solo se `not all_gestione_squadra`

#### Fix — Frontend (`user_home.html` — `showProductionActivityPopup()`)
Guard aggiuntivo come difesa in profondità:
```javascript
const isGestioneSquadra = allTurni.length > 0 && allTurni.every(t => t.gestione_squadra && !t.is_leader);
if (isGestioneSquadra) {
    showToast('✓ Timbratura registrata', 'success');
    return;
}
```

---

## 29. Telefoni Aziendali e Modalità Supervisor

> Feature pre-esistente, documentata qui per completezza.

### Descrizione
Sistema per assegnare telefoni aziendali ai caposquadra. Quando un operatore accede all'app tramite il link del telefono assegnato (`/login?phone=XXX`), viene promosso a **supervisor** con dashboard operativa dedicata, filtrata per il progetto e la funzione associati al telefono.

### Tabelle

| Tabella | Colonne principali | Scopo |
|---------|-------------------|-------|
| `company_phones` | `phone_code` (PK, VARCHAR 3), `label`, `active` | Registro telefoni aziendali |
| `phone_assignments` | `phone_code`, `project_code`, `activity_id`, `assigned_to`, `assigned_username`, `assigned_at`, `released_at` | Assegnazioni attive telefono→operatore→progetto |

### Flusso Assegnazione (Admin)

```
Admin pianificazione → header progetto → 📱 "Assegna telefono"
    ↓
Modal: griglia telefoni (liberi/occupati) + lista operatori del progetto
    ↓
POST /api/phones/assign → { phone_code, project_code, assigned_to, activity_id }
    ↓
Rilascio eventuale assegnazione precedente + INSERT nuova assegnazione
```

- **Dove**: `admin_rentman_planning.html` — pulsante "📱 Assegna telefono" nell'header progetto (solo se `gestione_squadra=1`)
- **Anche**: `index.html` (dashboard supervisor) — modal "📱 Assegna Telefono"

### Flusso Login Telefono

```
Operatore apre /login?phone=XXX → phone_code salvato in localStorage
    ↓
Login con credenziali → POST /api/login con phone_code
    ↓
Backend: cerca assegnazione attiva per phone_code in phone_assignments
    ↓
Verifica che assigned_username == utente loggato (o match fuzzy _resolve_crew_username)
    ↓
Se match → session: is_supervisor=True, user_role='supervisor',
           supervisor_project_code, supervisor_activity_id
    ↓
Redirect a index.html (dashboard supervisor) invece di user_home.html
```

### API

| Endpoint | Metodo | Scopo |
|----------|--------|-------|
| `GET /api/phones` | GET | Lista telefoni con stato assegnazione corrente |
| `POST /api/phones/assign` | POST | Assegna telefono a operatore per progetto |
| `POST /api/phones/release` | POST | Rilascia telefono (termina assegnazione) |
| `GET /api/phones/my-assignment` | GET | Assegnazione corrente dell'utente loggato |

### Sessione Supervisor
| Chiave sessione | Scopo |
|----------------|-------|
| `phone_code` | Codice telefono usato per login |
| `is_supervisor` | `True` — promuove a ruolo supervisor |
| `user_role` | `'supervisor'` — determina routing e permessi |
| `supervisor_project_code` | Progetto associato al telefono |
| `supervisor_activity_id` | Activity ID per filtrare funzione (opzionale) |

### Dashboard Supervisor (`index.html`)
- Mostra la stessa interfaccia del dashboard admin, ma filtrata per il progetto assegnato
- Se `supervisor_activity_id` è presente: team e attività filtrate per quella funzione
- `phone_mode` disabilita alcuni elementi di navigazione (menu ridotto)
- Variabili JS: `window.__SAVED_SUPERVISOR_PROJECT__`, `window.__PHONE_MODE__`

### Telefoni di Default
La funzione `seed_default_phones(db)` inserisce automaticamente i telefoni predefiniti se la tabella è vuota.

### Note
- Il link telefono persiste via `localStorage` (`preinstall_phone`): anche dopo l'installazione PWA il phone_code viene recuperato
- Se l'utente non corrisponde all'assegnazione, il login procede come utente normale (senza promozione supervisor)
- Il badge 📱 appare accanto al nome dell'operatore in pianificazione quando ha un telefono assegnato
