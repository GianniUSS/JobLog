# JOBLogApp — Handover Document

> **Ultimo aggiornamento:** 17 febbraio 2026  
> **Stack:** Flask 3.0 · Python 3.11 · MySQL 8 · PWA · Vanilla JS  
> **File principale:** `app.py` (~24.000 righe, ~400 funzioni, ~174 route)

---

## 1. Panoramica Progetto

**JOBLogApp** è una PWA aziendale per la gestione delle attività lavorative, integrata con **Rentman** (piattaforma noleggio attrezzature) e **CedolinoWeb** (gestione buste paga).

### Funzionalità principali
- **Timbrature** — Clock-in/out con QR code, GPS o manuale, arrotondamento configurabile
- **Pianificazione Rentman** — Sincronizzazione turni da Rentman, assegnazione veicoli/autisti
- **Gestione richieste** — Ferie, permessi, straordinari, ritardi, rimborsi
- **Timer produzione** — Tracciamento attività in tempo reale per operatori e magazzino
- **Push notifications** — Notifiche real-time via Web Push (VAPID)
- **Cedolino** — Sincronizzazione timbrature con CedolinoWeb per elaborazione buste paga
- **Documenti** — Distribuzione circolari, comunicazioni, buste paga con conferma lettura
- **Report** — Presenze mensili, analisi attività, export Excel

---

## 2. Struttura Progetto

```
JOBLogApp/
├── app.py                          # Backend monolitico Flask (~23.500 righe)
├── rentman_client.py               # Client API Rentman (~787 righe)
├── config.json                     # Configurazione (DB, VAPID, Cedolino, GPS)
├── requirements.txt                # Dipendenze Python
├── users.json / projects.json      # Dati legacy/demo
├── vapid.json                      # Chiavi VAPID per push
├── templates/                      # 32 template Jinja2
│   ├── admin_*.html                # 19 pagine admin
│   ├── user_*.html                 # 7 pagine utente
│   ├── magazzino.html              # Modulo magazzino
│   ├── login.html / index.html     # Auth e homepage
│   └── partials/admin_menu.html    # Menu laterale admin
├── static/
│   ├── sw.js                       # Service Worker PWA (~510 righe)
│   ├── manifest.json               # PWA manifest
│   ├── js/                         # JS modulari (dashboard, magazzino, ecc.)
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

## 4. Database — 33 Tabelle

### Tabelle Core

| Tabella | Scopo |
|---------|-------|
| `app_users` | Utenti (username, password hash, ruolo, gruppo, `cedolino_group_id`) |
| `user_groups` | Gruppi utenti (Produzione, Magazzino, Impiegati, ecc.) |
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
| `warehouse_active_timers` | Timer magazzino/produzione attivi |
| `warehouse_sessions` | Sessioni di lavoro completate |
| `warehouse_activities` | Attività magazzino configurate |
| `warehouse_manual_projects` | Progetti manuali magazzino |
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

---

## 5. Autenticazione e Ruoli

| Ruolo | Permessi |
|-------|----------|
| `user` | Timbrature, richieste personali, visualizzazione turni/documenti |
| `supervisor` | Come admin ma senza gestione utenti/sistema |
| `admin` | Tutto: gestione utenti, gruppi, regole, planning, review richieste |
| `magazzino` | Accesso al modulo magazzino |

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
| Magazzino | 12 | `/api/magazzino/...` | Progetti, attività, sessioni, timer |
| Push | 7 | `/api/push/...` | Subscribe, notifiche, status |
| Timbratura | 7 | `/api/timbratura/...` | Registrazione, validazione QR/GPS |
| Production | 4 | `/api/production/...` | Timer attività produzione, lookup progetto |

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
Verifica extra turno → _detect_extra_turno() → Se extra: _create_auto_extra_turno_request()
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
- Rilevato da `_detect_extra_turno()` quando:
  - Ingresso prima di `turno_start - anticipo_max_minuti`
  - Uscita dopo `turno_end + flessibilità` (in daily mode)
- Creato automaticamente: `_create_auto_extra_turno_request()`
- Value type: `minutes`

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
| `admin_rentman_planning.html` | Pianificazione turni Rentman (~3.420 righe) |
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
| `user_home.html` | Homepage + timbrature (~6.120 righe) |
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
| `magazzino.html` | Modulo magazzino |
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
| `_detect_extra_turno()` | Rileva ingresso anticipato / uscita posticipata |
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
