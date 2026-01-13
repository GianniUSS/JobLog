TODO
>Le richieste  di tipo permesso  (da migliorare)
>Chiusra automatica  quando non timbrano la chiusura 
>Menu Magazzino velocizzare l'avvio
>Menu storico timbraturee per mese  daa creare 
>Gestione timbratura manuale con processo di approvazione
>aadmin/dashboard problemi su inizio/fine  (pausa )
>admin/activity-analysis da sistemare e migliorare 



# JobLOG (Starter)

## Requisiti
- Python 3.10+
- pip


## Setup
```bash
python -m venv .venv
source .venv/bin/activate  # su Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

L'applicazione espone l'interfaccia su `http://localhost:5000`. In modalità debug il reloader è disattivato: riavvia `python app.py` dopo ogni modifica server-side.

## Installazione rapida su iPhone
> 💡 Questa sezione è pensata per gli utenti finali: condividila in onboarding o nei materiali di training.

1. Apri **Safari** e visita l'URL pubblico dell'app (es. `https://joblog.example.com`).
2. Tocca il pulsante **Condividi** (icona con il quadrato e la freccia) → scorri e scegli **Aggiungi a Home**.
3. Conferma il nome suggerito (es. *JobLog*) e premi **Aggiungi**: l'app apparirà tra le altre icone, in modalità full-screen.
4. Al primo avvio tappa **Consenti** quando Safari chiede il permesso per le **Notifiche push** e per l'utilizzo dello spazio di archiviazione offline.
5. Se l'app segnala "Aggiorna disponibile", tira giù per ricaricare oppure apri il menu ⋮ → **Ricarica** così da installare l'ultima release.

Suggerimenti rapidi:
- Per avere l'esperienza migliore assicurati che l'iPhone sia aggiornato a iOS 16+ (necessario per Web Push).
- Se l'app non riceve notifiche, controlla in **Impostazioni → Notifiche → JobLog** che siano abilitate "Consenti notifiche" e "Avvisi".
- In caso di problemi di cache forza la riapertura tenendo premuta l'icona sulla Home → **Rimuovi app** (mantiene i dati lato server) e ripeti i passaggi sopra.

## Gestione utenti
Gli account vengono salvati nella tabella `app_users` del database configurato (SQLite di default o MySQL/MariaDB se abilitato). Lo script `manage_users.py` utilizza automaticamente le stesse impostazioni definite in `config.json` o tramite variabili `JOBLOG_DB_*`.

- Elenca utenti: `python manage_users.py list`
- Crea un account: `python manage_users.py create USERNAME --name "Nome Cognome" --role supervisor`
- Aggiorna password: `python manage_users.py set-password USERNAME`
- Cambia ruolo: `python manage_users.py set-role USERNAME admin`
- Abilita/Disabilita: `python manage_users.py activate USERNAME` / `python manage_users.py deactivate USERNAME`
- Elimina account: `python manage_users.py delete USERNAME`

I ruoli supportati sono `user`, `supervisor` e `admin`. Durante la creazione puoi specificare `--role` (default `user`) ed eventualmente `--inactive` per creare un account disabilitato. Il vecchio `users.json` resta solo come fonte legacy per la migrazione automatica alla prima esecuzione: tutti gli aggiornamenti successivi devono passare dallo script e dal database.

## Dashboard sessioni (solo Admin)
- La UI mobile `/admin/sessions` è accessibile esclusivamente agli utenti con ruolo `admin` e mostra in tempo reale le sessioni calcolate dalla stessa logica usata per l'export.
- I dati sono forniti dall'endpoint `GET /api/admin/sessions`, che accetta filtri `start_date`, `end_date`, `search`, `member`, `activity_id` e `limit`.
- L'interfaccia è ottimizzata per Android: card responsive, pulsanti touch-friendly e possibilità di filtrare rapidamente operatori/attività.
- Dal menu laterale della dashboard principale compare automaticamente il link "Report Sessioni" quando l'utente autenticato possiede il ruolo amministratore.

## Autenticazione e sessioni
- L'accesso all'app è protetto: autenticati dalla UI su `/login` usando le credenziali create con `manage_users.py`.
- Dopo il login, la sessione HTTP resta valida 24 ore e viene salvata lato server nella directory `.flask_session` (o nel database se usi MySQL). In parallelo viene creato un cookie `joblog_auth` (HttpOnly, SameSite Lax) che permette di ripristinare automaticamente l'utente dopo un riavvio del backend.
- Se anche il cookie persistente viene rimosso, tutte le chiamate API ricevono `401`: il frontend blocca il polling, mostra un avviso di sessione scaduta e reindirizza automaticamente alla schermata di login.
- Per verificare il comportamento, dal browser puoi cancellare il cookie di sessione (DevTools → Application → Cookies) e tentare un'azione come "⏸️ Pausa selezione": il popup e il redirect confermeranno la gestione corretta dell'assenza di autenticazione.

## Configurazione database
- Backend di default: SQLite (`joblog.db` nella root del progetto).
- Per passare a MySQL/MariaDB crea `config.json` o usa variabili d'ambiente `JOBLOG_DB_*` (`vendor`, `host`, `port`, `user`, `password`, `name`). Imposta `vendor=mysql` per attivare l'adapter PyMySQL.
- Esempio rapido:
	```json
	{
		"database": {
			"vendor": "mysql",
			"host": "127.0.0.1",
			"port": 3306,
			"user": "joblog_user",
			"password": "supersegretissima",
			"name": "joblog"
		}
	}
	```
- All'avvio, se il database MySQL indicato non esiste viene creato automaticamente insieme alle tabelle necessarie.

## Notifiche Web Push
- Dipendenze: assicurati di installare anche `pywebpush` (`pip install -r requirements.txt`).
- Genera una coppia di chiavi VAPID (ad esempio: `python -m py_vapid --gen --json > vapid.json`).
- Aggiungi al `config.json` la sezione `webpush` oppure esporta le variabili `WEBPUSH_VAPID_PUBLIC`, `WEBPUSH_VAPID_PRIVATE`, `WEBPUSH_VAPID_SUBJECT`.
	```json
	{
		"webpush": {
			"vapid_public": "BExxxxxxxxx",
			"vapid_private": "Jaxxxxxxxxx",
			"subject": "mailto:ops@example.com"
		}
	}
	```
- Dalla UI apri **Impostazioni → Notifiche Push** e attiva le notifiche quando richiesto dal browser.
- Gli utenti abilitati alle notifiche vedranno la richiesta di permesso nel browser; il servizio invia alert automatici quando un'attività supera di 10 minuti il tempo pianificato.

## Caricare progetti reali
- Modifica `projects.json` inserendo i tuoi codici progetto, le attività e la squadra; l'esempio incluso può essere usato come base.
- Avvia l'app (`python app.py`) e inserisci il codice del progetto nel campo in alto: se il codice esiste in `projects.json` verrà caricato al posto dei progetti demo.

## Token Rentman
- Copia `config.example.json` in `config.json` e inserisci il valore di `rentman_api_token`.
- In alternativa puoi esportare `RENTMAN_API_TOKEN` nell'ambiente: l'app userà prima la variabile, poi `config.json`.
- Evita di committare `config.json` se contiene credenziali reali.
