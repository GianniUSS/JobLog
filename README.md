# JobLOG (Starter)

## Requisiti
- Python 3.10+
- pip


## Setup
```bash
python -m venv .venv
source .venv/bin/activate # su Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
python app.py
```

## Caricare progetti reali
- Modifica `projects.json` inserendo i tuoi codici progetto, le attività e la squadra; l'esempio incluso può essere usato come base.
- Avvia l'app (`python app.py`) e inserisci il codice del progetto nel campo in alto: se il codice esiste in `projects.json` verrà caricato al posto dei progetti demo.

## Token Rentman
- Copia `config.example.json` in `config.json` e inserisci il valore di `rentman_api_token`.
- In alternativa puoi esportare `RENTMAN_API_TOKEN` nell'ambiente: l'app userà prima la variabile, poi `config.json`.
- Evita di committare `config.json` se contiene credenziali reali.
