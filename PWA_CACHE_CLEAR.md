# 🔄 Come Cancellare la Cache della PWA su Android

Se vedi ancora l'icona vecchia dopo aver aggiornato il sito, segui questi passi:

## Metodo 1: Cancellare Cache dall'App (Consigliato)

1. **Apri Impostazioni Android** → **App**
2. Cerca **"JobLog"** oppure **"Chrome"**
3. Apri l'app → **Archiviazione e cache**
4. Clicca **"Cancella cache"** (non "Cancella dati")
5. Esci completamente dall'app
6. Riapri l'app (dovrebbe mostrare la nuova icona)

## Metodo 2: Disinstallare e Reinstallare

Se il Metodo 1 non funziona:

1. **Apri il browser** → vai a `http://localhost:5000/magazzino`
2. Apri il menu (⋮) → **App** → **JobLog**
3. Clicca **"Disinstalla app"**
4. Torna al sito
5. Apri menu (⋮) → **Installa app**
6. Conferma l'installazione

## Metodo 3: Hard Refresh nel Browser

Se usi il browser e non vedi l'aggiornamento:

1. Apri Developer Tools (F12)
2. Vedi la tab "Application" → "Service Workers"
3. Clicca **"Unregister"** su tutti i Service Worker
4. Ricaricare la pagina (Ctrl+Shift+R per hard refresh)

## Cosa è Stato Aggiornato

- ✅ Service Worker versione aggiornata (v2026.01.08a)
- ✅ Manifest.json con versione (2026.01.08)
- ✅ Icone nuove (con gradiente blu)
- ✅ Cache busting su tutti gli asset statici

## Se Continua a Non Funzionare

Prova questo comando nel browser console:

```javascript
if ('serviceWorker' in navigator) {
  navigator.serviceWorker.getRegistrations().then(registrations => {
    registrations.forEach(reg => reg.unregister());
    window.location.reload();
  });
}
```

---

**Nota**: Android impiega 24-48 ore a scaricare automaticamente gli aggiornamenti delle PWA. Se urgente, usa i metodi sopra.
