# Ore per Settimana - Documentazione

## 📊 Nuova Funzionalità: Visualizzazione Settimanale

### Overview
Aggiunta una sezione **"Ore per Settimana"** che visualizza in modo chiaro il carico di lavoro settimanale direttamente dalla pagina dei turni.

### 🎨 Interfaccia

#### Layout
```
┌─────────────────────────────────┐
│ 📊 Ore per Settimana            │
├─────────────────────────────────┤
│ < Questa settimana >            │
├─────────────────────────────────┤
│ LUN  MAR  MER  GIO  VEN  SAB  DOM
│ 1    8    8    8    8    8    0
│
│ Totale Settimana: 41h          │
└─────────────────────────────────┘
```

#### Componenti

1. **Intestazione**
   - Icona grafico (📊)
   - Titolo "Ore per Settimana"

2. **Navigazione Settimanale**
   - Freccia sinistra (`←`) per settimana precedente
   - Etichetta settimana (es: "Questa settimana", "Prossima settimana", "10/1")
   - Freccia destra (`→`) per settimana successiva

3. **Grafico Settimanale**
   - 7 giorni della settimana in colonne
   - Per ogni giorno mostra:
     - Abbreviazione giorno (LUN, MAR, ecc.)
     - Data (numero del giorno)
     - Ore totali (con decimali)
     - Badge "—" se nessun turno
   
4. **Totale Settimana**
   - Somma ore di tutti i 7 giorni
   - Formato: "Totale Settimana: 41h"

### 🎯 Interazioni

#### Cliccare su un giorno della settimana
- Seleziona quel giorno
- Cambia il filtro a "Oggi" (per mostrare turni di quel giorno)
- Aggiorna il date picker
- Scroll automatico ai turni

#### Navigare tra le settimane
- Pulsante `←` → Mostra settimana precedente
- Pulsante `→` → Mostra settimana successiva
- L'etichetta si aggiorna automaticamente:
  - **Offset 0**: "Questa settimana"
  - **Offset +1**: "Prossima settimana"
  - **Offset -1**: "Scorsa settimana"
  - **Altro**: Data di inizio settimana (es: "10/1")

### 🎨 Styling

#### Giorni Normali
```css
background: var(--bg);
border: 2px solid transparent;
hover: background gradiente blu, border blu, translateY(-2px)
```

#### Giorno di Oggi
```css
background: gradiente blu (brand colors)
color: white
border: 2px solid brand-dark
animation: nessuna (già sottolineato dal colore)
```

#### Ore Vuote
- Color: `var(--text-muted)` (grigio)
- Mostra: `—` (em-dash)

#### Ore Piene
- Color: `var(--brand)` (blu)
- Mostra: numero con decimale (es: "8.5")

### 🔄 Sincronizzazione con Date Picker

Quando navighi tra le settimane con i pulsanti `←` `→` del date picker:
- Se la data va in una settimana diversa, `currentWeekOffset` si aggiorna automaticamente
- Il grafico settimanale si rifà
- La data selezionata rimane coerente

### 📱 Mobile Responsive

- Griglia 7 colonne compatta
- Font ridotto su schermi < 400px
- Touch-friendly (padding sufficiente)
- Bottoni navigazione di 32px (toccabili)

### 🔢 Calcoli

#### getMonday(date)
Restituisce il lunedì della settimana di una data.

#### getWeekRange(offset)
Restituisce array di 7 date (lunedì-domenica) offset di settimane da oggi.

```javascript
getWeekRange(0)  // Questa settimana
getWeekRange(1)  // Prossima settimana
getWeekRange(-1) // Scorsa settimana
```

#### getHoursForDate(dateStr)
Somma le ore di tutti i turni di una data.

```javascript
getHoursForDate('2026-01-09')  // 7.5h
```

#### renderWeeklyChart()
Renderizza il grafico con i dati della settimana selezionata.

### 💾 Stato

Variabile di stato: `currentWeekOffset`
- `0` = Questa settimana
- `1` = Prossima settimana
- `-1` = Scorsa settimana
- Persiste solo durante la sessione

### 🚀 Performance

- Calcoli fatti al load dei turni
- Update grafico: O(7) ≈ costante
- Nessun re-fetch API durante navigazione settimanale
- Uso locale dei dati in memoria (`allTurni`)

### 🎯 Caso d'Uso

**Scenario:** Operaio vuole verificare il carico di lavoro settimanale
1. Apre "I Miei Turni"
2. Vede il grafico "Ore per Settimana"
3. Clicca su mercoledì per vedere turni di quel giorno
4. Naviga a prossima settimana con `→`
5. Vede che prossima settimana ha meno ore

### 📌 Integrazione

- Sezione inserita tra Stats Bar e Date Picker
- Condivide la stessa API `/api/user/turni`
- Utilizza stessi colori e temi del resto dell'app
- Dark mode supportato

### 🔧 Customizzazione

Per aggiungere funzionalità:

**Mostrare ore pianificate vs registrate:**
```javascript
// Modificare getHoursForDate per distinguere
function getHoursForDate(dateStr, type = 'planned') {
  return allTurni
    .filter(t => t.date === dateStr)
    .reduce((sum, t) => sum + (type === 'planned' ? t.hours : t.registered_hours), 0);
}
```

**Colorare giorni in base al carico:**
```javascript
// Nel renderWeeklyChart(), aggiungere classe
const className = hours > 8 ? 'overworked' : hours < 4 ? 'light' : '';
```

### 📊 Dati di Input

L'array `allTurni` deve contenere:
```javascript
{
  date: '2026-01-09',      // YYYY-MM-DD
  hours: 8.5,              // Ore totali
  break_minutes: 30,       // Pausa (opzionale)
  // ... altri campi
}
```

Le ore calcolate al netto della pausa tramite `getEffectiveHours()`.
