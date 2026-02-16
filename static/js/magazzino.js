(function() {
    'use strict';
    
    // === CONFIG ===
    const CACHE_KEY = 'wh-projects';
    const MANUAL_KEY = 'wh-projects-manual';
    const TIMER_KEY = 'wh-timer';
    const THEME_KEY = 'wh-theme';
    const CACHE_TTL = 20 * 60 * 1000; // 20 min
    const LAST_PROJ_KEY = 'wh-last-proj';
    const SERVER_MANUAL_KEY = 'wh-server-manual';
    const DEBOUNCE_MS = 300;
    
    // === DOM ===
    const $ = id => document.getElementById(id);
    const timerBox = $('timerBox');
    const timerDisplay = $('timerDisplay');
    const timerContext = $('timerContext');
    const timerStatus = $('timerStatus');
    const btnStart = $('btnStart');
    const btnPause = $('btnPause');
    const btnStop = $('btnStop');
    const projList = $('projList');
    const actGrid = $('actGrid');
    const sessionsList = $('sessionsList');
    const totalTimeEl = $('totalTime');
    const loadingEl = $('loading');
    const toastEl = $('toast');
    const themeBtn = $('themeBtn');
    const addBtn = $('addProjBtn');
    const addModal = $('addModal');
    const manualCodeEl = $('manualCode');
    const manualNameEl = $('manualName');
    const keypadEl = $('keypad');
    const cancelAdd = $('cancelAdd');
    const confirmAdd = $('confirmAdd');
    const notesModal = $('notesModal');
    const notesInput = $('notesInput');
    const notesHint = $('notesHint');
    const cancelNotes = $('cancelNotes');
    const confirmNotes = $('confirmNotes');
    const clearNotes = $('clearNotes');
    const notesModalTitle = $('notesModalTitle');
    const notesModalDesc = $('notesModalDesc');
    const notesRequiredStar = $('notesRequiredStar');
    const notesAddBtn = $('notesAddBtn');
    const notesBtnText = $('notesBtnText');
    const notesBtnBadge = $('notesBtnBadge');
    const notesPreview = $('notesPreview');
    
    // === STATE ===
    let projects = [];
    let manualProjects = [];
    let selectedProj = null;
    let selectedAct = null;
    let selectedNotes = '';
    let timer = { running: false, paused: false, start: 0, elapsed: 0, startedAt: 0, proj: null, act: null, notes: '' };
    let tickId = null;
    let toastTimeout = null;
    let darkMode = false;
    let resumeSessionId = null; // ID sessione da continuare
    let manualCode = '';
    
    // === ACTIVITIES CAROUSEL ===
    const ACTIVITIES = [
        { code: 'Preparazione', icon: '📦', label: 'Preparazione' },
        { code: 'Carico', icon: '🚚', label: 'Carico' },
        { code: 'Scarico', icon: '📥', label: 'Scarico' },
        { code: 'Controllo', icon: '🔍', label: 'Controllo' },
        { code: 'Manutenzione', icon: '🔧', label: 'Manutenzione' },
        { code: 'Altro', icon: '📝', label: 'Altro' }
    ];
    let activityPage = 0;
    const ACTIVITIES_PER_PAGE = 4;
    
    // === THEME ===
    function loadTheme() {
        const saved = localStorage.getItem(THEME_KEY);
        if (saved === 'dark') {
            darkMode = true;
        } else if (saved === 'light') {
            darkMode = false;
        } else {
            // Default: segui preferenza sistema
            darkMode = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
        }
        applyTheme();
    }
    
    function applyTheme() {
        if (darkMode) {
            document.documentElement.setAttribute('data-theme', 'dark');
            if (themeBtn) themeBtn.textContent = '☀️';
        } else {
            document.documentElement.removeAttribute('data-theme');
            if (themeBtn) themeBtn.textContent = '🌙';
        }
    }
    
    function toggleTheme() {
        darkMode = !darkMode;
        localStorage.setItem(THEME_KEY, darkMode ? 'dark' : 'light');
        applyTheme();
    }
    
    // === UTILS ===
    const fmt = n => String(n).padStart(2, '0');
    const fmtTime = ms => {
        const s = Math.floor(ms / 1000);
        return `${fmt(Math.floor(s / 3600))}:${fmt(Math.floor((s % 3600) / 60))}:${fmt(s % 60)}`;
    };
    const fmtHourMin = ts => {
        if (!ts) return '';
        const d = new Date(ts);
        return `${fmt(d.getHours())}:${fmt(d.getMinutes())}:${fmt(d.getSeconds())}`;
    };

    const projTitle = p => (p?.name || p?.displayname || p?.reference || '').trim();

    function renderTimerContext(proj, detail) {
        if (!proj) {
            timerContext.textContent = 'Seleziona progetto e attività';
            return;
        }
        const name = projTitle(proj);
        const namePart = name ? ` · ${name}` : '';
        const detailPart = detail ? ` · ${detail}` : '';
        timerContext.innerHTML = `<strong>${proj.code}</strong>${namePart}${detailPart}`;
    }
    
    function toast(msg, type) {
        toastEl.textContent = msg;
        toastEl.className = 'toast show ' + (type || '');
        clearTimeout(toastTimeout);
        toastTimeout = setTimeout(() => toastEl.classList.remove('show'), 2500);
    }
    
    function showLoading(show) {
        loadingEl.classList.toggle('hide', !show);
    }
    
    // === CACHE ===
    function loadManualProjects() {
        try {
            const raw = localStorage.getItem(MANUAL_KEY);
            if (!raw) return [];
            const arr = JSON.parse(raw);
            return Array.isArray(arr) ? arr : [];
        } catch { return []; }
    }

    function saveManualProjects(list) {
        try {
            localStorage.setItem(MANUAL_KEY, JSON.stringify(list));
        } catch {}
    }

    function mergeProjects(base, extras) {
        const map = new Map();
        (base || []).forEach(p => { if (p && p.code) map.set(p.code, p); });
        (extras || []).forEach(p => { if (p && p.code) map.set(p.code, p); });
        return Array.from(map.values()).sort((a, b) => (a.code || '').localeCompare(b.code || ''));
    }

    function normalizeProjects() {
        projects = mergeProjects(projects, manualProjects);
    }

    function loadCache() {
        try {
            const raw = localStorage.getItem(CACHE_KEY);
            if (!raw) return null;
            const data = JSON.parse(raw);
            if (Date.now() - data.ts > CACHE_TTL) return null;
            return data.projects;
        } catch { return null; }
    }
    
    function saveCache(projs) {
        try {
            localStorage.setItem(CACHE_KEY, JSON.stringify({ ts: Date.now(), projects: projs }));
        } catch {}
    }

    function saveServerManual(list) {
        try { localStorage.setItem(SERVER_MANUAL_KEY, JSON.stringify({ ts: Date.now(), items: list })); } catch {}
    }

    function loadServerManual() {
        try {
            const raw = localStorage.getItem(SERVER_MANUAL_KEY);
            if (!raw) return [];
            const data = JSON.parse(raw);
            if (!data || !Array.isArray(data.items)) return [];
            if (Date.now() - (data.ts || 0) > CACHE_TTL) return [];
            return data.items;
        } catch { return []; }
    }
    
    function loadTimerState() {
        try {
            const raw = localStorage.getItem(TIMER_KEY);
            if (raw) timer = { ...timer, ...JSON.parse(raw) };
        } catch {}
    }
    
    // Carica timer dal server (per timer avviati da timbratura produzione)
    async function loadTimerFromServer() {
        try {
            const res = await fetch('/api/production/timer');
            if (!res.ok) return false;
            const data = await res.json();
            if (data.ok && data.active && data.timer) {
                const serverTimer = data.timer;
                console.log('[MagTimer] Timer trovato sul server:', serverTimer);
                
                // Calcola elapsed attuale
                let currentElapsed = serverTimer.elapsed_ms || 0;
                if (!serverTimer.paused && serverTimer.start_ts) {
                    currentElapsed += Date.now() - serverTimer.start_ts;
                }
                
                // Imposta il timer locale
                timer = {
                    running: true,
                    paused: serverTimer.paused,
                    start: serverTimer.start_ts,
                    startedAt: serverTimer.start_ts,
                    elapsed: currentElapsed,
                    proj: { code: serverTimer.project_code, name: serverTimer.project_name || serverTimer.project_code },
                    act: serverTimer.activity_label,
                    notes: serverTimer.notes || ''
                };
                
                // Salva in localStorage per coerenza
                saveTimerState();
                return true;
            } else if (data.ok && !data.active) {
                // Timer non attivo sul server - se era attivo localmente, fermalo
                if (timer.running) {
                    console.log('[MagTimer] Timer fermato dal server (timbratura fine giornata)');
                    stopTick();
                    clearTimerState();
                    updateUI();
                    fetchSessions(); // Ricarica sessioni
                }
                return false;
            }
        } catch (err) {
            console.error('[MagTimer] Errore caricamento timer server:', err);
        }
        return false;
    }
    
    // Sincronizza stato timer dal server (per rilevare pause/stop da timbratura)
    async function syncTimerStateFromServer() {
        if (!timer.running) return;
        
        try {
            const res = await fetch('/api/production/timer');
            if (!res.ok) return;
            const data = await res.json();
            
            if (data.ok && data.active && data.timer) {
                const serverTimer = data.timer;
                
                // Controlla se lo stato di pausa è cambiato
                if (serverTimer.paused !== timer.paused) {
                    console.log('[MagTimer] Stato pausa cambiato dal server:', serverTimer.paused);
                    timer.paused = serverTimer.paused;
                    timer.elapsed = serverTimer.elapsed_ms || 0;
                    
                    if (serverTimer.paused) {
                        stopTick();
                    } else {
                        timer.start = serverTimer.start_ts;
                        timer.startedAt = serverTimer.start_ts;
                        startTick();
                    }
                    
                    saveTimerState();
                    updateUI();
                }
            } else if (data.ok && !data.active && timer.running) {
                // Timer fermato dal server (fine giornata)
                console.log('[MagTimer] Timer fermato dal server');
                stopTick();
                clearTimerState();
                updateUI();
                fetchSessions();
            }
        } catch (err) {
            // Ignora errori di rete
        }
    }
    
    function saveTimerState() {
        try {
            localStorage.setItem(TIMER_KEY, JSON.stringify(timer));
        } catch {}
        // Salva anche sul server per visibilità admin
        syncTimerToServer();
    }
    
    function clearTimerState() {
        timer = { running: false, paused: false, start: 0, elapsed: 0, proj: null, act: null, notes: '' };
        localStorage.removeItem(TIMER_KEY);
        selectedNotes = '';
        // Aggiorna pulsante note
        updateNotesButton();
        // Rimuovi dal server
        clearTimerFromServer();
    }

    // Sincronizza stato timer sul server per visibilità admin dashboard
    async function syncTimerToServer() {
        if (!timer.running || !timer.proj) {
            console.log('[MagTimer] Skip sync: timer not running or no proj', timer);
            return;
        }
        
        // Prima verifica se il timer è ancora attivo sul server
        // (potrebbe essere stato fermato da una timbratura fine_giornata)
        try {
            const checkRes = await fetch('/api/production/timer');
            if (checkRes.ok) {
                const checkData = await checkRes.json();
                if (checkData.ok && !checkData.active) {
                    // Timer fermato dal server! Non ricrearlo, aggiorna stato locale
                    console.log('[MagTimer] Timer fermato lato server, aggiorno stato locale');
                    stopTick();
                    clearTimerState();
                    updateUI();
                    fetchSessions();
                    return;
                }
            }
        } catch (err) {
            // Ignora errori di verifica
        }
        
        console.log('[MagTimer] Syncing timer to server:', timer);
        try {
            const res = await fetch('/api/magazzino/timer', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    project_code: timer.proj.code,
                    project_name: timer.proj.name,
                    activity_label: timer.act,
                    notes: timer.notes || '',
                    running: timer.running,
                    paused: timer.paused,
                    start_ts: timer.startedAt,
                    elapsed_ms: timer.elapsed || 0,
                    pause_start_ts: timer.paused ? Date.now() : null
                })
            });
            const data = await res.json();
            console.log('[MagTimer] Server response:', data);
        } catch (err) {
            console.error('[MagTimer] Sync error:', err);
        }
    }
    
    async function clearTimerFromServer() {
        try {
            await fetch('/api/magazzino/timer', { method: 'DELETE' });
        } catch (err) {
            // Ignora errori
        }
    }
    function rememberProject(code) {
        try { localStorage.setItem(LAST_PROJ_KEY, code || ''); } catch {}
    }

    function loadLastProject() {
        try { return localStorage.getItem(LAST_PROJ_KEY) || ''; } catch { return ''; }
    }

    // === MANUAL PROJECT ===
    function openAddModal() {
        manualCode = '';
        manualCodeEl.textContent = '—';
        manualNameEl.value = '';
        addModal.classList.remove('hide');
        manualNameEl.blur();
    }

    function closeAddModal() {
        addModal.classList.add('hide');
    }

    function applyManualCode(input) {
        if (input === 'clear') {
            manualCode = manualCode.slice(0, -1);
        } else if (input === 'ok') {
            // ignore; handled by confirm
            return;
        } else {
            manualCode = (manualCode + input).slice(0, 12);
        }
        manualCodeEl.textContent = manualCode || '—';
    }

    async function addManualProject() {
        const code = manualCode.trim();
        const nameInput = (manualNameEl.value || '').trim();
        if (!code) {
            toast('Inserisci un numero progetto', 'err');
            return;
        }

        let fetched = null;
        let lookedUp = false;
        showLoading(true);
        try {
            const res = await fetch(`/api/magazzino/projects/lookup?code=${encodeURIComponent(code)}`);
            lookedUp = true;
            if (res.ok) {
                const data = await res.json();
                fetched = data.project || null;
            } else if (res.status !== 404) {
                throw new Error(`HTTP ${res.status}`);
            }
        } catch (err) {
            console.error('addManualProject lookup error:', err);
            // Continua comunque con inserimento manuale
        } finally {
            showLoading(false);
        }

        if (lookedUp && !fetched && !nameInput) {
            toast('Progetto non trovato in Rentman, aggiungi un titolo', 'err');
            return;
        }

        const projToAdd = fetched || { code, name: nameInput || code };

        // Aggiorna lista progetti (merge con manuali)
        const existing = projects.find(p => p.code === projToAdd.code);
        if (!existing) {
            projects.push(projToAdd);
        } else {
            if (projToAdd.name && (!existing.name || existing.name === existing.code)) {
                existing.name = projToAdd.name;
            }
        }

        // Tieni traccia dei manuali
        const manualExisting = manualProjects.find(p => p.code === projToAdd.code);
        if (!manualExisting) {
            manualProjects.push({ code: projToAdd.code, name: projToAdd.name });
        } else {
            manualExisting.name = projToAdd.name;
        }
        manualProjects = mergeProjects([], manualProjects);
        saveManualProjects(manualProjects);

        projects = mergeProjects(projects, manualProjects);
        saveCache(projects);
        selectedProj = projects.find(p => p.code === projToAdd.code);
        rememberProject(projToAdd.code);
        saveManualProjectServer(projToAdd);
        renderProjects();
        updateUI();
        fetchSessions();
        if (!fetched && lookedUp) {
            toast('Aggiunto manualmente (non trovato in Rentman)', 'ok');
        } else {
            toast('Progetto aggiunto', 'ok');
        }
        closeAddModal();
    }
    
    // === API ===
    async function fetchManualProjectsServer() {
        // Recupera manuali salvati sul server per sincronizzare i device
        try {
            const res = await fetch('/api/magazzino/projects/manual');
            if (!res.ok) return;
            const data = await res.json();
            const serverList = Array.isArray(data.items) ? data.items : [];
            if (serverList.length) {
                manualProjects = mergeProjects(manualProjects, serverList);
                saveManualProjects(manualProjects);
                saveServerManual(serverList);
            }
        } catch (err) {
            const cached = loadServerManual();
            if (cached.length) {
                manualProjects = mergeProjects(manualProjects, cached);
            }
        }
    }

    async function saveManualProjectServer(proj) {
        try {
            await fetch('/api/magazzino/projects/manual', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ code: proj.code, name: proj.name })
            });
        } catch (err) {
            // offline: sarà ritentato al prossimo fetch manuale
        }
    }

    async function fetchProjects(force) {
        manualProjects = loadManualProjects();

        // Sincronizza manuali dal server prima di usare cache/lista
        await fetchManualProjectsServer();

        if (!force) {
            const cached = loadCache();
            if (cached && cached.length) {
                projects = mergeProjects(cached, manualProjects);
                renderProjects();
                return;
            }
        }
        
        showLoading(true);
        try {
            const res = await fetch('/api/magazzino/projects/today');
            const data = await res.json();
            const serverProjects = data.projects || [];
            projects = mergeProjects(serverProjects, manualProjects);
            saveCache(projects);
            renderProjects();
        } catch (e) {
            toast('Errore caricamento', 'err');
            projList.innerHTML = '<div style="color:var(--danger);padding:10px;">Errore</div>';
        } finally {
            showLoading(false);
        }
    }
    
    async function fetchSessions() {
        // Carica TUTTE le sessioni di oggi, non solo quelle del progetto selezionato
        try {
            const res = await fetch('/api/magazzino/sessions');
            if (!res.ok) {
                if (res.status === 401 || res.status === 302) {
                    window.location.reload();
                    return;
                }
                throw new Error(`HTTP ${res.status}`);
            }
            const data = await res.json();
            renderSessions(data.items || []);
        } catch (err) {
            console.error('fetchSessions error:', err);
            sessionsList.innerHTML = '<div class="sessions-empty">Errore caricamento sessioni</div>';
            totalTimeEl.textContent = '-';
            toast('Errore caricamento sessioni', 'err');
        }
    }

    async function saveSession() {
        const elapsed = getElapsed();
        const projCode = (timer.proj || selectedProj)?.code;
        const act = timer.act || selectedAct;
        if (!projCode || !act) {
            toast('Seleziona progetto e attività', 'err');
            return;
        }
        try {
            const endTs = Date.now();
            const startTs = timer.startedAt || (endTs - elapsed);
            
            // Se stiamo riprendendo una sessione, aggiorna quella esistente
            if (resumeSessionId) {
                const res = await fetch(`/api/magazzino/sessions/${resumeSessionId}`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ 
                        add_elapsed_ms: elapsed,
                        interval_start: startTs,  // Inizio di questo intervallo
                        end_ts: endTs
                    })
                });
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                const data = await res.json().catch(() => ({}));
                if (data && data.ok === false) throw new Error(data.error || 'update_failed');
                toast('✓ Tempo aggiunto alla sessione', 'ok');
                resumeSessionId = null; // Reset
                selectedNotes = '';
                await fetchSessions();
                return;
            }
            
            // Altrimenti crea nuova sessione
            const payload = {
                project_code: projCode,
                activity_label: act,
                elapsed_ms: elapsed,
                start_ts: startTs,
                end_ts: endTs
            };
            
            // Aggiungi note se presenti (attività "Altro") - il backend vuole "note" singolare
            if (timer.notes || selectedNotes) {
                payload.note = timer.notes || selectedNotes;
            }
            
            const res = await fetch('/api/magazzino/sessions', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json().catch(() => ({}));
            if (data && data.ok === false) throw new Error(data.error || 'save_failed');
            toast('✓ Sessione salvata', 'ok');
            selectedNotes = '';
            await fetchSessions();
        } catch (err) {
            console.error('saveSession error:', err);
            toast('Errore salvataggio', 'err');
        }
    }
    
    // === RENDER ===
    function renderProjects() {
        normalizeProjects();
        if (!projects.length) {
            projList.innerHTML = '<div style="color:var(--text-light);padding:12px;font-size:13px;">Nessun progetto</div>';
            return;
        }
        
        projList.innerHTML = projects.map(p => {
            const name = (p.name || p.displayname || p.reference || '').trim();
            const isSelected = selectedProj?.code === p.code;
            return `<button class="proj-item${isSelected ? ' selected' : ''}" 
                        data-code="${p.code}" ${timer.running ? 'disabled' : ''}>
                        <div class="proj-code">${p.code}</div>
                        ${name ? `<div class="proj-name">${name}</div>` : ''}
                    </button>`;
        }).join('');

        // NON auto-selezionare progetti all'avvio - l'utente deve scegliere manualmente
        // (Mantenuto solo per timer in corso)
    }
    
    function renderSessions(items) {
        if (!items.length) {
            sessionsList.innerHTML = '<div class="sessions-empty"><div class="sessions-empty-icon">⏱️</div><div>Nessuna sessione</div></div>';
            totalTimeEl.textContent = '—';
            return;
        }
        
        // Salva items per accesso successivo
        window._sessionsData = items;
        
        let total = 0;
        sessionsList.innerHTML = items.map((s, idx) => {
            total += s.elapsed_ms || 0;
            const hasNote = s.note && s.note.trim();
            const noteHtml = hasNote 
                ? `<div class="session-note">
                    <span class="session-note-label">📝</span>
                    <span class="session-note-text">${s.note}</span>
                   </div>` 
                : '';
            // Mostra orari inizio-fine (primo e ultimo)
            const intervals = s.intervals || [];
            const firstStart = intervals.length ? intervals[0].start : s.start_ts;
            const lastEnd = intervals.length ? intervals[intervals.length - 1].end : s.end_ts;
            const startTime = firstStart ? fmtHourMin(firstStart) : '';
            const endTime = lastEnd ? fmtHourMin(lastEnd) : '';
            const intervalsCount = intervals.length;
            const timeRangeHtml = (startTime && endTime) 
                ? `<span class="session-time-range">🕐 ${startTime} → ${endTime}${intervalsCount > 1 ? ` (${intervalsCount} int.)` : ''}</span>` 
                : '';
            // Pulsanti azioni
            const actionsHtml = `
                <div class="session-actions">
                    <button class="session-action-btn resume" data-session-id="${s.id}" data-project="${s.project_code}" data-activity="${s.activity_label}" data-note="${(s.note || '').replace(/"/g, '&quot;')}" title="Riprendi attività">
                        ▶ Riprendi
                    </button>
                    <button class="session-action-btn edit-time" data-session-idx="${idx}" title="Visualizza orari">
                        🕐 Orari
                    </button>
                </div>`;
            return `<div class="session-item" data-session-id="${s.id}" data-project="${s.project_code}" data-activity="${s.activity_label}" data-note="${(s.note || '').replace(/"/g, '&quot;')}" data-idx="${idx}">
                <div class="session-header">
                    <div class="session-details">
                        <div class="session-act">${s.activity_label}</div>
                        <div class="session-proj">
                            <span class="session-proj-code">${s.project_code}</span>
                            ${timeRangeHtml}
                        </div>
                    </div>
                    <div class="session-duration">${fmtTime(s.elapsed_ms || 0)}</div>
                </div>
                ${noteHtml}
            </div>`;
        }).join('');
        
        totalTimeEl.textContent = fmtTime(total);
        
        // Event listeners per click su sessione (toggle selezione)
        sessionsList.querySelectorAll('.session-item').forEach(item => {
            item.addEventListener('click', (e) => {
                // Toggle selezione
                const wasSelected = item.classList.contains('selected');
                // Deseleziona tutte
                sessionsList.querySelectorAll('.session-item').forEach(el => el.classList.remove('selected'));
                
                const actionsBar = document.getElementById('sessionActionsBar');
                const overlay = document.getElementById('sessionActionsOverlay');
                const mainEl = document.querySelector('main');
                
                // Se non era selezionata, selezionala e mostra la barra
                if (!wasSelected) {
                    item.classList.add('selected');
                    window._selectedSessionItem = item;
                    actionsBar.classList.add('visible');
                    overlay.classList.add('visible');
                    mainEl.classList.add('has-action-bar');
                    // Scroll per portare la sessione in cima alla lista
                    setTimeout(() => {
                        item.scrollIntoView({ behavior: 'smooth', block: 'start' });
                    }, 50);
                } else {
                    // Era selezionata, deseleziona e nascondi barra
                    hideSessionActionsBar();
                }
            });
        });
    }
    
    function hideSessionActionsBar() {
        const actionsBar = document.getElementById('sessionActionsBar');
        const overlay = document.getElementById('sessionActionsOverlay');
        const mainEl = document.querySelector('main');
        
        window._selectedSessionItem = null;
        actionsBar.classList.remove('visible');
        overlay.classList.remove('visible');
        mainEl.classList.remove('has-action-bar');
        sessionsList.querySelectorAll('.session-item').forEach(el => el.classList.remove('selected'));
    }
    
    // === SESSION ACTIONS BAR HANDLERS ===
    function initSessionActionsBar() {
        const barBtnResume = document.getElementById('barBtnResume');
        const barBtnIntervals = document.getElementById('barBtnIntervals');
        const overlay = document.getElementById('sessionActionsOverlay');
        
        // Chiudi cliccando sull'overlay
        if (overlay) {
            overlay.addEventListener('click', hideSessionActionsBar);
        }
        
        barBtnResume.addEventListener('click', () => {
            if (timer.running) {
                toast('Ferma il timer prima di riprendere', 'warn');
                return;
            }
            const item = window._selectedSessionItem;
            if (item) {
                resumeSession({
                    sessionId: item.dataset.sessionId,
                    project: item.dataset.project,
                    activity: item.dataset.activity,
                    note: item.dataset.note
                });
                // Nascondi barra dopo azione
                hideSessionActionsBar();
            }
        });
        
        barBtnIntervals.addEventListener('click', () => {
            const item = window._selectedSessionItem;
            if (item) {
                const idx = parseInt(item.dataset.idx);
                const session = window._sessionsData[idx];
                if (session) {
                    openIntervalsModal(session);
                    // Nascondi barra dopo azione
                    hideSessionActionsBar();
                }
            }
        });
    }
    
    // === RESUME SESSION ===
    function resumeSession(data) {
        const { sessionId, project, activity, note } = data;
        resumeSessionId = parseInt(sessionId);
        
        // Trova e seleziona il progetto
        const proj = projects.find(p => p.code === project);
        if (proj) {
            selectedProj = proj;
            document.querySelectorAll('.proj-item').forEach(el => {
                el.classList.toggle('selected', el.dataset.code === project);
            });
        } else {
            // Aggiungi progetto temporaneo se non esiste
            const tempProj = { code: project, name: '' };
            projects.push(tempProj);
            selectedProj = tempProj;
            renderProjects();
        }
        
        // Seleziona attività
        selectedAct = activity;
        document.querySelectorAll('.activity-btn').forEach(el => {
            el.classList.toggle('selected', el.dataset.act === activity);
        });
        
        // Imposta note
        selectedNotes = note || '';
        
        updateUI();
        
        // Scroll verso l'alto per vedere il timer
        document.querySelector('main').scrollTo({ top: 0, behavior: 'smooth' });
        
        // Avvia automaticamente il timer dopo un breve delay (per permettere lo scroll)
        setTimeout(() => {
            startTimer();
        }, 300);
    }
    
    // === INTERVALS MODAL (mostra tutti gli intervalli) ===
    function openIntervalsModal(session) {
        const modal = $('editTimeModal');
        if (!modal) return;
        
        const intervals = session.intervals || [];
        const totalMs = session.elapsed_ms || 0;
        
        // Genera HTML per gli intervalli
        let intervalsHtml = '';
        if (intervals.length === 0) {
            intervalsHtml = '<div class="interval-empty">Nessun intervallo registrato</div>';
        } else {
            intervalsHtml = intervals.map((iv, i) => {
                const startTime = iv.start ? fmtHourMin(iv.start) : '—';
                const endTime = iv.end ? fmtHourMin(iv.end) : '—';
                const duration = (iv.start && iv.end) ? fmtTime(iv.end - iv.start) : '—';
                return `
                    <div class="interval-row">
                        <span class="interval-num">${i + 1}.</span>
                        <span class="interval-times">${startTime} → ${endTime}</span>
                        <span class="interval-duration">${duration}</span>
                    </div>
                `;
            }).join('');
        }
        
        // Aggiorna contenuto modal
        const modalContent = modal.querySelector('.modal');
        if (modalContent) {
            modalContent.innerHTML = `
                <div class="modal-title">🕐 Dettaglio Orari</div>
                <p style="font-size:13px;color:var(--text-light);margin-bottom:12px;">
                    <strong>${session.activity_label}</strong> - Progetto ${session.project_code}
                </p>
                <div class="intervals-list">
                    ${intervalsHtml}
                </div>
                <div class="intervals-total">
                    <span>Tempo totale:</span>
                    <span class="total-value">${fmtTime(totalMs)}</span>
                </div>
                <div class="modal-actions">
                    <button class="modal-btn primary" id="closeIntervalsModal">Chiudi</button>
                </div>
            `;
            
            // Event listener per chiudere
            const closeBtn = modal.querySelector('#closeIntervalsModal');
            if (closeBtn) {
                closeBtn.addEventListener('click', () => modal.classList.add('hide'));
            }
        }
        
        modal.classList.remove('hide');
    }
    
    function closeEditTimeModal() {
        const modal = $('editTimeModal');
        if (modal) modal.classList.add('hide');
    }
    
    // === TIMER ===
    function getElapsed() {
        if (!timer.running) return timer.elapsed;
        if (timer.paused) return timer.elapsed;
        return timer.elapsed + (Date.now() - timer.start);
    }
    
    function updateDisplay() {
        timerDisplay.textContent = fmtTime(getElapsed());
    }
    
    function updateUI() {
        const hasProj = !!selectedProj;
        const hasAct = !!selectedAct;
        const canStart = hasProj && hasAct && !timer.running;
        
        // Timer box state
        timerBox.classList.toggle('active-session', timer.running || (hasProj && hasAct));
        
        // Context
        if (timer.running) {
            renderTimerContext(timer.proj, timer.act);
        } else if (hasProj && hasAct) {
            renderTimerContext(selectedProj, selectedAct);
        } else if (hasProj) {
            renderTimerContext(selectedProj, null);
        } else {
            timerContext.textContent = 'Seleziona progetto e attività';
        }
        
        // Status
        if (timer.running && !timer.paused) {
            timerStatus.textContent = '● IN CORSO';
            timerStatus.className = 'timer-status running';
        } else if (timer.running && timer.paused) {
            timerStatus.textContent = '⏸ IN PAUSA';
            timerStatus.className = 'timer-status';
        } else {
            timerStatus.textContent = '—';
            timerStatus.className = 'timer-status';
        }
        
        // Buttons
        btnStart.classList.toggle('hide', timer.running);
        btnStart.disabled = !canStart;
        btnPause.classList.toggle('hide', !timer.running);
        btnPause.textContent = timer.paused ? '▶ RIPRENDI' : '⏸ PAUSA';
        btnStop.classList.toggle('hide', !timer.running);
        
        // Disable selectors while running
        document.querySelectorAll('.proj-item').forEach(el => el.disabled = timer.running);
        document.querySelectorAll('.activity-btn').forEach(el => el.disabled = timer.running);
        
        // Aggiorna stato pulsante note
        updateNotesButton();
        
        updateDisplay();
    }
    
    function updateNotesButton() {
        if (!notesAddBtn) return;
        
        const isAltro = selectedAct === 'Altro';
        const hasNotes = selectedNotes && selectedNotes.trim().length > 0;
        
        // Disabilita durante il timer
        notesAddBtn.disabled = timer.running;
        
        // Aggiorna classi
        notesAddBtn.classList.toggle('has-notes', hasNotes);
        notesAddBtn.classList.toggle('required', isAltro && !hasNotes);
        
        // Aggiorna testo
        if (notesBtnText) {
            if (hasNotes) {
                notesBtnText.textContent = '✓ Note aggiunte (clicca per modificare)';
            } else if (isAltro) {
                notesBtnText.textContent = 'Aggiungi note (obbligatorio per Altro)';
            } else {
                notesBtnText.textContent = 'Aggiungi note (opzionale)';
            }
        }
        
        // Badge obbligatorio
        if (notesBtnBadge) {
            notesBtnBadge.classList.toggle('hide', !isAltro || hasNotes);
        }
        
        // Preview note
        if (notesPreview) {
            if (hasNotes && !timer.running) {
                notesPreview.textContent = selectedNotes;
                notesPreview.classList.remove('hide');
            } else {
                notesPreview.classList.add('hide');
            }
        }
    }
    
    function startTick() {
        if (tickId) return;
        tickId = setInterval(updateDisplay, 1000);
    }
    
    function stopTick() {
        if (tickId) { clearInterval(tickId); tickId = null; }
    }
    
    function startTimer() {
        if (!selectedProj || !selectedAct) return;
        
        // Apri sempre il popup note prima di avviare
        openNotesModal(true); // true = modalità avvio
    }
    
    function doStartTimer() {
        // Avvia effettivamente il timer
        timer.running = true;
        timer.paused = false;
        timer.start = Date.now();
        timer.startedAt = Date.now();
        timer.elapsed = 0;
        timer.proj = selectedProj;
        timer.act = selectedAct;
        timer.notes = selectedNotes;
        saveTimerState();
        startTick();
        updateUI();
    }
    
    function pauseTimer() {
        if (!timer.running) return;
        if (timer.paused) {
            // Resume
            timer.start = Date.now();
            timer.paused = false;
        } else {
            // Pause
            timer.elapsed = getElapsed();
            timer.paused = true;
        }
        saveTimerState();
        updateUI();
    }
    
    async function stopTimer() {
        if (!timer.running) return;
        stopTick();
        await saveSession();
        clearTimerState();
        // Deseleziona progetto e attività dopo aver salvato
        selectedProj = null;
        selectedAct = null;
        selectedNotes = '';
        // Pulisce ultimo progetto salvato
        rememberProject('');
        // Aggiorna UI e re-renderizza liste per rimuovere selezione visiva
        renderProjects();
        renderActivities();
        updateUI();
        fetchSessions();
    }
    
    // === NOTES MODAL ===
    let notesModalStartMode = false; // true = avvia timer dopo conferma
    
    function openNotesModal(startMode = false) {
        notesModalStartMode = startMode;
        const isAltro = selectedAct === 'Altro';
        const hasNotes = selectedNotes && selectedNotes.trim().length > 0;
        
        // Configura il modal
        if (notesModalTitle) {
            if (startMode) {
                notesModalTitle.textContent = isAltro ? '📝 Note (Obbligatorie)' : '📝 Aggiungi Note';
            } else {
                notesModalTitle.textContent = '📝 Modifica Note';
            }
        }
        if (notesModalDesc) {
            if (startMode) {
                notesModalDesc.textContent = isAltro 
                    ? 'Descrivi brevemente l\'attività "Altro" che stai per eseguire.'
                    : 'Vuoi aggiungere una nota per questa attività? (opzionale)';
            } else {
                notesModalDesc.textContent = 'Modifica la nota per questa attività.';
            }
        }
        if (notesRequiredStar) {
            notesRequiredStar.classList.toggle('hide', !isAltro);
        }
        if (clearNotes) {
            clearNotes.classList.toggle('hide', !hasNotes || startMode);
        }
        if (confirmNotes) {
            confirmNotes.textContent = startMode ? '▶ Avvia' : '✓ Salva';
        }
        
        // Popola con note esistenti
        if (notesInput) {
            notesInput.value = selectedNotes || '';
            updateNotesHint();
        }
        
        notesModal.classList.remove('hide');
        setTimeout(() => notesInput?.focus(), 100);
    }
    
    function closeNotesModal() {
        notesModal.classList.add('hide');
        notesModalStartMode = false;
    }
    
    function updateNotesHint() {
        if (notesHint && notesInput) {
            const len = notesInput.value.length;
            notesHint.textContent = `${len}/500`;
            notesHint.style.color = 'var(--text-light)';
        }
    }
    
    function submitNotes() {
        const notes = (notesInput?.value || '').trim();
        const isAltro = selectedAct === 'Altro';
        const shouldStartTimer = notesModalStartMode; // Salva prima di chiudere
        
        // Se è "Altro" e non ci sono note, mostra errore
        if (isAltro && !notes) {
            if (notesHint) {
                notesHint.textContent = '⚠️ Le note sono obbligatorie per "Altro"';
                notesHint.style.color = 'var(--danger)';
            }
            notesInput?.focus();
            return;
        }
        
        selectedNotes = notes;
        closeNotesModal();
        
        // Se siamo in modalità avvio, avvia il timer
        if (shouldStartTimer) {
            doStartTimer();
            if (notes) {
                toast('✓ Attività avviata con note', 'ok');
            } else {
                toast('▶ Attività avviata', 'ok');
            }
        } else if (notes) {
            toast('✓ Note salvate', 'ok');
        }
    }
    
    function clearNotesAction() {
        selectedNotes = '';
        if (notesInput) notesInput.value = '';
        closeNotesModal();
        updateNotesButton();
        toast('Note rimosse', 'ok');
    }
    
    // === EVENTS ===
    projList.addEventListener('click', e => {
        const card = e.target.closest('.proj-item');
        if (!card || card.disabled) return;

        const code = card.dataset.code;
        selectedProj = projects.find(p => p.code === code) || null;
        rememberProject(code);

        document.querySelectorAll('.proj-item').forEach(el => 
            el.classList.toggle('selected', el.dataset.code === code)
        );

        updateUI();
        fetchSessions();
    });
    
    actGrid.addEventListener('click', e => {
        const btn = e.target.closest('.activity-btn');
        if (!btn || btn.disabled) return;
        
        selectedAct = btn.dataset.act;
        
        document.querySelectorAll('.activity-btn').forEach(el => 
            el.classList.toggle('selected', el.dataset.act === selectedAct)
        );
        
        // Aggiorna pulsante note
        updateNotesButton();
        
        updateUI();
    });
    
    // Event listener per il pulsante note
    if (notesAddBtn) {
        notesAddBtn.addEventListener('click', openNotesModal);
    }
    
    btnStart.addEventListener('click', startTimer);
    btnPause.addEventListener('click', pauseTimer);
    btnStop.addEventListener('click', stopTimer);
    if (themeBtn) themeBtn.addEventListener('click', toggleTheme);
    if (addBtn) addBtn.addEventListener('click', openAddModal);
    if (cancelAdd) cancelAdd.addEventListener('click', closeAddModal);
    if (confirmAdd) confirmAdd.addEventListener('click', addManualProject);
    if (cancelNotes) cancelNotes.addEventListener('click', closeNotesModal);
    if (confirmNotes) confirmNotes.addEventListener('click', submitNotes);
    if (clearNotes) clearNotes.addEventListener('click', clearNotesAction);
    if (notesInput) {
        notesInput.addEventListener('input', updateNotesHint);
    }
    
    if (keypadEl) {
        keypadEl.addEventListener('click', e => {
            const k = e.target.getAttribute('data-k');
            if (!k) return;
            if (k === 'ok') {
                addManualProject();
                return;
            }
            applyManualCode(k);
        });
    }
    
    // Swipe to refresh projects
    let touchStartY = 0;
    projList.addEventListener('touchstart', e => touchStartY = e.touches[0].clientY);
    projList.addEventListener('touchend', e => {
        const diff = e.changedTouches[0].clientY - touchStartY;
        if (diff > 50 && projList.scrollLeft === 0) {
            fetchProjects(true);
            toast('Aggiornamento...', '');
        }
    });
    
    // === ACTIVITIES CAROUSEL RENDER ===
    function renderActivities() {
        const totalPages = Math.ceil(ACTIVITIES.length / ACTIVITIES_PER_PAGE);
        const start = activityPage * ACTIVITIES_PER_PAGE;
        const pageActivities = ACTIVITIES.slice(start, start + ACTIVITIES_PER_PAGE);
        
        actGrid.innerHTML = pageActivities.map(a => `
            <button class="activity-btn${selectedAct === a.code ? ' selected' : ''}" data-act="${a.code}" title="${a.label}">
                <span class="activity-icon">${a.icon}</span>
                <span>${a.label}</span>
            </button>
        `).join('');
        
        // Aggiorna stato frecce
        const prevBtn = $('actPrev');
        const nextBtn = $('actNext');
        if (prevBtn) prevBtn.disabled = activityPage === 0;
        if (nextBtn) nextBtn.disabled = activityPage >= totalPages - 1;
    }
    
    function initActivitiesCarousel() {
        const prevBtn = $('actPrev');
        const nextBtn = $('actNext');
        
        if (prevBtn) {
            prevBtn.addEventListener('click', () => {
                if (activityPage > 0) {
                    activityPage--;
                    renderActivities();
                }
            });
        }
        
        if (nextBtn) {
            nextBtn.addEventListener('click', () => {
                const totalPages = Math.ceil(ACTIVITIES.length / ACTIVITIES_PER_PAGE);
                if (activityPage < totalPages - 1) {
                    activityPage++;
                    renderActivities();
                }
            });
        }
        
        renderActivities();
    }
    
    // === INIT ===
    loadTheme();
    loadTimerState();
    initSessionActionsBar();
    initActivitiesCarousel();
    
    // Se non c'è un timer locale attivo, controlla se c'è sul server (avviato da timbratura)
    async function initTimerFromServer() {
        if (!timer.running) {
            const loaded = await loadTimerFromServer();
            if (loaded) {
                selectedProj = timer.proj;
                selectedAct = timer.act;
                if (!timer.paused) startTick();
                updateUI();
                console.log('[MagTimer] Timer caricato dal server');
            }
        }
    }
    initTimerFromServer();
    
    // Restore running timer
    if (timer.running) {
        selectedProj = timer.proj;
        selectedAct = timer.act;
        if (!timer.paused) startTick();
        // Sincronizza timer esistente sul server (per visibilità admin)
        syncTimerToServer();
    }
    
    fetchProjects(false);
    fetchSessions();
    updateUI();
    
    // Auto-refresh projects every 5 min in background
    setInterval(() => {
        if (!timer.running && document.visibilityState === 'visible') {
            fetchProjects(true);
        }
    }, 5 * 60 * 1000);
    
    // Sincronizza stato timer dal server ogni 5 secondi (per rilevare pause/stop da timbratura)
    setInterval(() => {
        if (timer.running && document.visibilityState === 'visible') {
            syncTimerStateFromServer();
        }
    }, 5000);
    
    // Sincronizza anche quando la pagina torna visibile
    document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible' && timer.running) {
            syncTimerStateFromServer();
        }
    });
    
})();
