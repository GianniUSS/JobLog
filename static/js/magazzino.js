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
    let manualCode = '';
    
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
    
    function saveTimerState() {
        try {
            localStorage.setItem(TIMER_KEY, JSON.stringify(timer));
        } catch {}
    }
    
    function clearTimerState() {
        timer = { running: false, paused: false, start: 0, elapsed: 0, proj: null, act: null, notes: '' };
        localStorage.removeItem(TIMER_KEY);
        selectedNotes = '';
        // Aggiorna pulsante note
        updateNotesButton();
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

        // Se non c'è un progetto selezionato (e timer non in corso), ripristina l'ultimo usato oppure seleziona il primo
        if (!selectedProj && !timer.running) {
            let toSelect = null;
            const lastCode = loadLastProject();
            if (lastCode) {
                toSelect = projects.find(p => p.code === lastCode) || null;
            }
            if (!toSelect && projects.length) {
                toSelect = projects[0];
            }
            if (toSelect) {
                selectedProj = toSelect;
                const btn = Array.from(projList.querySelectorAll('.proj-item'))
                    .find(el => el.dataset.code === toSelect.code);
                if (btn) btn.classList.add('selected');
                updateUI();
                fetchSessions();
            }
        }
    }
    
    function renderSessions(items) {
        if (!items.length) {
            sessionsList.innerHTML = '<div class="sessions-empty"><div class="sessions-empty-icon">⏱️</div><div>Nessuna sessione</div></div>';
            totalTimeEl.textContent = '—';
            return;
        }
        
        let total = 0;
        sessionsList.innerHTML = items.map(s => {
            total += s.elapsed_ms || 0;
            const hasNote = s.note && s.note.trim();
            const noteHtml = hasNote 
                ? `<div class="session-note">
                    <span class="session-note-label">📝 Note</span>
                    ${s.note}
                   </div>` 
                : '';
            // Mostra orari inizio-fine se disponibili
            const startTime = s.start_ts ? fmtHourMin(s.start_ts) : '';
            const endTime = s.end_ts ? fmtHourMin(s.end_ts) : '';
            const timeRangeHtml = (startTime && endTime) 
                ? `<span class="session-time-range">🕐 ${startTime} → ${endTime}</span>` 
                : '';
            return `<div class="session-item">
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
        updateUI();
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
    
    // === INIT ===
    loadTheme();
    loadTimerState();
    
    // Restore running timer
    if (timer.running) {
        selectedProj = timer.proj;
        selectedAct = timer.act;
        if (!timer.paused) startTick();
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
    
})();
