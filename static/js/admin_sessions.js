// v2.1 - Data odierna default + colonna Data nel grid
const root = document.documentElement;
const ctx = window.__ADMIN_SESSION_CONTEXT__ || {};

const startInput = document.getElementById('filterStart');
const endInput = document.getElementById('filterEnd');
const searchInput = document.getElementById('filterSearch');
const limitSelect = document.getElementById('filterLimit');
const refreshBtn = document.getElementById('refreshBtn');
const backBtn = document.getElementById('backHome');
const themeToggleBtn = document.getElementById('themeToggle');
const addSessionBtn = document.getElementById('addSessionBtn');
const listEl = document.getElementById('sessionsList');
const emptyEl = document.getElementById('emptyState');
const statusBanner = document.getElementById('statusBanner');
const sheet = document.getElementById('sessionSheet');
const sheetOverlay = document.getElementById('sheetOverlay');
const sheetTitle = document.getElementById('sheetTitle');
const closeSheetBtn = document.getElementById('closeSheetBtn');
const sessionForm = document.getElementById('sessionForm');
const overrideIdInput = document.getElementById('overrideId');
const deleteSessionBtn = document.getElementById('deleteSessionBtn');
const toastEl = document.getElementById('toast');

const formMemberKey = document.getElementById('formMemberKey');
const formOperator = document.getElementById('formOperator');
const formActivityId = document.getElementById('formActivityId');
const formActivityLabel = document.getElementById('formActivityLabel');
const formSource = document.getElementById('formSource');
const formProjectCode = document.getElementById('formProjectCode');
const formStart = document.getElementById('formStart');
const formEnd = document.getElementById('formEnd');
const formDuration = document.getElementById('formDuration');
const formPause = document.getElementById('formPause');
const formPauseStart = document.getElementById('formPauseStart');
const formPauseEnd = document.getElementById('formPauseEnd');
const formManualEntry = document.getElementById('formManualEntry');
const formNotes = document.getElementById('formNotes');
let durationTouched = false;
let pauseDurationTouched = false;


const dateTimeFormatter = new Intl.DateTimeFormat('it-IT', {
    dateStyle: 'short',
    timeStyle: 'medium'
});

const state = {
    loading: false,
    timer: null,
    abortController: null,
    sessions: [],
    activeSession: null,
    toastTimer: null,
};

function formatDateInput(date) {
    return date.toISOString().slice(0, 10);
}

function initDefaultFilters() {
    if (!startInput || !endInput) return;
    const today = new Date();
    if (!startInput.value) startInput.value = formatDateInput(today);
    if (!endInput.value) endInput.value = formatDateInput(today);
}

function setStatus(message) {
    if (!statusBanner) return;
    if (!message) {
        statusBanner.classList.add('hidden');
        statusBanner.textContent = '';
        return;
    }
    statusBanner.textContent = message;
    statusBanner.classList.remove('hidden');
}

function setLoading(isLoading) {
    state.loading = isLoading;
    if (refreshBtn) {
        refreshBtn.disabled = isLoading;
        refreshBtn.textContent = isLoading ? '⏳ Aggiornamento...' : '↻';
    }
}

function showToast(message, tone = 'info') {
    if (!toastEl || !message) return;
    toastEl.textContent = message;
    if (tone && tone !== 'info') {
        toastEl.dataset.tone = tone;
    } else {
        delete toastEl.dataset.tone;
    }
    toastEl.classList.remove('hidden');
    if (state.toastTimer) {
        clearTimeout(state.toastTimer);
    }
    state.toastTimer = setTimeout(() => {
        toastEl.classList.add('hidden');
    }, 2600);
}

function buildField(label, value) {
    const wrapper = document.createElement('div');
    wrapper.className = 'session-field';
    const labelEl = document.createElement('span');
    labelEl.textContent = label;
    const valueEl = document.createElement('span');
    valueEl.className = 'session-value';
    valueEl.textContent = value;
    wrapper.append(labelEl, valueEl);
    return wrapper;
}

function formatDateTime(ts) {
    if (!ts) return '-';
    try {
        return dateTimeFormatter.format(new Date(ts));
    } catch (_) {
        return '-';
    }
}

function formatDurationLabel(ms) {
    if (typeof ms !== 'number' || Number.isNaN(ms) || ms <= 0) {
        return '-';
    }
    const minutes = Math.round(ms / 60000);
    if (minutes >= 60) {
        const hours = (minutes / 60).toFixed(1);
        return `${hours} h`;
    }
    return `${minutes} min`;
}

function toDateTimeLocalValue(ms) {
    if (!ms) return '';
    const date = new Date(ms);
    if (Number.isNaN(date.getTime())) return '';
    const offset = date.getTimezoneOffset();
    const local = new Date(date.getTime() - offset * 60000);
    return local.toISOString().slice(0, 16);
}

function fromDateTimeLocalValue(value) {
    if (!value) return null;
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return null;
    return date.getTime();
}

function minutesToMs(value) {
    if (value === '' || value === null || value === undefined) return null;
    const minutes = Number(value);
    if (!Number.isFinite(minutes) || minutes < 0) return null;
    return Math.round(minutes * 60000);
}

function msToMinutes(ms) {
    if (typeof ms !== 'number' || Number.isNaN(ms)) return '';
    return Math.round(ms / 60000);
}

function updateThemeToggle() {
    if (!themeToggleBtn) return;
    const theme = root?.dataset?.theme || 'light';
    themeToggleBtn.textContent = theme === 'dark' ? '🌙' : '☀️';
}

function toggleTheme() {
    const current = root?.dataset?.theme || 'light';
    const next = current === 'dark' ? 'light' : 'dark';
    if (root) {
        root.dataset.theme = next;
    }
    localStorage.setItem('joblog-theme', next);
    updateThemeToggle();
}

function renderSessions(sessions) {
    if (!listEl || !emptyEl) return;
    state.sessions = sessions;
    listEl.innerHTML = '';
    if (!sessions.length) {
        emptyEl.classList.remove('hidden');
        return;
    }
    emptyEl.classList.add('hidden');

    sessions.forEach((session) => {
        const card = document.createElement('article');
        card.className = 'session-card';

        const header = document.createElement('div');
        header.className = 'session-header';

        const infoWrap = document.createElement('div');
        const operator = document.createElement('h3');
        operator.className = 'operator';
        const operatorLabel = session.member_name || session.member_key || 'Operatore';
        operator.textContent = `👷 ${operatorLabel}`;
        const activity = document.createElement('small');
        activity.textContent = session.activity_label || session.activity_id || 'Attività';
        infoWrap.append(operator, activity);

        const badges = document.createElement('div');
        badges.className = 'session-badges';

        // Badge fonte (Squadra/Magazzino)
        const sourceChip = document.createElement('span');
        sourceChip.className = 'session-badge';
        sourceChip.style.background = session.source === 'Magazzino' ? 'rgba(251, 146, 60, 0.2)' : 'rgba(34, 197, 94, 0.2)';
        sourceChip.style.color = session.source === 'Magazzino' ? '#ea580c' : '#16a34a';
        sourceChip.textContent = session.source || 'Squadra';
        badges.appendChild(sourceChip);

        const statusChip = document.createElement('span');
        statusChip.className = `status-chip ${session.status === 'completed' ? 'status-completed' : 'status-running'}`;
        statusChip.textContent = session.status === 'completed' ? 'Completata' : 'In corso';
        const manualChip = document.createElement('span');
        manualChip.className = `session-badge ${session.manual_entry ? 'manual' : 'auto'}`;
        manualChip.textContent = session.manual_entry ? 'Manuale' : 'Da log';
        badges.append(statusChip, manualChip);
        header.append(infoWrap, badges);
        card.appendChild(header);

        const body = document.createElement('div');
        body.className = 'session-body';
        // Estrai la data dalla sessione
        const sessionDate = session.start_ts ? new Date(session.start_ts).toLocaleDateString('it-IT', { weekday: 'short', day: '2-digit', month: '2-digit', year: 'numeric' }) : '—';
        body.appendChild(buildField('Data', sessionDate));
        const projectField = session.project_code ? buildField('Progetto', session.project_code) : null;
        if (projectField) body.appendChild(projectField);
        body.append(
            buildField('Inizio', formatDateTime(session.start_ts)),
            buildField('Fine', session.status === 'completed' ? formatDateTime(session.end_ts) : 'In corso'),
            buildField('Durata netta', session.net_hms || formatDurationLabel(session.net_ms)),
            buildField('Tempo pausa', session.pause_hms || formatDurationLabel(session.pause_ms)),
            buildField('N° pause', session.pause_count ?? 0)
        );
        card.appendChild(body);

        if (session.note) {
            const note = document.createElement('p');
            note.className = 'session-note';
            note.textContent = session.note;
            card.appendChild(note);
        }

        const footer = document.createElement('div');
        footer.className = 'session-footer';
        const meta = document.createElement('div');
        meta.className = 'session-meta';
        const operatorId = session.member_key || '—';
        const activityId = session.activity_id || '—';
        const parts = [
            `Operatore: ${operatorId}`,
            `Attività: ${activityId}`,
        ];
        if (session.override_id) {
            parts.push(`#${session.override_id}`);
        }
        footer.appendChild(meta);
        meta.textContent = parts.join(' • ');

        const actions = document.createElement('div');
        actions.className = 'session-actions';

        // Mostra modifica solo se editable (non per magazzino)
        if (session.editable !== false) {
            const editBtn = document.createElement('button');
            editBtn.type = 'button';
            editBtn.className = 'edit-btn';
            editBtn.textContent = '✏️ Modifica';
            editBtn.addEventListener('click', () => openSheetFor(session));
            actions.appendChild(editBtn);
        }

        if (session.override_id) {
            const delBtn = document.createElement('button');
            delBtn.type = 'button';
            delBtn.className = 'delete-btn';
            delBtn.textContent = '🗑 Elimina';
            delBtn.addEventListener('click', () => performDelete(session.override_id));
            actions.appendChild(delBtn);
        }

        footer.appendChild(actions);
        card.appendChild(footer);
        listEl.appendChild(card);
    });
}

function getQueryParams() {
    const params = new URLSearchParams();
    if (startInput?.value) params.set('start_date', startInput.value);
    if (endInput?.value) params.set('end_date', endInput.value);
    if (limitSelect?.value) params.set('limit', limitSelect.value);
    const searchValue = (searchInput?.value || '').trim();
    if (searchValue.length >= 2) {
        params.set('search', searchValue);
    }
    return params.toString();
}

async function fetchSessions() {
    if (!ctx.refreshUrl) return;
    if (state.abortController) {
        state.abortController.abort();
    }
    const controller = new AbortController();
    state.abortController = controller;

    setLoading(true);
    setStatus('Aggiornamento dati in corso...');

    try {
        const url = `${ctx.refreshUrl}?${getQueryParams()}`;
        const response = await fetch(url, {
            signal: controller.signal,
            headers: { 'Accept': 'application/json' },
        });
        if (response.status === 403) {
            setStatus('Accesso negato: serve un account Admin.');
            renderSessions([]);
            return;
        }
        if (!response.ok) {
            throw new Error(`Richiesta fallita (${response.status})`);
        }
        const payload = await response.json();
        const sessions = payload.sessions || [];
        renderSessions(sessions);
        const count = payload.count ?? sessions.length;
        setStatus(`Ultimo aggiornamento: ${dateTimeFormatter.format(new Date())} • ${count} sessioni`);
    } catch (error) {
        if (error.name === 'AbortError') return;
        console.error('Errore caricamento sessioni', error);
        setStatus('Impossibile aggiornare i dati. Controlla la connessione.');
    } finally {
        setLoading(false);
    }
}

function debounceFetch() {
    if (state.timer) {
        clearTimeout(state.timer);
    }
    state.timer = setTimeout(fetchSessions, 400);
}

function openSheetFor(session = null) {
    if (!sheet || !sheetOverlay || !sessionForm) return;
    state.activeSession = session;
    if (sheetTitle) {
        if (!session) {
            sheetTitle.textContent = 'Nuova sessione';
        } else if (session.override_id) {
            sheetTitle.textContent = 'Modifica sessione manuale';
        } else {
            sheetTitle.textContent = 'Sostituisci sessione';
        }
    }

    sessionForm.reset();
    populateForm(session);

    if (deleteSessionBtn) {
        if (session?.override_id) {
            deleteSessionBtn.classList.remove('hidden');
        } else {
            deleteSessionBtn.classList.add('hidden');
        }
    }

    sheet.classList.remove('hidden');
    sheetOverlay.classList.remove('hidden');
    requestAnimationFrame(() => {
        sheet.classList.add('visible');
        sheetOverlay.classList.add('visible');
    });
}

function closeSheet() {
    if (!sheet || !sheetOverlay) return;
    sheet.classList.remove('visible');
    sheetOverlay.classList.remove('visible');
    setTimeout(() => {
        sheet.classList.add('hidden');
        sheetOverlay.classList.add('hidden');
    }, 220);
    state.activeSession = null;
}

function populateForm(session) {
    if (!sessionForm) return;
    const isEditing = Boolean(session);
    durationTouched = false;
    pauseDurationTouched = false;
    overrideIdInput.value = session?.override_id ?? '';
    if (formSource) formSource.value = session?.source || 'Squadra';
    if (formProjectCode) formProjectCode.value = session?.project_code || '';
    formMemberKey.value = session?.member_key ?? '';
    formOperator.value = session?.member_name ?? '';
    formActivityId.value = session?.activity_id ?? '';
    formActivityLabel.value = session?.activity_label ?? '';
    formStart.value = session ? toDateTimeLocalValue(session.start_ts) : '';
    formEnd.value = session && session.status === 'completed' ? toDateTimeLocalValue(session.end_ts) : '';
    formDuration.value = isEditing && typeof session.net_ms === 'number' ? msToMinutes(session.net_ms) : '';
    formPause.value = isEditing && typeof session.pause_ms === 'number' ? msToMinutes(session.pause_ms) : '';
    if (formPauseStart) formPauseStart.value = '';
    if (formPauseEnd) formPauseEnd.value = '';
    formNotes.value = session?.note || '';
    if (formManualEntry) {
        if (!isEditing) {
            formManualEntry.checked = true;
        } else if (session.override_id) {
            formManualEntry.checked = Boolean(session.manual_entry);
        } else {
            formManualEntry.checked = false;
        }
    }
}

function gatherPayload() {
    const source = formSource ? formSource.value : 'Squadra';
    const projectCode = formProjectCode ? formProjectCode.value.trim() : '';
    const memberKey = formMemberKey.value.trim();
    const memberName = formOperator.value.trim() || memberKey;
    const activityId = formActivityId.value.trim();
    const activityLabel = formActivityLabel.value.trim() || activityId;
    if (!memberKey) throw new Error('ID operatore / Username obbligatorio.');
    if (!activityLabel) throw new Error('Descrizione attività obbligatoria.');

    const startMs = fromDateTimeLocalValue(formStart.value);
    if (startMs === null) throw new Error('Data/ora di inizio non valida.');
    const endMs = fromDateTimeLocalValue(formEnd.value);
    const durationMs = minutesToMs(formDuration.value);
    let pauseMs = minutesToMs(formPause.value) ?? 0;
    const pauseCount = 0;
    const manualEntry = formManualEntry ? Boolean(formManualEntry.checked) : true;
    const netMs = durationMs ?? (endMs ? Math.max(0, endMs - startMs) : 0);

    if ((!pauseDurationTouched || pauseMs === null) && formPauseStart && formPauseEnd) {
        const pauseStartMs = fromDateTimeLocalValue(formPauseStart.value);
        const pauseEndMs = fromDateTimeLocalValue(formPauseEnd.value);
        if (pauseStartMs !== null && pauseEndMs !== null && pauseEndMs >= pauseStartMs) {
            pauseMs = Math.round(pauseEndMs - pauseStartMs);
        }
    }
    if (pauseMs === null || pauseMs < 0) {
        pauseMs = 0;
    }

    const payload = {
        source: source,
        project_code: projectCode,
        member_key: memberKey,
        member_name: memberName,
        activity_id: activityId || activityLabel,
        activity_label: activityLabel,
        start_ts: startMs,
        end_ts: endMs,
        net_ms: netMs,
        pause_ms: pauseMs,
        pause_count: pauseCount,
        note: formNotes.value.trim(),
        manual_entry: manualEntry,
    };

    const overrideId = Number(overrideIdInput.value);
    if (overrideId) {
        payload.override_id = overrideId;
    }

    const activeSession = state.activeSession;
    if (activeSession) {
        const sourceMemberKey = activeSession.source_member_key || activeSession.member_key;
        const sourceActivityId = activeSession.source_activity_id || activeSession.activity_id;
        const sourceStart = activeSession.source_start_ts || activeSession.start_ts;
        if (sourceMemberKey && sourceActivityId && sourceStart) {
            payload.source_member_key = sourceMemberKey;
            payload.source_activity_id = sourceActivityId;
            payload.source_start_ts = sourceStart;
        }
    }

    return payload;
}

async function handleSave(event) {
    event.preventDefault();
    if (!ctx.saveUrl) {
        showToast('Endpoint di salvataggio non configurato', 'danger');
        return;
    }
    try {
        const payload = gatherPayload();
        const response = await fetch(ctx.saveUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
            body: JSON.stringify(payload),
        });
        const result = await response.json().catch(() => ({}));
        if (!response.ok || result.error) {
            throw new Error(result.error || 'Salvataggio non riuscito');
        }
        showToast('Sessione salvata', 'success');
        closeSheet();
        fetchSessions();
    } catch (error) {
        console.error('Errore salvataggio sessione', error);
        showToast(error.message || 'Errore durante il salvataggio', 'danger');
    }
}

async function performDelete(overrideId) {
    if (!overrideId) return;
    if (!ctx.deleteUrlTemplate) {
        showToast('Endpoint eliminazione non configurato', 'danger');
        return;
    }
    const confirmed = window.confirm('Eliminare questa sessione manuale?');
    if (!confirmed) return;
    try {
        const url = `${ctx.deleteUrlTemplate}${overrideId}`;
        const response = await fetch(url, {
            method: 'DELETE',
            headers: { 'Accept': 'application/json' },
        });
        const result = await response.json().catch(() => ({}));
        if (!response.ok || result.error) {
            throw new Error(result.error || 'Eliminazione non riuscita');
        }
        showToast('Sessione eliminata', 'success');
        closeSheet();
        fetchSessions();
    } catch (error) {
        console.error('Errore eliminazione sessione', error);
        showToast(error.message || 'Errore durante l\'eliminazione', 'danger');
    }
}

function handleSheetDelete() {
    const overrideId = Number(overrideIdInput.value);
    if (!overrideId) return;
    performDelete(overrideId);
}

function recalcDurationFromRange() {
    if (!formStart || !formEnd || !formDuration) return;
    if (durationTouched) return;
    const startMs = fromDateTimeLocalValue(formStart.value);
    const endMs = fromDateTimeLocalValue(formEnd.value);
    if (startMs === null || endMs === null) return;
    if (endMs < startMs) return;
    const diffMinutes = Math.round((endMs - startMs) / 60000);
    formDuration.value = diffMinutes;
}

function recalcPauseFromRange() {
    if (!formPause || !formPauseStart || !formPauseEnd) return;
    if (pauseDurationTouched) return;
    const startMs = fromDateTimeLocalValue(formPauseStart.value);
    const endMs = fromDateTimeLocalValue(formPauseEnd.value);
    if (startMs === null || endMs === null) return;
    if (endMs < startMs) return;
    const diffMinutes = Math.round((endMs - startMs) / 60000);
    formPause.value = diffMinutes;
}


function initEvents() {
    if (refreshBtn) refreshBtn.addEventListener('click', fetchSessions);
    if (backBtn && ctx.homeUrl) {
        backBtn.addEventListener('click', () => window.location.assign(ctx.homeUrl));
    }
    [startInput, endInput, limitSelect].forEach((input) => {
        if (!input) return;
        input.addEventListener('change', fetchSessions);
    });
    if (searchInput) {
        searchInput.addEventListener('input', debounceFetch);
    }
    if (themeToggleBtn) {
        themeToggleBtn.addEventListener('click', toggleTheme);
        updateThemeToggle();
    }
    if (addSessionBtn) {
        addSessionBtn.addEventListener('click', () => openSheetFor(null));
    }
    if (closeSheetBtn) {
        closeSheetBtn.addEventListener('click', closeSheet);
    }
    if (sheetOverlay) {
        sheetOverlay.addEventListener('click', closeSheet);
    }
    if (sessionForm) {
        sessionForm.addEventListener('submit', handleSave);
    }
    if (deleteSessionBtn) {
        deleteSessionBtn.addEventListener('click', handleSheetDelete);
    }
    if (formDuration) {
        formDuration.addEventListener('input', () => {
            durationTouched = true;
        });
    }
    if (formStart) {
        formStart.addEventListener('change', () => {
            durationTouched = false;
            recalcDurationFromRange();
        });
    }
    if (formEnd) {
        formEnd.addEventListener('change', () => {
            durationTouched = false;
            recalcDurationFromRange();
        });
    }
    if (formPause) {
        formPause.addEventListener('input', () => {
            pauseDurationTouched = true;
        });
    }
    if (formPauseStart) {
        formPauseStart.addEventListener('change', () => {
            pauseDurationTouched = false;
            recalcPauseFromRange();
        });
    }
    if (formPauseEnd) {
        formPauseEnd.addEventListener('change', () => {
            pauseDurationTouched = false;
            recalcPauseFromRange();
        });
    }
    document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') {
            closeSheet();
        }
    });
}

function init() {
    if (!listEl) return;
    initDefaultFilters();
    initEvents();
    fetchSessions();
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}
