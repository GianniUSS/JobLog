const ctx = window.__ADMIN_DASHBOARD__ || {};

const refreshBtn = document.getElementById('refreshBtn');
const hoursTotalEl = document.getElementById('hoursTotal');
const hoursTeamEl = document.getElementById('hoursTeam');
const hoursStatusEl = document.getElementById('hoursStatus');
const hoursLabelEl = document.getElementById('hoursLabel');
const sessionsTotalEl = document.getElementById('sessionsTotal');
const sessionsStatusEl = document.getElementById('sessionsStatus');
const summaryTable = document.getElementById('summaryTable');
const summaryBody = summaryTable ? summaryTable.querySelector('tbody') : null;
const summaryEmptyEl = document.getElementById('summaryEmpty');
const summaryDateEl = document.getElementById('summaryDate');
const dateInputStart = document.getElementById('dateInputStart');
const dateInputEnd = document.getElementById('dateInputEnd');
const applyDateBtn = document.getElementById('applyDate');
const themeToggle = document.getElementById('themeToggle');
const exportExcelBtn = document.getElementById('exportExcelBtn');
const sessionsTableBody = document.querySelector('#sessionsTable tbody');
const sessionsEmptyEl = document.getElementById('sessionsEmpty');

const _coerce = (v) => {
    if (v === null || v === undefined) return 0;
    if (typeof v === 'number') return v;
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
};

const fmt2 = (n) => String(n).padStart(2, '0');
function formatMs(ms) {
    const total = Math.max(0, Math.floor((ms || 0) / 1000));
    const h = Math.floor(total / 3600);
    const m = Math.floor((total % 3600) / 60);
    const s = total % 60;
    return `${fmt2(h)}:${fmt2(m)}:${fmt2(s)}`;
}

function setLoading(text) {
    if (hoursStatusEl) hoursStatusEl.textContent = text;
    if (sessionsStatusEl) sessionsStatusEl.textContent = text;
}

function renderSummary(items, dateLabel) {
    summaryDateEl.textContent = dateLabel || 'Oggi';
    if (!summaryBody) return;
    summaryBody.innerHTML = '';
    if (!items.length) {
        summaryEmptyEl.style.display = '';
        return;
    }
    summaryEmptyEl.style.display = 'none';
    items.forEach((item) => {
        const tr = document.createElement('tr');
        const tdCode = document.createElement('td');
        tdCode.textContent = item.project_code || '—';
        const tdHours = document.createElement('td');
        tdHours.textContent = formatMs(item.total_ms);
        const tdSessions = document.createElement('td');
        tdSessions.innerHTML = `<span class="badge">${item.sessions || 0}</span>`;
        tr.append(tdCode, tdHours, tdSessions);
        summaryBody.appendChild(tr);
    });
}

function setTotals(
    { total_ms = 0, total_sessions = 0, team_total_ms = 0, team_total_sessions = 0 },
    teamFallbackMs = 0,
    sessionsFallbackCount = 0,
    dateLabel = 'di oggi',
) {
    hoursTotalEl.textContent = formatMs(total_ms);
    const teamValue = team_total_ms || teamFallbackMs || 0;
    if (hoursTeamEl) hoursTeamEl.textContent = formatMs(teamValue);
    const sessionsValue = (total_sessions || 0) + (team_total_sessions || 0) || (sessionsFallbackCount || 0);
    sessionsTotalEl.textContent = sessionsValue;
    if (hoursLabelEl) hoursLabelEl.textContent = 'Ore registrate';
    hoursStatusEl.textContent = `Totale ore ${dateLabel}`;
    sessionsStatusEl.textContent = 'Sessioni registrate';
}

function renderSessionsGrid(items) {
    if (!sessionsTableBody) return;
    sessionsTableBody.innerHTML = '';
    if (!items.length) {
        if (sessionsEmptyEl) sessionsEmptyEl.style.display = '';
        return;
    }
    if (sessionsEmptyEl) sessionsEmptyEl.style.display = 'none';
    items.forEach((it) => {
        const tr = document.createElement('tr');
        const tdDate = document.createElement('td'); tdDate.textContent = it.date_label || '—';
        const tdSource = document.createElement('td'); tdSource.textContent = it.source || '—';
        const tdProj = document.createElement('td'); tdProj.textContent = it.project_code || '—';
        const tdUser = document.createElement('td'); tdUser.textContent = it.user || '—';
        const tdAct = document.createElement('td'); tdAct.textContent = it.activity || '—';
        const tdTime = document.createElement('td'); tdTime.textContent = formatMs(it.ms);
        tr.append(tdDate, tdSource, tdProj, tdUser, tdAct, tdTime);
        sessionsTableBody.appendChild(tr);
    });
}

async function fetchSummary(dateStart, dateEnd) {
    if (!ctx.summaryUrl) return { items: [] };
    const url = new URL(ctx.summaryUrl, window.location.origin);
    if (dateStart) url.searchParams.set('date_start', dateStart);
    if (dateEnd) url.searchParams.set('date_end', dateEnd);
    // Retrocompatibilità: se uguale, usa anche date singola
    if (dateStart && dateStart === dateEnd) url.searchParams.set('date', dateStart);
    const res = await fetch(url.toString());
    if (res.status === 403) throw new Error('Accesso negato');
    if (!res.ok) throw new Error(`Riepilogo: HTTP ${res.status}`);
    return res.json();
}

async function fetchDayData(dateStart, dateEnd) {
    if (!ctx.dayUrl) return { team_sessions: [], magazzino_sessions: [] };
    const url = new URL(ctx.dayUrl, window.location.origin);
    if (dateStart) url.searchParams.set('date_start', dateStart);
    if (dateEnd) url.searchParams.set('date_end', dateEnd);
    // Retrocompatibilità: se uguale, usa anche date singola
    if (dateStart && dateStart === dateEnd) url.searchParams.set('date', dateStart);
    const res = await fetch(url.toString());
    if (res.status === 403) throw new Error('Accesso negato');
    if (!res.ok) throw new Error(`Sessioni: HTTP ${res.status}`);
    return res.json();
}

function todayIso() {
    return new Date().toISOString().slice(0, 10);
}

function applyTheme(theme) {
    const root = document.documentElement;
    const next = theme || 'dark';
    root.dataset.theme = next;
    if (themeToggle) {
        themeToggle.textContent = next === 'dark' ? '☀️' : '🌙';
    }
    try { localStorage.setItem('joblog-theme', next); } catch {}
}

function initTheme() {
    const stored = localStorage.getItem('joblog-theme');
    const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
    applyTheme(stored || (prefersDark ? 'dark' : 'light'));
}

function formatDateLabel(ts) {
    if (!ts) return '—';
    try {
        return new Date(ts).toLocaleDateString('it-IT', { day: '2-digit', month: '2-digit', year: 'numeric' });
    } catch (_) {
        return '—';
    }
}

async function loadData(dateStart, dateEnd) {
    setLoading('Caricamento...');
    try {
        const [summaryData, dayData] = await Promise.all([
            fetchSummary(dateStart, dateEnd),
            fetchDayData(dateStart, dateEnd),
        ]);
        const magSessions = Array.isArray(dayData.magazzino_sessions) ? dayData.magazzino_sessions : [];
        const teamSessions = Array.isArray(dayData.team_sessions) ? dayData.team_sessions : [];
        const teamTotalMs = teamSessions.reduce((acc, s) => acc + (_coerce(s?.net_ms) || 0), 0);

        const today = todayIso();
        let statusLabel = 'di oggi';
        if (dateStart === dateEnd) {
            statusLabel = dateStart === today ? 'di oggi' : `del ${dateStart}`;
        } else {
            statusLabel = `dal ${dateStart} al ${dateEnd}`;
        }
        const dateLabel = (dateStart === dateEnd) ? (summaryData.date || dateStart || 'Oggi') : `${dateStart} → ${dateEnd}`;
        const summaryItems = Array.isArray(summaryData.items) ? summaryData.items : [];
        renderSummary(summaryItems, dateLabel);
        const sessionsFallbackCount = (teamSessions.length || 0) + (magSessions.length || 0);
        setTotals(summaryData, teamTotalMs, sessionsFallbackCount, statusLabel);
        const merged = [
            ...teamSessions.map((s) => ({
                source: 'Squadra',
                project_code: s.project_code,
                user: s.member_name || s.member_key,
                activity: s.activity_label || s.activity_id,
                ms: _coerce(s.net_ms) || 0,
                sort_ts: _coerce(s.end_ts || s.start_ts) || 0,
                date_label: formatDateLabel(s.start_ts || s.end_ts),
            })),
            ...magSessions.map((s) => ({
                source: 'Magazzino',
                project_code: s.project_code,
                user: s.username,
                activity: s.activity_label,
                ms: _coerce(s.elapsed_ms) || 0,
                sort_ts: _coerce(s.created_ts) || 0,
                date_label: formatDateLabel(s.created_ts),
            })),
        ].sort((a, b) => (b.sort_ts || 0) - (a.sort_ts || 0));
        renderSessionsGrid(merged);
    } catch (err) {
        console.error(err);
        const msg = err && err.message ? err.message : 'Errore imprevisto';
        if (hoursStatusEl) hoursStatusEl.textContent = msg;
        if (sessionsStatusEl) sessionsStatusEl.textContent = msg;
        summaryEmptyEl.style.display = '';
        if (sessionsEmptyEl) sessionsEmptyEl.style.display = '';
    }
}

function initDate() {
    const today = todayIso();
    if (dateInputStart && !dateInputStart.value) {
        dateInputStart.value = today;
    }
    if (dateInputEnd && !dateInputEnd.value) {
        dateInputEnd.value = today;
    }
}

function initEvents() {
    if (refreshBtn) {
        refreshBtn.addEventListener('click', () => {
            const start = dateInputStart?.value || todayIso();
            const end = dateInputEnd?.value || todayIso();
            loadData(start, end);
        });
    }
    if (applyDateBtn) {
        applyDateBtn.addEventListener('click', () => {
            const start = dateInputStart?.value || todayIso();
            const end = dateInputEnd?.value || todayIso();
            loadData(start, end);
        });
    }
    if (themeToggle) {
        themeToggle.addEventListener('click', () => {
            const current = document.documentElement.dataset.theme === 'light' ? 'dark' : 'light';
            applyTheme(current);
        });
    }

    if (exportExcelBtn) {
        exportExcelBtn.addEventListener('click', () => {
            if (!ctx.exportUrl) {
                console.warn('exportUrl non configurato');
                return;
            }
            const start = dateInputStart?.value || todayIso();
            const end = dateInputEnd?.value || todayIso();
            const url = new URL(ctx.exportUrl, window.location.origin);
            url.searchParams.set('date_start', start);
            url.searchParams.set('date_end', end);
            // Download diretto (server risponde con attachment)
            window.location.href = url.toString();
        });
    }
}

initDate();
initTheme();
initEvents();
loadData(dateInputStart?.value || todayIso(), dateInputEnd?.value || todayIso());
