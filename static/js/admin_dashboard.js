const ctx = window.__ADMIN_DASHBOARD__ || {};

const refreshBtn = document.getElementById('refreshBtn');
const hoursTotalEl = document.getElementById('hoursTotal');
const hoursTeamEl = document.getElementById('hoursTeam');
const hoursCombinedEl = document.getElementById('hoursCombined');
const combinedCardEl = document.getElementById('combinedCard');
const combinedProjectEl = document.getElementById('combinedProject');
const hoursStatusEl = document.getElementById('hoursStatus');
const dateInputStart = document.getElementById('dateInputStart');
const dateInputEnd = document.getElementById('dateInputEnd');
const projectFilterEl = document.getElementById('projectFilter');
const sourceFilterEl = document.getElementById('sourceFilter');
const applyDateBtn = document.getElementById('applyDate');
const themeToggle = document.getElementById('themeToggle');
const sessionsTableBody = document.querySelector('#sessionsTable tbody');
const sessionsEmptyEl = document.getElementById('sessionsEmpty');
const openSessionsTableBody = document.querySelector('#openSessionsTable tbody');
const openSessionsEmptyEl = document.getElementById('openSessionsEmpty');
const openSessionsSection = document.getElementById('openSessionsSection');

// Nuovi elementi per statistiche progetto
const projectStatsSection = document.getElementById('projectStatsSection');
const hoursPlannedEl = document.getElementById('hoursPlanned');
const hoursRealizedEl = document.getElementById('hoursRealized');
const progressBarEl = document.getElementById('progressBar');
const progressPercentEl = document.getElementById('progressPercent');
const hoursRemainingEl = document.getElementById('hoursRemaining');
const pieChartEl = document.getElementById('pieChart');
const pieLegendEl = document.getElementById('pieLegend');

// Colori per il grafico a torta
const PIE_COLORS = [
    '#6366f1', '#22c55e', '#f59e0b', '#ef4444', '#8b5cf6', 
    '#06b6d4', '#ec4899', '#14b8a6', '#f97316', '#84cc16',
    '#a855f7', '#0ea5e9', '#10b981', '#fbbf24', '#f43f5e'
];

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

function formatHours(ms) {
    const hours = (ms || 0) / 3600000;
    return hours.toFixed(1) + 'h';
}

function setLoading(text) {
    if (hoursStatusEl) hoursStatusEl.textContent = text;
}

function renderProjectStats(plannedMs, realizedMs, activityBreakdown) {
    if (!projectStatsSection) return;
    
    const projectFilter = projectFilterEl ? projectFilterEl.value : '';
    
    if (!projectFilter) {
        projectStatsSection.style.display = 'none';
        return;
    }
    
    projectStatsSection.style.display = '';
    
    // Aggiorna ore pianificate vs realizzate
    if (hoursPlannedEl) hoursPlannedEl.textContent = formatMs(plannedMs);
    if (hoursRealizedEl) hoursRealizedEl.textContent = formatMs(realizedMs);
    
    // Calcola percentuale di avanzamento per la barra
    const percent = plannedMs > 0 ? Math.min(100, Math.round((realizedMs / plannedMs) * 100)) : 0;
    if (progressBarEl) progressBarEl.style.width = percent + '%';
    
    // Ore rimanenti
    const remainingMs = Math.max(0, plannedMs - realizedMs);
    if (hoursRemainingEl) {
        if (plannedMs > 0) {
            if (realizedMs > plannedMs) {
                hoursRemainingEl.textContent = '⚠️ Superato di ' + formatMs(realizedMs - plannedMs);
                hoursRemainingEl.style.color = 'var(--warning, #f59e0b)';
            } else {
                hoursRemainingEl.textContent = 'Rimangono: ' + formatMs(remainingMs);
                hoursRemainingEl.style.color = 'var(--muted)';
            }
        } else {
            hoursRemainingEl.textContent = 'Nessuna pianificazione';
        }
    }
    
    // Renderizza grafico a torta
    renderPieChart(activityBreakdown);
}

function renderPieChart(breakdown) {
    if (!pieChartEl || !pieLegendEl) return;
    
    if (!breakdown || !breakdown.length) {
        pieChartEl.style.background = 'var(--border, #e2e8f0)';
        pieLegendEl.innerHTML = '<span style="color: var(--muted);">Nessun dato</span>';
        return;
    }
    
    // Crea grafico a torta con conic-gradient
    let gradientParts = [];
    let currentPercent = 0;
    
    breakdown.forEach((item, idx) => {
        const color = PIE_COLORS[idx % PIE_COLORS.length];
        const startPercent = currentPercent;
        currentPercent += item.percent;
        gradientParts.push(`${color} ${startPercent}% ${currentPercent}%`);
    });
    
    pieChartEl.style.background = `conic-gradient(${gradientParts.join(', ')})`;
    
    // Crea legenda
    let legendHtml = '';
    breakdown.forEach((item, idx) => {
        const color = PIE_COLORS[idx % PIE_COLORS.length];
        legendHtml += `
            <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 4px;">
                <span style="width: 12px; height: 12px; border-radius: 2px; background: ${color}; flex-shrink: 0;"></span>
                <span style="flex: 1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;" title="${item.label}">${item.label}</span>
                <span style="font-weight: 600; color: var(--text);">${item.percent}%</span>
            </div>
        `;
    });
    pieLegendEl.innerHTML = legendHtml;
}

function renderSummary(items, dateLabel) {
    // Nel nuovo layout non c'è una tabella riepilogo separata
    return;
}

function setTotals(
    { total_ms = 0, total_sessions = 0, team_total_ms = 0, team_total_sessions = 0 },
    teamFallbackMs = 0,
    sessionsFallbackCount = 0,
    dateLabel = 'di oggi',
) {
    if (hoursTotalEl) hoursTotalEl.textContent = formatMs(total_ms);
    const teamValue = team_total_ms || teamFallbackMs || 0;
    if (hoursTeamEl) hoursTeamEl.textContent = formatMs(teamValue);
    if (hoursStatusEl) hoursStatusEl.textContent = `Totale ore ${dateLabel}`;
}

function renderSessionsGrid(items) {
    if (!sessionsTableBody) return;
    sessionsTableBody.innerHTML = '';
    if (!items.length) {
        if (sessionsEmptyEl) sessionsEmptyEl.style.display = '';
        return;
    }
    if (sessionsEmptyEl) sessionsEmptyEl.style.display = 'none';
    
    // Helper per formattare timestamp in ora HH:MM:SS
    function formatTime(ts) {
        if (!ts) return '—';
        const d = new Date(ts);
        if (isNaN(d.getTime())) return '—';
        return d.toLocaleTimeString('it-IT', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    }
    
    items.forEach((it) => {
        const tr = document.createElement('tr');
        const tdDate = document.createElement('td'); tdDate.textContent = it.date_label || '—';
        const tdSource = document.createElement('td'); tdSource.textContent = it.source || '—';
        const tdProj = document.createElement('td'); tdProj.textContent = it.project_code || '—';
        const tdUser = document.createElement('td'); tdUser.textContent = it.user || '—';
        const tdAct = document.createElement('td'); tdAct.textContent = it.activity || '—';
        const tdNotes = document.createElement('td'); 
        tdNotes.textContent = it.notes || '—';
        tdNotes.style.maxWidth = '200px';
        tdNotes.style.overflow = 'hidden';
        tdNotes.style.textOverflow = 'ellipsis';
        tdNotes.style.whiteSpace = 'nowrap';
        tdNotes.title = it.notes || '';
        
        const tdPause = document.createElement('td');
        if (it.pause_count > 0) {
            tdPause.innerHTML = '<span style="font-size: 11px;">⏸️ ' + it.pause_count + '</span>' +
                '<br><span style="font-size: 10px; color: var(--muted, #94a3b8);">' + formatMs(it.pause_ms) + '</span>';
            tdPause.title = it.pause_count + ' pause, totale: ' + formatMs(it.pause_ms);
        } else {
            tdPause.textContent = '—';
            tdPause.style.color = 'var(--muted, #94a3b8)';
        }
        
        const tdStart = document.createElement('td'); tdStart.textContent = formatTime(it.start_ts);
        const tdEnd = document.createElement('td'); tdEnd.textContent = formatTime(it.end_ts);
        const tdTime = document.createElement('td'); tdTime.textContent = formatMs(it.ms);
        tr.append(tdDate, tdSource, tdProj, tdUser, tdAct, tdNotes, tdPause, tdStart, tdEnd, tdTime);
        sessionsTableBody.appendChild(tr);
    });
}

function renderOpenSessionsGrid(items) {
    if (!openSessionsTableBody) return;
    openSessionsTableBody.innerHTML = '';
    
    if (!items || !items.length) {
        if (openSessionsEmptyEl) openSessionsEmptyEl.style.display = '';
        if (openSessionsSection) openSessionsSection.style.display = 'none';
        return;
    }
    
    if (openSessionsEmptyEl) openSessionsEmptyEl.style.display = 'none';
    if (openSessionsSection) openSessionsSection.style.display = '';
    
    // Helper per formattare timestamp in ora HH:MM:SS
    function formatTime(ts) {
        if (!ts) return '—';
        const d = new Date(ts);
        if (isNaN(d.getTime())) return '—';
        return d.toLocaleTimeString('it-IT', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    }
    
    // Helper per formattare data
    function formatDate(ts) {
        if (!ts) return '—';
        const d = new Date(ts);
        if (isNaN(d.getTime())) return '—';
        return d.toLocaleDateString('it-IT', { day: '2-digit', month: '2-digit', year: 'numeric' });
    }
    
    items.forEach((it) => {
        const tr = document.createElement('tr');
        
        const tdDate = document.createElement('td');
        tdDate.textContent = formatDate(it.start_ts);
        
        const tdProj = document.createElement('td'); 
        tdProj.textContent = it.project_code || '—';
        
        const tdUser = document.createElement('td'); 
        tdUser.textContent = it.member_name || '—';
        
        const tdAct = document.createElement('td'); 
        tdAct.textContent = it.activity_label || it.activity_id || '—';
        
        const tdNotes = document.createElement('td');
        tdNotes.textContent = it.notes || '—';
        tdNotes.style.maxWidth = '200px';
        tdNotes.style.overflow = 'hidden';
        tdNotes.style.textOverflow = 'ellipsis';
        tdNotes.style.whiteSpace = 'nowrap';
        tdNotes.style.fontSize = '12px';
        tdNotes.style.color = 'var(--muted, #64748b)';
        if (it.notes) tdNotes.title = it.notes;
        
        const tdStatus = document.createElement('td');
        let statusHtml = '';
        if (it.paused) {
            statusHtml = '<span style="color: var(--warning, #f59e0b);">⏸️ In pausa</span>';
        } else if (it.running) {
            statusHtml = '<span style="color: var(--success, #22c55e);">▶️ In corso</span>';
        } else {
            statusHtml = '—';
        }
        // Aggiungi conteggio pause se > 0
        if (it.pause_count > 0) {
            statusHtml += '<br><span style="font-size: 11px; color: var(--muted, #94a3b8);">🔄 ' + it.pause_count + ' pause</span>';
        }
        tdStatus.innerHTML = statusHtml;
        
        const tdStart = document.createElement('td'); 
        tdStart.textContent = formatTime(it.start_ts);
        
        const tdTime = document.createElement('td'); 
        tdTime.textContent = formatMs(it.elapsed_ms || 0);
        tdTime.style.fontWeight = '600';
        if (it.running && !it.paused) {
            tdTime.style.color = 'var(--primary, #6366f1)';
        }
        
        tr.append(tdDate, tdProj, tdUser, tdAct, tdNotes, tdStatus, tdStart, tdTime);
        openSessionsTableBody.appendChild(tr);
    });
}

async function fetchOpenSessions() {
    if (!ctx.openSessionsUrl) return { open_sessions: [] };
    const res = await fetch(ctx.openSessionsUrl);
    if (res.status === 403) throw new Error('Accesso negato');
    if (!res.ok) throw new Error(`Sessioni aperte: HTTP ${res.status}`);
    return res.json();
}

async function fetchProjectsList() {
    try {
        const res = await fetch('/api/admin/projects-list');
        if (!res.ok) return { projects: [] };
        return res.json();
    } catch (e) {
        console.error('Errore caricamento progetti:', e);
        return { projects: [] };
    }
}

async function fetchSummary(dateStart, dateEnd, project = null) {
    if (!ctx.summaryUrl) return { items: [] };
    const url = new URL(ctx.summaryUrl, window.location.origin);
    if (dateStart) url.searchParams.set('date_start', dateStart);
    if (dateEnd) url.searchParams.set('date_end', dateEnd);
    // Retrocompatibilità: se uguale, usa anche date singola
    if (dateStart && dateStart === dateEnd) url.searchParams.set('date', dateStart);
    if (project) url.searchParams.set('project', project);
    const res = await fetch(url.toString());
    if (res.status === 403) throw new Error('Accesso negato');
    if (!res.ok) throw new Error(`Riepilogo: HTTP ${res.status}`);
    return res.json();
}

async function fetchDayData(dateStart, dateEnd, project = null) {
    if (!ctx.dayUrl) return { team_sessions: [], magazzino_sessions: [] };
    const url = new URL(ctx.dayUrl, window.location.origin);
    if (dateStart) url.searchParams.set('date_start', dateStart);
    if (dateEnd) url.searchParams.set('date_end', dateEnd);
    // Retrocompatibilità: se uguale, usa anche date singola
    if (dateStart && dateStart === dateEnd) url.searchParams.set('date', dateStart);
    if (project) url.searchParams.set('project', project);
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
    const projectFilter = projectFilterEl ? projectFilterEl.value : '';
    
    try {
        const [summaryData, dayData, openSessionsData] = await Promise.all([
            fetchSummary(dateStart, dateEnd, projectFilter || null),
            fetchDayData(dateStart, dateEnd, projectFilter || null),
            fetchOpenSessions(),
        ]);
        const magSessions = Array.isArray(dayData.magazzino_sessions) ? dayData.magazzino_sessions : [];
        const teamSessions = Array.isArray(dayData.team_sessions) ? dayData.team_sessions : [];
        const teamTotalMs = dayData.team_total_ms || teamSessions.reduce((acc, s) => acc + (_coerce(s?.net_ms) || 0), 0);
        const magTotalMs = dayData.magazzino_total_ms || magSessions.reduce((acc, s) => acc + (_coerce(s?.elapsed_ms) || 0), 0);
        const combinedTotalMs = dayData.combined_total_ms || (teamTotalMs + magTotalMs);

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
        
        // Mostra/nascondi card totale combinato se c'è un filtro progetto
        if (projectFilter && combinedCardEl) {
            combinedCardEl.style.display = '';
            if (hoursCombinedEl) hoursCombinedEl.textContent = formatMs(combinedTotalMs);
            if (combinedProjectEl) combinedProjectEl.textContent = `Progetto: ${projectFilter}`;
        } else if (combinedCardEl) {
            combinedCardEl.style.display = 'none';
        }
        
        // Render statistiche progetto (ore pianificate vs realizzate + torta)
        const plannedMs = dayData.planned_total_ms || 0;
        const activityBreakdown = dayData.activity_breakdown || [];
        renderProjectStats(plannedMs, combinedTotalMs, activityBreakdown);
        
        // Render sessioni aperte
        const openSessions = Array.isArray(openSessionsData.open_sessions) ? openSessionsData.open_sessions : [];
        renderOpenSessionsGrid(openSessions);
        
        // Filtra solo le sessioni COMPLETATE (status !== 'running') per le Sessioni Registrate
        const completedTeamSessions = teamSessions.filter((s) => s.status === 'completed');
        
        let merged = [
            ...completedTeamSessions.map((s) => ({
                source: 'Squadra',
                project_code: s.project_code,
                user: s.member_name || s.member_key,
                activity: s.activity_label || s.activity_id,
                notes: s.note || '',
                pause_count: s.pause_count || 0,
                pause_ms: s.pause_ms || 0,
                start_ts: _coerce(s.start_ts) || null,
                end_ts: _coerce(s.end_ts) || null,
                ms: _coerce(s.net_ms) || 0,
                sort_ts: _coerce(s.end_ts || s.start_ts) || 0,
                date_label: formatDateLabel(s.start_ts || s.end_ts),
            })),
            ...magSessions.map((s) => ({
                source: 'Magazzino',
                project_code: s.project_code,
                user: s.username,
                activity: s.activity_label,
                notes: s.note || '',
                pause_count: 0,
                pause_ms: 0,
                start_ts: _coerce(s.start_ts) || null,
                end_ts: _coerce(s.end_ts) || null,
                ms: _coerce(s.elapsed_ms) || 0,
                sort_ts: _coerce(s.created_ts) || 0,
                date_label: formatDateLabel(s.created_ts),
            })),
        ].sort((a, b) => (b.sort_ts || 0) - (a.sort_ts || 0));
        
        // Applica filtro fonte (Squadra/Magazzino)
        const sourceFilter = sourceFilterEl ? sourceFilterEl.value : '';
        if (sourceFilter) {
            merged = merged.filter(s => s.source === sourceFilter);
        }
        
        renderSessionsGrid(merged);
    } catch (err) {
        console.error(err);
        const msg = err && err.message ? err.message : 'Errore imprevisto';
        if (hoursStatusEl) hoursStatusEl.textContent = msg;
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

async function loadProjectsList() {
    if (!projectFilterEl) return;
    try {
        const data = await fetchProjectsList();
        const projects = data.projects || [];
        // Mantieni l'opzione "Tutti"
        projectFilterEl.innerHTML = '<option value="">Tutti i progetti</option>';
        projects.forEach(p => {
            const opt = document.createElement('option');
            opt.value = p;
            opt.textContent = p;
            projectFilterEl.appendChild(opt);
        });
    } catch (e) {
        console.error('Errore caricamento lista progetti:', e);
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
}

initDate();
initTheme();
initEvents();
loadProjectsList();
loadData(dateInputStart?.value || todayIso(), dateInputEnd?.value || todayIso());

// Auto-refresh ogni 30 secondi per aggiornare i totali delle sessioni in corso
setInterval(() => {
    const start = dateInputStart?.value || todayIso();
    const end = dateInputEnd?.value || todayIso();
    loadData(start, end);
}, 30000);
