// Admin Dashboard - Full Featured (PC optimized)
const ctx = window.__ADMIN_DASHBOARD__ || {};

// ============ DOM Elements ============
const dateStart = document.getElementById('dateStart');
const dateEnd = document.getElementById('dateEnd');
const btnToday = document.getElementById('btnToday');
const btnWeek = document.getElementById('btnWeek');
const btnMonth = document.getElementById('btnMonth');
const btnApply = document.getElementById('btnApply');
const filterSource = document.getElementById('filterSource');
const filterProject = document.getElementById('filterProject');
const filterUser = document.getElementById('filterUser');
const refreshBtn = document.getElementById('refreshBtn');
const exportBtn = document.getElementById('exportBtn');
const themeToggle = document.getElementById('themeToggle');
const tableSearch = document.getElementById('tableSearch');
const btnExportTable = document.getElementById('btnExportTable');
const mainTableBody = document.getElementById('mainTableBody');
const tableEmpty = document.getElementById('tableEmpty');

// Stats elements
const cardTotalHours = document.getElementById('cardTotalHours');
const cardTeamHours = document.getElementById('cardTeamHours');
const cardMagHours = document.getElementById('cardMagHours');
const cardTotalSessions = document.getElementById('cardTotalSessions');
const cardPeriodLabel = document.getElementById('cardPeriodLabel');
const cardTeamSessions = document.getElementById('cardTeamSessions');
const cardMagSessions = document.getElementById('cardMagSessions');
const cardAvgDuration = document.getElementById('cardAvgDuration');
const statTotalHours = document.getElementById('statTotalHours');
const statTeamHours = document.getElementById('statTeamHours');
const statMagHours = document.getElementById('statMagHours');
const statSessions = document.getElementById('statSessions');
const statProjects = document.getElementById('statProjects');
const statOperators = document.getElementById('statOperators');

// ============ State ============
let state = {
    teamSessions: [],
    magSessions: [],
    allSessions: [],
    filteredSessions: [],
    sortColumn: 'date',
    sortDir: 'desc',
    charts: {
        projects: null,
        trend: null,
        activities: null,
        comparison: null
    }
};

// ============ Utilities ============
const fmt2 = n => String(n).padStart(2, '0');

function formatMs(ms) {
    const total = Math.max(0, Math.floor((ms || 0) / 1000));
    const h = Math.floor(total / 3600);
    const m = Math.floor((total % 3600) / 60);
    const s = total % 60;
    return `${fmt2(h)}:${fmt2(m)}:${fmt2(s)}`;
}

function formatMsShort(ms) {
    const total = Math.max(0, Math.floor((ms || 0) / 1000));
    const h = Math.floor(total / 3600);
    const m = Math.floor((total % 3600) / 60);
    return `${fmt2(h)}:${fmt2(m)}`;
}

function formatDate(ts) {
    if (!ts) return '—';
    try {
        return new Date(ts).toLocaleDateString('it-IT', { day: '2-digit', month: '2-digit', year: 'numeric' });
    } catch { return '—'; }
}

function formatTime(ts) {
    if (!ts) return '—';
    try {
        return new Date(ts).toLocaleTimeString('it-IT', { hour: '2-digit', minute: '2-digit' });
    } catch { return '—'; }
}

function formatDateISO(date) {
    return date.toISOString().slice(0, 10);
}

function todayISO() {
    return formatDateISO(new Date());
}

function getDateRangeLabel(start, end) {
    const today = todayISO();
    if (start === end) {
        return start === today ? 'Oggi' : start;
    }
    return `${start} → ${end}`;
}

// ============ Theme ============
function applyTheme(theme) {
    document.documentElement.dataset.theme = theme;
    if (themeToggle) themeToggle.textContent = theme === 'dark' ? '☀️' : '🌙';
    localStorage.setItem('joblog-theme', theme);
    updateChartsTheme();
}

function toggleTheme() {
    const current = document.documentElement.dataset.theme || 'dark';
    applyTheme(current === 'dark' ? 'light' : 'dark');
}

function initTheme() {
    const stored = localStorage.getItem('joblog-theme');
    const prefersDark = window.matchMedia?.('(prefers-color-scheme: dark)').matches;
    applyTheme(stored || (prefersDark ? 'dark' : 'light'));
}

function getChartColors() {
    const isDark = document.documentElement.dataset.theme === 'dark';
    return {
        text: isDark ? '#e2e8f0' : '#0f172a',
        muted: isDark ? '#94a3b8' : '#475569',
        grid: isDark ? 'rgba(148, 163, 184, 0.1)' : 'rgba(15, 23, 42, 0.1)',
        brand: '#38bdf8',
        success: '#22c55e',
        warning: '#f59e0b',
        danger: '#ef4444'
    };
}

// ============ Date Presets ============
function setToday() {
    const today = todayISO();
    dateStart.value = today;
    dateEnd.value = today;
}

function setWeek() {
    const end = new Date();
    const start = new Date();
    start.setDate(start.getDate() - 6);
    dateStart.value = formatDateISO(start);
    dateEnd.value = formatDateISO(end);
}

function setMonth() {
    const end = new Date();
    const start = new Date();
    start.setDate(1);
    dateStart.value = formatDateISO(start);
    dateEnd.value = formatDateISO(end);
}

// ============ API ============
async function fetchDayData(startDate, endDate) {
    if (!ctx.dayUrl) return { team_sessions: [], magazzino_sessions: [] };
    const url = new URL(ctx.dayUrl, window.location.origin);
    url.searchParams.set('date_start', startDate);
    url.searchParams.set('date_end', endDate);
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
}

// ============ Data Processing ============
function processData(teamSessions, magSessions) {
    state.teamSessions = teamSessions || [];
    state.magSessions = magSessions || [];
    
    // Merge all sessions with normalized structure
    const all = [
        ...state.teamSessions.map(s => ({
            source: 'Squadra',
            date: s.start_ts,
            dateLabel: formatDate(s.start_ts),
            project: s.project_code || '—',
            user: s.member_name || s.member_key || '—',
            activity: s.activity_label || s.activity_id || '—',
            activityId: s.activity_id,
            start: s.start_ts,
            end: s.end_ts,
            startLabel: formatTime(s.start_ts),
            endLabel: s.status === 'completed' ? formatTime(s.end_ts) : 'In corso',
            duration: s.net_ms || 0,
            durationLabel: formatMs(s.net_ms),
            planned: s.planned_ms || 0,
            raw: s
        })),
        ...state.magSessions.map(s => ({
            source: 'Magazzino',
            date: s.created_ts,
            dateLabel: formatDate(s.created_ts),
            project: s.project_code || '—',
            user: s.username || '—',
            activity: s.activity_label || '—',
            start: s.created_ts,
            end: s.created_ts + (s.elapsed_ms || 0),
            startLabel: formatTime(s.created_ts),
            endLabel: formatTime(s.created_ts + (s.elapsed_ms || 0)),
            duration: s.elapsed_ms || 0,
            durationLabel: formatMs(s.elapsed_ms),
            raw: s
        }))
    ];
    
    state.allSessions = all;
    applyFilters();
}

function applyFilters() {
    let filtered = [...state.allSessions];
    
    // Source filter
    const source = filterSource?.value;
    if (source) {
        filtered = filtered.filter(s => s.source === source);
    }
    
    // Project filter
    const project = filterProject?.value?.trim().toLowerCase();
    if (project) {
        filtered = filtered.filter(s => s.project.toLowerCase().includes(project));
    }
    
    // User filter
    const user = filterUser?.value?.trim().toLowerCase();
    if (user) {
        filtered = filtered.filter(s => s.user.toLowerCase().includes(user));
    }
    
    // Search filter
    const search = tableSearch?.value?.trim().toLowerCase();
    if (search) {
        filtered = filtered.filter(s =>
            s.project.toLowerCase().includes(search) ||
            s.user.toLowerCase().includes(search) ||
            s.activity.toLowerCase().includes(search) ||
            s.source.toLowerCase().includes(search)
        );
    }
    
    // Sort
    filtered.sort((a, b) => {
        let valA = a[state.sortColumn];
        let valB = b[state.sortColumn];
        if (typeof valA === 'string') valA = valA.toLowerCase();
        if (typeof valB === 'string') valB = valB.toLowerCase();
        if (valA < valB) return state.sortDir === 'asc' ? -1 : 1;
        if (valA > valB) return state.sortDir === 'asc' ? 1 : -1;
        return 0;
    });
    
    state.filteredSessions = filtered;
    render();
}

// ============ Rendering ============
function render() {
    renderStats();
    renderMainTable();
    renderCharts();
}

function renderStats() {
    const teamMs = state.teamSessions.reduce((sum, s) => sum + (s.net_ms || 0), 0);
    const magMs = state.magSessions.reduce((sum, s) => sum + (s.elapsed_ms || 0), 0);
    const totalMs = teamMs + magMs;
    const teamCount = state.teamSessions.length;
    const magCount = state.magSessions.length;
    const totalCount = teamCount + magCount;
    
    // Unique projects and operators
    const projects = new Set([
        ...state.teamSessions.map(s => s.project_code).filter(Boolean),
        ...state.magSessions.map(s => s.project_code).filter(Boolean)
    ]);
    const operators = new Set([
        ...state.teamSessions.map(s => s.member_key || s.member_name).filter(Boolean),
        ...state.magSessions.map(s => s.username).filter(Boolean)
    ]);
    
    // Average duration
    const avgMs = totalCount > 0 ? totalMs / totalCount : 0;
    
    // Update cards
    if (cardTotalHours) cardTotalHours.textContent = formatMs(totalMs);
    if (cardTeamHours) cardTeamHours.textContent = formatMs(teamMs);
    if (cardMagHours) cardMagHours.textContent = formatMs(magMs);
    if (cardTotalSessions) cardTotalSessions.textContent = totalCount;
    if (cardTeamSessions) cardTeamSessions.textContent = `${teamCount} sessioni`;
    if (cardMagSessions) cardMagSessions.textContent = `${magCount} sessioni`;
    if (cardAvgDuration) cardAvgDuration.textContent = `Media: ${formatMsShort(avgMs)}`;
    if (cardPeriodLabel) cardPeriodLabel.textContent = getDateRangeLabel(dateStart?.value, dateEnd?.value);
    
    // Update sidebar stats
    if (statTotalHours) statTotalHours.textContent = formatMsShort(totalMs);
    if (statTeamHours) statTeamHours.textContent = formatMsShort(teamMs);
    if (statMagHours) statMagHours.textContent = formatMsShort(magMs);
    if (statSessions) statSessions.textContent = totalCount;
    if (statProjects) statProjects.textContent = projects.size;
    if (statOperators) statOperators.textContent = operators.size;
}

function renderMainTable() {
    if (!mainTableBody) return;
    
    const data = state.filteredSessions;
    
    if (!data.length) {
        mainTableBody.innerHTML = '';
        tableEmpty?.classList.remove('hidden');
        return;
    }
    
    tableEmpty?.classList.add('hidden');
    
    mainTableBody.innerHTML = data.map(s => `
        <tr>
            <td>${s.dateLabel}</td>
            <td><span class="badge badge-${s.source.toLowerCase()}">${s.source}</span></td>
            <td>${s.project}</td>
            <td>${s.user}</td>
            <td>${s.activity}</td>
            <td>${s.startLabel}</td>
            <td>${s.endLabel}</td>
            <td>${s.durationLabel}</td>
        </tr>
    `).join('');
    
    // Update sort indicators
    document.querySelectorAll('#mainTable th[data-sort]').forEach(th => {
        th.classList.remove('sorted-asc', 'sorted-desc');
        if (th.dataset.sort === state.sortColumn) {
            th.classList.add(state.sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc');
        }
    });
}

// ============ Charts ============
function renderCharts() {
    renderProjectsChart();
    renderTrendChart();
    renderActivitiesChart();
    renderComparisonChart();
}

function updateChartsTheme() {
    if (state.charts.projects || state.charts.trend || state.charts.activities || state.charts.comparison) {
        renderCharts();
    }
}

function renderProjectsChart() {
    const canvas = document.getElementById('chartProjects');
    if (!canvas) return;
    
    const colors = getChartColors();
    
    // Aggregate hours per project
    const projectHours = {};
    state.filteredSessions.forEach(s => {
        const key = s.project || 'N/D';
        projectHours[key] = (projectHours[key] || 0) + (s.duration / 3600000); // to hours
    });
    
    // Sort and take top 10
    const sorted = Object.entries(projectHours)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10);
    
    const labels = sorted.map(([k]) => k);
    const data = sorted.map(([, v]) => v.toFixed(2));
    
    if (state.charts.projects) state.charts.projects.destroy();
    
    state.charts.projects = new Chart(canvas, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Ore',
                data,
                backgroundColor: colors.brand,
                borderRadius: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false }
            },
            scales: {
                x: {
                    ticks: { color: colors.muted },
                    grid: { display: false }
                },
                y: {
                    ticks: { color: colors.muted },
                    grid: { color: colors.grid }
                }
            }
        }
    });
}

function renderTrendChart() {
    const canvas = document.getElementById('chartTrend');
    if (!canvas) return;
    
    const colors = getChartColors();
    
    // Aggregate by day
    const dayHours = {};
    state.filteredSessions.forEach(s => {
        const day = s.dateLabel;
        if (!dayHours[day]) dayHours[day] = { team: 0, mag: 0 };
        if (s.source === 'Squadra') {
            dayHours[day].team += s.duration / 3600000;
        } else {
            dayHours[day].mag += s.duration / 3600000;
        }
    });
    
    const sortedDays = Object.keys(dayHours).sort((a, b) => {
        const [dA, mA, yA] = a.split('/').map(Number);
        const [dB, mB, yB] = b.split('/').map(Number);
        return new Date(yA, mA - 1, dA) - new Date(yB, mB - 1, dB);
    });
    
    if (state.charts.trend) state.charts.trend.destroy();
    
    state.charts.trend = new Chart(canvas, {
        type: 'line',
        data: {
            labels: sortedDays,
            datasets: [
                {
                    label: 'Squadra',
                    data: sortedDays.map(d => dayHours[d].team.toFixed(2)),
                    borderColor: colors.success,
                    backgroundColor: colors.success + '33',
                    fill: true,
                    tension: 0.3
                },
                {
                    label: 'Magazzino',
                    data: sortedDays.map(d => dayHours[d].mag.toFixed(2)),
                    borderColor: colors.warning,
                    backgroundColor: colors.warning + '33',
                    fill: true,
                    tension: 0.3
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    labels: { color: colors.text }
                }
            },
            scales: {
                x: {
                    ticks: { color: colors.muted },
                    grid: { display: false }
                },
                y: {
                    ticks: { color: colors.muted },
                    grid: { color: colors.grid }
                }
            }
        }
    });
}

function renderActivitiesChart() {
    const canvas = document.getElementById('chartActivities');
    if (!canvas) return;
    
    const colors = getChartColors();
    
    // Aggregate hours per activity
    const activityHours = {};
    state.filteredSessions.forEach(s => {
        const key = s.activity || 'N/D';
        activityHours[key] = (activityHours[key] || 0) + (s.duration / 3600000);
    });
    
    // Sort and take top 10
    const sorted = Object.entries(activityHours)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10);
    
    const labels = sorted.map(([k]) => k.length > 20 ? k.substring(0, 18) + '...' : k);
    const data = sorted.map(([, v]) => v.toFixed(2));
    
    // Colors palette
    const palette = [
        '#38bdf8', '#22c55e', '#f59e0b', '#ef4444', '#a855f7',
        '#ec4899', '#06b6d4', '#84cc16', '#f97316', '#6366f1'
    ];
    
    if (state.charts.activities) state.charts.activities.destroy();
    
    state.charts.activities = new Chart(canvas, {
        type: 'doughnut',
        data: {
            labels,
            datasets: [{
                data,
                backgroundColor: palette.slice(0, data.length),
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: { 
                        color: colors.text,
                        font: { size: 11 },
                        boxWidth: 12
                    }
                }
            }
        }
    });
}

function renderComparisonChart() {
    const canvas = document.getElementById('chartComparison');
    if (!canvas) return;
    
    const colors = getChartColors();
    
    // Aggrega per attività solo sessioni Squadra
    const activityData = {};
    state.filteredSessions
        .filter(s => s.source === 'Squadra')
        .forEach(s => {
            const key = s.activity || 'N/D';
            if (!activityData[key]) {
                activityData[key] = { actual: 0, planned: 0 };
            }
            activityData[key].actual += s.duration / 3600000; // ore effettive
            // Il planned è per attività, non per sessione, quindi lo prendiamo una volta sola
            if (s.planned > 0 && activityData[key].planned === 0) {
                activityData[key].planned = s.planned / 3600000; // ore preventivate
            }
        });
    
    // Filtra solo attività con almeno un valore > 0
    const filtered = Object.entries(activityData)
        .filter(([, v]) => v.actual > 0 || v.planned > 0)
        .sort((a, b) => (b[1].actual + b[1].planned) - (a[1].actual + a[1].planned))
        .slice(0, 10);
    
    const labels = filtered.map(([k]) => k.length > 25 ? k.substring(0, 23) + '...' : k);
    const actualData = filtered.map(([, v]) => v.actual.toFixed(2));
    const plannedData = filtered.map(([, v]) => v.planned.toFixed(2));
    
    if (state.charts.comparison) state.charts.comparison.destroy();
    
    state.charts.comparison = new Chart(canvas, {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'Preventivato',
                    data: plannedData,
                    backgroundColor: 'rgba(99, 102, 241, 0.7)',
                    borderColor: '#6366f1',
                    borderWidth: 1,
                    borderRadius: 4
                },
                {
                    label: 'Effettivo',
                    data: actualData,
                    backgroundColor: 'rgba(34, 197, 94, 0.7)',
                    borderColor: '#22c55e',
                    borderWidth: 1,
                    borderRadius: 4
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    labels: { color: colors.text }
                },
                tooltip: {
                    callbacks: {
                        label: function(ctx) {
                            return `${ctx.dataset.label}: ${ctx.raw} ore`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    ticks: { color: colors.muted },
                    grid: { display: false }
                },
                y: {
                    ticks: { 
                        color: colors.muted,
                        callback: function(value) { return value + 'h'; }
                    },
                    grid: { color: colors.grid },
                    beginAtZero: true
                }
            }
        }
    });
}

// ============ Load Data ============
async function loadData() {
    const start = dateStart?.value || todayISO();
    const end = dateEnd?.value || todayISO();
    
    try {
        const data = await fetchDayData(start, end);
        processData(data.team_sessions || [], data.magazzino_sessions || []);
    } catch (err) {
        console.error('Load error:', err);
    }
}

// ============ Export ============
function exportExcel() {
    if (!ctx.exportUrl) return;
    const start = dateStart?.value || todayISO();
    const end = dateEnd?.value || todayISO();
    const url = new URL(ctx.exportUrl, window.location.origin);
    url.searchParams.set('date_start', start);
    url.searchParams.set('date_end', end);
    window.location.href = url.toString();
}

// ============ Sort ============
function handleSort(column) {
    if (state.sortColumn === column) {
        state.sortDir = state.sortDir === 'asc' ? 'desc' : 'asc';
    } else {
        state.sortColumn = column;
        state.sortDir = 'desc';
    }
    applyFilters();
}

// ============ Keyboard Shortcuts ============
function handleKeyboard(e) {
    // Ignore if typing in input
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') {
        return;
    }
    
    switch (e.key.toLowerCase()) {
        case 'r':
            e.preventDefault();
            loadData();
            break;
        case 'e':
            e.preventDefault();
            exportExcel();
            break;
        case 't':
            e.preventDefault();
            setToday();
            loadData();
            break;
        case 'arrowleft':
            e.preventDefault();
            // Previous day
            if (dateStart?.value && dateEnd?.value) {
                const d = new Date(dateStart.value);
                d.setDate(d.getDate() - 1);
                dateStart.value = formatDateISO(d);
                dateEnd.value = formatDateISO(d);
                loadData();
            }
            break;
        case 'arrowright':
            e.preventDefault();
            // Next day
            if (dateStart?.value && dateEnd?.value) {
                const d = new Date(dateEnd.value);
                d.setDate(d.getDate() + 1);
                dateStart.value = formatDateISO(d);
                dateEnd.value = formatDateISO(d);
                loadData();
            }
            break;
    }
}

// ============ Event Listeners ============
function initEvents() {
    // Hamburger menu
    const menuToggle = document.getElementById('menuToggle');
    const hamburgerMenu = document.getElementById('hamburgerMenu');
    const menuExport = document.getElementById('menuExport');
    
    menuToggle?.addEventListener('click', (e) => {
        e.stopPropagation();
        hamburgerMenu?.classList.toggle('open');
    });
    
    // Chiudi menu quando si clicca fuori
    document.addEventListener('click', (e) => {
        if (!hamburgerMenu?.contains(e.target) && !menuToggle?.contains(e.target)) {
            hamburgerMenu?.classList.remove('open');
        }
    });
    
    // Aggiorna link export nel menu con le date correnti
    if (menuExport) {
        menuExport.addEventListener('click', (e) => {
            e.preventDefault();
            exportExcel();
        });
    }
    
    // Date presets
    btnToday?.addEventListener('click', () => { setToday(); loadData(); });
    btnWeek?.addEventListener('click', () => { setWeek(); loadData(); });
    btnMonth?.addEventListener('click', () => { setMonth(); loadData(); });
    btnApply?.addEventListener('click', loadData);
    
    // Filters
    filterSource?.addEventListener('change', applyFilters);
    filterProject?.addEventListener('input', debounce(applyFilters, 300));
    filterUser?.addEventListener('input', debounce(applyFilters, 300));
    tableSearch?.addEventListener('input', debounce(applyFilters, 300));
    
    // Actions
    refreshBtn?.addEventListener('click', loadData);
    exportBtn?.addEventListener('click', exportExcel);
    btnExportTable?.addEventListener('click', exportExcel);
    themeToggle?.addEventListener('click', toggleTheme);
    
    // Sort
    document.querySelectorAll('#mainTable th[data-sort]').forEach(th => {
        th.addEventListener('click', () => handleSort(th.dataset.sort));
    });
    
    // Keyboard
    document.addEventListener('keydown', handleKeyboard);
}

function debounce(fn, delay) {
    let timeout;
    return (...args) => {
        clearTimeout(timeout);
        timeout = setTimeout(() => fn(...args), delay);
    };
}

// ============ Init ============
function init() {
    initTheme();
    setToday();
    initEvents();
    loadData();
}

init();
