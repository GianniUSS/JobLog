/**
 * HR / Payroll Dashboard — Professional v2
 * Chart.js 4.x + datalabels plugin
 */
(function () {
    'use strict';

    /* ── Config ── */
    const ctx = window.__PAYROLL_DASHBOARD__ || {};
    const CIRCUMFERENCE = 2 * Math.PI * 22; // 138.23

    /* ── DOM refs ── */
    const $ = (id) => document.getElementById(id);
    const monthInput     = $('monthInput');
    const yearInput      = $('yearInput');
    const applyBtn       = $('applyFilters');
    const groupFilter    = $('groupFilter');
    const loadingOverlay = $('loadingOverlay');
    const periodLabel    = $('periodLabel');

    /* KPIs */
    const kpiEmployees  = $('kpiEmployees');
    const kpiTotalHours = $('kpiTotalHours');
    const kpiOvertime   = $('kpiOvertime');
    const kpiAbsences   = $('kpiAbsences');

    /* Charts – kept as references for destroy */
    let charts = {};

    /* ── Constants ── */
    const MONTH_NAMES = [
        'Gennaio','Febbraio','Marzo','Aprile','Maggio','Giugno',
        'Luglio','Agosto','Settembre','Ottobre','Novembre','Dicembre'
    ];
    const PALETTE = [
        '#6366f1','#10b981','#f59e0b','#ef4444','#8b5cf6',
        '#06b6d4','#ec4899','#14b8a6','#f97316','#84cc16',
        '#a855f7','#0ea5e9','#d946ef','#fbbf24','#f43f5e'
    ];
    const AVATAR_COLORS = [
        '#6366f1','#10b981','#f59e0b','#ef4444','#8b5cf6',
        '#06b6d4','#ec4899','#14b8a6'
    ];

    /* ── Helpers ── */
    const now = new Date();
    function showLoading() { if (loadingOverlay) loadingOverlay.style.display = 'flex'; }
    function hideLoading() { if (loadingOverlay) loadingOverlay.style.display = 'none'; }

    function fmtHours(minutes) {
        if (!minutes && minutes !== 0) return '0h';
        const h = Math.floor(Math.abs(minutes) / 60);
        const m = Math.abs(minutes) % 60;
        const sign = minutes < 0 ? '-' : '';
        return m > 0 ? `${sign}${h}h ${m}m` : `${sign}${h}h`;
    }

    function fmtHoursShort(minutes) {
        const h = Math.round(Math.abs(minutes) / 60 * 10) / 10;
        return (minutes < 0 ? '-' : '') + h + 'h';
    }

    function destroyChart(key) {
        if (charts[key]) { charts[key].destroy(); charts[key] = null; }
    }

    function emptyState(canvasId, show) {
        const el = $(canvasId);
        if (!el) return;
        const es = el.parentElement.querySelector('.empty-state');
        if (es) es.classList.toggle('hidden', !show);
    }

    function getCSS(prop) {
        return getComputedStyle(document.documentElement).getPropertyValue(prop).trim();
    }

    function chartDefaults() {
        Chart.defaults.color = getCSS('--text-secondary') || '#94a3b8';
        Chart.defaults.borderColor = getCSS('--border') || 'rgba(148,163,184,0.08)';
        Chart.defaults.font.family = "'Inter', -apple-system, system-ui, sans-serif";
        Chart.defaults.font.size = 11;
        Chart.defaults.font.weight = 500;
        // Disable global datalabels, enable per-chart
        Chart.defaults.plugins.datalabels = { display: false };
    }

    function avatarColor(name) {
        let hash = 0;
        for (let i = 0; i < (name || '').length; i++) hash = name.charCodeAt(i) + ((hash << 5) - hash);
        return AVATAR_COLORS[Math.abs(hash) % AVATAR_COLORS.length];
    }

    function deltaHTML(val, unit, invertColor) {
        // invertColor: for metrics where higher is worse (absences, late)
        if (val === 0) return '<span class="delta neutral">— 0</span>';
        const isPositive = val > 0;
        let cls = isPositive ? 'up' : 'down';
        if (invertColor) cls = isPositive ? 'down' : 'up'; // invert
        const arrow = isPositive ? '↑' : '↓';
        const display = unit ? `${arrow} ${Math.abs(val)}${unit}` : `${arrow} ${fmtHoursShort(Math.abs(val) * 60)}`;
        return `<span class="delta ${cls}">${display}</span>`;
    }

    /* ── Ring gauge update ── */
    function updateRing(ringId, value, max, label) {
        const ring = $(ringId);
        if (!ring) return;
        const pct = max > 0 ? Math.min(value / max, 1) : 0;
        const fill = ring.querySelector('.ring-fill');
        const text = ring.querySelector('.ring-text');
        if (fill) fill.setAttribute('stroke-dashoffset', CIRCUMFERENCE * (1 - pct));
        if (text) text.textContent = label || `${Math.round(pct * 100)}%`;
    }

    /* ── Data fetch ── */
    async function fetchPayrollData(month, year, group) {
        const url = new URL(ctx.apiUrl || '/api/admin/payroll-dashboard', location.origin);
        url.searchParams.set('month', month);
        url.searchParams.set('year', year);
        if (group) url.searchParams.set('group', group);
        const res = await fetch(url.toString());
        if (res.status === 403) throw new Error('Accesso negato');
        if (!res.ok) throw new Error(`Errore HTTP ${res.status}`);
        return res.json();
    }

    async function fetchGroups() {
        try {
            const res = await fetch('/api/admin/groups');
            if (!res.ok) return [];
            const data = await res.json();
            return data.groups || [];
        } catch { return []; }
    }

    /* ══════════════════════════════ RENDERERS ══════════════════════════════ */

    /* ── KPIs ── */
    function renderKPIs(data) {
        if (kpiEmployees) kpiEmployees.textContent = data.total_employees || 0;
        if (kpiTotalHours) kpiTotalHours.textContent = fmtHours(data.total_worked_minutes || 0);
        if (kpiOvertime) kpiOvertime.textContent = fmtHours(data.total_overtime_minutes || 0);
        if (kpiAbsences) kpiAbsences.textContent = data.total_absence_days || 0;

        const sub = $('kpiEmployeesSub');
        if (sub) sub.textContent = `${data.business_days_in_month || 0} giorni lavorativi`;

        // Deltas
        const delta = data.delta || {};
        const dHours = $('kpiHoursDelta');
        if (dHours) dHours.outerHTML = deltaHTML(delta.worked_minutes || 0, null, false);

        const dOt = $('kpiOvertimeDelta');
        if (dOt) dOt.outerHTML = deltaHTML(delta.overtime_minutes || 0, null, false);

        const dAbs = $('kpiAbsenceDelta');
        if (dAbs) dAbs.outerHTML = deltaHTML(delta.absence_days || 0, 'gg', true);
    }

    /* ── Rate Rings ── */
    function renderRates(data) {
        updateRing('ringPunctuality', data.punctuality_rate || 0, 100,
            `${data.punctuality_rate || 0}%`);
        updateRing('ringAbsenteeism', data.absenteeism_rate || 0, 100,
            `${data.absenteeism_rate || 0}%`);
        updateRing('ringAvgHours', data.avg_hours_per_day || 0, 10,
            `${data.avg_hours_per_day || 0}h`);
        updateRing('ringLate', data.total_late_count || 0,
            Math.max(data.total_late_count || 1, 20),
            `${data.total_late_count || 0}`);
    }

    /* ── Daily Presences (area chart) ── */
    function renderDailyPresences(data) {
        const id = 'chartDailyPresences';
        destroyChart(id);
        const el = $(id);
        if (!el) return;

        const items = data.daily_presences || [];
        if (!items.length) { emptyState(id, true); return; }
        emptyState(id, false);

        charts[id] = new Chart(el, {
            type: 'bar',
            data: {
                labels: items.map(i => i.label),
                datasets: [
                    {
                        label: 'Presenti',
                        data: items.map(i => i.present),
                        backgroundColor: getCSS('--success') + '99',
                        borderColor: getCSS('--success'),
                        borderWidth: 1,
                        borderRadius: 3,
                        order: 2,
                    },
                    {
                        label: 'In ritardo',
                        data: items.map(i => i.late),
                        backgroundColor: getCSS('--warning') + '99',
                        borderColor: getCSS('--warning'),
                        borderWidth: 1,
                        borderRadius: 3,
                        order: 1,
                    },
                    {
                        label: 'Assenti',
                        type: 'line',
                        data: items.map(i => i.absent),
                        borderColor: getCSS('--danger'),
                        backgroundColor: getCSS('--danger') + '15',
                        fill: true,
                        tension: 0.3,
                        pointRadius: 2,
                        borderWidth: 2,
                        order: 0,
                    }
                ]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: { legend: { position: 'top', labels: { boxWidth: 12, padding: 14 } } },
                scales: {
                    x: { grid: { display: false } },
                    y: { beginAtZero: true, ticks: { stepSize: 1 } }
                }
            }
        });
    }

    /* ── Headcount by group (doughnut + list) ── */
    function renderHeadcount(data) {
        const id = 'chartHeadcount';
        destroyChart(id);
        const el = $(id);
        if (!el) return;

        const items = data.headcount_by_group || [];
        const list = $('headcountList');
        if (!items.length) { emptyState(id, true); if (list) list.innerHTML = ''; return; }
        emptyState(id, false);

        charts[id] = new Chart(el, {
            type: 'doughnut',
            data: {
                labels: items.map(i => i.group),
                datasets: [{
                    data: items.map(i => i.count),
                    backgroundColor: PALETTE.slice(0, items.length),
                    borderWidth: 0,
                    hoverOffset: 6,
                }]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                cutout: '65%',
                plugins: {
                    legend: { display: false },
                    tooltip: { callbacks: { label: (c) => `${c.label}: ${c.raw} dipendenti` } }
                }
            }
        });

        // Render list below
        if (list) {
            list.innerHTML = items.map((item, i) => `
                <div class="hc-item">
                    <div class="hc-dot" style="background:${PALETTE[i % PALETTE.length]}"></div>
                    <span class="hc-name">${item.group}</span>
                    <span class="hc-count">${item.count}</span>
                </div>
            `).join('');
        }
    }

    /* ── Requests by type (horizontal bar) ── */
    function renderRequestsByType(data) {
        const id = 'chartRequestsByType';
        destroyChart(id);
        const el = $(id);
        if (!el) return;

        const items = data.requests_by_type || [];
        if (!items.length) { emptyState(id, true); return; }
        emptyState(id, false);

        charts[id] = new Chart(el, {
            type: 'bar',
            data: {
                labels: items.map(i => i.type_name),
                datasets: [
                    { label: 'Approvate', data: items.map(i => i.approved), backgroundColor: getCSS('--success'), borderRadius: 4 },
                    { label: 'In attesa', data: items.map(i => i.pending),  backgroundColor: getCSS('--warning'), borderRadius: 4 },
                    { label: 'Rifiutate', data: items.map(i => i.rejected), backgroundColor: getCSS('--danger'),  borderRadius: 4 },
                ]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: { legend: { position: 'top', labels: { boxWidth: 10, padding: 12 } } },
                scales: {
                    x: { stacked: true, beginAtZero: true, ticks: { stepSize: 1 }, grid: { color: getCSS('--border') } },
                    y: { stacked: true, grid: { display: false } }
                }
            }
        });
    }

    /* ── Request Status (doughnut with center text) ── */
    function renderRequestStatus(data) {
        const id = 'chartRequestStatus';
        destroyChart(id);
        const el = $(id);
        if (!el) return;

        const stats = data.request_status_summary || {};
        const values = [stats.approved || 0, stats.pending || 0, stats.rejected || 0];
        const total = values.reduce((a, b) => a + b, 0);
        if (total === 0) { emptyState(id, true); return; }
        emptyState(id, false);

        charts[id] = new Chart(el, {
            type: 'doughnut',
            data: {
                labels: ['Approvate', 'In attesa', 'Rifiutate'],
                datasets: [{
                    data: values,
                    backgroundColor: [getCSS('--success'), getCSS('--warning'), getCSS('--danger')],
                    borderWidth: 0,
                    hoverOffset: 6,
                }]
            },
            plugins: [{
                id: 'centerText',
                afterDraw(chart) {
                    const { ctx: c, chartArea } = chart;
                    const cx = (chartArea.left + chartArea.right) / 2;
                    const cy = (chartArea.top + chartArea.bottom) / 2;
                    c.save();
                    c.textAlign = 'center';
                    c.textBaseline = 'middle';
                    c.font = '800 24px Inter';
                    c.fillStyle = getCSS('--text');
                    c.fillText(total, cx, cy - 8);
                    c.font = '500 11px Inter';
                    c.fillStyle = getCSS('--muted');
                    c.fillText('richieste', cx, cy + 12);
                    c.restore();
                }
            }],
            options: {
                responsive: true, maintainAspectRatio: false,
                cutout: '68%',
                plugins: {
                    legend: { position: 'bottom', labels: { boxWidth: 10, padding: 14 } },
                    tooltip: { callbacks: { label: (c) => `${c.label}: ${c.raw} (${Math.round(c.raw/total*100)}%)` } }
                }
            }
        });
    }

    /* ── Overtime Trend (area + line) ── */
    function renderOvertimeTrend(data) {
        const id = 'chartOvertimeTrend';
        destroyChart(id);
        const el = $(id);
        if (!el) return;

        const items = data.overtime_by_week || [];
        if (!items.length) { emptyState(id, true); return; }
        emptyState(id, false);

        const gradient = el.getContext('2d').createLinearGradient(0, 0, 0, 250);
        gradient.addColorStop(0, 'rgba(139,92,246,0.3)');
        gradient.addColorStop(1, 'rgba(139,92,246,0)');

        charts[id] = new Chart(el, {
            type: 'line',
            data: {
                labels: items.map(i => i.label),
                datasets: [{
                    label: 'Minuti straordinario',
                    data: items.map(i => i.minutes),
                    borderColor: getCSS('--purple'),
                    backgroundColor: gradient,
                    fill: true,
                    tension: 0.4,
                    pointRadius: 4,
                    pointBackgroundColor: getCSS('--purple'),
                    pointBorderColor: getCSS('--card'),
                    pointBorderWidth: 2,
                    borderWidth: 2.5,
                }]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: { callbacks: { label: (c) => `${fmtHours(c.raw)}` } }
                },
                scales: {
                    x: { grid: { display: false } },
                    y: { beginAtZero: true, ticks: { callback: v => fmtHoursShort(v * 60) } }
                }
            }
        });
    }

    /* ── Absence Types (pie) ── */
    function renderAbsenceTypes(data) {
        const id = 'chartAbsenceTypes';
        destroyChart(id);
        const el = $(id);
        if (!el) return;

        const items = data.absence_breakdown || [];
        if (!items.length) { emptyState(id, true); return; }
        emptyState(id, false);

        charts[id] = new Chart(el, {
            type: 'pie',
            data: {
                labels: items.map(i => i.type),
                datasets: [{
                    data: items.map(i => i.days),
                    backgroundColor: PALETTE.slice(0, items.length),
                    borderWidth: 0,
                    hoverOffset: 8,
                }]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'right', labels: { boxWidth: 12, padding: 10, font: { size: 11 } } },
                    tooltip: { callbacks: { label: (c) => `${c.label}: ${c.raw} giorni` } }
                }
            }
        });
    }

    /* ── Monthly Trend (combo: bar + line) ── */
    function renderMonthlyTrend(data) {
        const id = 'chartMonthlyTrend';
        destroyChart(id);
        const el = $(id);
        if (!el) return;

        const items = data.monthly_trend || [];
        if (!items.length) { emptyState(id, true); return; }
        emptyState(id, false);

        charts[id] = new Chart(el, {
            type: 'bar',
            data: {
                labels: items.map(i => i.label),
                datasets: [
                    {
                        label: 'Timbrature',
                        data: items.map(i => i.clock_ins),
                        backgroundColor: getCSS('--brand') + '60',
                        borderColor: getCSS('--brand'),
                        borderWidth: 1,
                        borderRadius: 4,
                        yAxisID: 'y',
                        order: 2,
                    },
                    {
                        label: 'Dip. attivi',
                        type: 'line',
                        data: items.map(i => i.active_employees),
                        borderColor: getCSS('--success'),
                        backgroundColor: getCSS('--success'),
                        pointRadius: 5,
                        pointBackgroundColor: getCSS('--success'),
                        pointBorderColor: getCSS('--card'),
                        pointBorderWidth: 2,
                        borderWidth: 2.5,
                        tension: 0.3,
                        yAxisID: 'y1',
                        order: 1,
                    }
                ]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: { legend: { position: 'top', labels: { boxWidth: 10, padding: 14 } } },
                scales: {
                    x: { grid: { display: false } },
                    y: {
                        beginAtZero: true,
                        position: 'left',
                        title: { display: true, text: 'Timbrature', font: { size: 10 } },
                        grid: { color: getCSS('--border') },
                    },
                    y1: {
                        beginAtZero: true,
                        position: 'right',
                        title: { display: true, text: 'Dipendenti', font: { size: 10 } },
                        grid: { drawOnChartArea: false },
                    }
                }
            }
        });
    }

    /* ── Top Late Employees ── */
    function renderLateList(data) {
        const list = $('lateList');
        if (!list) return;

        const items = data.top_late_employees || [];
        if (!items.length) {
            list.innerHTML = '<div class="empty-state" style="position:static;padding:20px 0;"><div class="empty-icon">✅</div>Nessun ritardo nel periodo</div>';
            return;
        }

        const maxCount = Math.max(...items.map(i => i.count), 1);
        list.innerHTML = items.map(item => {
            const pct = Math.round((item.count / maxCount) * 100);
            return `
                <div class="late-item">
                    <span class="late-name">${item.name}</span>
                    <div class="mini-bar" style="flex:1;margin:0 12px;">
                        <div class="bar-track">
                            <div class="bar-fill" style="width:${pct}%;background:var(--warning)"></div>
                        </div>
                    </div>
                    <span class="late-count">${item.count}</span>
                </div>
            `;
        }).join('');
    }

    /* ── Employee Hours Table ── */
    function renderEmployeeHoursTable(data) {
        const tbody = document.querySelector('#employeeHoursTable tbody');
        if (!tbody) return;

        const items = data.employee_hours || [];
        if (!items.length) {
            tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;padding:24px;color:var(--muted);">Nessun dato disponibile</td></tr>';
            return;
        }

        const maxMins = Math.max(...items.map(i => i.worked_minutes + i.overtime_minutes), 1);

        tbody.innerHTML = items.map(item => {
            const col = avatarColor(item.name);
            const initials = (item.name || '?').split(' ').map(w => w[0]).join('').slice(0, 2).toUpperCase();
            const totalMins = item.worked_minutes + item.overtime_minutes;
            const pct = Math.round((totalMins / maxMins) * 100);

            return `<tr>
                <td>
                    <div class="name-cell">
                        <div class="mini-avatar" style="background:${col}">${initials}</div>
                        <span class="emp-name">${item.name}</span>
                    </div>
                </td>
                <td>${item.days_worked || 0}</td>
                <td>
                    <div class="mini-bar">
                        <div class="bar-track"><div class="bar-fill" style="width:${pct}%;background:var(--brand)"></div></div>
                        <span class="bar-val">${fmtHoursShort(item.worked_minutes)}</span>
                    </div>
                </td>
                <td><span class="badge ${item.overtime_minutes > 0 ? 'badge-warning' : 'badge-muted'}">${fmtHoursShort(item.overtime_minutes)}</span></td>
                <td>${fmtHoursShort(item.avg_daily_minutes || 0)}</td>
                <td>${item.late_count > 0 ? '<span class="badge badge-warning">' + item.late_count + '</span>' : '<span class="badge badge-muted">0</span>'}</td>
                <td>${item.absence_days > 0 ? '<span class="badge badge-danger">' + item.absence_days + 'gg</span>' : '<span class="badge badge-muted">0</span>'}</td>
                <td>${item.missed_clocks > 0 ? '<span class="badge badge-danger">' + item.missed_clocks + '</span>' : '<span class="badge badge-muted">0</span>'}</td>
            </tr>`;
        }).join('');
    }

    /* ── Requests Table ── */
    function renderRequestsTable(data) {
        const tbody = document.querySelector('#topRequestsTable tbody');
        if (!tbody) return;

        const items = data.recent_requests || [];
        if (!items.length) {
            tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;padding:24px;color:var(--muted);">Nessuna richiesta nel periodo</td></tr>';
            return;
        }

        tbody.innerHTML = items.map(r => {
            const statusBadge = r.status === 'approved'
                ? '<span class="badge badge-success">Approvata</span>'
                : r.status === 'rejected'
                    ? '<span class="badge badge-danger">Rifiutata</span>'
                    : '<span class="badge badge-warning">In attesa</span>';

            return `<tr>
                <td><span style="font-weight:600;color:var(--text)">${r.employee || '—'}</span></td>
                <td>${r.type_name || '—'}</td>
                <td>${r.date_from || '—'}</td>
                <td>${r.value_display || '—'}</td>
                <td>${statusBadge}</td>
                <td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${r.notes || ''}">${r.notes || '—'}</td>
            </tr>`;
        }).join('');
    }

    /* ══════════════════════════════ MAIN LOAD ══════════════════════════════ */

    async function loadDashboard() {
        const month = monthInput ? parseInt(monthInput.value) : (now.getMonth() + 1);
        const year  = yearInput  ? parseInt(yearInput.value)  : now.getFullYear();
        const group = groupFilter ? groupFilter.value : '';

        // Update period label
        if (periodLabel) periodLabel.textContent = `${MONTH_NAMES[month - 1]} ${year}`;

        showLoading();
        try {
            const data = await fetchPayrollData(month, year, group);

            renderKPIs(data);
            renderRates(data);
            renderDailyPresences(data);
            renderHeadcount(data);
            renderRequestsByType(data);
            renderRequestStatus(data);
            renderOvertimeTrend(data);
            renderAbsenceTypes(data);
            renderMonthlyTrend(data);
            renderLateList(data);
            renderEmployeeHoursTable(data);
            renderRequestsTable(data);
        } catch (err) {
            console.error('Errore caricamento dashboard payroll:', err);
            alert('Errore: ' + (err.message || 'Errore imprevisto'));
        } finally {
            hideLoading();
        }
    }

    /* ══════════════════════════════ INIT ══════════════════════════════ */

    async function init() {
        chartDefaults();

        if (monthInput) monthInput.value = now.getMonth() + 1;
        if (yearInput)  yearInput.value  = now.getFullYear();

        // Load groups
        const groups = await fetchGroups();
        if (groupFilter && groups.length) {
            groups.forEach(g => {
                const opt = document.createElement('option');
                opt.value = g.id || g.name;
                opt.textContent = g.name;
                groupFilter.appendChild(opt);
            });
        }

        // Event listeners
        if (applyBtn) applyBtn.addEventListener('click', loadDashboard);

        $('prevMonth')?.addEventListener('click', () => {
            let m = parseInt(monthInput.value), y = parseInt(yearInput.value);
            m--;
            if (m < 1) { m = 12; y--; }
            monthInput.value = m; yearInput.value = y;
            loadDashboard();
        });
        $('nextMonth')?.addEventListener('click', () => {
            let m = parseInt(monthInput.value), y = parseInt(yearInput.value);
            m++;
            if (m > 12) { m = 1; y++; }
            monthInput.value = m; yearInput.value = y;
            loadDashboard();
        });

        loadDashboard();
    }

    init();
})();
