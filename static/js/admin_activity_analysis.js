/**
 * Admin Activity Analysis - Cross-Project Comparison
 * JobLog Professional Dashboard
 */

(function () {
    'use strict';

    // ══════════════════════════════════════════════════════════════════════════════
    //  STATE
    // ══════════════════════════════════════════════════════════════════════════════
    
    const state = {
        dateStart: null,
        dateEnd: null,
        allActivities: [],
        selectedActivities: new Set(),
        analysisData: null,
        sortColumn: 'total',
        sortDirection: 'desc',
        chartInstance: null,
        currentView: 'grouped'
    };

    // Project colors palette
    const PROJECT_COLORS = [
        '#a855f7', '#ec4899', '#06b6d4', '#22c55e', '#f59e0b',
        '#ef4444', '#3b82f6', '#8b5cf6', '#14b8a6', '#f97316',
        '#6366f1', '#10b981', '#f43f5e', '#0ea5e9', '#84cc16'
    ];

    // ══════════════════════════════════════════════════════════════════════════════
    //  UTILITIES
    // ══════════════════════════════════════════════════════════════════════════════

    function formatHours(ms) {
        const hours = ms / 3600000;
        return hours.toFixed(1) + 'h';
    }

    function formatHoursShort(ms) {
        const hours = ms / 3600000;
        if (hours < 1) {
            return Math.round(ms / 60000) + 'm';
        }
        return hours.toFixed(1) + 'h';
    }

    function formatDuration(ms) {
        const totalSeconds = Math.floor(ms / 1000);
        const hours = Math.floor(totalSeconds / 3600);
        const minutes = Math.floor((totalSeconds % 3600) / 60);
        const seconds = totalSeconds % 60;
        return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
    }

    function getProjectColor(index) {
        return PROJECT_COLORS[index % PROJECT_COLORS.length];
    }

    function showLoading(show) {
        const overlay = document.getElementById('loadingOverlay');
        if (overlay) {
            overlay.classList.toggle('hidden', !show);
        }
    }

    function isoDate(date) {
        return date.toISOString().split('T')[0];
    }

    // ══════════════════════════════════════════════════════════════════════════════
    //  DATE PRESETS
    // ══════════════════════════════════════════════════════════════════════════════

    function setDateRange(days) {
        const end = new Date();
        const start = new Date();
        start.setDate(end.getDate() - days);
        
        document.getElementById('dateStart').value = isoDate(start);
        document.getElementById('dateEnd').value = isoDate(end);
        state.dateStart = isoDate(start);
        state.dateEnd = isoDate(end);
    }

    // ══════════════════════════════════════════════════════════════════════════════
    //  API CALLS
    // ══════════════════════════════════════════════════════════════════════════════

    async function fetchActivitiesList() {
        const params = new URLSearchParams();
        if (state.dateStart) params.set('date_start', state.dateStart);
        if (state.dateEnd) params.set('date_end', state.dateEnd);
        params.set('mode', 'list');

        const url = `${window.__ACTIVITY_ANALYSIS__.analysisUrl}?${params}`;
        const resp = await fetch(url);
        if (!resp.ok) throw new Error('Failed to fetch activities');
        return resp.json();
    }

    async function fetchAnalysisData(activities) {
        showLoading(true);
        const params = new URLSearchParams();
        if (state.dateStart) params.set('date_start', state.dateStart);
        if (state.dateEnd) params.set('date_end', state.dateEnd);
        params.set('mode', 'analysis');
        activities.forEach(a => params.append('activity', a));

        const url = `${window.__ACTIVITY_ANALYSIS__.analysisUrl}?${params}`;
        try {
            const resp = await fetch(url);
            if (!resp.ok) throw new Error('Failed to fetch analysis');
            return await resp.json();
        } finally {
            showLoading(false);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════════
    //  RENDER ACTIVITIES LIST
    // ══════════════════════════════════════════════════════════════════════════════

    function renderActivitiesList(filter = '') {
        const container = document.getElementById('activityList');
        if (!container) return;

        const filterLower = filter.toLowerCase();
        const filtered = state.allActivities.filter(a => 
            a.name.toLowerCase().includes(filterLower)
        );

        container.innerHTML = filtered.map(activity => {
            const isSelected = state.selectedActivities.has(activity.name);
            return `
                <div class="activity-pill ${isSelected ? 'selected' : ''}" data-activity="${escapeHtml(activity.name)}">
                    <div class="activity-pill-check">${isSelected ? '✓' : ''}</div>
                    <div class="activity-pill-info">
                        <div class="activity-pill-name">${escapeHtml(activity.name)}</div>
                        <div class="activity-pill-stats">${activity.sessions} sessioni · ${activity.projects} progetti</div>
                    </div>
                    <div class="activity-pill-hours">${formatHoursShort(activity.total_ms)}</div>
                </div>
            `;
        }).join('');

        // Add click handlers
        container.querySelectorAll('.activity-pill').forEach(pill => {
            pill.addEventListener('click', () => {
                const name = pill.dataset.activity;
                if (state.selectedActivities.has(name)) {
                    state.selectedActivities.delete(name);
                } else {
                    state.selectedActivities.add(name);
                }
                renderActivitiesList(filter);
                updateSummaryCards();
            });
        });
    }

    function escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    // ══════════════════════════════════════════════════════════════════════════════
    //  UPDATE SUMMARY CARDS
    // ══════════════════════════════════════════════════════════════════════════════

    function updateSummaryCards() {
        document.getElementById('summaryActivities').textContent = state.selectedActivities.size;
        document.getElementById('summaryTotalAct').textContent = state.allActivities.length;

        if (state.analysisData) {
            const data = state.analysisData;
            document.getElementById('summaryProjects').textContent = data.projects?.length || 0;
            document.getElementById('summaryHours').textContent = formatHours(data.total_ms || 0);
            document.getElementById('summarySessions').textContent = `${data.total_sessions || 0} sessioni`;
            
            const avgMs = data.projects?.length > 0 ? 
                (data.total_ms / data.projects.length) : 0;
            document.getElementById('summaryAvg').textContent = formatHours(avgMs);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════════
    //  RENDER CHART
    // ══════════════════════════════════════════════════════════════════════════════

    function renderChart() {
        const canvas = document.getElementById('comparisonChart');
        if (!canvas || !state.analysisData) return;

        const ctx = canvas.getContext('2d');
        if (state.chartInstance) {
            state.chartInstance.destroy();
        }

        const data = state.analysisData;
        const projects = data.projects || [];
        const activities = [...state.selectedActivities];

        // Build datasets
        const datasets = projects.map((project, idx) => {
            const color = getProjectColor(idx);
            const values = activities.map(actName => {
                const actData = data.matrix?.[project]?.[actName];
                return actData ? actData.total_ms / 3600000 : 0; // Convert to hours
            });

            return {
                label: project,
                data: values,
                backgroundColor: color + (state.currentView === 'stacked' ? 'CC' : '99'),
                borderColor: color,
                borderWidth: 2,
                borderRadius: 6
            };
        });

        const chartType = state.currentView === 'radar' ? 'radar' : 'bar';
        const stacked = state.currentView === 'stacked';

        state.chartInstance = new Chart(ctx, {
            type: chartType,
            data: {
                labels: activities,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false
                },
                plugins: {
                    legend: {
                        position: 'top',
                        labels: {
                            color: getComputedStyle(document.body).getPropertyValue('--text').trim(),
                            usePointStyle: true,
                            padding: 20,
                            font: { weight: 600, size: 12 }
                        }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(15, 23, 42, 0.95)',
                        titleColor: '#f8fafc',
                        bodyColor: '#e2e8f0',
                        borderColor: 'rgba(148, 163, 184, 0.3)',
                        borderWidth: 1,
                        padding: 12,
                        cornerRadius: 8,
                        callbacks: {
                            label: function(context) {
                                return `${context.dataset.label}: ${context.raw.toFixed(2)}h`;
                            }
                        }
                    }
                },
                scales: chartType === 'radar' ? {} : {
                    x: {
                        stacked: stacked,
                        grid: { display: false },
                        ticks: { 
                            color: getComputedStyle(document.body).getPropertyValue('--muted').trim(),
                            font: { weight: 600 }
                        }
                    },
                    y: {
                        stacked: stacked,
                        beginAtZero: true,
                        grid: { 
                            color: 'rgba(148, 163, 184, 0.1)'
                        },
                        ticks: { 
                            color: getComputedStyle(document.body).getPropertyValue('--muted').trim(),
                            callback: (v) => v + 'h',
                            font: { weight: 600 }
                        }
                    }
                }
            }
        });
    }

    // ══════════════════════════════════════════════════════════════════════════════
    //  RENDER TABLE
    // ══════════════════════════════════════════════════════════════════════════════

    function renderTable(filter = '') {
        const tbody = document.getElementById('detailTableBody');
        const emptyState = document.getElementById('emptyState');
        if (!tbody) return;

        if (!state.analysisData || !state.analysisData.details) {
            tbody.innerHTML = '';
            emptyState?.classList.remove('hidden');
            return;
        }

        const filterLower = filter.toLowerCase();
        let rows = state.analysisData.details.filter(row => 
            row.project.toLowerCase().includes(filterLower) ||
            row.activity.toLowerCase().includes(filterLower)
        );

        // Sort
        rows.sort((a, b) => {
            let aVal = a[state.sortColumn];
            let bVal = b[state.sortColumn];
            
            if (typeof aVal === 'string') {
                aVal = aVal.toLowerCase();
                bVal = bVal.toLowerCase();
            }
            
            const cmp = aVal > bVal ? 1 : aVal < bVal ? -1 : 0;
            return state.sortDirection === 'asc' ? cmp : -cmp;
        });

        if (rows.length === 0) {
            tbody.innerHTML = '';
            emptyState?.classList.remove('hidden');
            return;
        }

        emptyState?.classList.add('hidden');

        tbody.innerHTML = rows.map((row, idx) => {
            const projectIdx = state.analysisData.projects.indexOf(row.project);
            const color = getProjectColor(projectIdx >= 0 ? projectIdx : idx);
            
            return `
                <tr>
                    <td>
                        <span class="project-dot" style="background:${color}"></span>
                        <span class="badge badge-project">${escapeHtml(row.project)}</span>
                    </td>
                    <td>${escapeHtml(row.activity)}</td>
                    <td>${row.sessions}</td>
                    <td><strong>${formatDuration(row.total_ms)}</strong></td>
                    <td>${formatDuration(row.avg_ms)}</td>
                    <td>${formatDuration(row.min_ms)}</td>
                    <td>${formatDuration(row.max_ms)}</td>
                    <td>${row.variance_pct.toFixed(1)}%</td>
                </tr>
            `;
        }).join('');
    }

    // ══════════════════════════════════════════════════════════════════════════════
    //  LOAD DATA
    // ══════════════════════════════════════════════════════════════════════════════

    async function loadActivities() {
        try {
            const data = await fetchActivitiesList();
            state.allActivities = data.activities || [];
            renderActivitiesList();
            updateSummaryCards();
        } catch (err) {
            console.error('Failed to load activities:', err);
        }
    }

    async function runAnalysis() {
        if (state.selectedActivities.size === 0) {
            alert('Seleziona almeno una attività da analizzare');
            return;
        }

        try {
            const activities = [...state.selectedActivities];
            state.analysisData = await fetchAnalysisData(activities);
            updateSummaryCards();
            renderChart();
            renderTable();
        } catch (err) {
            console.error('Analysis failed:', err);
            alert('Errore durante l\'analisi');
        }
    }

    // ══════════════════════════════════════════════════════════════════════════════
    //  EXPORT
    // ══════════════════════════════════════════════════════════════════════════════

    function exportData() {
        if (!state.analysisData || state.selectedActivities.size === 0) {
            alert('Esegui prima un\'analisi');
            return;
        }

        const params = new URLSearchParams();
        if (state.dateStart) params.set('date_start', state.dateStart);
        if (state.dateEnd) params.set('date_end', state.dateEnd);
        [...state.selectedActivities].forEach(a => params.append('activity', a));

        window.location.href = `${window.__ACTIVITY_ANALYSIS__.exportUrl}?${params}`;
    }

    // ══════════════════════════════════════════════════════════════════════════════
    //  THEME
    // ══════════════════════════════════════════════════════════════════════════════

    function toggleTheme() {
        const html = document.documentElement;
        const current = html.dataset.theme || 'dark';
        const next = current === 'dark' ? 'light' : 'dark';
        html.dataset.theme = next;
        localStorage.setItem('joblog-theme', next);
        
        // Re-render chart with new colors
        if (state.chartInstance) {
            renderChart();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════════
    //  INIT
    // ══════════════════════════════════════════════════════════════════════════════

    function init() {
        // Set default date range (last month)
        setDateRange(30);

        // Date inputs
        const dateStart = document.getElementById('dateStart');
        const dateEnd = document.getElementById('dateEnd');
        
        dateStart?.addEventListener('change', (e) => {
            state.dateStart = e.target.value;
            loadActivities();
        });
        
        dateEnd?.addEventListener('change', (e) => {
            state.dateEnd = e.target.value;
            loadActivities();
        });

        // Date presets
        document.getElementById('btnWeek')?.addEventListener('click', () => {
            setDateRange(7);
            loadActivities();
        });
        document.getElementById('btnMonth')?.addEventListener('click', () => {
            setDateRange(30);
            loadActivities();
        });
        document.getElementById('btn3Months')?.addEventListener('click', () => {
            setDateRange(90);
            loadActivities();
        });

        // Activity search
        document.getElementById('activitySearch')?.addEventListener('input', (e) => {
            renderActivitiesList(e.target.value);
        });

        // Select all
        document.getElementById('btnSelectAll')?.addEventListener('click', () => {
            if (state.selectedActivities.size === state.allActivities.length) {
                state.selectedActivities.clear();
            } else {
                state.allActivities.forEach(a => state.selectedActivities.add(a.name));
            }
            renderActivitiesList(document.getElementById('activitySearch')?.value || '');
            updateSummaryCards();
        });

        // Analyze button
        document.getElementById('btnAnalyze')?.addEventListener('click', runAnalysis);

        // Chart view tabs
        document.querySelectorAll('.comparison-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                document.querySelectorAll('.comparison-tab').forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
                state.currentView = tab.dataset.view;
                renderChart();
            });
        });

        // Table search
        document.getElementById('detailSearch')?.addEventListener('input', (e) => {
            renderTable(e.target.value);
        });

        // Table sorting
        document.querySelectorAll('#detailTable th[data-sort]').forEach(th => {
            th.addEventListener('click', () => {
                const col = th.dataset.sort;
                if (state.sortColumn === col) {
                    state.sortDirection = state.sortDirection === 'asc' ? 'desc' : 'asc';
                } else {
                    state.sortColumn = col;
                    state.sortDirection = 'desc';
                }
                renderTable(document.getElementById('detailSearch')?.value || '');
            });
        });

        // Header buttons
        document.getElementById('refreshBtn')?.addEventListener('click', () => {
            loadActivities();
            if (state.selectedActivities.size > 0) {
                runAnalysis();
            }
        });
        document.getElementById('exportBtn')?.addEventListener('click', exportData);
        document.getElementById('themeToggle')?.addEventListener('click', toggleTheme);

        // Load initial data
        loadActivities().then(() => {
            showLoading(false);
        });
    }

    // Start
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
