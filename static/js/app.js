const fmt2 = (value) => String(value).padStart(2, "0");

const timers = {};
let refreshTimer = null;
let cachedActivities = [];
let allPaused = false;
let isLoadingProject = false;
const PROJECT_CODE_MAX = 8;
let projectCodeBuffer = "";
let projectVisible = false;
let keypadVisible = false;
let suppressSelectionRestore = false;
let eventsCache = [];
let timelineOpen = false;
let activitySearchTerm = "";
let activitySearchInitialized = false;
let selectionToolbarWasVisible = false;
const collapsedActivities = new Set();
const activityTotalDisplays = new Map();
const APP_RELEASE = "v2025.11.14";
let menuOpen = false;
let feedbackContext = null;
let feedbackToolbarWasVisible = false;
let darkMode = false;
let exportModalOpen = false;
let pollingSuspended = false;
let unauthorizedNotified = false;

async function postJson(url, payload) {

    const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload || {}),
    });
    if (!res.ok) {
        const error = new Error(`HTTP ${res.status} for ${url}`);
        error.status = res.status;
        error.url = url;
        throw error;
    }
    return res.json();
}

async function fetchJson(url) {
    const res = await fetch(url);
    if (!res.ok) {
        const error = new Error(`HTTP ${res.status} for ${url}`);
        error.status = res.status;
        error.url = url;
        throw error;
    }
    return res.json();
}

function formatTime(ms) {
    const totalSeconds = Math.max(0, Math.floor(ms / 1000));
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    return `${fmt2(hours)}:${fmt2(minutes)}:${fmt2(seconds)}`;
}

const planningDateFormatter = new Intl.DateTimeFormat("it-IT", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
});

const planningTimeFormatter = new Intl.DateTimeFormat("it-IT", {
    hour: "2-digit",
    minute: "2-digit",
});

function parsePlanningDate(value) {
    if (!value) {
        return null;
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return null;
    }
    return date;
}

function sortActivitiesForPicker(activities) {
    return [...activities].sort((a, b) => {
        const aDate = parsePlanningDate(a.plan_start) || parsePlanningDate(a.plan_end);
        const bDate = parsePlanningDate(b.plan_start) || parsePlanningDate(b.plan_end);
        if (aDate && bDate) {
            const diff = aDate.getTime() - bDate.getTime();
            if (diff !== 0) {
                return diff;
            }
        } else if (aDate && !bDate) {
            return -1;
        } else if (!aDate && bDate) {
            return 1;
        }
        return a.label.localeCompare(b.label, "it", { sensitivity: "base" });
    });
}

function formatPlanningRange(startIso, endIso) {
    const start = parsePlanningDate(startIso);
    const end = parsePlanningDate(endIso);
    if (!start && !end) {
        return "";
    }
    if (start && end) {
        const sameDay =
            start.getFullYear() === end.getFullYear() &&
            start.getMonth() === end.getMonth() &&
            start.getDate() === end.getDate();
        const startLabel = planningDateFormatter.format(start);
        const endLabel = sameDay
            ? planningTimeFormatter.format(end)
            : planningDateFormatter.format(end);
        return `${startLabel} - ${endLabel}`;
    }
    const single = start || end;
    const prefix = start ? "Inizio" : "Fine";
    return `${prefix}: ${planningDateFormatter.format(single)}`;
}

function domId(key) {
    return (key || "").toString().replace(/[^a-zA-Z0-9_-]/g, "_");
}

function safeKey(name) {
    return name.toLowerCase().replace(/[^a-z0-9]+/g, "-");
}

function clearTimers() {
    Object.values(timers).forEach(clearInterval);
    Object.keys(timers).forEach((k) => delete timers[k]);
}

function escapeAttribute(value) {
    const raw = String(value ?? "");
    if (typeof CSS !== "undefined" && CSS.escape) {
        return CSS.escape(raw);
    }
    return raw.replace(/"/g, '\\"');
}

function forEachMemberNode(memberKey, callback) {
    if (memberKey === undefined || memberKey === null) {
        return;
    }
    const escaped = escapeAttribute(memberKey);
    const selector = `.team-member[data-key="${escaped}"], .member-task[data-key="${escaped}"]`;
    document.querySelectorAll(selector).forEach((node) => {
        callback(node);
    });
}

function computeTotalRunningMilliseconds() {
    const totals = new Map();
    const collect = (selector) => {
        document.querySelectorAll(selector).forEach((node) => {
            const key = node.dataset.key;
            if (!key || totals.has(key)) {
                return;
            }
            if (node.dataset.running !== "true") {
                return;
            }
            const value = Number(node.dataset.elapsedMs || 0);
            if (Number.isFinite(value)) {
                totals.set(key, value);
            }
        });
    };
    collect(".team-member");
    if (totals.size === 0) {
        collect(".member-task");
    }
    let total = 0;
    totals.forEach((value) => {
        total += value;
    });
    return total;
}

function refreshTotalRunningTimeDisplay() {
    const node = document.getElementById("totalRunningTime");
    if (!node) {
        return;
    }
    const totalMs = computeTotalRunningMilliseconds();
    node.textContent = formatTime(totalMs);
}

function calculateActivityRunningTime(members) {
    if (!Array.isArray(members)) {
        return 0;
    }
    return members.reduce((total, member) => {
        if (!member || !member.running) {
            return total;
        }
        const value = Number(member.elapsed) || 0;
        return total + (Number.isFinite(value) ? value : 0);
    }, 0);
}

function updateActivityTotalDisplay(activityId) {
    if (!activityId) {
        return;
    }
    const key = String(activityId);
    const display = activityTotalDisplays.get(key);
    if (!display) {
        return;
    }
    const card = display.closest(".task-card");
    if (!card) {
        return;
    }
    let total = 0;
    card.querySelectorAll(".member-task").forEach((node) => {
        if (node.dataset.running !== "true") {
            return;
        }
        const value = Number(node.dataset.elapsedMs || 0);
        if (Number.isFinite(value)) {
            total += value;
        }
    });
    display.textContent = formatTime(total);
}

function handleGlobalPointerForKeypad(event, projectInputGroup) {
    if (!projectInputGroup) {
        return;
    }
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
        return;
    }
    if (projectInputGroup.contains(target)) {
        return;
    }
    setKeypadVisibility(false);
}

function showPopup(message) {
    const node = document.getElementById("popup");
    if (!node) {
        return;
    }
    node.textContent = message;
    node.classList.add("show");
    setTimeout(() => node.classList.remove("show"), 2200);
}

function toggleSelection(node) {
    node.classList.toggle("selected");
    updateSelectionToolbar();
}

function updateProjectInput() {
    const input = document.getElementById("projectInput");
    if (input) {
        input.value = projectCodeBuffer;
    }
}

function setProjectCodeBuffer(newCode) {
    const digitsOnly = String(newCode || "").replace(/\D/g, "");
    projectCodeBuffer = digitsOnly.slice(0, PROJECT_CODE_MAX);
    updateProjectInput();
}

function appendProjectDigit(digit) {
    if (projectCodeBuffer.length >= PROJECT_CODE_MAX) {
        return;
    }
    setProjectCodeBuffer(`${projectCodeBuffer}${digit}`);
}

function backspaceProjectDigit() {
    if (!projectCodeBuffer) {
        return;
    }
    setProjectCodeBuffer(projectCodeBuffer.slice(0, -1));
}

function clearProjectCode() {
    setProjectCodeBuffer("");
}

function getProjectCode() {
    return projectCodeBuffer;
}

function setKeypadVisibility(visible) {
    if (keypadVisible === visible) {
        return;
    }
    keypadVisible = visible;
    const keypad = document.getElementById("projectKeypad");
    if (keypad) {
        keypad.classList.toggle("hidden", !visible);
    }
}

function formatEventTime(timestamp) {
    if (!timestamp) {
        return "";
    }
    const date = new Date(Number(timestamp));
    if (Number.isNaN(date.getTime())) {
        return "";
    }
    return `${fmt2(date.getHours())}:${fmt2(date.getMinutes())}:${fmt2(date.getSeconds())}`;
}

function renderEvents(events) {
    eventsCache = Array.isArray(events) ? events : [];
    if (!projectVisible) {
        eventsCache = [];
    }
    const list = document.getElementById("eventList");
    if (!list) {
        return;
    }
    list.innerHTML = "";
    if (eventsCache.length === 0) {
        const empty = document.createElement("div");
        empty.className = "event-empty";
        empty.textContent = "Nessun evento registrato";
        list.appendChild(empty);
        return;
    }
    eventsCache.forEach((event) => {
        const item = document.createElement("div");
        item.className = "event-item";
        const summary = document.createElement("div");
        summary.className = "event-summary";
        summary.textContent = event.summary || event.kind;
        const meta = document.createElement("div");
        meta.className = "event-meta";
        meta.textContent = formatEventTime(event.timestamp);
        item.appendChild(summary);
        item.appendChild(meta);
        list.appendChild(item);
    });
}

function setTimelineVisibility(open) {
    const panel = document.getElementById("timelinePanel");
    const overlay = document.getElementById("timelineOverlay");
    const toggleBtn = document.getElementById("timelineBtn");

    timelineOpen = !!projectVisible && !!open;

    if (panel) {
        panel.classList.toggle("open", timelineOpen);
    }
    if (overlay) {
        overlay.classList.toggle("hidden", !timelineOpen);
    }
    if (toggleBtn) {
        toggleBtn.classList.toggle("active", timelineOpen);
        toggleBtn.setAttribute("aria-pressed", timelineOpen ? "true" : "false");
    }

    if (timelineOpen) {
        renderEvents(eventsCache);
    }
}

function closeTimeline() {
    setTimelineVisibility(false);
}

function setMenuVisibility(open) {
    menuOpen = !!open;
    const menu = document.getElementById("sideMenu");
    const overlay = document.getElementById("menuOverlay");
    if (!menu || !overlay) {
        return;
    }
    if (menuOpen) {
        closeTimeline();
    }
    menu.classList.toggle("open", menuOpen);
    menu.setAttribute("aria-hidden", menuOpen ? "false" : "true");
    overlay.classList.toggle("hidden", !menuOpen);
    document.body.classList.toggle("menu-open", menuOpen);
}

function closeMenu() {
    setMenuVisibility(false);
}

function toggleMenu() {
    setMenuVisibility(!menuOpen);
}

function initTheme() {
    const savedTheme = localStorage.getItem('joblog-theme');
    const systemPrefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    
    if (savedTheme === 'dark' || (!savedTheme && systemPrefersDark)) {
        darkMode = true;
        document.documentElement.setAttribute('data-theme', 'dark');
    } else {
        darkMode = false;
        document.documentElement.removeAttribute('data-theme');
    }
    
    updateThemeUI();
}

function toggleTheme() {
    darkMode = !darkMode;
    
    if (darkMode) {
        document.documentElement.setAttribute('data-theme', 'dark');
        localStorage.setItem('joblog-theme', 'dark');
    } else {
        document.documentElement.removeAttribute('data-theme');
        localStorage.setItem('joblog-theme', 'light');
    }
    
    updateThemeUI();
}

function updateThemeUI() {
    const themeIcon = document.querySelector('#themeToggle .side-menu-item-icon');
    const themeStatus = document.getElementById('themeStatus');
    
    if (themeIcon) {
        themeIcon.textContent = darkMode ? '☀️' : '🌙';
    }
    
    if (themeStatus) {
        themeStatus.textContent = darkMode ? 'Tema scuro attivo' : 'Tema scuro disattivato';
    }
}

function openExportModal() {
    const modal = document.getElementById('exportModal');
    if (!modal) {
        return;
    }
    
    exportModalOpen = true;
    closeMenu();
    markBodyModalOpen();
    
    // Imposta date predefinite (oggi e una settimana fa)
    const today = new Date();
    const weekAgo = new Date();
    weekAgo.setDate(today.getDate() - 7);
    
    const startDateInput = document.getElementById('exportStartDate');
    const endDateInput = document.getElementById('exportEndDate');
    
    if (startDateInput) {
        startDateInput.value = weekAgo.toISOString().split('T')[0];
    }
    if (endDateInput) {
        endDateInput.value = today.toISOString().split('T')[0];
    }
    
    modal.style.display = 'flex';
}

function closeExportModal() {
    const modal = document.getElementById('exportModal');
    if (modal) {
        modal.style.display = 'none';
    }
    exportModalOpen = false;
    releaseBodyModalState();
}

async function performExport(format) {
    const startDate = document.getElementById('exportStartDate')?.value || '';
    const endDate = document.getElementById('exportEndDate')?.value || '';
    
    const params = new URLSearchParams();
    params.append('format', format);
    if (startDate) {
        params.append('start_date', startDate);
    }
    if (endDate) {
        params.append('end_date', endDate);
    }
    
    try {
        const url = `/api/export?${params.toString()}`;
        
        // Mostra feedback loading
        showPopup('⏳ Generazione report in corso...');
        
        const response = await fetch(url);
        
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Export fallito');
        }
        
        // Download file
        const blob = await response.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = downloadUrl;
        
        // Estrai filename dall'header Content-Disposition o genera uno di default
        const contentDisposition = response.headers.get('Content-Disposition');
        let filename = `joblog_report_${new Date().toISOString().split('T')[0]}.${format === 'excel' ? 'xlsx' : 'csv'}`;
        
        if (contentDisposition) {
            const matches = /filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/.exec(contentDisposition);
            if (matches != null && matches[1]) {
                filename = matches[1].replace(/['"]/g, '');
            }
        }
        
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(downloadUrl);
        document.body.removeChild(a);
        
        showPopup(`✅ Report ${format === 'excel' ? 'Excel' : 'CSV'} scaricato!`);
        closeExportModal();
    } catch (error) {
        console.error('Export error:', error);
        showPopup('⚠️ Errore durante l\'export');
    }
}

function setProjectVisibility(active) {
    projectVisible = active;

    ["teamCard", "activities"].forEach((id) => {
        const section = document.getElementById(id);
        if (section) {
            section.style.display = active ? "" : "none";
        }
    });

    const emptyState = document.getElementById("emptyState");
    if (emptyState) {
        emptyState.style.display = active ? "none" : "flex";
    }

    const timelineBtn = document.getElementById("timelineBtn");
    if (timelineBtn) {
        timelineBtn.disabled = !active;
        if (!active) {
            timelineBtn.classList.remove("active");
            timelineBtn.setAttribute("aria-pressed", "false");
        }
    }

    ["togglePauseBtn", "finishAllBtn", "moveBtn"].forEach((id) => {
        const btn = document.getElementById(id);
        if (btn) {
            btn.disabled = !active;
        }
    });

    if (!active) {
        document
            .querySelectorAll(".team-member.selected, .member-task.selected")
            .forEach((node) => {
                node.classList.remove("selected");
            });
        setKeypadVisibility(false);
        setTimelineVisibility(false);
        eventsCache = [];
        renderEvents([]);
    }

    updateSelectionToolbar();
}

function setProjectDefaultDate() {
    const dateInput = document.getElementById("projectDateInput");
    if (!dateInput || dateInput.value) {
        return;
    }
    const today = new Date();
    const iso = today.toISOString().split("T")[0];
    dateInput.value = iso;
}

function getSelectedMemberNodes() {
    return Array.from(
        document.querySelectorAll(".team-member.selected, .member-task.selected")
    );
}

function getSelectedKeys() {
    const keys = getSelectedMemberNodes()
        .map((node) => node.dataset.key)
        .filter(Boolean);
    return Array.from(new Set(keys));
}

function determineSelectionPauseIntent(nodes) {
    if (!nodes || nodes.length === 0) {
        return "pause";
    }
    const anyRunning = nodes.some((node) => node.dataset.running === "true");
    if (anyRunning) {
        return "pause";
    }
    const anyPaused = nodes.some((node) => node.dataset.paused === "true");
    if (anyPaused) {
        return "resume";
    }
    return "pause";
}

function getEligiblePauseKeys(nodes, intent) {
    if (!nodes || nodes.length === 0) {
        return [];
    }
    const predicate =
        intent === "pause"
            ? (node) => node.dataset.running === "true"
            : (node) => node.dataset.paused === "true";
    const unique = new Set();
    nodes.forEach((node) => {
        const key = node.dataset.key;
        if (!key || unique.has(key)) {
            return;
        }
        if (predicate(node)) {
            unique.add(key);
        }
    });
    return Array.from(unique);
}

function getActivityMemberNodes(activityId) {
    if (!activityId) {
        return [];
    }
    const escaped = escapeAttribute(activityId);
    return Array.from(
        document.querySelectorAll(`.member-task[data-activity-id="${escaped}"]`)
    );
}

function updateActivitySelectButtons() {
    document.querySelectorAll("[data-activity-select]").forEach((button) => {
        const activityId = button.dataset.activitySelect;
        const nodes = getActivityMemberNodes(activityId);
        const allSelected =
            nodes.length > 0 && nodes.every((node) => node.classList.contains("selected"));
        button.textContent = allSelected ? "Deseleziona tutti" : "Seleziona tutti";
    });
}

function toggleActivitySelection(activityId) {
    const nodes = getActivityMemberNodes(activityId);
    if (nodes.length === 0) {
        return;
    }
    const shouldSelect = !nodes.every((node) => node.classList.contains("selected"));
    nodes.forEach((node) => {
        node.classList.toggle("selected", shouldSelect);
    });
    updateSelectionToolbar();
}

function toggleActivityCollapse(card, activityId) {
    if (!card) {
        return;
    }
    const targetId = activityId ? String(activityId) : "";
    const collapsed = card.classList.toggle("collapsed");
    const members = card.querySelector(".task-members");
    if (members) {
        members.classList.toggle("hidden", collapsed);
    }
    const button = card.querySelector("[data-collapse-toggle]");
    if (button) {
        button.textContent = collapsed ? "Mostra operatori" : "Nascondi operatori";
        button.setAttribute("aria-expanded", collapsed ? "false" : "true");
    }
    if (targetId) {
        if (collapsed) {
            collapsedActivities.add(targetId);
        } else {
            collapsedActivities.delete(targetId);
        }
    }
    updateActivitySelectButtons();
    if (activityId) {
        updateActivityTotalDisplay(activityId);
    }
}

async function performSelectionAction(memberKeys, endpoint, successMessage, errorMessage) {
    if (!memberKeys || memberKeys.length === 0) {
        showPopup("⚠️ Nessuna risorsa selezionata");
        return false;
    }
    const toolbarButtons = [
        document.getElementById("selectionMoveBtn"),
        document.getElementById("selectionPauseBtn"),
        document.getElementById("selectionFinishBtn"),
    ].filter(Boolean);
    toolbarButtons.forEach((btn) => {
        btn.disabled = true;
    });
    try {
        for (const memberKey of memberKeys) {
            await postJson(endpoint, { member_key: memberKey });
        }
        suppressSelectionRestore = true;
        if (successMessage) {
            showPopup(successMessage);
        }
        await refreshState();
        return true;
    } catch (err) {
        console.error("performSelectionAction", endpoint, err);
        showPopup(errorMessage || "⚠️ Operazione non riuscita");
        return false;
    } finally {
        updateSelectionToolbar();
    }
}

async function toggleSelectionPause() {
    const nodes = getSelectedMemberNodes();
    const keys = getSelectedKeys();
    if (keys.length === 0) {
        showPopup("⚠️ Nessuna risorsa selezionata");
        return;
    }
    const intent = determineSelectionPauseIntent(nodes);
    const eligibleKeys = getEligiblePauseKeys(nodes, intent);
    if (eligibleKeys.length === 0) {
        showPopup(
            intent === "pause"
                ? "⚠️ Nessun operatore in attività nella selezione"
                : "⚠️ Nessun operatore in pausa nella selezione"
        );
        return;
    }
    if (intent === "pause") {
        await performSelectionAction(
            eligibleKeys,
            "/api/member/pause",
            "⏸️ Operatori in pausa",
            "⚠️ Impossibile mettere in pausa la selezione"
        );
    } else {
        await performSelectionAction(
            eligibleKeys,
            "/api/member/resume",
            "▶️ Operatori ripresi",
            "⚠️ Impossibile riprendere la selezione"
        );
    }
}

async function finishSelection() {
    const keys = getSelectedKeys();
    if (keys.length === 0) {
        showPopup("⚠️ Nessuna risorsa selezionata");
        return;
    }
    await performSelectionAction(
        keys,
        "/api/member/finish",
        "✅ Attività concluse",
        "⚠️ Impossibile chiudere le attività selezionate"
    );
}

function syncSelectionToolbarOffset(inputToolbar) {
    const toolbar = inputToolbar || document.getElementById("selectionToolbar");
    const root = document.documentElement;
    if (!toolbar || !root) {
        return;
    }
    if (toolbar.classList.contains("hidden")) {
        root.style.setProperty("--selection-toolbar-height", "0px");
        return;
    }
    requestAnimationFrame(() => {
        const measured = toolbar.offsetHeight || 0;
        const spacing = measured > 0 ? measured + 12 : 0;
        root.style.setProperty("--selection-toolbar-height", `${spacing}px`);
    });
}

function updateSelectionToolbar() {
    const toolbar = document.getElementById("selectionToolbar");
    if (!toolbar) {
        return;
    }

    const selectedNodes = getSelectedMemberNodes();
    const keys = getSelectedKeys();
    const count = keys.length;
    const hasSelection = count > 0 && projectVisible;
    const label = document.getElementById("selectionCount");
    const suppressed = toolbar.dataset.modalSuppressed === "true";
    const shouldShow = hasSelection && !suppressed;

    if (label) {
        label.textContent =
            count === 1 ? "1 operatore selezionato" : `${count} operatori selezionati`;
    }

    toolbar.classList.toggle("hidden", !shouldShow);
    syncSelectionToolbarOffset(toolbar);

    const moveBtn = document.getElementById("selectionMoveBtn");
    const pauseBtn = document.getElementById("selectionPauseBtn");
    const finishBtn = document.getElementById("selectionFinishBtn");
    const startBtn = document.getElementById("selectionStartBtn");
    [moveBtn, pauseBtn, finishBtn, startBtn]
        .filter(Boolean)
        .forEach((btn) => {
            btn.disabled = !hasSelection;
        });

    if (pauseBtn) {
        const intent = determineSelectionPauseIntent(selectedNodes);
        pauseBtn.textContent =
            intent === "pause" ? "⏸️ Pausa selezione" : "▶️ Riprendi selezione";
    }

    // Mostra/nascondi il pulsante Avvia in base allo stato degli operatori selezionati
    if (startBtn) {
        const canStart = selectedNodes.some(node => 
            node.dataset.running !== "true" && 
            node.dataset.paused !== "true" && 
            node.dataset.activityId && 
            node.dataset.activityId !== "" &&
            node.dataset.activityId !== "null" &&
            node.dataset.activityId !== "undefined"
        );
        startBtn.style.display = canStart ? "" : "none";
    }

    updateActivitySelectButtons();
}

function restoreSelection(keys) {
    keys.forEach((key) => {
        forEachMemberNode(key, (node) => {
            node.classList.add("selected");
        });
    });

    updateSelectionToolbar();
}

function attachTimer(member) {
    const key = member.member_key;
    const timerId = `timer-${domId(key)}`;
    const displays = Array.from(document.querySelectorAll(`#${timerId}`));
    if (displays.length === 0) {
        return;
    }
    if (timers[key]) {
        clearInterval(timers[key]);
    }
    let elapsed = Number(member.elapsed) || 0;

    const syncNodes = () => {
        displays.forEach((display) => {
            display.textContent = formatTime(elapsed);
        });
        forEachMemberNode(key, (node) => {
            node.dataset.elapsedMs = String(elapsed);
            node.dataset.running = member.running ? "true" : "false";
            node.dataset.paused = member.paused ? "true" : "false";
        });
    };

    syncNodes();
    refreshTotalRunningTimeDisplay();
    if (member.activity_id) {
        updateActivityTotalDisplay(member.activity_id);
    }

    if (!member.running) {
        delete timers[key];
        return;
    }

    timers[key] = setInterval(() => {
        elapsed += 1000;
        syncNodes();
        refreshTotalRunningTimeDisplay();
        if (member.activity_id) {
            updateActivityTotalDisplay(member.activity_id);
        }
    }, 1000);
}

function createMemberNode(member, baseClass) {
    const node = document.createElement("div");
    node.className = baseClass;
    node.dataset.key = member.member_key;
    node.dataset.name = member.member_name;
    node.dataset.activityId = member.activity_id || "";
    node.dataset.running = member.running ? "true" : "false";
    node.dataset.paused = member.paused ? "true" : "false";
    node.dataset.elapsedMs = String(Number(member.elapsed) || 0);
    const timerId = `timer-${domId(member.member_key)}`;
    const statusLabel = member.running
        ? "In attività"
        : member.paused
        ? "In pausa"
        : "In attesa";
    
    node.innerHTML = `
        <div class="task-header-row">
            <span>${member.member_name}</span>
            <span class="timer-display" id="${timerId}">${formatTime(member.elapsed)}</span>
        </div>
        <div class="pause-info">${statusLabel}</div>
    `;

    node.addEventListener("click", () => toggleSelection(node));
    return node;
}

function renderTeam(team) {
    const list = document.getElementById("memberList");
    if (!list) {
        return;
    }
    list.innerHTML = "";
    team.forEach((member) => {
        const node = createMemberNode(member, "team-member");
        list.appendChild(node);
    });
}

function renderActivities(activities) {
    const container = document.getElementById("activities");
    if (!container) {
        return;
    }
    container.innerHTML = "";
    activityTotalDisplays.clear();
    activities.forEach((activity) => {
        const card = document.createElement("div");
        card.className = "task-card";
        const activityId = activity.activity_id ? String(activity.activity_id) : "";
        card.dataset.activityId = activityId;
        const scheduleLabel = formatPlanningRange(activity.plan_start, activity.plan_end);
        const isCollapsed = activityId ? collapsedActivities.has(activityId) : false;

        const header = document.createElement("div");
        header.className = "task-header";

        const info = document.createElement("div");
        info.className = "task-header-info";
        const title = document.createElement("span");
        title.textContent = activity.label;
        info.appendChild(title);
        if (scheduleLabel) {
            const schedule = document.createElement("div");
            schedule.className = "task-schedule";
            schedule.textContent = scheduleLabel;
            info.appendChild(schedule);
        }

        const meta = document.createElement("div");
        meta.className = "task-header-meta";
        const count = document.createElement("span");
        count.className = "timer-display";
        count.textContent = `${activity.members.length} operatori`;
        meta.appendChild(count);

        const timeSummary = document.createElement("div");
        timeSummary.className = "activity-time-summary";
        const timeLabel = document.createElement("span");
        timeLabel.className = "activity-time-label";
        timeLabel.textContent = "Tempo in corso";
        const timeValue = document.createElement("span");
        timeValue.className = "activity-time-value";
        timeValue.textContent = formatTime(calculateActivityRunningTime(activity.members));
        if (activityId) {
            activityTotalDisplays.set(activityId, timeValue);
        }
        timeSummary.appendChild(timeLabel);
        timeSummary.appendChild(timeValue);
        meta.appendChild(timeSummary);

        const collapseBtn = document.createElement("button");
        collapseBtn.type = "button";
        collapseBtn.className = "activity-collapse-btn";
        collapseBtn.dataset.collapseToggle = activityId;
        collapseBtn.textContent = isCollapsed ? "Mostra operatori" : "Nascondi operatori";
        collapseBtn.setAttribute("aria-expanded", isCollapsed ? "false" : "true");
        collapseBtn.addEventListener("click", (event) => {
            event.stopPropagation();
            toggleActivityCollapse(card, activityId);
        });
        meta.appendChild(collapseBtn);

        const selectBtn = document.createElement("button");
        selectBtn.type = "button";
        selectBtn.className = "activity-select-btn";
        selectBtn.dataset.activitySelect = activity.activity_id || "";
        selectBtn.textContent = "Seleziona tutti";
        selectBtn.addEventListener("click", () =>
            toggleActivitySelection(selectBtn.dataset.activitySelect)
        );
        meta.appendChild(selectBtn);

        const completeBtn = document.createElement("button");
        completeBtn.type = "button";
        completeBtn.className = "activity-complete-btn";
        completeBtn.textContent = "Attività completata";
        completeBtn.addEventListener("click", () => openFeedbackModalForActivity(activity));
        meta.appendChild(completeBtn);

        header.appendChild(info);
        header.appendChild(meta);
        card.appendChild(header);

        const body = document.createElement("div");
        body.className = "task-members";
        activity.members.forEach((member) => {
            const node = createMemberNode(member, "member-task");
            body.appendChild(node);
        });
        if (isCollapsed) {
            card.classList.add("collapsed");
            body.classList.add("hidden");
        }
        card.appendChild(body);
        container.appendChild(card);
        if (activityId) {
            updateActivityTotalDisplay(activityId);
        }
    });
    updateActivitySelectButtons();
}

function updateToggleButton() {
    const toggle = document.getElementById("togglePauseBtn");
    if (!toggle) {
        return;
    }
    toggle.textContent = allPaused ? "▶️ Riprendi Tutti" : "⏸️ Pausa Tutti";
}

function setProjectLabel(project) {
    const projectLabel = document.getElementById("projectLabel");
    if (!projectLabel) {
        return;
    }
    if (project) {
        projectLabel.textContent = `${project.code} · ${project.name}`;
    } else {
        projectLabel.textContent = "Nessun progetto attivo";
    }
}

function resetProjectStateUI() {
    clearTimers();
    renderTeam([]);
    renderActivities([]);
    cachedActivities = [];
    allPaused = true;
    suppressSelectionRestore = false;
    setProjectVisibility(false);
    setProjectLabel(null);
    eventsCache = [];
    renderEvents([]);
    updateToggleButton();
    updateSelectionToolbar();
    activitySearchTerm = "";
    collapsedActivities.clear();
    activityTotalDisplays.clear();
    const search = document.getElementById("activitySearch");
    if (search) {
        search.value = "";
    }
    const totalRunning = document.getElementById("totalRunningTime");
    if (totalRunning) {
        totalRunning.textContent = "00:00:00";
    }
}

function applyState(state) {
    const previouslySelected = getSelectedKeys();
    clearTimers();
    renderTeam(state.team);
    renderActivities(state.activities);

    const everyone = [
        ...state.team,
        ...state.activities.flatMap((activity) => activity.members),
    ];
    everyone.forEach(attachTimer);

    const hasProject = Boolean(state.project);
    if (hasProject && previouslySelected.length > 0 && !suppressSelectionRestore) {
        restoreSelection(previouslySelected);
    }
    suppressSelectionRestore = false;

    cachedActivities = state.activities || [];
    allPaused = state.allPaused;
    setProjectVisibility(hasProject);
    setProjectLabel(state.project || null);
    updateToggleButton();
    updateSelectionToolbar();
    refreshTotalRunningTimeDisplay();
}

function stopRefreshTimer() {
    if (!refreshTimer) {
        return;
    }
    clearTimeout(refreshTimer);
    refreshTimer = null;
}

function scheduleRefresh() {
    if (pollingSuspended) {
        return;
    }
    stopRefreshTimer();
    refreshTimer = setTimeout(refreshState, 5000);
}

function handleUnauthenticated() {
    if (pollingSuspended) {
        return;
    }
    pollingSuspended = true;
    stopRefreshTimer();
    if (!unauthorizedNotified) {
        unauthorizedNotified = true;
        showPopup("⚠️ Sessione scaduta. Effettua di nuovo l'accesso.");
        setTimeout(() => {
            window.location.href = "/login";
        }, 1500);
    }
}

async function refreshState() {
    try {
        const [stateData, eventsData] = await Promise.all([
            fetchJson("/api/state"),
            fetchJson("/api/events"),
        ]);
        applyState(stateData);
        renderEvents(eventsData.events || []);
    } catch (err) {
        if (err && err.status === 401) {
            console.warn("refreshState unauthorized", err);
            handleUnauthenticated();
        } else {
            console.error("refreshState", err);
        }
    } finally {
        scheduleRefresh();
    }
}

async function moveTo(activityId) {
    const selectedNodes = getSelectedMemberNodes();
    if (selectedNodes.length === 0) {
        closeActivityModal();
        return;
    }
    const uniqueMembers = new Map();
    selectedNodes.forEach((node) => {
        const name = node.dataset.name || node.textContent.trim();
        const key = node.dataset.key || safeKey(name);
        if (!key || uniqueMembers.has(key)) {
            return;
        }
        uniqueMembers.set(key, {
            member_key: key,
            member_name: name,
            activity_id: activityId,
        });
    });
    const payloads = Array.from(uniqueMembers.values());
    if (payloads.length === 0) {
        closeActivityModal();
        return;
    }
    try {
        await Promise.all(
            payloads.map((body) =>
                fetch("/api/move", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(body),
                })
            )
        );
        showPopup("✅ Risorse aggiornate");
        closeActivityModal();
        suppressSelectionRestore = true;
        selectedNodes.forEach((node) => {
            node.classList.remove("selected");
        });
        updateSelectionToolbar();
        await refreshState();
    } catch (err) {
        console.error("moveTo", err);
        showPopup("⚠️ Impossibile spostare le risorse");
    }
}

async function pauseAll() {
    await fetch("/api/pause_all", { method: "POST" });
    showPopup("⏸️ Tutti in pausa");
    await refreshState();
}

async function startActivity(activityId) {
    if (!activityId) {
        showPopup("⚠️ Attività non valida");
        return;
    }
    try {
        const res = await fetch("/api/start_activity", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ activity_id: activityId }),
        });
        if (res.ok) {
            const data = await res.json();
            showPopup(`▶️ Timer avviati per ${data.affected || 0} operatori`);
            await refreshState();
        } else {
            throw new Error(`HTTP ${res.status}`);
        }
    } catch (err) {
        console.error("startActivity", err);
        showPopup("⚠️ Impossibile avviare l'attività");
    }
}

async function startMember(memberKey) {
    if (!memberKey) {
        showPopup("⚠️ Operatore non valido");
        return;
    }
    try {
        const res = await fetch("/api/start_member", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ member_key: memberKey }),
        });
        if (res.ok) {
            showPopup("▶️ Timer avviato");
            await refreshState();
        } else {
            throw new Error(`HTTP ${res.status}`);
        }
    } catch (err) {
        console.error("startMember", err);
        showPopup("⚠️ Impossibile avviare l'operatore");
    }
}

async function startSelection() {
    const selectedNodes = getSelectedMemberNodes();
    const keys = selectedNodes
        .filter(node => {
            const hasActivity = node.dataset.activityId && 
                               node.dataset.activityId !== "" &&
                               node.dataset.activityId !== "null" &&
                               node.dataset.activityId !== "undefined";
            const notRunning = node.dataset.running !== "true";
            const notPaused = node.dataset.paused !== "true";
            return hasActivity && notRunning && notPaused;
        })
        .map(node => node.dataset.key)
        .filter(Boolean);
    
    if (keys.length === 0) {
        showPopup("⚠️ Nessun operatore da avviare");
        return;
    }
    
    try {
        let started = 0;
        for (const memberKey of keys) {
            const res = await fetch("/api/start_member", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ member_key: memberKey }),
            });
            if (res.ok) {
                started++;
            }
        }
        showPopup(`▶️ ${started} timer avviati`);
        
        // Deseleziona tutti gli operatori avviati
        suppressSelectionRestore = true;
        selectedNodes.forEach((node) => {
            node.classList.remove("selected");
        });
        updateSelectionToolbar();
        
        await refreshState();
    } catch (err) {
        console.error("startSelection", err);
        showPopup("⚠️ Impossibile avviare gli operatori");
    }
}

async function resumeAll() {
    await fetch("/api/resume_all", { method: "POST" });
    showPopup("▶️ Attività riprese");
    await refreshState();
}

async function finishAll() {
    try {
        await fetch("/api/finish_all", { method: "POST" });
        suppressSelectionRestore = true;
        document
            .querySelectorAll(".team-member.selected, .member-task.selected")
            .forEach((node) => {
                node.classList.remove("selected");
            });
        updateSelectionToolbar();
        showPopup("✅ Attività chiuse");
        await refreshState();
    } catch (err) {
        console.error("finishAll", err);
        showPopup("⚠️ Impossibile chiudere le attività");
    }
}

function anyModalVisible() {
    return Array.from(document.querySelectorAll(".modal")).some((node) => {
        return node.style.display && node.style.display !== "none";
    });
}

function markBodyModalOpen() {
    if (document.body) {
        document.body.classList.add("modal-open");
        syncSelectionToolbarOffset();
    }
}

function releaseBodyModalState() {
    if (!document.body) {
        return;
    }
    if (!anyModalVisible()) {
        document.body.classList.remove("modal-open");
        syncSelectionToolbarOffset();
    }
}

function clearMembersSelection(memberKeys) {
    if (!Array.isArray(memberKeys)) {
        return;
    }
    memberKeys.forEach((key) => {
        if (!key) {
            return;
        }
        forEachMemberNode(key, (node) => {
            node.classList.remove("selected");
        });
    });
}

function openActivityModal() {
    const modal = document.getElementById("activityModal");
    if (!modal) {
        return;
    }
    markBodyModalOpen();
    const toolbar = document.getElementById("selectionToolbar");
    if (toolbar) {
        selectionToolbarWasVisible = !toolbar.classList.contains("hidden");
        toolbar.dataset.modalSuppressed = "true";
        toolbar.classList.add("hidden");
        syncSelectionToolbarOffset(toolbar);
    }
    const search = document.getElementById("activitySearch");
    if (search) {
        search.value = activitySearchTerm;
        search.setAttribute('readonly', 'readonly');
    }
    renderActivityChoices();
    modal.style.display = "flex";
}

function closeActivityModal(options) {
    const opts = {
        clearSelection: false,
        ...((typeof options === "object" && options) || {}),
    };
    const modal = document.getElementById("activityModal");
    if (modal) {
        modal.style.display = "none";
    }
    releaseBodyModalState();
    const toolbar = document.getElementById("selectionToolbar");
    if (toolbar && toolbar.dataset.modalSuppressed) {
        delete toolbar.dataset.modalSuppressed;
        if (opts.clearSelection) {
            toolbar.classList.add("hidden");
        } else if (selectionToolbarWasVisible) {
            toolbar.classList.remove("hidden");
        } else {
            toolbar.classList.add("hidden");
        }
    }
    selectionToolbarWasVisible = false;
    if (opts.clearSelection) {
        document
            .querySelectorAll(".team-member.selected, .member-task.selected")
            .forEach((node) => {
                node.classList.remove("selected");
            });
    }
    updateSelectionToolbar();
}

function updateFeedbackStars(selected) {
    const stars = document.querySelectorAll(".feedback-star");
    stars.forEach((star) => {
        const value = Number(star.dataset.rating || "0");
        const active = value > 0 && value <= selected;
        star.classList.toggle("active", active);
        star.setAttribute("aria-pressed", active ? "true" : "false");
    });
    const ratingDisplay = document.getElementById("feedbackRatingValue");
    if (ratingDisplay) {
        ratingDisplay.textContent = `${selected}/5`;
    }
}

function openFeedbackModalForActivity(activity) {
    const modal = document.getElementById("feedbackModal");
    if (!modal) {
        return;
    }
    feedbackContext = {
        activityId: activity.activity_id || null,
        label: activity.label || "",
        memberKeys: Array.isArray(activity.members)
            ? activity.members
                  .map((member) => member && member.member_key)
                  .filter((key) => Boolean(key))
            : [],
        rating: 0,
    };
    const labelNode = document.getElementById("feedbackActivityName");
    if (labelNode) {
        labelNode.textContent = feedbackContext.label || "Attività";
    }
    const submitBtn = document.getElementById("feedbackSubmitBtn");
    if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.textContent = "Salva feedback";
    }
    updateFeedbackStars(0);
    modal.style.display = "flex";
    markBodyModalOpen();
    const toolbar = document.getElementById("selectionToolbar");
    if (toolbar) {
        feedbackToolbarWasVisible = !toolbar.classList.contains("hidden");
        toolbar.classList.add("hidden");
        syncSelectionToolbarOffset(toolbar);
    } else {
        feedbackToolbarWasVisible = false;
    }
}

function closeFeedbackModal() {
    const modal = document.getElementById("feedbackModal");
    if (modal) {
        modal.style.display = "none";
    }
    releaseBodyModalState();
    feedbackContext = null;
    updateFeedbackStars(0);
    const toolbar = document.getElementById("selectionToolbar");
    if (toolbar && feedbackToolbarWasVisible) {
        toolbar.classList.remove("hidden");
    }
    feedbackToolbarWasVisible = false;
    updateSelectionToolbar();
}

function selectFeedbackRating(value) {
    if (!feedbackContext) {
        return;
    }
    const rating = Number(value);
    if (!Number.isFinite(rating) || rating < 1) {
        return;
    }
    feedbackContext.rating = Math.min(5, rating);
    updateFeedbackStars(feedbackContext.rating);
    const submitBtn = document.getElementById("feedbackSubmitBtn");
    if (submitBtn) {
        submitBtn.disabled = false;
    }
}

async function submitFeedback() {
    if (!feedbackContext) {
        closeFeedbackModal();
        return;
    }
    const submitBtn = document.getElementById("feedbackSubmitBtn");
    if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.textContent = "Invio...";
    }
    const { memberKeys, rating, label } = feedbackContext;
    let success = true;
    try {
        if (memberKeys.length > 0) {
            clearMembersSelection(memberKeys);
            success = await performSelectionAction(
                memberKeys,
                "/api/member/finish",
                "",
                "⚠️ Impossibile chiudere l'attività"
            );
        }
        if (success) {
            console.log("Feedback attività", {
                activity: label,
                rating,
            });
            showPopup(`⭐ Grazie! Valutazione ${rating}/5 registrata`);
        }
    } finally {
        closeFeedbackModal();
        if (submitBtn) {
            submitBtn.disabled = false;
            submitBtn.textContent = "Salva feedback";
        }
    }
}

function handleActivitySearchInput(event) {
    activitySearchTerm = event.target.value || "";
    renderActivityChoices();
}

function ensureActivitySearch() {
    const input = document.getElementById("activitySearch");
    if (!input || activitySearchInitialized) {
        return input;
    }
    activitySearchInitialized = true;
    input.addEventListener("input", handleActivitySearchInput);
    return input;
}

function renderActivityChoices() {
    const choices = document.getElementById("choices");
    if (!choices) {
        return;
    }

    choices.innerHTML = "";
    const normalized = (activitySearchTerm || "").trim().toLowerCase();
    const items = sortActivitiesForPicker(cachedActivities);
    const filtered = normalized
        ? items.filter((activity) =>
              activity.label.toLowerCase().includes(normalized)
          )
        : items;

    if (filtered.length === 0) {
        const empty = document.createElement("div");
        empty.className = "activity-empty";
        empty.textContent = "Nessuna attività corrispondente";
        choices.appendChild(empty);
        return;
    }

    filtered.forEach((activity) => {
        const button = document.createElement("button");
        button.className = "choice";
        button.textContent = activity.label;
        button.addEventListener("click", () => moveTo(activity.activity_id));
        choices.appendChild(button);
    });
}

function requestMoveSelection() {
    const selectedCount = getSelectedKeys().length;
    if (selectedCount === 0) {
        showPopup("⚠️ Nessuna risorsa selezionata");
        return;
    }
    if (cachedActivities.length === 0) {
        showPopup("⚠️ Nessuna attività disponibile");
        return;
    }
    openActivityModal();
}

async function loadProject(projectCode) {
    const code = String(projectCode ?? "").replace(/\D/g, "");
    setProjectCodeBuffer(code);
    setKeypadVisibility(false);
    if (!code) {
        showPopup("⚠️ Inserisci un codice progetto numerico");
        return;
    }
    const dateInput = document.getElementById("projectDateInput");
    const projectDate = dateInput ? dateInput.value : "";
    if (!projectDate) {
        showPopup("⚠️ Seleziona una data di riferimento");
        return;
    }
    if (isLoadingProject) {
        return;
    }
    isLoadingProject = true;
    const btn = document.getElementById("loadProjectBtn");
    const original = btn ? btn.dataset.label || btn.textContent : "";
    if (btn) {
        btn.disabled = true;
        btn.textContent = "⏳ Carico...";
    }
    try {
        const res = await fetch("/api/load_project", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ project_code: code, project_date: projectDate }),
        });
        if (res.ok) {
            const data = await res.json();
            showPopup(`📦 Progetto ${data.project.code} pronto`);
            await refreshState();
        } else if (res.status === 404) {
            resetProjectStateUI();
            showPopup("⚠️ Progetto non trovato");
            await refreshState();
        } else {
            throw new Error(`HTTP ${res.status}`);
        }
    } catch (err) {
        console.error("loadProject", err);
        showPopup("⚠️ Impossibile caricare il progetto");
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.textContent = original || "🚚 Carica Progetto";
        }
        isLoadingProject = false;
    }
}

function bindUI() {
    const startAllBtn = document.getElementById("startAllBtn");
    const toggle = document.getElementById("togglePauseBtn");
    const finishAllBtn = document.getElementById("finishAllBtn");
    const moveBtn = document.getElementById("moveBtn");
    const selectionMoveBtn = document.getElementById("selectionMoveBtn");
    const selectionPauseBtn = document.getElementById("selectionPauseBtn");
    const selectionFinishBtn = document.getElementById("selectionFinishBtn");
    const selectionStartBtn = document.getElementById("selectionStartBtn");
    const cancelBtn = document.getElementById("cancelBtn");
    const modal = document.getElementById("activityModal");
    const loadBtn = document.getElementById("loadProjectBtn");
    const projectInput = document.getElementById("projectInput");
    const projectInputGroup = document.getElementById("projectInputGroup");
    const menuToggleBtn = document.getElementById("menuToggle");
    const menuCloseBtn = document.getElementById("menuCloseBtn");
    const menuOverlay = document.getElementById("menuOverlay");
    const feedbackModal = document.getElementById("feedbackModal");
    const feedbackSubmitBtn = document.getElementById("feedbackSubmitBtn");
    const feedbackCancelBtn = document.getElementById("feedbackCancelBtn");
    const feedbackStars = document.querySelectorAll(".feedback-star");

    if (startAllBtn) {
        startAllBtn.addEventListener("click", async () => {
            await startAll();
        });
    }

    if (toggle) {
        toggle.addEventListener("click", async () => {
            if (allPaused) {
                await resumeAll();
            } else {
                await pauseAll();
            }
        });
    }

    if (finishAllBtn) {
        finishAllBtn.addEventListener("click", () => {
            finishAll();
        });
    }

    if (moveBtn) {
        moveBtn.addEventListener("click", requestMoveSelection);
    }

    if (selectionMoveBtn) {
        selectionMoveBtn.addEventListener("click", requestMoveSelection);
    }

    if (selectionPauseBtn) {
        selectionPauseBtn.addEventListener("click", toggleSelectionPause);
    }

    if (selectionFinishBtn) {
        selectionFinishBtn.addEventListener("click", finishSelection);
    }

    if (selectionStartBtn) {
        selectionStartBtn.addEventListener("click", startSelection);
    }

    if (cancelBtn) {
        cancelBtn.addEventListener("click", () => {
            closeActivityModal({ clearSelection: true });
        });
    }

    if (loadBtn) {
        loadBtn.dataset.label = loadBtn.textContent;
        loadBtn.addEventListener("click", () => {
            loadProject(getProjectCode());
        });
    }

    if (projectInput) {
        projectInput.addEventListener("click", () => {
            clearProjectCode();
            setKeypadVisibility(true);
        });
    }

    feedbackStars.forEach((star) => {
        star.addEventListener("click", () => {
            selectFeedbackRating(Number(star.dataset.rating || "0"));
        });
    });

    if (feedbackSubmitBtn) {
        feedbackSubmitBtn.addEventListener("click", submitFeedback);
    }

    if (feedbackCancelBtn) {
        feedbackCancelBtn.addEventListener("click", closeFeedbackModal);
    }

    if (menuToggleBtn) {
        menuToggleBtn.addEventListener("click", toggleMenu);
    }

    if (menuCloseBtn) {
        menuCloseBtn.addEventListener("click", closeMenu);
    }

    if (menuOverlay) {
        menuOverlay.addEventListener("click", closeMenu);
    }

    const themeToggle = document.getElementById('themeToggle');
    if (themeToggle) {
        themeToggle.addEventListener('click', () => {
            toggleTheme();
        });
    }

    const exportToggle = document.getElementById('exportToggle');
    if (exportToggle) {
        exportToggle.addEventListener('click', openExportModal);
    }

    const exportExcelBtn = document.getElementById('exportExcelBtn');
    if (exportExcelBtn) {
        exportExcelBtn.addEventListener('click', () => performExport('excel'));
    }

    const exportCsvBtn = document.getElementById('exportCsvBtn');
    if (exportCsvBtn) {
        exportCsvBtn.addEventListener('click', () => performExport('csv'));
    }

    const exportCancelBtn = document.getElementById('exportCancelBtn');
    if (exportCancelBtn) {
        exportCancelBtn.addEventListener('click', closeExportModal);
    }

    const exportModal = document.getElementById('exportModal');
    if (exportModal) {
        exportModal.addEventListener('click', (event) => {
            if (event.target === exportModal) {
                closeExportModal();
            }
        });
    }

    document.querySelectorAll("[data-menu-action]").forEach((node) => {
        node.addEventListener("click", closeMenu);
    });

    ensureActivitySearch();

    if (modal) {
        modal.addEventListener("click", (event) => {
            if (event.target === modal) {
                closeActivityModal();
            }
        });
    }

    if (feedbackModal) {
        feedbackModal.addEventListener("click", (event) => {
            if (event.target === feedbackModal) {
                closeFeedbackModal();
            }
        });
    }

    document.addEventListener("mousedown", (event) => {
        handleGlobalPointerForKeypad(event, projectInputGroup);
    });
    document.addEventListener("touchstart", (event) => {
        handleGlobalPointerForKeypad(event, projectInputGroup);
    });

    const keypad = document.getElementById("projectKeypad");
    if (keypad) {
        keypad.addEventListener("click", (event) => {
            const target = event.target;
            if (!(target instanceof HTMLElement)) {
                return;
            }
            const digit = target.dataset.digit;
            if (digit) {
                appendProjectDigit(digit);
                return;
            }
            const action = target.dataset.action;
            if (action === "clear") {
                clearProjectCode();
            } else if (action === "back") {
                backspaceProjectDigit();
            }
        });
    }

    const timelineBtn = document.getElementById("timelineBtn");
    if (timelineBtn) {
        timelineBtn.addEventListener("click", () => {
            closeMenu();
            setTimelineVisibility(!timelineOpen);
        });
    }

    const closeTimelineBtn = document.getElementById("closeTimelineBtn");
    if (closeTimelineBtn) {
        closeTimelineBtn.addEventListener("click", closeTimeline);
    }

    const timelineOverlay = document.getElementById("timelineOverlay");
    if (timelineOverlay) {
        timelineOverlay.addEventListener("click", closeTimeline);
    }

    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
            closeFeedbackModal();
            closeActivityModal();
            closeTimeline();
            closeMenu();
            closeExportModal();
        }
    });

    setProjectVisibility(false);
    updateProjectInput();
}

function updateClock() {
    const clock = document.getElementById("clock");
    if (!clock) {
        return;
    }
    const now = new Date();
    const date = `${fmt2(now.getDate())}/${fmt2(now.getMonth() + 1)}/${now.getFullYear()}`;
    const time = `${fmt2(now.getHours())}:${fmt2(now.getMinutes())}`;
    clock.textContent = `${date} | ${time}`;
}

async function init() {
    initTheme();
    bindUI();
    setProjectDefaultDate();
    const releaseTarget = document.getElementById("menuReleaseVersion");
    if (releaseTarget) {
        releaseTarget.textContent = APP_RELEASE;
    }
    refreshTotalRunningTimeDisplay();
    updateClock();
    setInterval(updateClock, 1000);
    await refreshState();
}

window.addEventListener("DOMContentLoaded", init);
window.addEventListener("resize", () => {
    syncSelectionToolbarOffset();
});
