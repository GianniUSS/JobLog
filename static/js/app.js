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
let pushNotificationsCache = [];
let timelineOpen = false;
let activitySearchTerm = "";
let activitySearchInitialized = false;
let selectionToolbarWasVisible = false;
const collapsedActivities = new Set();
const activityTotalDisplays = new Map();
const activityOverdueTrackers = new Map();
const activityRuntimeOffsets = new Map();
const activityTotalValues = new Map();
const clientElapsedState = new Map();
const seenActivityIds = new Set();
const ACTIVITY_DELAY_GRACE_MS = 0;
const APP_RELEASE = "v2025.11.20a";
let menuOpen = false;
let feedbackContext = null;
let feedbackToolbarWasVisible = false;
let darkMode = false;
let exportModalOpen = false;
let pollingSuspended = false;
let unauthorizedNotified = false;
let pushNotificationsLoading = false;
let pushNotificationsModalOpen = false;
let lastKnownState = null;
let teamCollapsed = true;
const pushState = {
    supported: typeof window !== "undefined" && "serviceWorker" in navigator && "PushManager" in window && typeof Notification !== "undefined",
    configured: false,
    subscribed: false,
    publicKey: null,
};
const LAST_STATE_KEY = 'joblog-cache-state';
const LAST_EVENTS_KEY = 'joblog-cache-events';
const LAST_PUSH_KEY = 'joblog-cache-push';
let offlineMode = typeof navigator !== 'undefined' ? !navigator.onLine : false;
let offlineHydrated = false;
let offlineNotified = false;
const SERVICE_WORKER_READY_TIMEOUT = 2000;
const POPUP_DISPLAY_MS = 6000;
const NOTIFICATION_KIND_LABELS = {
    overdue_activity: "Attività oltre termine",
    test_message: "Notifica di test",
    long_running_member: "Operatore prolungato",
};
const QUEUE_ACTION_LABELS = {
    '/api/move': 'Spostamento operatori',
    '/api/member/pause': 'Pausa operatore',
    '/api/member/resume': 'Ripresa operatore',
    '/api/member/finish': 'Chiusura operatore',
    '/api/start_member': 'Avvio operatore',
    '/api/start_activity': 'Avvio attività',
    '/api/pause_all': 'Pausa di gruppo',
    '/api/resume_all': 'Ripresa di gruppo',
    '/api/finish_all': 'Chiusura di gruppo',
};
const PUSH_NOTIFICATIONS_LIMIT = 'all';

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
        if (res.status === 401) {
            handleUnauthenticated();
        }
        throw error;
    }
    const data = await res.json();
    const queuedHeader = res.headers.get('X-JobLog-Queued');
    if (queuedHeader === '1' && data && typeof data === 'object') {
        data.__queued = true;
    }
    return data;
}

async function fetchJson(url) {
    const res = await fetch(url);
    if (!res.ok) {
        const error = new Error(`HTTP ${res.status} for ${url}`);
        error.status = res.status;
        error.url = url;
        if (res.status === 401) {
            handleUnauthenticated();
        }
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

function formatDurationFromMs(ms) {
    if (!Number.isFinite(ms) || ms <= 0) {
        return "";
    }
    const totalMinutes = Math.round(ms / 60000);
    const hours = Math.floor(totalMinutes / 60);
    const minutes = totalMinutes % 60;
    return `${hours}h ${fmt2(minutes)}m`;
}

function urlBase64ToUint8Array(base64String) {
    const padding = "=".repeat((4 - (base64String.length % 4)) % 4);
    const base64 = (base64String + padding).replace(/-/g, "+").replace(/_/g, "/");
    const rawData = window.atob(base64);
    const outputArray = new Uint8Array(rawData.length);
    for (let i = 0; i < rawData.length; i += 1) {
        outputArray[i] = rawData.charCodeAt(i);
    }
    return outputArray;
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

const notificationTimestampFormatter = new Intl.DateTimeFormat("it-IT", {
    dateStyle: "short",
    timeStyle: "short",
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

function formatPlannedDuration(startIso, endIso, multiplier = 1) {
    const start = parsePlanningDate(startIso);
    const end = parsePlanningDate(endIso);
    if (!start || !end) {
        return "";
    }
    const diffMs = Math.max(0, end.getTime() - start.getTime());
    if (diffMs === 0) {
        return "";
    }
    const normalizedMultiplier = Number.isFinite(multiplier) && multiplier > 0 ? multiplier : 0;
    if (normalizedMultiplier === 0) {
        return "0h 00m";
    }
    const totalMinutes = Math.round((diffMs * normalizedMultiplier) / 60000);
    const hours = Math.floor(totalMinutes / 60);
    const minutes = totalMinutes % 60;
    return `${hours}h ${fmt2(minutes)}m`;
}

function getPlannedMemberMultiplier(activity) {
    if (!activity) {
        return 1;
    }
    const stored = Number(activity.planned_members);
    if (Number.isFinite(stored) && stored > 0) {
        return stored;
    }
    if (Array.isArray(activity.members) && activity.members.length > 0) {
        return activity.members.length;
    }
    return 1;
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
    let total = 0;
    activityTotalValues.forEach((value) => {
        if (Number.isFinite(value)) {
            total += value;
        }
    });

    document.querySelectorAll(".team-member").forEach((node) => {
        if (node.dataset.running !== "true") {
            return;
        }
        const value = Number(node.dataset.elapsedMs || 0);
        if (Number.isFinite(value)) {
            total += value;
        }
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

function getActivityPlannedDurationMs(activity) {
    if (!activity) {
        return null;
    }
    const storedDuration = Number(activity.planned_duration_ms);
    if (Number.isFinite(storedDuration) && storedDuration > 0) {
        return storedDuration;
    }
    const start = parsePlanningDate(activity.plan_start);
    const end = parsePlanningDate(activity.plan_end);
    if (!start || !end) {
        return null;
    }
    const baseDuration = Math.max(0, end.getTime() - start.getTime());
    if (baseDuration === 0) {
        return null;
    }
    const multiplier = getPlannedMemberMultiplier(activity);
    if (!Number.isFinite(multiplier) || multiplier <= 0) {
        return baseDuration;
    }
    return baseDuration * multiplier;
}

function formatDelayBadgeLabel(overdueMs) {
    const totalMinutes = Math.max(1, Math.floor(overdueMs / 60000));
    if (totalMinutes >= 60) {
        const hours = Math.floor(totalMinutes / 60);
        const minutes = totalMinutes % 60;
        if (minutes === 0) {
            return `+${hours}h ritardo`;
        }
        return `+${hours}h ${minutes}m ritardo`;
    }
    return `+${totalMinutes}m ritardo`;
}

function setActivityRuntimeOffset(activityId, value) {
    if (!activityId) {
        return;
    }
    const normalized = Math.max(0, Number(value) || 0);
    activityRuntimeOffsets.set(activityId, normalized);
    const tracker = activityOverdueTrackers.get(activityId);
    if (tracker) {
        tracker.baseMs = normalized;
    }
}

function addActivityRuntimeOffset(activityId, deltaMs) {
    if (!activityId) {
        return;
    }
    const contribution = Number(deltaMs);
    if (!Number.isFinite(contribution) || contribution <= 0) {
        return;
    }
    const nextValue = Math.max(0, (activityRuntimeOffsets.get(activityId) || 0) + contribution);
    activityRuntimeOffsets.set(activityId, nextValue);
    const tracker = activityOverdueTrackers.get(activityId);
    if (tracker) {
        tracker.baseMs = nextValue;
    }
    updateActivityTotalDisplay(activityId);
    refreshTotalRunningTimeDisplay();
}

function updateActivityDelayUI(activityId, runningMs) {
    if (!activityId) {
        return;
    }
    const key = String(activityId);
    const tracker = activityOverdueTrackers.get(key);
    if (!tracker || typeof tracker.plannedMs !== "number" || tracker.plannedMs <= 0) {
        return;
    }
    const baseMs = Number(tracker.baseMs) || 0;
    let total = runningMs;
    if (!Number.isFinite(total) && tracker.card instanceof HTMLElement) {
        total = 0;
        tracker.card.querySelectorAll(".member-task").forEach((node) => {
            if (node.dataset.running !== "true") {
                return;
            }
            const value = Number(node.dataset.elapsedMs || 0);
            if (Number.isFinite(value)) {
                total += value;
            }
        });
        total += baseMs;
    }
    if (!Number.isFinite(total)) {
        return;
    }
    const overdueMs = Math.max(0, total - tracker.plannedMs);
    const hasDelay = overdueMs > ACTIVITY_DELAY_GRACE_MS;
    if (tracker.wrapper) {
        tracker.wrapper.classList.toggle("hidden", !hasDelay);
    }
    if (tracker.valueNode) {
        tracker.valueNode.textContent = hasDelay ? formatTime(overdueMs) : "00:00:00";
    }
    if (tracker.badge) {
        tracker.badge.classList.toggle("hidden", !hasDelay);
        if (hasDelay) {
            tracker.badge.textContent = formatDelayBadgeLabel(overdueMs);
        }
    }
    if (tracker.card) {
        tracker.card.classList.toggle("task-card-overdue", hasDelay);
    }
}

function updateActivityTotalDisplay(activityId) {
    if (!activityId) {
        return;
    }
    const key = String(activityId);
    const display = activityTotalDisplays.get(key);
    const tracker = activityOverdueTrackers.get(key);
    let card = null;
    if (display instanceof HTMLElement) {
        card = display.closest(".task-card");
    }
    if (!card && tracker && tracker.card) {
        card = tracker.card;
    }
    if (!card) {
        return;
    }
    let runningMembersTotal = 0;
    card.querySelectorAll(".member-task").forEach((node) => {
        if (node.dataset.running !== "true") {
            return;
        }
        const value = Number(node.dataset.elapsedMs || 0);
        if (Number.isFinite(value)) {
            runningMembersTotal += value;
        }
    });
    const offset = activityRuntimeOffsets.get(key) || 0;
    const naiveTotal = runningMembersTotal + offset;
    const previousTotal = activityTotalValues.get(key) || 0;
    const correctedTotal = Math.max(naiveTotal, previousTotal);
    if (display) {
        display.textContent = formatTime(correctedTotal);
    }
    activityTotalValues.set(key, correctedTotal);
    updateActivityDelayUI(key, correctedTotal);
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
    setTimeout(() => node.classList.remove("show"), POPUP_DISPLAY_MS);
}

const STORAGE_AVAILABLE = (() => {
    if (typeof window === 'undefined' || typeof window.localStorage === 'undefined') {
        return false;
    }
    try {
        const key = '__joblog_cache_test__';
        window.localStorage.setItem(key, 'ok');
        window.localStorage.removeItem(key);
        return true;
    } catch (error) {
        console.warn('LocalStorage non disponibile', error);
        return false;
    }
})();

function saveCachedPayload(key, value) {
    if (!STORAGE_AVAILABLE) {
        return;
    }
    try {
        window.localStorage.setItem(key, JSON.stringify({ ts: Date.now(), data: value }));
    } catch (error) {
        console.warn('saveCachedPayload', key, error);
    }
}

function readCachedPayload(key) {
    if (!STORAGE_AVAILABLE) {
        return null;
    }
    try {
        const raw = window.localStorage.getItem(key);
        if (!raw) {
            return null;
        }
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed === 'object' && 'data' in parsed) {
            return parsed.data;
        }
    } catch (error) {
        console.warn('readCachedPayload', key, error);
    }
    return null;
}

function loadCachedStateAndEvents() {
    const cachedState = readCachedPayload(LAST_STATE_KEY);
    if (cachedState) {
        applyState(cachedState);
    }
    const cachedEvents = readCachedPayload(LAST_EVENTS_KEY);
    if (cachedEvents) {
        renderEvents(cachedEvents);
    }
}

function hydrateCacheOnce() {
    if (offlineHydrated) {
        return false;
    }
    loadCachedStateAndEvents();
    offlineHydrated = true;
    return true;
}

function loadCachedNotifications(options) {
    const cachedNotifications = readCachedPayload(LAST_PUSH_KEY);
    if (cachedNotifications) {
        pushNotificationsCache = cachedNotifications;
        if (!options || !options.skipRender) {
            renderPushNotifications();
        }
        return true;
    }
    return false;
}

function hydrateInitialContentFromCache() {
    loadCachedStateAndEvents();
    loadCachedNotifications({ skipRender: true });
}

function updateOfflineBanner() {
    const banner = document.getElementById('offlineBanner');
    if (!banner) {
        return;
    }
    banner.classList.toggle('hidden', !offlineMode);
}

function setOfflineMode(value, options) {
    const silent = options && options.silent;
    const previous = offlineMode;
    offlineMode = Boolean(value);
    if (!offlineMode) {
        offlineNotified = false;
        offlineHydrated = false;
    } else if (!offlineNotified && !silent) {
        showPopup('⚠️ Modalità offline attiva. Mostro i dati salvati');
        offlineNotified = true;
    }
    if (!previous && offlineMode) {
        offlineHydrated = false;
    }
    updateOfflineBanner();
}

function handleOfflineEvent() {
    setOfflineMode(true, { silent: false });
    hydrateCacheOnce();
    loadCachedNotifications();
}

function handleOnlineEvent() {
    setOfflineMode(false, { silent: true });
    showPopup('🔌 Connessione ripristinata');
    requestQueueFlush();
    refreshState();
    fetchPushNotifications({ silent: true });
}

function requestQueueFlush() {
    if (typeof navigator === 'undefined' || !navigator.serviceWorker) {
        return;
    }
    try {
        const controller = navigator.serviceWorker.controller;
        if (controller) {
            controller.postMessage({ type: 'flush-offline-queue' });
        }
    } catch (error) {
        console.warn('Impossibile richiedere il flush della coda offline', error);
    }
}

function cloneStateSnapshot(state) {
    if (!state) {
        return null;
    }
    try {
        return JSON.parse(JSON.stringify(state));
    } catch (error) {
        console.warn('cloneStateSnapshot fallita', error);
    }
    return null;
}

function updateLastKnownElapsed(memberKey, elapsed) {
    if (!lastKnownState || !memberKey) {
        return;
    }
    const updateCollection = (collection) => {
        if (!Array.isArray(collection)) {
            return false;
        }
        let changed = false;
        collection.forEach((member) => {
            if (member && member.member_key === memberKey) {
                member.elapsed = elapsed;
                changed = true;
            }
        });
        return changed;
    };
    if (updateCollection(lastKnownState.team)) {
        return;
    }
    if (Array.isArray(lastKnownState.activities)) {
        lastKnownState.activities.forEach((activity) => {
            updateCollection(activity.members);
        });
    }
}

function recordClientElapsed(memberKey, elapsed) {
    if (!memberKey) {
        return;
    }
    clientElapsedState.set(memberKey, {
        elapsed,
        syncedAt: Date.now(),
    });
}

function getClientElapsed(memberKey, fallbackElapsed, running) {
    if (!memberKey) {
        return fallbackElapsed;
    }
    const info = clientElapsedState.get(memberKey);
    if (!info) {
        return fallbackElapsed;
    }
    const safeFallback = Number.isFinite(fallbackElapsed) ? fallbackElapsed : 0;
    const now = Date.now();
    const resetThreshold = 1500;

    if (!running) {
        if (safeFallback + resetThreshold < info.elapsed) {
            clientElapsedState.set(memberKey, { elapsed: safeFallback, syncedAt: now });
            return safeFallback;
        }
        return Math.max(safeFallback, info.elapsed);
    }

    const delta = Math.max(0, now - info.syncedAt);
    const projected = info.elapsed + delta;

    if (safeFallback + resetThreshold < info.elapsed) {
        clientElapsedState.set(memberKey, { elapsed: safeFallback, syncedAt: now });
        return safeFallback;
    }

    if (safeFallback > projected + resetThreshold) {
        clientElapsedState.set(memberKey, { elapsed: safeFallback, syncedAt: now });
        return safeFallback;
    }

    return Math.max(safeFallback, projected);
}

function mutateMemberSnapshot(state, memberKey, mutator) {
    if (!state || !memberKey || typeof mutator !== 'function') {
        return false;
    }
    let mutated = false;
    const visitMember = (member) => {
        if (member && member.member_key === memberKey) {
            mutator(member);
            mutated = true;
        }
    };
    if (Array.isArray(state.team)) {
        state.team.forEach(visitMember);
    }
    if (Array.isArray(state.activities)) {
        state.activities.forEach((activity) => {
            if (Array.isArray(activity.members)) {
                activity.members.forEach(visitMember);
            }
        });
    }
    return mutated;
}

function removeMemberFromSnapshot(state, memberKey) {
    if (!state || !memberKey) {
        return false;
    }
    let removed = false;
    if (Array.isArray(state.team)) {
        const index = state.team.findIndex((member) => member && member.member_key === memberKey);
        if (index !== -1) {
            state.team.splice(index, 1);
            removed = true;
        }
    }
    if (Array.isArray(state.activities)) {
        state.activities.forEach((activity) => {
            if (!Array.isArray(activity.members)) {
                return;
            }
            const index = activity.members.findIndex((member) => member && member.member_key === memberKey);
            if (index !== -1) {
                activity.members.splice(index, 1);
                removed = true;
            }
        });
    }
    return removed;
}

function moveMemberBetweenActivities(state, memberKey, targetActivityId, fallbackPayload) {
    if (!state || !memberKey) {
        return false;
    }

    const removeFromList = (collection) => {
        if (!Array.isArray(collection)) {
            return null;
        }
        const index = collection.findIndex((member) => member && member.member_key === memberKey);
        if (index === -1) {
            return null;
        }
        return collection.splice(index, 1)[0];
    };

    let memberData = removeFromList(state.team);
    if (Array.isArray(state.activities)) {
        state.activities.forEach((activity) => {
            if (!Array.isArray(activity.members)) {
                return;
            }
            const removed = removeFromList(activity.members);
            if (removed && !memberData) {
                memberData = removed;
            }
        });
    }

    if (!memberData) {
        memberData = {
            member_key: memberKey,
            member_name: fallbackPayload?.member_name || memberKey,
            elapsed: Number(fallbackPayload?.elapsed) || 0,
        };
    }

    const previousActivityId = memberData.activity_id ? String(memberData.activity_id) : "";
    const elapsedBeforeMove = Number(memberData.elapsed) || 0;
    const resolvedActivityId = targetActivityId ? String(targetActivityId) : "";
    const resolvedRunning =
        fallbackPayload && Object.prototype.hasOwnProperty.call(fallbackPayload, "running")
            ? Boolean(fallbackPayload.running)
            : Boolean(resolvedActivityId);
    const resolvedPaused =
        fallbackPayload && Object.prototype.hasOwnProperty.call(fallbackPayload, "paused")
            ? Boolean(fallbackPayload.paused)
            : false;

    memberData.activity_id = resolvedActivityId;
    memberData.running = resolvedRunning;
    memberData.paused = resolvedPaused;
    const movingBetweenActivities = Boolean(previousActivityId) && previousActivityId !== resolvedActivityId;
    if (movingBetweenActivities && elapsedBeforeMove > 0) {
        addActivityRuntimeOffset(previousActivityId, elapsedBeforeMove);
        if (Array.isArray(state.activities)) {
            const sourceActivity = state.activities.find(
                (activity) => activity && String(activity.activity_id) === previousActivityId,
            );
            if (sourceActivity) {
                const currentBase = Number(sourceActivity.actual_runtime_ms) || 0;
                sourceActivity.actual_runtime_ms = currentBase + elapsedBeforeMove;
            }
        }
    }
    const fallbackElapsed = Number(fallbackPayload && fallbackPayload.elapsed);
    const shouldResetElapsed = Boolean(resolvedActivityId) && movingBetweenActivities;
    if (shouldResetElapsed) {
        memberData.elapsed = 0;
        clientElapsedState.delete(memberKey);
    } else if (Number.isFinite(fallbackElapsed) && fallbackElapsed >= 0) {
        memberData.elapsed = fallbackElapsed;
    }

    let inserted = false;
    if (resolvedActivityId) {
        const targetActivity = Array.isArray(state.activities)
            ? state.activities.find((activity) => String(activity.activity_id) === resolvedActivityId)
            : null;
        if (targetActivity) {
            if (!Array.isArray(targetActivity.members)) {
                targetActivity.members = [];
            }
            const existingIndex = targetActivity.members.findIndex((m) => m.member_key === memberData.member_key);
            if (existingIndex !== -1) {
                targetActivity.members.splice(existingIndex, 1, memberData);
            } else {
                targetActivity.members.push(memberData);
            }
            inserted = true;
        }
    }

    if (!inserted) {
        if (!Array.isArray(state.team)) {
            state.team = [];
        }
        const teamIndex = state.team.findIndex((m) => m.member_key === memberData.member_key);
        if (teamIndex !== -1) {
            state.team.splice(teamIndex, 1, memberData);
        } else {
            state.team.push(memberData);
        }
    }

    mutateMemberSnapshot(state, memberKey, (target) => {
        target.activity_id = memberData.activity_id;
        target.running = memberData.running;
        target.paused = memberData.paused;
        if (Number.isFinite(memberData.elapsed)) {
            target.elapsed = memberData.elapsed;
        }
    });
    const touchedActivities = [];
    if (previousActivityId) {
        touchedActivities.push(previousActivityId);
    }
    if (resolvedActivityId) {
        touchedActivities.push(resolvedActivityId);
    }
    touchedActivities.forEach((activityKey) => {
        updateActivityTotalDisplay(activityKey);
    });
    refreshTotalRunningTimeDisplay();
    return true;
}

const OPTIMISTIC_SELECTION_HANDLERS = {
    '/api/member/pause': (member) => {
        member.running = false;
        member.paused = true;
    },
    '/api/member/resume': (member) => {
        member.running = true;
        member.paused = false;
    },
};

function applyOptimisticSelectionState(endpoint, memberKeys) {
    if (!Array.isArray(memberKeys) || memberKeys.length === 0) {
        return;
    }
    if (endpoint === '/api/member/finish') {
        optimisticRemoveMembers(memberKeys);
        return;
    }
    const handler = OPTIMISTIC_SELECTION_HANDLERS[endpoint];
    if (!handler) {
        return;
    }
    const snapshot = cloneStateSnapshot(lastKnownState);
    if (!snapshot) {
        return;
    }
    let updated = false;
    memberKeys.forEach((memberKey) => {
        const changed = mutateMemberSnapshot(snapshot, memberKey, handler);
        if (changed) {
            updated = true;
        }
    });
    if (!updated) {
        return;
    }
    lastKnownState = snapshot;
    saveCachedPayload(LAST_STATE_KEY, snapshot);
    suppressSelectionRestore = true;
    applyState(snapshot);
}

function optimisticRemoveMembers(memberKeys) {
    const snapshot = cloneStateSnapshot(lastKnownState);
    if (!snapshot) {
        return;
    }
    let updated = false;
    memberKeys.forEach((memberKey) => {
        if (removeMemberFromSnapshot(snapshot, memberKey)) {
            updated = true;
        }
    });
    if (!updated) {
        return;
    }
    lastKnownState = snapshot;
    saveCachedPayload(LAST_STATE_KEY, snapshot);
    suppressSelectionRestore = true;
    applyState(snapshot);
}

function applyOptimisticMoveState(payloads) {
    if (!Array.isArray(payloads) || payloads.length === 0) {
        return;
    }
    const snapshot = cloneStateSnapshot(lastKnownState);
    if (!snapshot) {
        return;
    }
    let updated = false;
    payloads.forEach((payload) => {
        const memberKey = payload && payload.member_key;
        if (!memberKey) {
            return;
        }
        const changed = moveMemberBetweenActivities(snapshot, memberKey, payload.activity_id, payload);
        if (changed) {
            updated = true;
        }
    });
    if (!updated) {
        return;
    }
    lastKnownState = snapshot;
    saveCachedPayload(LAST_STATE_KEY, snapshot);
    applyState(snapshot);
}

function optimisticStartMembers(memberKeys) {
    if (!Array.isArray(memberKeys) || memberKeys.length === 0) {
        return;
    }
    const snapshot = cloneStateSnapshot(lastKnownState);
    if (!snapshot) {
        return;
    }
    const keySet = new Set(memberKeys.filter(Boolean));
    if (keySet.size === 0) {
        return;
    }
    let updated = false;
    const visitCollection = (collection) => {
        if (!Array.isArray(collection)) {
            return;
        }
        collection.forEach((member) => {
            if (member && keySet.has(member.member_key)) {
                if (!member.running || member.paused) {
                    member.running = true;
                    member.paused = false;
                    updated = true;
                }
            }
        });
    };
    visitCollection(snapshot.team);
    if (Array.isArray(snapshot.activities)) {
        snapshot.activities.forEach((activity) => visitCollection(activity.members));
    }
    if (!updated) {
        return;
    }
    lastKnownState = snapshot;
    saveCachedPayload(LAST_STATE_KEY, snapshot);
    applyState(snapshot);
}

function optimisticStartActivity(activityId) {
    if (!activityId) {
        return;
    }
    const snapshot = cloneStateSnapshot(lastKnownState);
    if (!snapshot || !Array.isArray(snapshot.activities)) {
        return;
    }
    const normalized = String(activityId);
    let affectedKeys = [];
    snapshot.activities.forEach((activity) => {
        if (!activity || String(activity.activity_id) !== normalized || !Array.isArray(activity.members)) {
            return;
        }
        activity.members.forEach((member) => {
            if (member) {
                member.running = true;
                member.paused = false;
                affectedKeys.push(member.member_key);
            }
        });
    });
    if (affectedKeys.length === 0) {
        return;
    }
    const keySet = new Set(affectedKeys);
    if (Array.isArray(snapshot.team)) {
        snapshot.team.forEach((member) => {
            if (member && keySet.has(member.member_key)) {
                member.running = true;
                member.paused = false;
            }
        });
    }
    lastKnownState = snapshot;
    saveCachedPayload(LAST_STATE_KEY, snapshot);
    applyState(snapshot);
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
    if (projectCodeBuffer.length === 0) {
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
    const group = document.getElementById("projectInputGroup");
    if (group) {
        group.classList.toggle("keypad-open", visible);
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

function formatNotificationTimestamp(value) {
    if (value === undefined || value === null) {
        return "";
    }
    const date = new Date(Number(value));
    if (Number.isNaN(date.getTime())) {
        return "";
    }
    return notificationTimestampFormatter.format(date);
}

function formatNotificationKind(kind) {
    if (!kind) {
        return "";
    }
    const label = NOTIFICATION_KIND_LABELS[kind];
    if (label) {
        return label;
    }
    return kind.replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

function renderPushNotifications() {
    const list = document.getElementById("pushNotificationsList");
    const emptyNode = document.getElementById("pushNotificationsEmpty");
    const counter = document.getElementById("pushNotificationsCount");
    if (!list || !emptyNode) {
        return;
    }

    list.innerHTML = "";
    const items = Array.isArray(pushNotificationsCache) ? pushNotificationsCache : [];
    if (items.length === 0) {
        emptyNode.classList.remove("hidden");
        if (counter) {
            counter.classList.add("hidden");
        }
        return;
    }

    if (counter) {
        counter.textContent = `${items.length} notifiche`;
        counter.classList.remove("hidden");
    }
    emptyNode.classList.add("hidden");
    const fragment = document.createDocumentFragment();
    items.forEach((item) => {
        const node = document.createElement("div");
        node.className = "notification-item";

        const title = document.createElement("div");
        title.className = "notification-item-title";
        title.textContent = item.title || "Notifica push";
        node.appendChild(title);

        if (item.body) {
            const body = document.createElement("div");
            body.className = "notification-item-body";
            body.textContent = item.body;
            node.appendChild(body);
        }

        const meta = document.createElement("div");
        meta.className = "notification-item-meta";
        const parts = [];
        const kindLabel = formatNotificationKind(item.kind);
        const timeLabel = formatNotificationTimestamp(item.sent_ts || item.created_ts);
        if (kindLabel) {
            parts.push(kindLabel);
        }
        if (timeLabel) {
            parts.push(timeLabel);
        }
        if (item.activity_id) {
            parts.push(`#${item.activity_id}`);
        }
        meta.textContent = parts.join(" · ") || "";
        node.appendChild(meta);

        fragment.appendChild(node);
    });
    list.appendChild(fragment);
}

async function fetchPushNotifications(options) {
    const settings = options || {};
    const silent = Boolean(settings.silent);
    if (pushNotificationsLoading && !silent) {
        return;
    }

    const button = document.getElementById("refreshPushNotificationsBtn");
    if (button && !button.dataset.label) {
        button.dataset.label = button.textContent || "Aggiorna";
    }
    if (button && !silent) {
        button.disabled = true;
        button.textContent = "Aggiorno...";
    }

    pushNotificationsLoading = true;
    try {
        const data = await fetchJson(`/api/push/notifications?limit=${PUSH_NOTIFICATIONS_LIMIT}`);
        pushNotificationsCache = data && Array.isArray(data.items) ? data.items : [];
        saveCachedPayload(LAST_PUSH_KEY, pushNotificationsCache);
        renderPushNotifications();
        if (!silent) {
            showPopup("📥 Storico notifiche aggiornato");
        }
    } catch (error) {
        console.warn("fetchPushNotifications", error);
        let handledOffline = false;
        if (!navigator.onLine) {
            handledOffline = loadCachedNotifications();
        }
        if (!silent) {
            if (handledOffline) {
                showPopup('⚠️ Offline: mostro lo storico salvato');
            } else {
                showPopup("⚠️ Impossibile caricare lo storico notifiche");
            }
        }
    } finally {
        pushNotificationsLoading = false;
        if (button && !silent) {
            button.disabled = false;
            button.textContent = button.dataset.label || "Aggiorna";
        }
    }
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

function openPushNotificationsModal() {
    const modal = document.getElementById("pushNotificationsModal");
    if (!modal || pushNotificationsModalOpen) {
        return;
    }
    pushNotificationsModalOpen = true;
    modal.style.display = "flex";
    markBodyModalOpen();
    fetchPushNotifications({ silent: true });
}

function closePushNotificationsModal() {
    const modal = document.getElementById("pushNotificationsModal");
    if (!modal) {
        return;
    }
    if (!pushNotificationsModalOpen) {
        modal.style.display = "none";
        return;
    }
    pushNotificationsModalOpen = false;
    modal.style.display = "none";
    releaseBodyModalState();
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

function updatePushUI() {
    const toggle = document.getElementById('pushToggle');
    const statusLabel = document.getElementById('pushStatus');
    const textLabel = toggle ? toggle.querySelector('.side-menu-item-text') : null;
    const testBtn = document.getElementById('pushTestBtn');
    const testHint = document.getElementById('pushTestHint');
    const setTestState = (enabled, hint) => {
        if (testBtn) {
            testBtn.disabled = !enabled;
        }
        if (testHint && typeof hint === 'string') {
            testHint.textContent = hint;
        }
    };

    if (!toggle || !statusLabel || !textLabel) {
        setTestState(false, 'Attiva le notifiche per provarle');
        return;
    }

    if (!pushState.supported) {
        toggle.disabled = true;
        textLabel.textContent = 'Notifiche Push';
        statusLabel.textContent = 'Non supportato';
        setTestState(false, 'Non disponibile nel browser');
        return;
    }

    if (!pushState.configured) {
        toggle.disabled = true;
        textLabel.textContent = 'Notifiche Push';
        statusLabel.textContent = 'Disattivate dal server';
        setTestState(false, 'Server non configurato');
        return;
    }

    if (typeof Notification !== 'undefined' && Notification.permission === 'denied') {
        toggle.disabled = true;
        textLabel.textContent = 'Notifiche Push';
        statusLabel.textContent = 'Permesso negato';
        setTestState(false, 'Permesso notifiche negato');
        return;
    }

    toggle.disabled = false;
    if (pushState.subscribed) {
        textLabel.textContent = 'Disattiva notifiche';
        statusLabel.textContent = 'Attive';
        setTestState(true, 'Invia notifica immediata');
    } else {
        textLabel.textContent = 'Attiva notifiche';
        statusLabel.textContent = (typeof Notification !== 'undefined' && Notification.permission === 'granted')
            ? 'Disponibili'
            : 'Richiedono permesso';
        setTestState(false, 'Attiva le notifiche per provarle');
    }
}

async function refreshPushState() {
    if (!pushState.supported) {
        updatePushUI();
        return;
    }

    try {
        const status = await fetchJson('/api/push/status');
        pushState.configured = Boolean(status.enabled);
        pushState.publicKey = status.publicKey || null;
        pushState.subscribed = Boolean(status.subscribed);
    } catch (error) {
        console.warn('refreshPushState', error);
        pushState.configured = false;
    }

    let registration = null;
    try {
        registration = await Promise.race([
            navigator.serviceWorker.ready,
            new Promise((resolve) => setTimeout(() => resolve(null), SERVICE_WORKER_READY_TIMEOUT)),
        ]);
    } catch (error) {
        console.warn('navigator.serviceWorker.ready', error);
    }

    if (!registration) {
        try {
            registration = await navigator.serviceWorker.getRegistration() || null;
        } catch (error) {
            console.warn('navigator.serviceWorker.getRegistration', error);
        }
    }

    if (registration) {
        try {
            const subscription = await registration.pushManager.getSubscription();
            pushState.subscribed = Boolean(subscription);
        } catch (error) {
            console.warn('pushManager.getSubscription', error);
        }
    }

    updatePushUI();
}

async function subscribeToPush() {
    if (!pushState.supported) {
        showPopup('⚠️ Notifiche non supportate');
                return false;
    }

    if (!pushState.publicKey) {
        showPopup('⚠️ Server non configurato per le notifiche');
        return false;
    }

    if (typeof Notification !== 'undefined' && Notification.permission === 'denied') {
        showPopup('⚠️ Permesso notifiche negato dal browser');
        return false;
    }

    if (typeof Notification !== 'undefined' && Notification.permission !== 'granted') {
        const permission = await Notification.requestPermission();
        if (permission !== 'granted') {
            showPopup('⚠️ Permesso notifiche non concesso');
            return false;
        }
    }

    try {
        const registration = await navigator.serviceWorker.ready;
        const applicationServerKey = urlBase64ToUint8Array(pushState.publicKey);
        const subscription = await registration.pushManager.subscribe({
            userVisibleOnly: true,
            applicationServerKey,
        });

        const payload = subscription.toJSON();
        payload.userAgent = navigator.userAgent || null;
        payload.contentEncoding = 'aes128gcm';
        await postJson('/api/push/subscribe', payload);
        pushState.subscribed = true;
        showPopup('🔔 Notifiche attivate');
        return true;
    } catch (error) {
        console.error('subscribeToPush', error);
        showPopup('⚠️ Attivazione notifiche fallita');
        return false;
    }
}

async function unsubscribeFromPush() {
    if (!pushState.supported) {
        return;
    }

    try {
        const registration = await navigator.serviceWorker.ready;
        const subscription = await registration.pushManager.getSubscription();
        if (!subscription) {
            pushState.subscribed = false;
            return;
        }

        const payload = subscription.toJSON();
        try {
            await postJson('/api/push/unsubscribe', { endpoint: payload.endpoint });
        } catch (error) {
            console.warn('unsubscribe backend', error);
        }

        await subscription.unsubscribe();
        pushState.subscribed = false;
        showPopup('🔕 Notifiche disattivate');
    } catch (error) {
        console.error('unsubscribeFromPush', error);
        showPopup('⚠️ Disattivazione notifiche fallita');
    }
}

async function handlePushToggle() {
    const toggle = document.getElementById('pushToggle');
    if (toggle) {
        toggle.disabled = true;
    }

    try {
        if (pushState.subscribed) {
            await unsubscribeFromPush();
        } else {
            const enabled = await subscribeToPush();
            if (!enabled) {
                return;
            }
        }
    } finally {
        await refreshPushState();
        if (toggle) {
            toggle.disabled = false;
        }
    }
}

async function sendPushTest() {
    if (!pushState.supported || !pushState.configured || !pushState.subscribed) {
        showPopup('⚠️ Attiva prima le notifiche');
        return;
    }

    const testBtn = document.getElementById('pushTestBtn');
    if (testBtn) {
        testBtn.disabled = true;
    }

    try {
        await postJson('/api/push/test', {});
        await fetchPushNotifications({ silent: true });
        showPopup('📬 Notifica di prova inviata');
    } catch (error) {
        console.error('sendPushTest', error);
        if (error && error.status === 404) {
            showPopup('⚠️ Nessuna iscrizione push trovata');
        } else if (error && error.status === 400) {
            showPopup('⚠️ Server push non configurato');
        } else {
            showPopup('⚠️ Invio notifica di prova fallito');
        }
    } finally {
        await refreshPushState();
    }
}

function registerServiceWorkerMessaging() {
    if (typeof window === 'undefined' || !('serviceWorker' in navigator)) {
        return;
    }
    try {
        navigator.serviceWorker.addEventListener('message', (event) => {
            const message = event.data;
            if (!message || typeof message.type !== 'string') {
                return;
            }
            if (message.type === 'push-notification') {
                handleServiceWorkerPushMessage(message);
                return;
            }
            if (message.type === 'offline-queue') {
                handleOfflineQueueMessage(message);
            }
        });
    } catch (error) {
        console.warn('Impossibile ascoltare i messaggi del service worker', error);
    }
}

function handleServiceWorkerPushMessage(message) {
    const meta = message.meta || {};
    if (meta && meta.permission && meta.permission !== 'granted') {
        showPopup('⚠️ Notifiche bloccate dal browser');
        return;
    }
    if (meta && meta.error) {
        showPopup(`⚠️ Errore notifica: ${meta.error}`);
        return;
    }
    const payload = message.payload || {};
    const title = payload.title || 'Notifica push';
    const body = payload.body ? `: ${payload.body}` : '';
    showPopup(`🔔 ${title}${body}`);
    fetchPushNotifications({ silent: true });
}

function describeQueuedAction(url) {
    if (!url) {
        return 'Operazione';
    }
    let pathname = url;
    try {
        pathname = new URL(url, window.location.origin).pathname;
    } catch (error) {
        // noop
    }
    const match = Object.keys(QUEUE_ACTION_LABELS).find((key) => pathname.startsWith(key));
    if (match) {
        return QUEUE_ACTION_LABELS[match];
    }
    if (pathname.startsWith('/api/')) {
        return pathname.replace('/api/', '').replace(/_/g, ' ');
    }
    return pathname;
}

function handleOfflineQueueMessage(message) {
    if (!message || !message.action) {
        return;
    }
    const label = describeQueuedAction(message.pathname || message.url || '');
    if (message.action === 'queued') {
        showPopup(`💾 ${label} salvata offline`);
        return;
    }
    if (message.action === 'delivered') {
        showPopup(`📡 ${label} sincronizzata`);
        return;
    }
    if (message.action === 'error') {
        const errorLabel = message.error ? `: ${message.error}` : '';
        showPopup(`⚠️ Sync ${label} fallito${errorLabel}`);
    }
}

async function initPushNotifications() {
    updatePushUI();
    if (!pushState.supported) {
        return;
    }

    if ('serviceWorker' in navigator) {
        try {
            navigator.serviceWorker.ready
                .then(() => refreshPushState())
                .catch((error) => console.warn('Service worker non pronto', error));
            navigator.serviceWorker.addEventListener('controllerchange', () => {
                refreshPushState();
            });
        } catch (error) {
            console.warn('Impossibile monitorare il service worker', error);
        }
    }

    await refreshPushState();
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
    updateTeamCollapseUI();
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
        let queued = false;
        for (const memberKey of memberKeys) {
            const result = await postJson(endpoint, { member_key: memberKey });
            if (result && result.__queued) {
                queued = true;
            }
        }
        suppressSelectionRestore = true;
        if (queued) {
            showPopup('💾 Operazione salvata offline');
            applyOptimisticSelectionState(endpoint, memberKeys);
        } else if (successMessage) {
            showPopup(successMessage);
            await refreshState();
        } else {
            await refreshState();
        }
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
    updateTeamSelectButton();
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
    let elapsed = getClientElapsed(key, Number(member.elapsed) || 0, member.running);

    const syncNodes = () => {
        displays.forEach((display) => {
            display.textContent = formatTime(elapsed);
        });
        forEachMemberNode(key, (node) => {
            node.dataset.elapsedMs = String(elapsed);
            node.dataset.running = member.running ? "true" : "false";
            node.dataset.paused = member.paused ? "true" : "false";
        });
        updateLastKnownElapsed(key, elapsed);
        recordClientElapsed(key, elapsed);
    };

    syncNodes();
    refreshTotalRunningTimeDisplay();
    if (member.activity_id) {
        updateActivityTotalDisplay(member.activity_id);
    }

    if (!member.running) {
        delete timers[key];
        clientElapsedState.delete(key);
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
    const members = Array.isArray(team) ? team : [];
    const list = document.getElementById("memberList");
    if (list) {
        list.innerHTML = "";
        members.forEach((member) => {
            const node = createMemberNode(member, "team-member");
            list.appendChild(node);
        });
    }
    setTeamCount(members.length);
    updateTeamCollapseUI();
    updateTeamSelectButton();
}

function setTeamCount(count) {
    const label = document.getElementById("teamMemberCount");
    if (!label) {
        return;
    }
    const total = Number(count) || 0;
    label.textContent = total === 1 ? "1 operatore" : `${total} operatori`;
}

function updateTeamSelectButton() {
    const btn = document.getElementById("teamSelectBtn");
    if (!btn) {
        return;
    }
    const nodes = Array.from(document.querySelectorAll(".team-member"));
    const allSelected =
        nodes.length > 0 && nodes.every((node) => node.classList.contains("selected"));
    btn.textContent = allSelected ? "Deseleziona tutti" : "Seleziona tutti";
    btn.setAttribute("aria-pressed", allSelected ? "true" : "false");
    btn.disabled = !projectVisible || nodes.length === 0;
}

function toggleTeamSelection() {
    const nodes = Array.from(document.querySelectorAll(".team-member"));
    if (nodes.length === 0) {
        showPopup("⚠️ Nessun operatore disponibile");
        return;
    }
    const shouldSelect = !nodes.every((node) => node.classList.contains("selected"));
    nodes.forEach((node) => {
        node.classList.toggle("selected", shouldSelect);
    });
    updateSelectionToolbar();
}

function updateTeamCollapseUI() {
    const list = document.getElementById('memberList');
    const card = document.getElementById('teamCard');
    const toggleBtn = document.getElementById('teamCollapseBtn');
    if (list) {
        list.classList.toggle('hidden', teamCollapsed);
    }
    if (card) {
        card.classList.toggle('collapsed', teamCollapsed);
    }
    if (toggleBtn) {
        toggleBtn.textContent = teamCollapsed ? 'Mostra squadra' : 'Nascondi squadra';
        toggleBtn.setAttribute('aria-expanded', teamCollapsed ? 'false' : 'true');
    }
}

function setTeamCollapsed(collapsed) {
    const nextValue = Boolean(collapsed);
    if (teamCollapsed === nextValue) {
        updateTeamCollapseUI();
        return;
    }
    teamCollapsed = nextValue;
    updateTeamCollapseUI();
}

function renderActivities(activities) {
    const container = document.getElementById("activities");
    if (!container) {
        return;
    }
    container.innerHTML = "";
    const previousTotals = new Map(activityTotalValues);
    activityTotalDisplays.clear();
    activityOverdueTrackers.clear();
    activityRuntimeOffsets.clear();
    activityTotalValues.clear();
    activities.forEach((activity) => {
        const card = document.createElement("div");
        card.className = "task-card";
        const activityId = activity.activity_id ? String(activity.activity_id) : "";
        const members = Array.isArray(activity.members) ? activity.members : [];
        const memberCount = members.length;
        const plannedMultiplier = getPlannedMemberMultiplier(activity);
        const plannedDurationMs = getActivityPlannedDurationMs(activity);
        const runningMembersMs = calculateActivityRunningTime(members);
        const baseRuntimeMs = Number(activity.actual_runtime_ms) || 0;
        const naiveTotalMs = baseRuntimeMs + runningMembersMs;
        const previousTotalMs = activityId ? previousTotals.get(activityId) || 0 : 0;
        const correctedTotalMs = Math.max(naiveTotalMs, previousTotalMs);
        const correctedOffsetMs = Math.max(0, correctedTotalMs - runningMembersMs);
        if (activityId) {
            setActivityRuntimeOffset(activityId, correctedOffsetMs);
            activityTotalValues.set(activityId, correctedTotalMs);
        }
        const plannedDurationLabel = plannedDurationMs !== null
            ? formatDurationFromMs(plannedDurationMs)
            : formatPlannedDuration(
                  activity.plan_start,
                  activity.plan_end,
                  plannedMultiplier
              );
        if (activityId && !seenActivityIds.has(activityId)) {
            seenActivityIds.add(activityId);
            collapsedActivities.add(activityId);
        }
        card.dataset.activityId = activityId;
        const scheduleLabel = formatPlanningRange(activity.plan_start, activity.plan_end);
        const isCollapsed = activityId ? collapsedActivities.has(activityId) : true;

        const header = document.createElement("div");
        header.className = "task-header";

        const info = document.createElement("div");
        info.className = "task-header-info";
        const titleRow = document.createElement("div");
        titleRow.className = "task-title-row";
        const title = document.createElement("span");
        title.className = "task-title";
        title.textContent = activity.label;
        titleRow.appendChild(title);
        const delayBadge = document.createElement("span");
        delayBadge.className = "activity-delay-badge hidden";
        delayBadge.textContent = "In ritardo";
        titleRow.appendChild(delayBadge);
        info.appendChild(titleRow);
        if (scheduleLabel) {
            const schedule = document.createElement("div");
            schedule.className = "task-schedule";
            schedule.textContent = scheduleLabel;
            info.appendChild(schedule);
        }
        if (plannedDurationLabel) {
            const duration = document.createElement("div");
            duration.className = "task-duration";
            duration.textContent = `Durata prevista: ${plannedDurationLabel}`;
            info.appendChild(duration);
        }

        const meta = document.createElement("div");
        meta.className = "task-header-meta";
        const count = document.createElement("span");
        count.className = "timer-display";
        count.textContent = `${memberCount} operatori`;
        meta.appendChild(count);

        if (plannedDurationLabel) {
            const plannedSummary = document.createElement("div");
            plannedSummary.className = "activity-time-summary activity-duration-summary";
            const plannedLabel = document.createElement("span");
            plannedLabel.className = "activity-time-label";
            plannedLabel.textContent = "Durata prevista";
            const plannedValue = document.createElement("span");
            plannedValue.className = "activity-time-value";
            plannedValue.textContent = plannedDurationLabel;
            plannedSummary.appendChild(plannedLabel);
            plannedSummary.appendChild(plannedValue);
            meta.appendChild(plannedSummary);
        }

        const timeSummary = document.createElement("div");
        timeSummary.className = "activity-time-summary";
        const timeLabel = document.createElement("span");
        timeLabel.className = "activity-time-label";
        timeLabel.textContent = "Tempo in corso";
        const timeValue = document.createElement("span");
        timeValue.className = "activity-time-value";
        timeValue.textContent = formatTime(correctedTotalMs);
        if (activityId) {
            activityTotalDisplays.set(activityId, timeValue);
        }
        timeSummary.appendChild(timeLabel);
        timeSummary.appendChild(timeValue);
        meta.appendChild(timeSummary);

        if (plannedDurationMs !== null) {
            const delaySummary = document.createElement("div");
            delaySummary.className = "activity-time-summary activity-delay-summary hidden";
            const delayLabel = document.createElement("span");
            delayLabel.className = "activity-time-label";
            delayLabel.textContent = "Ritardo";
            const delayValue = document.createElement("span");
            delayValue.className = "activity-time-value";
            delayValue.textContent = "00:00:00";
            delaySummary.appendChild(delayLabel);
            delaySummary.appendChild(delayValue);
            meta.appendChild(delaySummary);
            if (activityId) {
                activityOverdueTrackers.set(activityId, {
                    plannedMs: plannedDurationMs,
                    wrapper: delaySummary,
                    valueNode: delayValue,
                    card,
                    badge: delayBadge,
                    baseMs: correctedOffsetMs,
                });
                updateActivityDelayUI(activityId, correctedTotalMs);
            }
        }

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
        members.forEach((member) => {
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
    seenActivityIds.clear();
    activityTotalDisplays.clear();
    activityOverdueTrackers.clear();
    activityRuntimeOffsets.clear();
    activityTotalValues.clear();
    setTeamCount(0);
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
    const normalizeElapsed = (member) => {
        if (!member) {
            return;
        }
        const current = Number(member.elapsed) || 0;
        if (member.running) {
            const adjusted = getClientElapsed(member.member_key, current, true);
            member.elapsed = adjusted;
            recordClientElapsed(member.member_key, adjusted);
        } else {
            clientElapsedState.delete(member.member_key);
            member.elapsed = current;
        }
    };
    if (Array.isArray(state.team)) {
        state.team.forEach(normalizeElapsed);
    }
    if (Array.isArray(state.activities)) {
        state.activities.forEach((activity) => {
            if (activity && Array.isArray(activity.members)) {
                activity.members.forEach(normalizeElapsed);
            }
        });
    }
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
    lastKnownState = cloneStateSnapshot(state);
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
    if (typeof navigator !== 'undefined' && navigator.onLine === false) {
        setOfflineMode(true, { silent: offlineNotified });
        hydrateCacheOnce();
        scheduleRefresh();
        return;
    }
    offlineHydrated = false;
    try {
        const [stateData, eventsData] = await Promise.all([
            fetchJson("/api/state"),
            fetchJson("/api/events"),
        ]);
        saveCachedPayload(LAST_STATE_KEY, stateData);
        saveCachedPayload(LAST_EVENTS_KEY, eventsData.events || []);
        setOfflineMode(false, { silent: true });
        applyState(stateData);
        renderEvents(eventsData.events || []);
    } catch (err) {
        if (err && err.status === 401) {
            console.warn("refreshState unauthorized", err);
            handleUnauthenticated();
        } else {
            console.error("refreshState", err);
            if (!navigator.onLine) {
                setOfflineMode(true, { silent: offlineNotified });
                hydrateCacheOnce();
            }
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
        const elapsed = Number(node.dataset.elapsedMs || 0);
        const running = node.dataset.running === "true";
        const paused = node.dataset.paused === "true";
        uniqueMembers.set(key, {
            member_key: key,
            member_name: name,
            activity_id: activityId,
            elapsed,
            running,
            paused,
        });
    });
    const payloads = Array.from(uniqueMembers.values());
    if (payloads.length === 0) {
        closeActivityModal();
        return;
    }
    try {
        let queued = false;
        for (const body of payloads) {
            const result = await postJson("/api/move", body);
            if (result && result.__queued) {
                queued = true;
            }
        }
        closeActivityModal();
        suppressSelectionRestore = true;
        selectedNodes.forEach((node) => {
            node.classList.remove("selected");
        });
        updateSelectionToolbar();
        if (queued) {
            showPopup("💾 Spostamento salvato offline");
            applyOptimisticMoveState(payloads);
        } else {
            showPopup("✅ Risorse aggiornate");
            await refreshState();
        }
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
        const data = await postJson("/api/start_activity", { activity_id: activityId });
        if (data && data.__queued) {
            showPopup('💾 Avvio attività salvato offline');
            optimisticStartActivity(activityId);
        } else {
            showPopup(`▶️ Timer avviati per ${data?.affected || 0} operatori`);
            await refreshState();
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
        const data = await postJson("/api/start_member", { member_key: memberKey });
        if (data && data.__queued) {
            showPopup('💾 Timer salvato offline');
            optimisticStartMembers([memberKey]);
        } else {
            showPopup("▶️ Timer avviato");
            await refreshState();
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
        const queuedKeys = [];
        for (const memberKey of keys) {
            const result = await postJson("/api/start_member", { member_key: memberKey });
            if (result && result.__queued) {
                queuedKeys.push(memberKey);
            } else {
                started++;
            }
        }
        if (queuedKeys.length > 0) {
            optimisticStartMembers(queuedKeys);
            const label = queuedKeys.length === keys.length
                ? '💾 Timer salvati offline'
                : '💾 Alcuni timer salvati offline';
            showPopup(label);
        }
        if (started > 0) {
            showPopup(`▶️ ${started} timer avviati`);
        }
        
        // Deseleziona tutti gli operatori avviati
        suppressSelectionRestore = true;
        selectedNodes.forEach((node) => {
            node.classList.remove("selected");
        });
        updateSelectionToolbar();
        
        if (started > 0) {
            await refreshState();
        }
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
    const pushToggle = document.getElementById("pushToggle");
    const pushTestBtn = document.getElementById("pushTestBtn");
    const refreshPushNotificationsBtn = document.getElementById("refreshPushNotificationsBtn");
    const notificationsBtn = document.getElementById("notificationsBtn");
    const closePushNotificationsBtn = document.getElementById("closePushNotificationsBtn");
    const pushNotificationsModal = document.getElementById("pushNotificationsModal");
    const teamSelectBtn = document.getElementById("teamSelectBtn");
    const teamCollapseBtn = document.getElementById("teamCollapseBtn");

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

    if (pushToggle) {
        pushToggle.addEventListener("click", handlePushToggle);
    }

    if (pushTestBtn) {
        pushTestBtn.addEventListener("click", sendPushTest);
    }

    if (refreshPushNotificationsBtn) {
        refreshPushNotificationsBtn.dataset.label = refreshPushNotificationsBtn.textContent || "Aggiorna";
        refreshPushNotificationsBtn.addEventListener("click", () => {
            fetchPushNotifications({ silent: false });
        });
    }

    if (notificationsBtn) {
        notificationsBtn.addEventListener("click", () => {
            closeMenu();
            openPushNotificationsModal();
        });
    }

    if (closePushNotificationsBtn) {
        closePushNotificationsBtn.addEventListener("click", closePushNotificationsModal);
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

    document.querySelectorAll("[data-open-notifications]").forEach((node) => {
        node.addEventListener("click", () => {
            closeMenu();
            openPushNotificationsModal();
        });
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

    if (pushNotificationsModal) {
        pushNotificationsModal.addEventListener("click", (event) => {
            if (event.target === pushNotificationsModal) {
                closePushNotificationsModal();
            }
        });
    }

    if (teamSelectBtn) {
        teamSelectBtn.addEventListener("click", toggleTeamSelection);
    }

    if (teamCollapseBtn) {
        teamCollapseBtn.addEventListener("click", () => {
            setTeamCollapsed(!teamCollapsed);
        });
    }

    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
            closeFeedbackModal();
            closeActivityModal();
            closeTimeline();
            closeMenu();
            closeExportModal();
            closePushNotificationsModal();
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
    setTeamCollapsed(true);
    setProjectDefaultDate();
    hydrateInitialContentFromCache();
    setOfflineMode(offlineMode, { silent: true });
    const releaseTarget = document.getElementById("menuReleaseVersion");
    if (releaseTarget) {
        releaseTarget.textContent = APP_RELEASE;
    }
    refreshTotalRunningTimeDisplay();
    updateClock();
    setInterval(updateClock, 1000);
    renderPushNotifications();
    registerServiceWorkerMessaging();
    await initPushNotifications();
    await fetchPushNotifications({ silent: true });
    await refreshState();
}

window.addEventListener('online', handleOnlineEvent);
window.addEventListener('offline', handleOfflineEvent);
window.addEventListener("DOMContentLoaded", init);
window.addEventListener("resize", () => {
    syncSelectionToolbarOffset();
});
