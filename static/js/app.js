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
let newActivityModalOpen = false;
let newActivityToolbarWasVisible = false;
let newActivitySaving = false;
const collapsedActivities = new Set();
const activityTotalDisplays = new Map();
const activityOverdueTrackers = new Map();
const activityRuntimeOffsets = new Map();
const activityTotalValues = new Map();
const clientElapsedState = new Map();
const seenActivityIds = new Set();
const ACTIVITY_DELAY_GRACE_MS = 0;
const APP_RELEASE = "v2025.11.22f";
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
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
});
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

// ═══════════════════════════════════════════════════════════════════════════════
//  PWA INSTALL PROMPT
// ═══════════════════════════════════════════════════════════════════════════════
let deferredInstallPrompt = null;
let pwaInstallDismissed = false;

// Cattura l'evento beforeinstallprompt per mostrare il banner personalizzato
window.addEventListener('beforeinstallprompt', (e) => {
    console.log('[PWA] beforeinstallprompt event fired');
    e.preventDefault();
    deferredInstallPrompt = e;
    
    // Mostra il pulsante nel menu
    updatePwaInstallButton();
    
    // Mostra il banner solo se non è stato già dismesso in questa sessione
    if (!pwaInstallDismissed && !localStorage.getItem('pwa-install-dismissed')) {
        showPwaInstallBanner();
    }
});

// Rileva quando l'app viene installata
window.addEventListener('appinstalled', () => {
    console.log('[PWA] App installed successfully');
    deferredInstallPrompt = null;
    hidePwaInstallBanner();
    updatePwaInstallButton();
});

// Aggiorna visibilità del pulsante installa nel menu
function updatePwaInstallButton() {
    const menuItem = document.getElementById('pwa-install-menu-item');
    if (menuItem) {
        // Mostra se c'è il prompt o se siamo su iOS (e non già installata)
        const shouldShow = deferredInstallPrompt || (isIOS() && !isStandalone());
        menuItem.style.display = shouldShow ? 'block' : 'none';
    }
}

// Inizializza pulsante PWA per iOS al caricamento
document.addEventListener('DOMContentLoaded', () => {
    if (isIOS() && !isStandalone()) {
        updatePwaInstallButton();
    }
});

function showPwaInstallBanner() {
    // Rimuovi banner esistente se presente
    hidePwaInstallBanner();
    
    const banner = document.createElement('div');
    banner.id = 'pwa-install-banner';
    banner.innerHTML = `
        <div style="position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%); 
                    background: linear-gradient(135deg, #0ea5e9 0%, #0284c7 100%); 
                    color: white; padding: 16px 20px; border-radius: 12px; 
                    box-shadow: 0 4px 20px rgba(0,0,0,0.3); z-index: 10000;
                    display: flex; align-items: center; gap: 12px; max-width: 90vw;
                    font-family: system-ui, -apple-system, sans-serif;">
            <div style="font-size: 28px;">📱</div>
            <div style="flex: 1;">
                <div style="font-weight: 600; margin-bottom: 2px;">Installa JobLog</div>
                <div style="font-size: 13px; opacity: 0.9;">Aggiungi alla schermata Home per un accesso rapido</div>
            </div>
            <button id="pwa-install-btn" style="background: white; color: #0284c7; border: none; 
                    padding: 8px 16px; border-radius: 8px; font-weight: 600; cursor: pointer;
                    white-space: nowrap;">
                Installa
            </button>
            <button id="pwa-dismiss-btn" style="background: transparent; border: none; 
                    color: white; opacity: 0.7; cursor: pointer; padding: 4px; font-size: 18px;">
                ✕
            </button>
        </div>
    `;
    document.body.appendChild(banner);
    
    document.getElementById('pwa-install-btn').addEventListener('click', installPwa);
    document.getElementById('pwa-dismiss-btn').addEventListener('click', dismissPwaInstall);
}

function hidePwaInstallBanner() {
    const banner = document.getElementById('pwa-install-banner');
    if (banner) {
        banner.remove();
    }
}

// Rileva iOS
function isIOS() {
    return /iPad|iPhone|iPod/.test(navigator.userAgent) || 
           (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
}

// Rileva se è già installata come PWA
function isStandalone() {
    return window.matchMedia('(display-mode: standalone)').matches || 
           window.navigator.standalone === true;
}

function showIOSInstallInstructions() {
    const modal = document.createElement('div');
    modal.id = 'ios-install-modal';
    modal.innerHTML = `
        <div style="position: fixed; inset: 0; background: rgba(0,0,0,0.8); z-index: 10001; 
                    display: flex; align-items: center; justify-content: center; padding: 20px;">
            <div style="background: white; border-radius: 20px; max-width: 340px; width: 100%; 
                        overflow: hidden; box-shadow: 0 20px 60px rgba(0,0,0,0.3);">
                <!-- Header -->
                <div style="background: linear-gradient(135deg, #007AFF 0%, #5856D6 100%); 
                            padding: 24px 20px; text-align: center; color: white;">
                    <div style="font-size: 48px; margin-bottom: 12px;">📲</div>
                    <div style="font-size: 20px; font-weight: 700;">Installa JobLog</div>
                    <div style="font-size: 14px; opacity: 0.9; margin-top: 4px;">su iPhone/iPad</div>
                </div>
                
                <!-- Steps -->
                <div style="padding: 24px 20px;">
                    <div style="display: flex; align-items: flex-start; gap: 16px; margin-bottom: 20px;">
                        <div style="width: 32px; height: 32px; background: #007AFF; color: white; 
                                    border-radius: 50%; display: flex; align-items: center; 
                                    justify-content: center; font-weight: 700; flex-shrink: 0;">1</div>
                        <div>
                            <div style="font-weight: 600; color: #1e293b; margin-bottom: 4px;">
                                Tocca il pulsante Condividi
                            </div>
                            <div style="font-size: 36px;">
                                <svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="#007AFF" stroke-width="2">
                                    <path d="M4 12v8a2 2 0 002 2h12a2 2 0 002-2v-8"/>
                                    <polyline points="16,6 12,2 8,6"/>
                                    <line x1="12" y1="2" x2="12" y2="15"/>
                                </svg>
                            </div>
                            <div style="font-size: 13px; color: #64748b;">
                                In basso nella barra di Safari
                            </div>
                        </div>
                    </div>
                    
                    <div style="display: flex; align-items: flex-start; gap: 16px; margin-bottom: 20px;">
                        <div style="width: 32px; height: 32px; background: #007AFF; color: white; 
                                    border-radius: 50%; display: flex; align-items: center; 
                                    justify-content: center; font-weight: 700; flex-shrink: 0;">2</div>
                        <div>
                            <div style="font-weight: 600; color: #1e293b; margin-bottom: 4px;">
                                Scorri e tocca
                            </div>
                            <div style="display: inline-flex; align-items: center; gap: 8px; 
                                        background: #f1f5f9; padding: 8px 14px; border-radius: 10px;
                                        font-size: 15px; color: #1e293b;">
                                <span style="font-size: 20px;">➕</span>
                                <span style="font-weight: 600;">Aggiungi a Home</span>
                            </div>
                        </div>
                    </div>
                    
                    <div style="display: flex; align-items: flex-start; gap: 16px;">
                        <div style="width: 32px; height: 32px; background: #22c55e; color: white; 
                                    border-radius: 50%; display: flex; align-items: center; 
                                    justify-content: center; font-weight: 700; flex-shrink: 0;">✓</div>
                        <div>
                            <div style="font-weight: 600; color: #1e293b; margin-bottom: 4px;">
                                Conferma con "Aggiungi"
                            </div>
                            <div style="font-size: 13px; color: #64748b;">
                                L'icona apparirà nella Home
                            </div>
                        </div>
                    </div>
                </div>
                
                <!-- Footer -->
                <div style="padding: 16px 20px; border-top: 1px solid #e2e8f0;">
                    <button onclick="document.getElementById('ios-install-modal').remove()" 
                            style="width: 100%; padding: 14px; background: #007AFF; color: white; 
                                   border: none; border-radius: 12px; font-size: 16px; 
                                   font-weight: 600; cursor: pointer;">
                        Ho capito
                    </button>
                </div>
            </div>
        </div>
    `;
    document.body.appendChild(modal);
}

async function installPwa() {
    // Se siamo già in standalone mode, l'app è già installata
    if (isStandalone()) {
        alert('✅ JobLog è già installata!');
        return;
    }
    
    // Se abbiamo il prompt nativo (Android/Desktop Chrome)
    if (deferredInstallPrompt) {
        hidePwaInstallBanner();
        deferredInstallPrompt.prompt();
        const { outcome } = await deferredInstallPrompt.userChoice;
        console.log('[PWA] User choice:', outcome);
        deferredInstallPrompt = null;
        updatePwaInstallButton();
        return;
    }
    
    // Se siamo su iOS, mostra istruzioni specifiche
    if (isIOS()) {
        showIOSInstallInstructions();
        return;
    }
    
    // Fallback per altri browser
    alert('Per installare l\'app:\n\n📱 Android: Menu (⋮) → "Installa app"\n💻 Desktop: Icona + nella barra indirizzi');
}

function dismissPwaInstall() {
    pwaInstallDismissed = true;
    localStorage.setItem('pwa-install-dismissed', Date.now().toString());
    hidePwaInstallBanner();
}

const initialAttachmentsPayload = (typeof window !== "undefined" && window.__INITIAL_ATTACHMENTS__) || {};
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

const attachmentsState = {
    project: initialAttachmentsPayload && initialAttachmentsPayload.project ? initialAttachmentsPayload.project : null,
    items:
        initialAttachmentsPayload && Array.isArray(initialAttachmentsPayload.items)
            ? initialAttachmentsPayload.items
            : [],
    loading: false,
    lastUpdated: null,
};

// Chiave localStorage per allegati
const ATTACHMENTS_CACHE_KEY = 'joblog-attachments-cache';

// Carica allegati da localStorage
function loadAttachmentsFromCache() {
    if (!STORAGE_AVAILABLE) return null;
    try {
        const cached = localStorage.getItem(ATTACHMENTS_CACHE_KEY);
        if (!cached) return null;
        const data = JSON.parse(cached);
        if (data && data.projectCode && Array.isArray(data.items)) {
            return data;
        }
    } catch (e) {
        console.warn('Errore caricamento cache allegati', e);
    }
    return null;
}

// Salva allegati in localStorage
function saveAttachmentsToCache() {
    if (!STORAGE_AVAILABLE) return;
    try {
        const projectCode = attachmentsState.project && attachmentsState.project.code;
        if (!projectCode || !attachmentsState.items.length) {
            localStorage.removeItem(ATTACHMENTS_CACHE_KEY);
            return;
        }
        const data = {
            projectCode: String(projectCode),
            project: attachmentsState.project,
            items: attachmentsState.items,
            savedAt: Date.now()
        };
        localStorage.setItem(ATTACHMENTS_CACHE_KEY, JSON.stringify(data));
    } catch (e) {
        console.warn('Errore salvataggio cache allegati', e);
    }
}

if (attachmentsState.project && attachmentsState.items.length > 0) {
    attachmentsState.lastUpdated = Date.now();
}
let attachmentsModalOpen = false;
const initialMaterialsPayload = (typeof window !== "undefined" && window.__INITIAL_MATERIALS__) || {};
const materialsState = {
    project: initialMaterialsPayload && initialMaterialsPayload.project ? initialMaterialsPayload.project : null,
    items:
        initialMaterialsPayload && Array.isArray(initialMaterialsPayload.items)
            ? initialMaterialsPayload.items
            : [],
    folders:
        initialMaterialsPayload && Array.isArray(initialMaterialsPayload.folders)
            ? initialMaterialsPayload.folders
            : [],
    loading: false,
    lastUpdated: null,
};
const materialsTreeExpansion = new Map();
const EQUIPMENT_CHECKS_KEY = "joblog-equipment-checks";
let equipmentChecksStore = loadEquipmentChecksStore();
const equipmentViewState = {
    tree: [],
    itemKeys: [],
};
let localEquipmentItems = [];

// Stato foto progetto
const photosState = {
    project: null,
    items: [],
    loading: false,
};
let currentPreviewPhotoId = null;

const LAST_STATE_KEY = 'joblog-cache-state';
const LAST_EVENTS_KEY = 'joblog-cache-events';
const LAST_PUSH_KEY = 'joblog-cache-push';
if (
    initialMaterialsPayload &&
    initialMaterialsPayload.project &&
    initialMaterialsPayload.equipment_checks &&
    typeof initialMaterialsPayload.equipment_checks === "object"
) {
    replaceEquipmentChecksForProject(initialMaterialsPayload.project.code, initialMaterialsPayload.equipment_checks);
}
const initialMaterialsTimestamp = Number(initialMaterialsPayload && initialMaterialsPayload.updated_ts);
if (Number.isFinite(initialMaterialsTimestamp) && initialMaterialsTimestamp > 0) {
    materialsState.lastUpdated = initialMaterialsTimestamp;
} else if (
    materialsState.project &&
    (materialsState.items.length > 0 || materialsState.folders.length > 0) &&
    !materialsState.lastUpdated
) {
    materialsState.lastUpdated = Date.now();
}
let materialsModalOpen = false;
let materialPhotoModalOpen = false;
let equipmentModalOpen = false;
const pushState = {
    supported: typeof window !== "undefined" && "serviceWorker" in navigator && "PushManager" in window && typeof Notification !== "undefined",
    configured: false,
    subscribed: false,
    publicKey: null,
};
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
    '/api/activities': 'Nuova attività',
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
            if (materialsModalOpen) {
                closeMaterialsModal();
            }
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

function formatAttachmentSize(bytes) {
    const value = Number(bytes);
    if (!Number.isFinite(value) || value <= 0) {
        return "";
    }
    const units = ["B", "KB", "MB", "GB", "TB"];
    let unitIndex = 0;
    let normalized = value;
    while (normalized >= 1024 && unitIndex < units.length - 1) {
        normalized /= 1024;
        unitIndex += 1;
    }
    const formatted = normalized >= 10 ? normalized.toFixed(0) : normalized.toFixed(1);
    return `${formatted} ${units[unitIndex]}`;
}

function formatAttachmentTimestamp(value) {
    if (!value) {
        return "";
    }
    let date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        const numeric = Number(value);
        if (Number.isFinite(numeric)) {
            date = new Date(numeric);
        }
    }
    if (Number.isNaN(date.getTime())) {
        return "";
    }
    const dateLabel = date.toLocaleDateString("it-IT", { day: "2-digit", month: "2-digit", year: "numeric" });
    const timeLabel = date.toLocaleTimeString("it-IT", { hour: "2-digit", minute: "2-digit" });
    return `${dateLabel} · ${timeLabel}`;
}

function formatMaterialDate(value) {
    if (!value) {
        return "-";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return "-";
    }
    return date.toLocaleString("it-IT", {
        day: "2-digit",
        month: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
    });
}

function formatMaterialPeriod(material) {
    if (!material) {
        return "-";
    }
    const start = formatMaterialDate(material.period_start);
    const end = formatMaterialDate(material.period_end);
    if (start === "-" && end === "-") {
        return "Impegno non disponibile";
    }
    if (start !== "-" && end !== "-") {
        return `${start} → ${end}`;
    }
    return start !== "-" ? `Da ${start}` : `Fino a ${end}`;
}

function resolveMaterialGroupSegments(material) {
    if (!material) {
        return ["Altri materiali"];
    }
    const path = typeof material.group_path === "string" ? material.group_path.trim() : "";
    if (path) {
        const parts = path
            .split("/")
            .map((part) => part.trim())
            .filter(Boolean);
        if (parts.length) {
            return parts;
        }
    }
    const name = typeof material.group_name === "string" ? material.group_name.trim() : "";
    if (name) {
        return [name];
    }
    return ["Altri materiali"];
}

function resolveMaterialGroupLabel(material) {
    return resolveMaterialGroupSegments(material).join(" / ");
}

function buildMaterialsTree(items) {
    if (!Array.isArray(items) || items.length === 0) {
        return [];
    }
    const createNode = (label, path) => ({ label, path, children: [], materials: [], _key: label.toLowerCase() });
    const rootNodes = [];

    items.forEach((item) => {
        const segments = resolveMaterialGroupSegments(item);
        let currentList = rootNodes;
        let parentPath = "";
        let currentNode = null;
        segments.forEach((segment) => {
            const normalized = segment.toLowerCase();
            let node = currentList.find((entry) => entry._key === normalized);
            if (!node) {
                const path = parentPath ? `${parentPath} / ${segment}` : segment;
                node = createNode(segment, path);
                currentList.push(node);
            }
            currentNode = node;
            parentPath = node.path;
            currentList = node.children;
        });
        if (!currentNode) {
            currentNode = createNode("Altri materiali", "Altri materiali");
            rootNodes.push(currentNode);
        }
        currentNode.materials.push(item);
    });

    const finalizeTree = (nodes) => {
        nodes.sort((a, b) => a.label.localeCompare(b.label, "it", { sensitivity: "base" }));
        nodes.forEach((node) => {
            finalizeTree(node.children || []);
            const mats = Array.isArray(node.materials) ? node.materials : [];
            const childTotal = (node.children || []).reduce((acc, child) => acc + (child.totalMaterials || 0), 0);
            node.totalMaterials = mats.length + childTotal;
            if (node._key) {
                delete node._key;
            }
        });
    };

    finalizeTree(rootNodes);
    return rootNodes;
}

function isEquipmentFolderLabel(label) {
    if (!label) {
        return false;
    }
    const normalized = String(label)
        .toLowerCase()
        .normalize("NFD")
        .replace(/[^a-z0-9]/g, "");
    return normalized.includes("attrezz");
}

function partitionMaterialsTree(nodes) {
    const materialsTree = [];
    const equipmentTree = [];
    nodes.forEach((node) => {
        if (!node || typeof node !== "object") {
            return;
        }
        if (isEquipmentFolderLabel(node.label)) {
            equipmentTree.push(node);
        } else {
            materialsTree.push(node);
        }
    });
    return { materialsTree, equipmentTree };
}

function collectTreeItemKeys(nodes) {
    const keys = [];
    if (!Array.isArray(nodes)) {
        return keys;
    }
    nodes.forEach((node) => {
        if (!node) {
            return;
        }
        if (Array.isArray(node.materials)) {
            node.materials.forEach((item) => {
                keys.push(buildEquipmentItemKey(item));
            });
        }
        if (Array.isArray(node.children) && node.children.length > 0) {
            keys.push(...collectTreeItemKeys(node.children));
        }
    });
    return keys;
}

function buildEquipmentItemKey(item) {
    const parts = [];
    if (item && item.id !== undefined && item.id !== null) {
        parts.push(String(item.id));
    }
    if (parts.length === 0 && item && item.name) {
        parts.push(String(item.name));
    }
    if (item && item.group_path) {
        parts.push(String(item.group_path));
    } else if (item && item.group_name) {
        parts.push(String(item.group_name));
    }
    if (parts.length === 0) {
        const fallback = item && (item.period_start || item.period_end || item.status_code)
            ? [item.period_start, item.period_end, item.status_code].join("::")
            : "material";
        parts.push(fallback);
    }
    return parts.join("::");
}

function formatEquipmentTimestamp(value) {
    const ts = Number(value);
    if (!Number.isFinite(ts)) {
        return "Non verificato";
    }
    const date = new Date(ts);
    if (Number.isNaN(date.getTime())) {
        return "Non verificato";
    }
    return date.toLocaleString("it-IT", {
        day: "2-digit",
        month: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
    });
}

function updateEquipmentStatusLabel(node, itemKeys, checks) {
    if (!node) {
        return;
    }
    const total = Array.isArray(itemKeys) ? itemKeys.length : 0;
    if (total === 0) {
        node.textContent = "";
        node.classList.add("hidden");
        return;
    }
    const map = checks && typeof checks === "object" ? checks : {};
    const checked = itemKeys.reduce((count, key) => (map[key] ? count + 1 : count), 0);
    node.textContent = `${checked}/${total} spuntate`;
    node.classList.remove("hidden");
}

function createMaterialRow(item) {
    const row = document.createElement("div");
    row.className = "material-row material-row-compact";
    row.setAttribute("role", "listitem");

    // Header compatto: nome + quantità sulla stessa riga
    const header = document.createElement("div");
    header.className = "material-header-compact";
    
    const name = document.createElement("span");
    name.className = "material-name";
    name.textContent = item.name || "Materiale";
    header.appendChild(name);

    const qtyBadge = document.createElement("span");
    qtyBadge.className = "material-qty-badge";
    qtyBadge.textContent = `Qtà: ${item.quantity_label || String(item.quantity || "0")}`;
    header.appendChild(qtyBadge);
    
    row.appendChild(header);

    // Note (se presente)
    if (item.note && item.note.trim()) {
        const note = document.createElement("div");
        note.className = "material-note";
        note.textContent = item.note;
        row.appendChild(note);
    }

    // Info compatte su una riga: periodo + peso + dimensioni
    const infoRow = document.createElement("div");
    infoRow.className = "material-info-row";
    
    const period = formatMaterialPeriod(item);
    const weight = getMaterialWeightLabel(item);
    const dimensions = getMaterialDimensionsLabel(item);
    
    // Periodo
    const periodSpan = document.createElement("span");
    periodSpan.className = "material-info-item";
    periodSpan.innerHTML = `📅 ${period}`;
    infoRow.appendChild(periodSpan);
    
    // Peso (solo se disponibile)
    if (weight && weight !== "---") {
        const weightSpan = document.createElement("span");
        weightSpan.className = "material-info-item";
        weightSpan.innerHTML = `⚖️ ${weight}`;
        infoRow.appendChild(weightSpan);
    }
    
    // Dimensioni (solo se disponibili)
    if (dimensions && dimensions !== "---") {
        const dimSpan = document.createElement("span");
        dimSpan.className = "material-info-item";
        dimSpan.innerHTML = `📐 ${dimensions}`;
        infoRow.appendChild(dimSpan);
    }
    
    row.appendChild(infoRow);

    // Footer: stato + pulsante foto
    const footer = document.createElement("div");
    footer.className = "material-footer-compact";

    const status = document.createElement("span");
    const statusClass = getMaterialStatusClass(item.status_code);
    status.className = `material-status ${statusClass}`;
    status.textContent = item.status || "Pianificato";
    footer.appendChild(status);

    const photoBtn = document.createElement("button");
    photoBtn.type = "button";
    photoBtn.className = "materials-photo-btn compact";
    if (materialHasPhoto(item)) {
        photoBtn.textContent = "👁️ Foto";
        photoBtn.addEventListener("click", () =>
            openMaterialPhotoPreview({
                name: item.name,
                path: item.group_path || resolveMaterialGroupLabel(item) || formatMaterialPeriod(item),
                photo: item.photo,
            })
        );
    } else {
        photoBtn.textContent = "No foto";
        photoBtn.disabled = true;
        photoBtn.classList.add("secondary");
    }
    footer.appendChild(photoBtn);
    
    row.appendChild(footer);

    return row;
}

function createEquipmentRow(item, options) {
    const context = options || {};
    const row = createMaterialRow(item);
    row.classList.add("equipment-row");
    const projectKey = context.projectKey || getMaterialsProjectKey();
    const checks = context.checks || {};
    const itemKey = buildEquipmentItemKey(item);
    const storedTs = checks[itemKey];
    let lastKnownTimestamp = storedTs || null;

    const checkboxColumn = document.createElement("div");
    checkboxColumn.className = "equipment-checkbox-column";
    const checkboxLabel = document.createElement("label");
    checkboxLabel.className = "equipment-checkbox";
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.className = "equipment-checkbox-input";
    checkbox.checked = Boolean(storedTs);
    const checkboxText = document.createElement("span");
    checkboxText.textContent = "Verificato";
    checkboxLabel.appendChild(checkbox);
    checkboxLabel.appendChild(checkboxText);
    checkboxColumn.appendChild(checkboxLabel);
    row.insertBefore(checkboxColumn, row.firstChild);

    const timestamp = document.createElement("div");
    timestamp.className = "equipment-timestamp";
    timestamp.textContent = storedTs ? formatEquipmentTimestamp(storedTs) : "Non verificato";
    row.appendChild(timestamp);

    checkbox.addEventListener("change", async () => {
        if (checkbox.dataset.syncing === "true") {
            return;
        }
        const desiredChecked = checkbox.checked;
        const previousTimestamp = lastKnownTimestamp;
        const previousChecked = Boolean(previousTimestamp);
        checkbox.disabled = true;
        checkbox.dataset.syncing = "true";
        timestamp.textContent = desiredChecked ? "Salvo la verifica..." : "Aggiorno la checklist...";
        try {
            const nextTs = await persistEquipmentCheckStateOnServer(projectKey, itemKey, desiredChecked);
            lastKnownTimestamp = nextTs || null;
            timestamp.textContent = nextTs ? formatEquipmentTimestamp(nextTs) : "Non verificato";
            if (typeof context.onStatusChange === "function") {
                context.onStatusChange();
            }
        } catch (error) {
            console.warn("persistEquipmentCheckStateOnServer", error);
            const message = desiredChecked
                ? "⚠️ Impossibile salvare la verifica. Riprova."
                : "⚠️ Impossibile annullare la verifica. Riprova.";
            showPopup(message);
            checkbox.checked = previousChecked;
            const fallbackTs = previousTimestamp;
            timestamp.textContent = fallbackTs ? formatEquipmentTimestamp(fallbackTs) : "Non verificato";
        } finally {
            checkbox.disabled = false;
            delete checkbox.dataset.syncing;
        }
    });

    return row;
}

function isMaterialsNodeExpanded(key) {
    if (!key) {
        return false;
    }
    if (!materialsTreeExpansion.has(key)) {
        materialsTreeExpansion.set(key, false);
    }
    return Boolean(materialsTreeExpansion.get(key));
}

function setMaterialsNodeExpanded(key, value) {
    if (!key) {
        return;
    }
    materialsTreeExpansion.set(key, Boolean(value));
}

function toggleMaterialsNode(key) {
    if (!key) {
        return;
    }
    const current = isMaterialsNodeExpanded(key);
    materialsTreeExpansion.set(key, !current);
    renderMaterials();
}

function renderMaterialsTree(target, nodes, depth, options) {
    if (!target || !Array.isArray(nodes) || nodes.length === 0) {
        return;
    }
    const settings = options || {};
    const nodeKeyPrefix = settings.nodeKeyPrefix || "";
    const rowRenderer = typeof settings.rowRenderer === "function" ? settings.rowRenderer : createMaterialRow;
    nodes.forEach((node) => {
        const section = document.createElement("div");
        section.className = "materials-group";
        section.dataset.depth = String(depth);
        if (depth > 0) {
            section.style.marginLeft = `${depth * 18}px`;
        }

        const header = document.createElement("div");
        header.className = "materials-group-header";
        const toggleBtn = document.createElement("button");
        toggleBtn.type = "button";
        toggleBtn.className = "materials-group-toggle";
        const nodeKey = nodeKeyPrefix ? `${nodeKeyPrefix}:${node.path}` : node.path;
        const expanded = isMaterialsNodeExpanded(nodeKey);
        toggleBtn.textContent = expanded ? "▼" : "▶";
        toggleBtn.title = expanded ? "Comprimi cartella" : "Espandi cartella";
        toggleBtn.addEventListener("click", () => toggleMaterialsNode(nodeKey));
        header.appendChild(toggleBtn);

        const title = document.createElement("div");
        title.className = "materials-group-title";
        title.textContent = `📂 ${node.label}`;
        header.appendChild(title);

        const count = document.createElement("span");
        count.className = "materials-group-count";
        const materialsArr = Array.isArray(node.materials) ? node.materials : [];
        const total = Number(node.totalMaterials || materialsArr.length);
        count.textContent = total === 1 ? "1 materiale" : `${total} materiali`;
        header.appendChild(count);
        section.appendChild(header);

        const body = document.createElement("div");
        body.className = "materials-group-body";
        if (!expanded) {
            section.classList.add("collapsed");
        }

        if (Array.isArray(node.materials) && node.materials.length > 0) {
            const materialsContainer = document.createElement("div");
            materialsContainer.className = "materials-group-materials";
            node.materials.forEach((item) => {
                const rowNode = rowRenderer(item);
                if (rowNode) {
                    materialsContainer.appendChild(rowNode);
                }
            });
            body.appendChild(materialsContainer);
        }

        if (Array.isArray(node.children) && node.children.length > 0) {
            const childrenContainer = document.createElement("div");
            childrenContainer.className = "materials-group-children";
            renderMaterialsTree(childrenContainer, node.children, depth + 1, options);
            body.appendChild(childrenContainer);
        }

        section.appendChild(body);

        target.appendChild(section);
    });
}

function getMaterialWeightLabel(material) {
    if (!material) {
        return "---";
    }
    return material.weight_label || "---";
}

function getMaterialDimensionsLabel(material) {
    if (!material) {
        return "---";
    }
    return material.dimensions_label || "---";
}

function materialHasPhoto(material) {
    return Boolean(material && material.photo && (material.photo.preview_url || material.photo.url));
}

function formatMemberStartLabel(member) {
    if (!member) {
        return "";
    }
    let numeric = Number(member.last_start_ts);
    if (!Number.isFinite(numeric) || numeric <= 0) {
        const elapsedMs = Number(member.elapsed);
        if (member.running && Number.isFinite(elapsedMs) && elapsedMs > 0) {
            numeric = Date.now() - elapsedMs;
        }
    }
    if (!Number.isFinite(numeric) || numeric <= 0) {
        return "";
    }
    const date = new Date(numeric);
    if (Number.isNaN(date.getTime())) {
        return "";
    }
    const now = new Date();
    const sameDay =
        date.getFullYear() === now.getFullYear() &&
        date.getMonth() === now.getMonth() &&
        date.getDate() === now.getDate();
    const timeLabel = date.toLocaleTimeString("it-IT", { hour: "2-digit", minute: "2-digit" });
    if (sameDay) {
        return `Ultimo avvio ${timeLabel}`;
    }
    const dateLabel = date.toLocaleDateString("it-IT", { day: "2-digit", month: "2-digit" });
    return `Ultimo avvio ${dateLabel} ${timeLabel}`;
}

function getAttachmentIcon(type) {
    const slug = String(type || "").toUpperCase();
    if (!slug) {
        return "📎";
    }
    if (slug.includes("PDF")) {
        return "📄";
    }
    if (["JPG", "JPEG", "PNG", "GIF", "SVG", "WEBP", "BMP"].includes(slug)) {
        return "🖼️";
    }
    if (["XLS", "XLSX", "CSV", "ODS"].includes(slug)) {
        return "📊";
    }
    if (["DOC", "DOCX", "TXT", "RTF"].includes(slug)) {
        return "📝";
    }
    if (["MP4", "MOV", "AVI", "MKV"].includes(slug)) {
        return "🎬";
    }
    if (["MP3", "WAV", "AAC", "M4A", "OGG"].includes(slug)) {
        return "🎧";
    }
    if (["ZIP", "RAR", "7Z", "TAR"].includes(slug)) {
        return "🗜️";
    }
    return "📎";
}

function openAttachment(item) {
    if (!item) {
        return;
    }
    const target = item.preview_url || item.url;
    if (!target) {
        showPopup("⚠️ Link non disponibile");
        return;
    }
    window.open(target, "_blank", "noopener");
}

function downloadAttachment(item) {
    if (!item) {
        return;
    }
    const target = item.url || item.preview_url;
    if (!target) {
        showPopup("⚠️ Download non disponibile");
        return;
    }
    const anchor = document.createElement("a");
    anchor.href = target;
    anchor.target = "_blank";
    anchor.rel = "noopener";
    anchor.download = (item.name || "allegato").replace(/\s+/g, "_");
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
}

function renderAttachments() {
    const card = document.getElementById("attachmentsCard");
    const grid = document.getElementById("attachmentsGrid");
    const emptyState = document.getElementById("attachmentsEmpty");
    const countNode = document.getElementById("attachmentsCount");
    const projectLabel = document.getElementById("attachmentsProjectLabel");
    const loadingNode = document.getElementById("attachmentsLoading");
    const refreshBtn = document.getElementById("attachmentsRefreshBtn");
    const uploadBtn = document.getElementById("attachmentsUploadBtn");
    if (!card) {
        return;
    }
    card.classList.remove("hidden");
    const activeProject = attachmentsState.project;
    if (projectLabel) {
        if (activeProject && (activeProject.code || activeProject.name)) {
            const { code, name } = activeProject;
            projectLabel.textContent = code && name ? `${code} · ${name}` : name || code;
        } else {
            projectLabel.textContent = "Nessun progetto attivo";
        }
    }
    if (countNode) {
        countNode.textContent = attachmentsState.items.length;
    }
    const hasProject = Boolean(activeProject);
    if (refreshBtn) {
        refreshBtn.disabled = !hasProject || attachmentsState.loading;
    }
    if (uploadBtn) {
        uploadBtn.disabled = !hasProject;
    }
    if (loadingNode) {
        loadingNode.classList.toggle("hidden", !attachmentsState.loading);
    }
    if (!grid || !emptyState) {
        return;
    }
    grid.innerHTML = "";
    if (!activeProject) {
        grid.classList.add("hidden");
        emptyState.classList.remove("hidden");
        emptyState.textContent = "Carica un progetto per visualizzare gli allegati disponibili.";
        return;
    }
    if (!attachmentsState.items.length) {
        grid.classList.add("hidden");
        emptyState.classList.remove("hidden");
        emptyState.textContent = attachmentsState.loading
            ? "Sto recuperando gli allegati dal server..."
            : "Premi \"Aggiorna allegati\" per caricare la lista.";
        return;
    }
    grid.classList.remove("hidden");
    emptyState.classList.add("hidden");
    const fragment = document.createDocumentFragment();
    attachmentsState.items.forEach((item) => {
        const cardNode = document.createElement("div");
        cardNode.className = "attachment-card";

        const icon = document.createElement("div");
        icon.className = "attachment-icon";
        icon.textContent = getAttachmentIcon(item.type || item.extension);
        cardNode.appendChild(icon);

        const details = document.createElement("div");
        details.className = "attachment-details";
        const nameNode = document.createElement("div");
        nameNode.className = "attachment-name";
        nameNode.textContent = item.name || `Allegato ${item.id || ""}`;
        details.appendChild(nameNode);

        const meta = document.createElement("div");
        meta.className = "attachment-meta";
        const parts = [];
        if (item.type) {
            parts.push(item.type);
        }
        const sizeLabel = formatAttachmentSize(item.size);
        if (sizeLabel) {
            parts.push(sizeLabel);
        }
        const dateLabel = formatAttachmentTimestamp(item.created);
        if (dateLabel) {
            parts.push(dateLabel);
        }
        meta.textContent = parts.join(" · ");
        details.appendChild(meta);

        cardNode.appendChild(details);

        const actions = document.createElement("div");
        actions.className = "attachment-actions";

        const openBtn = document.createElement("button");
        openBtn.type = "button";
        openBtn.className = "attachment-action";
        openBtn.textContent = "Apri";
        openBtn.disabled = !item.url && !item.preview_url;
        openBtn.addEventListener("click", () => openAttachment(item));
        actions.appendChild(openBtn);

        const downloadBtn = document.createElement("button");
        downloadBtn.type = "button";
        downloadBtn.className = "attachment-action download";
        downloadBtn.textContent = "Download";
        downloadBtn.disabled = !item.url;
        downloadBtn.addEventListener("click", () => downloadAttachment(item));
        actions.appendChild(downloadBtn);

        cardNode.appendChild(actions);
        fragment.appendChild(cardNode);
    });
    grid.appendChild(fragment);
}

function openAttachmentsModal(options) {
    const modal = document.getElementById("attachmentsModal");
    if (!modal || attachmentsModalOpen) {
        if (attachmentsModalOpen) {
            renderAttachments();
        }
        return;
    }
    attachmentsModalOpen = true;
    modal.style.display = "flex";
    markBodyModalOpen();
    
    // Carica da cache se disponibile, altrimenti fetch
    const projectCode = attachmentsState.project && attachmentsState.project.code;
    if (projectCode && attachmentsState.items.length === 0) {
        const cached = loadAttachmentsFromCache();
        if (cached && cached.projectCode === String(projectCode)) {
            attachmentsState.items = cached.items;
            attachmentsState.lastUpdated = cached.savedAt || Date.now();
            renderAttachments();
        } else {
            // Nessuna cache, fetch automatico
            fetchProjectAttachments({ silent: true });
        }
    } else {
        renderAttachments();
    }
}

function closeAttachmentsModal() {
    const modal = document.getElementById("attachmentsModal");
    if (!modal) {
        return;
    }
    attachmentsModalOpen = false;
    modal.style.display = "none";
    releaseBodyModalState();
}

async function fetchProjectAttachments(options) {
    const settings = options || {};
    const silent = Boolean(settings.silent);
    const mode = settings.mode === "deep" ? "deep" : null;
    const refreshBtn = document.getElementById("attachmentsRefreshBtn");
    if (refreshBtn && !refreshBtn.dataset.label) {
        refreshBtn.dataset.label = refreshBtn.textContent || "Aggiorna";
    }
    const currentCode = attachmentsState.project && attachmentsState.project.code;
    if (!currentCode) {
        attachmentsState.project = null;
        attachmentsState.items = [];
        attachmentsState.lastUpdated = null;
        renderAttachments();
        if (!silent) {
            showPopup("⚠️ Nessun progetto attivo");
        }
        return;
    }
    attachmentsState.loading = true;
    if (refreshBtn && !silent) {
        refreshBtn.disabled = true;
        refreshBtn.textContent = "Aggiorno...";
    }
    renderAttachments();
    try {
        const url = mode === "deep" ? "/api/project/attachments?mode=deep" : "/api/project/attachments";
        const data = await fetchJson(url);
        const project = data && data.project ? data.project : attachmentsState.project;
        attachmentsState.project = project || null;
        attachmentsState.items = data && Array.isArray(data.attachments) ? data.attachments : [];
        attachmentsState.lastUpdated = Date.now();
        saveAttachmentsToCache(); // Salva in localStorage
        renderAttachments();
        if (!silent) {
            showPopup("📎 Allegati aggiornati");
        }
    } catch (error) {
        console.warn("fetchProjectAttachments", error);
        if (!silent) {
            showPopup("⚠️ Impossibile aggiornare gli allegati");
        }
    } finally {
        attachmentsState.loading = false;
        if (refreshBtn) {
            refreshBtn.disabled = false;
            refreshBtn.textContent = refreshBtn.dataset.label || "Aggiorna";
        }
        renderAttachments();
    }
}

function syncAttachmentsProject(project) {
    const nextCode = project && project.code ? String(project.code) : "";
    const currentCode = attachmentsState.project && attachmentsState.project.code ? String(attachmentsState.project.code) : "";
    if (!nextCode) {
        attachmentsState.project = null;
        attachmentsState.items = [];
        attachmentsState.lastUpdated = null;
        renderAttachments();
        if (attachmentsModalOpen) {
            closeAttachmentsModal();
        }
        return;
    }
    attachmentsState.project = project;
    const changed = currentCode !== nextCode;
    if (changed) {
        // Prova a caricare da cache
        const cached = loadAttachmentsFromCache();
        if (cached && cached.projectCode === nextCode) {
            attachmentsState.project = cached.project || project;
            attachmentsState.items = cached.items;
            attachmentsState.lastUpdated = cached.savedAt || Date.now();
            renderAttachments();
            // Aggiorna in background se la cache è vecchia (più di 5 minuti)
            const cacheAge = Date.now() - (cached.savedAt || 0);
            if (cacheAge > 5 * 60 * 1000) {
                fetchProjectAttachments({ silent: true });
            }
        } else {
            // Nessuna cache, fetch automatico
            attachmentsState.items = [];
            attachmentsState.lastUpdated = null;
            renderAttachments();
            fetchProjectAttachments({ silent: true });
        }
        return;
    }
    renderAttachments();
}

function openMaterialsModal(options) {
    if (equipmentModalOpen) {
        closeEquipmentModal();
    }
    const modal = document.getElementById("materialsModal");
    if (!modal) {
        return;
    }
    if (materialsModalOpen) {
        renderMaterials();
        return;
    }
    materialsModalOpen = true;
    modal.style.display = "flex";
    markBodyModalOpen();
    renderMaterials();
}

function closeMaterialsModal() {
    const modal = document.getElementById("materialsModal");
    if (!modal) {
        return;
    }
    if (!materialsModalOpen) {
        modal.style.display = "none";
        return;
    }
    materialsModalOpen = false;
    modal.style.display = "none";
    releaseBodyModalState();
}

function openEquipmentModal(options) {
    if (materialsModalOpen) {
        closeMaterialsModal();
    }
    const modal = document.getElementById("equipmentModal");
    if (!modal) {
        return;
    }
    if (equipmentModalOpen) {
        renderEquipment();
        return;
    }
    equipmentModalOpen = true;
    modal.style.display = "flex";
    markBodyModalOpen();
    renderEquipment();
}

function closeEquipmentModal() {
    const modal = document.getElementById("equipmentModal");
    if (!modal) {
        return;
    }
    if (!equipmentModalOpen) {
        modal.style.display = "none";
        return;
    }
    equipmentModalOpen = false;
    modal.style.display = "none";
    releaseBodyModalState();
}

function getMaterialStatusClass(statusCode) {
    switch (statusCode) {
        case "missing":
            return "material-status-missing";
        case "delayed":
            return "material-status-delayed";
        case "reserved":
            return "material-status-reserved";
        case "subrent":
            return "material-status-subrent";
        case "option":
            return "material-status-option";
        default:
            return "material-status-planned";
    }
}

function formatMaterialsTimestamp(value) {
    if (!value) {
        return "";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return "";
    }
    return planningTimeFormatter.format(date);
}

function renderMaterials() {
    const card = document.getElementById("materialsCard");
    if (!card) {
        return;
    }
    card.classList.remove("hidden");
    const list = document.getElementById("materialsList");
    const emptyNode = document.getElementById("materialsEmpty");
    const countLabel = document.getElementById("materialsCountLabel");
    const subtitle = document.getElementById("materialsSubtitle");
    const refreshBtn = document.getElementById("materialsRefreshBtn");
    const pill = document.getElementById("materialsStatusPill");
    const folderSection = document.getElementById("materialsFoldersSection");
    const folderList = document.getElementById("materialsFolderList");
    const folderEmpty = document.getElementById("materialsFoldersEmpty");
    const equipBtn = document.getElementById("equipmentOpenBtn");
    const hasProject = Boolean(materialsState.project);
    const items = Array.isArray(materialsState.items) ? materialsState.items : [];
    const folders = Array.isArray(materialsState.folders) ? materialsState.folders : [];
    if (subtitle) {
        if (materialsState.project) {
            const { code, name } = materialsState.project;
            subtitle.textContent = code && name ? `${code} · ${name}` : name || code || "Progetto attivo";
        } else {
            subtitle.textContent = "Nessun progetto attivo";
        }
    }
    if (countLabel) {
        countLabel.textContent = `${items.length} materiale${items.length === 1 ? "" : "i"}`;
    }
    if (refreshBtn) {
        refreshBtn.disabled = !hasProject || materialsState.loading;
    }
    if (pill) {
        if (materialsState.lastUpdated) {
            pill.textContent = `Aggiornato ${formatMaterialsTimestamp(materialsState.lastUpdated)}`;
            pill.classList.remove("hidden");
        } else {
            pill.classList.add("hidden");
        }
    }
    card.classList.toggle("active", hasProject && items.length > 0);
    if (!list || !emptyNode) {
        return;
    }
    list.innerHTML = "";
    const noItems = items.length === 0;
    if (!hasProject) {
        emptyNode.classList.remove("hidden");
        emptyNode.textContent = "Carica un progetto per visualizzare i materiali.";
        list.classList.add("hidden");
        equipmentViewState.tree = [];
        equipmentViewState.itemKeys = [];
        if (equipBtn) {
            equipBtn.disabled = true;
        }
        renderEquipment();
        return;
    }
    if (materialsState.loading) {
        emptyNode.classList.remove("hidden");
        emptyNode.textContent = "Sto recuperando i materiali dal server...";
        list.classList.add("hidden");
        if (equipBtn) {
            equipBtn.disabled = true;
        }
        renderEquipment();
        return;
    }
    if (noItems) {
        emptyNode.classList.remove("hidden");
        emptyNode.textContent = "Premi \"Aggiorna materiali\" per scaricare l'elenco da Rentman.";
        list.classList.add("hidden");
        equipmentViewState.tree = [];
        equipmentViewState.itemKeys = [];
        if (equipBtn) {
            equipBtn.disabled = true;
        }
        renderEquipment();
        return;
    }
    const treeNodes = buildMaterialsTree(items);
    const { materialsTree, equipmentTree } = partitionMaterialsTree(treeNodes);
    
    // Rinomina i folder di attrezzatura pianificata
    equipmentTree.forEach((node) => {
        if (isEquipmentFolderLabel(node.label)) {
            node.label = "Attrezzature pianificate";
        }
    });
    
    // Aggiungi le attrezzature extra (locali) all'equipment tree
    const finalEquipmentTree = [...equipmentTree];
    if (localEquipmentItems.length > 0) {
        const localGroup = {
            type: "folder",
            label: "Attrezzature extra",
            materials: localEquipmentItems.map((item) => ({
                type: "item",
                name: item.name,
                quantity: item.quantity,
                key: `local-${item.id}`,
                notes: item.notes,
                isLocal: true,
                localId: item.id,
            })),
            children: [],
        };
        finalEquipmentTree.push(localGroup);
    }
    
    const hasGeneralMaterials = materialsTree.length > 0;
    const hasEquipmentMaterials = finalEquipmentTree.length > 0;

    equipmentViewState.tree = finalEquipmentTree;
    equipmentViewState.itemKeys = collectTreeItemKeys(finalEquipmentTree);
    if (equipBtn) {
        equipBtn.disabled = !hasProject || materialsState.loading || finalEquipmentTree.length === 0;
    }

    if (hasGeneralMaterials) {
        emptyNode.classList.add("hidden");
        list.classList.remove("hidden");
        list.innerHTML = "";
        const fragment = document.createDocumentFragment();
        renderMaterialsTree(fragment, materialsTree, 0, { nodeKeyPrefix: "materials" });
        list.appendChild(fragment);
    } else {
        list.classList.add("hidden");
        emptyNode.classList.remove("hidden");
        emptyNode.textContent = hasEquipmentMaterials
            ? "I materiali generali non sono disponibili. Consulta la sezione Attrezzature."
            : "Nessun materiale disponibile per questo progetto.";
    }

    renderEquipment();

    if (folderSection && folderList && folderEmpty) {
        if (!hasProject) {
            folderSection.classList.add("hidden");
            folderList.innerHTML = "";
            folderList.classList.add("hidden");
            folderEmpty.textContent = "Carica un progetto per visualizzare le cartelle disponibili.";
            folderEmpty.classList.remove("hidden");
        } else {
            folderSection.classList.remove("hidden");
            folderList.innerHTML = "";
            if (!folders.length) {
                folderEmpty.textContent = materialsState.loading
                    ? "Sto recuperando le cartelle collegate..."
                    : "Nessuna cartella disponibile per questo progetto.";
                folderEmpty.classList.remove("hidden");
                folderList.classList.add("hidden");
            } else {
                folderEmpty.classList.add("hidden");
                folderList.classList.remove("hidden");
                const foldersFragment = document.createDocumentFragment();
                folders.forEach((folder) => {
                    if (!folder) {
                        return;
                    }
                    const row = document.createElement("div");
                    row.className = "materials-folder-row";
                    row.setAttribute("role", "listitem");

                    const info = document.createElement("div");
                    info.className = "materials-folder-info";
                    const name = document.createElement("div");
                    name.className = "materials-folder-name";
                    name.textContent = folder.name || "Cartella";
                    info.appendChild(name);
                    const path = document.createElement("div");
                    path.className = "materials-folder-path";
                    path.textContent = folder.path || "Percorso non disponibile";
                    info.appendChild(path);
                    row.appendChild(info);

                    const meta = document.createElement("div");
                    meta.className = "materials-folder-meta";
                    const metaParts = [];
                    if (folder.id !== undefined && folder.id !== null) {
                        metaParts.push(`#${folder.id}`);
                    }
                    if (typeof folder.file_count === "number") {
                        metaParts.push(folder.file_count === 1 ? "1 file" : `${folder.file_count} file`);
                    }
                    meta.textContent = metaParts.length ? metaParts.join(" · ") : "Cartella";
                    row.appendChild(meta);

                    const actions = document.createElement("div");
                    actions.className = "materials-folder-actions";
                    const photoBtn = document.createElement("button");
                    photoBtn.type = "button";
                    photoBtn.className = "materials-photo-btn";
                    const photo = folder.photo || {};
                    const previewUrl = photo.preview_url || photo.url;
                    if (previewUrl) {
                        photoBtn.textContent = "👁️ Mostra foto";
                        photoBtn.addEventListener("click", () => openMaterialPhotoPreview(folder));
                    } else {
                        photoBtn.textContent = "Nessuna foto";
                        photoBtn.disabled = true;
                        photoBtn.classList.add("secondary");
                    }
                    actions.appendChild(photoBtn);
                    row.appendChild(actions);

                    foldersFragment.appendChild(row);
                });
                folderList.appendChild(foldersFragment);
            }
        }
    }
}

function renderEquipment() {
    const card = document.getElementById("equipmentCard");
    if (!card) {
        return;
    }
    card.classList.remove("hidden");
    const list = document.getElementById("equipmentList");
    const emptyNode = document.getElementById("equipmentEmpty");
    const countLabel = document.getElementById("equipmentCountLabel");
    const subtitle = document.getElementById("equipmentSubtitle");
    const pill = document.getElementById("equipmentStatusPill");
    const summary = document.getElementById("equipmentCheckedSummary");
    const refreshBtn = document.getElementById("equipmentRefreshBtn");
    const hasProject = Boolean(materialsState.project);
    const isLoading = materialsState.loading;
    const tree = Array.isArray(equipmentViewState.tree) ? equipmentViewState.tree : [];
    const equipmentContext = getEquipmentCheckContext();
    const itemKeys = collectTreeItemKeys(tree);
    equipmentViewState.itemKeys = itemKeys;

    if (subtitle) {
        if (materialsState.project) {
            const { code, name } = materialsState.project;
            subtitle.textContent = code && name ? `${code} · ${name}` : name || code || "Progetto attivo";
        } else {
            subtitle.textContent = "Nessun progetto attivo";
        }
    }

    if (countLabel) {
        countLabel.textContent = `${itemKeys.length} attrezzatura${itemKeys.length === 1 ? "" : "e"}`;
    }

    if (pill) {
        if (materialsState.lastUpdated) {
            pill.textContent = `Aggiornato ${formatMaterialsTimestamp(materialsState.lastUpdated)}`;
            pill.classList.remove("hidden");
        } else {
            pill.classList.add("hidden");
        }
    }

    if (refreshBtn) {
        if (!refreshBtn.dataset.label) {
            refreshBtn.dataset.label = refreshBtn.textContent || "Aggiorna attrezzature";
        }
        refreshBtn.disabled = !hasProject || isLoading;
        if (isLoading) {
            refreshBtn.textContent = "Aggiorno...";
        } else {
            refreshBtn.textContent = refreshBtn.dataset.label;
        }
    }

    if (!list || !emptyNode) {
        return;
    }

    if (!hasProject) {
        emptyNode.classList.remove("hidden");
        emptyNode.textContent = "Carica un progetto per visualizzare la checklist.";
        list.classList.add("hidden");
        if (summary) {
            summary.classList.add("hidden");
        }
        return;
    }

    if (isLoading) {
        emptyNode.classList.remove("hidden");
        emptyNode.textContent = "Sto recuperando le attrezzature dal server...";
        list.classList.add("hidden");
        if (summary) {
            summary.classList.add("hidden");
        }
        return;
    }

    if (!tree.length) {
        emptyNode.classList.remove("hidden");
        emptyNode.textContent = "Nessuna cartella Attrezzature disponibile per questo progetto.";
        list.classList.add("hidden");
        if (summary) {
            summary.classList.add("hidden");
        }
        return;
    }

    emptyNode.classList.add("hidden");
    list.classList.remove("hidden");
    list.innerHTML = "";
    const fragment = document.createDocumentFragment();
    const statusUpdater = () => updateEquipmentStatusLabel(summary, itemKeys, equipmentContext.checks);
    const equipmentRowRenderer = (item) =>
        createEquipmentRow(item, {
            projectKey: equipmentContext.projectKey,
            checks: equipmentContext.checks,
            onStatusChange: statusUpdater,
        });
    renderMaterialsTree(fragment, tree, 0, {
        nodeKeyPrefix: "equipment",
        rowRenderer: equipmentRowRenderer,
    });
    list.appendChild(fragment);
    if (summary) {
        statusUpdater();
    }
}

function openMaterialPhotoPreview(source) {
    if (!source || !source.photo) {
        showPopup("⚠️ Nessuna foto disponibile per questo elemento");
        return;
    }
    const photo = source.photo;
    const previewUrl = photo.preview_url || photo.url;
    if (!previewUrl) {
        showPopup("⚠️ Foto non disponibile");
        return;
    }
    const modal = document.getElementById("materialPhotoModal");
    if (!modal) {
        window.open(previewUrl, "_blank", "noopener,noreferrer");
        return;
    }
    const image = document.getElementById("materialPhotoImage");
    if (image) {
        image.src = previewUrl;
        image.alt = source.name ? `Foto ${source.name}` : "Foto materiale";
    }
    const caption = document.getElementById("materialPhotoCaption");
    if (caption) {
        const parts = [];
        if (source.name) {
            parts.push(source.name);
        }
        if (source.path) {
            parts.push(source.path);
        }
        caption.textContent = parts.length ? parts.join(" · ") : "Anteprima foto materiale";
    }
    const link = document.getElementById("materialPhotoLink");
    if (link) {
        const href = photo.url || previewUrl;
        if (href) {
            link.href = href;
            link.classList.remove("hidden");
        } else {
            link.href = "#";
            link.classList.add("hidden");
        }
    }
    modal.style.display = "flex";
    materialPhotoModalOpen = true;
    markBodyModalOpen();
}

function closeMaterialPhotoPreview() {
    const modal = document.getElementById("materialPhotoModal");
    if (!modal) {
        return;
    }
    if (!materialPhotoModalOpen) {
        return;
    }
    modal.style.display = "none";
    const image = document.getElementById("materialPhotoImage");
    if (image) {
        image.src = "";
    }
    materialPhotoModalOpen = false;
    releaseBodyModalState();
}

async function fetchProjectMaterials(options) {
    const settings = options || {};
    const silent = Boolean(settings.silent);
    const refresh = Boolean(settings.refresh);
    const refreshBtn = document.getElementById("materialsRefreshBtn");
    if (refreshBtn && !refreshBtn.dataset.label) {
        refreshBtn.dataset.label = refreshBtn.textContent || "Aggiorna materiali";
    }
    const currentCode = materialsState.project && materialsState.project.code;
    if (!currentCode) {
        materialsState.project = null;
        materialsState.items = [];
        materialsState.folders = [];
        materialsState.lastUpdated = null;
        materialsTreeExpansion.clear();
        renderMaterials();
        if (!silent) {
            showPopup("⚠️ Nessun progetto attivo");
        }
        return;
    }
    materialsState.loading = true;
    if (refreshBtn && !silent) {
        refreshBtn.disabled = true;
        refreshBtn.textContent = "Aggiorno...";
    }
    renderMaterials();
    try {
        const endpoint = refresh ? "/api/project/materials?mode=refresh" : "/api/project/materials";
        const [data, localEquipData] = await Promise.all([
            fetchJson(endpoint),
            fetchJson("/api/project/local-equipment").catch(() => ({ ok: true, items: [] })),
        ]);
        const project = data && data.project ? data.project : materialsState.project;
        materialsState.project = project || null;
        materialsState.items = data && Array.isArray(data.materials) ? data.materials : [];
        materialsState.folders = data && Array.isArray(data.folders) ? data.folders : [];
        if (data && Object.prototype.hasOwnProperty.call(data, "equipment_checks")) {
            replaceEquipmentChecksForProject(project && project.code, data.equipment_checks);
        }
        localEquipmentItems = localEquipData && localEquipData.ok && Array.isArray(localEquipData.items) ? localEquipData.items : [];
        const updatedTs = data && Number(data.updated_ts);
        materialsState.lastUpdated = Number.isFinite(updatedTs) ? updatedTs : Date.now();
        renderMaterials();
        if (!silent) {
            const message = refresh ? "🧰 Materiali aggiornati" : "🧰 Elenco attrezzature caricato";
            showPopup(message);
        }
    } catch (error) {
        console.warn("fetchProjectMaterials", error);
        if (!silent) {
            showPopup("⚠️ Impossibile aggiornare i materiali");
        }
    } finally {
        materialsState.loading = false;
        if (refreshBtn) {
            refreshBtn.disabled = false;
            refreshBtn.textContent = refreshBtn.dataset.label || "Aggiorna materiali";
        }
        renderMaterials();
    }
}

async function fetchLocalEquipment(options) {
    const settings = options || {};
    const silent = Boolean(settings.silent);
    try {
        const data = await fetchJson("/api/project/local-equipment");
        if (data && data.ok && Array.isArray(data.items)) {
            localEquipmentItems = data.items;
        } else {
            localEquipmentItems = [];
        }
    } catch (error) {
        console.warn("fetchLocalEquipment", error);
        localEquipmentItems = [];
        if (!silent) {
            showPopup("⚠️ Impossibile caricare le attrezzature locali");
        }
    }
    renderMaterials();
}

function syncMaterialsProject(project) {
    const nextCode = project && project.code ? String(project.code) : "";
    const currentCode = materialsState.project && materialsState.project.code ? String(materialsState.project.code) : "";
    if (!nextCode) {
        materialsState.project = null;
        materialsState.items = [];
        materialsState.folders = [];
        materialsState.lastUpdated = null;
        localEquipmentItems = [];
        materialsTreeExpansion.clear();
        renderMaterials();
        return;
    }
    materialsState.project = project;
    const changed = nextCode !== currentCode;
    if (changed) {
        materialsState.items = [];
        materialsState.folders = [];
        materialsState.lastUpdated = null;
        localEquipmentItems = [];
        materialsTreeExpansion.clear();
        renderMaterials();
        // Carica automaticamente le attrezzature quando cambia progetto
        fetchProjectMaterials({ silent: true, refresh: false });
    }
    renderMaterials();
}

// ──────────────────────────────────────────────────────────────────────────────
// Foto Progetto
// ──────────────────────────────────────────────────────────────────────────────

function renderPhotos() {
    const grid = document.getElementById("photosGrid");
    const empty = document.getElementById("photosEmpty");
    const loading = document.getElementById("photosLoading");
    const countEl = document.getElementById("photosCount");
    const projectLabel = document.getElementById("photosProjectLabel");

    if (projectLabel) {
        projectLabel.textContent = photosState.project
            ? `Progetto ${photosState.project.code}`
            : "Nessun progetto attivo";
    }

    if (countEl) {
        countEl.textContent = String(photosState.items.length);
    }

    if (photosState.loading) {
        if (loading) loading.classList.remove("hidden");
        if (grid) grid.classList.add("hidden");
        if (empty) empty.classList.add("hidden");
        return;
    }

    if (loading) loading.classList.add("hidden");

    if (!photosState.project || photosState.items.length === 0) {
        if (grid) grid.classList.add("hidden");
        if (empty) {
            empty.classList.remove("hidden");
            empty.textContent = photosState.project
                ? "Nessuna foto caricata per questo progetto."
                : "Carica un progetto per visualizzare e aggiungere foto.";
        }
        return;
    }

    if (empty) empty.classList.add("hidden");
    if (grid) {
        grid.classList.remove("hidden");
        grid.innerHTML = "";

        const fragment = document.createDocumentFragment();
        photosState.items.forEach((photo) => {
            const thumb = document.createElement("div");
            thumb.className = "photo-thumb";
            thumb.dataset.photoId = photo.id;

            const img = document.createElement("img");
            img.src = `/api/project/photos/${photo.filename}`;
            img.alt = photo.original_name || "Foto";
            img.loading = "lazy";

            thumb.appendChild(img);

            if (photo.caption) {
                const caption = document.createElement("div");
                caption.className = "photo-thumb-caption";
                caption.textContent = photo.caption;
                thumb.appendChild(caption);
            }

            thumb.addEventListener("click", () => openPhotoPreview(photo));
            fragment.appendChild(thumb);
        });

        grid.appendChild(fragment);
    }
}

function openPhotosModal() {
    const modal = document.getElementById("photosModal");
    if (modal) {
        modal.classList.add("open");
        fetchPhotos({ silent: true });
    }
}

function closePhotosModal() {
    const modal = document.getElementById("photosModal");
    if (modal) {
        modal.classList.remove("open");
    }
}

async function fetchPhotos(options) {
    const settings = options || {};
    const silent = Boolean(settings.silent);

    if (!materialsState.project) {
        photosState.project = null;
        photosState.items = [];
        renderPhotos();
        return;
    }

    photosState.project = materialsState.project;
    photosState.loading = true;
    renderPhotos();

    try {
        const data = await fetchJson("/api/project/photos");
        if (data && data.ok && Array.isArray(data.items)) {
            photosState.items = data.items;
        } else {
            photosState.items = [];
        }
        if (!silent && data.items.length > 0) {
            showPopup(`📷 ${data.items.length} foto caricate`);
        }
    } catch (error) {
        console.warn("fetchPhotos", error);
        photosState.items = [];
        if (!silent) {
            showPopup("⚠️ Impossibile caricare le foto");
        }
    } finally {
        photosState.loading = false;
        renderPhotos();
    }
}

async function uploadPhoto(file) {
    if (!materialsState.project) {
        showPopup("⚠️ Carica prima un progetto");
        return;
    }

    const formData = new FormData();
    formData.append("photo", file);

    try {
        showPopup("📤 Caricamento in corso...");
        const response = await fetch("/api/project/photos", {
            method: "POST",
            body: formData,
        });
        const data = await response.json();
        if (!response.ok || !data.ok) {
            const errorMsg = data.error === "file_too_large" 
                ? "File troppo grande (max 10 MB)"
                : data.error === "invalid_file_type"
                ? "Tipo file non supportato"
                : data.error || "Errore durante il caricamento";
            showPopup("⚠️ " + errorMsg);
            return;
        }
        showPopup("✅ Foto caricata con successo");
        fetchPhotos({ silent: true });
    } catch (error) {
        console.error("uploadPhoto", error);
        showPopup("⚠️ Errore di rete");
    }
}

function openPhotoPreview(photo) {
    const modal = document.getElementById("photoPreviewModal");
    const img = document.getElementById("photoPreviewImage");
    const caption = document.getElementById("photoPreviewCaption");

    if (!modal || !img) return;

    currentPreviewPhotoId = photo.id;
    img.src = `/api/project/photos/${photo.filename}`;
    img.alt = photo.original_name || "Foto";

    if (caption) {
        caption.textContent = photo.caption || photo.original_name || "";
    }

    modal.classList.add("open");
}

function closePhotoPreview() {
    const modal = document.getElementById("photoPreviewModal");
    if (modal) {
        modal.classList.remove("open");
        currentPreviewPhotoId = null;
    }
}

async function deleteCurrentPhoto() {
    if (!currentPreviewPhotoId) return;

    const confirmed = confirm("Eliminare questa foto?");
    if (!confirmed) return;

    try {
        const response = await fetch(`/api/project/photos/${currentPreviewPhotoId}`, {
            method: "DELETE",
        });
        const data = await response.json();
        if (!response.ok || !data.ok) {
            showPopup("⚠️ Impossibile eliminare la foto");
            return;
        }
        showPopup("🗑️ Foto eliminata");
        closePhotoPreview();
        fetchPhotos({ silent: true });
    } catch (error) {
        console.error("deleteCurrentPhoto", error);
        showPopup("⚠️ Errore di rete");
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Fotocamera nativa
// ──────────────────────────────────────────────────────────────────────────────

let cameraStream = null;

async function openCameraModal() {
    if (!materialsState.project) {
        showPopup("⚠️ Carica prima un progetto");
        return;
    }

    const modal = document.getElementById("cameraModal");
    const video = document.getElementById("cameraVideo");
    
    if (!modal || !video) {
        // Fallback: prova con input capture
        const input = document.getElementById("photosCameraInput");
        if (input) input.click();
        return;
    }

    try {
        // Richiedi accesso alla fotocamera posteriore
        cameraStream = await navigator.mediaDevices.getUserMedia({
            video: {
                facingMode: { ideal: "environment" },
                width: { ideal: 1920 },
                height: { ideal: 1080 }
            },
            audio: false
        });

        video.srcObject = cameraStream;
        await video.play();
        modal.classList.add("open");
    } catch (error) {
        console.error("openCameraModal", error);
        if (error.name === "NotAllowedError") {
            showPopup("⚠️ Permesso fotocamera negato");
        } else if (error.name === "NotFoundError") {
            showPopup("⚠️ Nessuna fotocamera trovata");
            // Fallback: apri input file
            const input = document.getElementById("photosCameraInput");
            if (input) input.click();
        } else {
            showPopup("⚠️ Impossibile aprire la fotocamera");
            // Fallback: apri input file
            const input = document.getElementById("photosCameraInput");
            if (input) input.click();
        }
    }
}

function closeCameraModal() {
    const modal = document.getElementById("cameraModal");
    const video = document.getElementById("cameraVideo");

    if (cameraStream) {
        cameraStream.getTracks().forEach(track => track.stop());
        cameraStream = null;
    }

    if (video) {
        video.srcObject = null;
    }

    if (modal) {
        modal.classList.remove("open");
    }
}

async function capturePhoto() {
    const video = document.getElementById("cameraVideo");
    const canvas = document.getElementById("cameraCanvas");

    if (!video || !canvas) return;

    // Imposta dimensioni canvas uguali al video
    canvas.width = video.videoWidth;
    canvas.height = video.videoHeight;

    // Disegna il frame corrente sul canvas
    const ctx = canvas.getContext("2d");
    ctx.drawImage(video, 0, 0);

    // Converti in blob
    canvas.toBlob(async (blob) => {
        if (!blob) {
            showPopup("⚠️ Errore durante la cattura");
            return;
        }

        // Crea un file dalla foto
        const filename = `foto_${Date.now()}.jpg`;
        const file = new File([blob], filename, { type: "image/jpeg" });

        // Chiudi la fotocamera
        closeCameraModal();

        // Carica la foto
        await uploadPhoto(file);
    }, "image/jpeg", 0.9);
}

function syncPhotosProject(project) {
    const nextCode = project && project.code ? String(project.code) : "";
    const currentCode = photosState.project && photosState.project.code ? String(photosState.project.code) : "";

    if (!nextCode) {
        photosState.project = null;
        photosState.items = [];
        renderPhotos();
        return;
    }

    photosState.project = project;
    const changed = nextCode !== currentCode;
    if (changed) {
        photosState.items = [];
        renderPhotos();
    }
}

function handleAttachmentUpload() {
    if (!attachmentsState.project) {
        showPopup("⚠️ Carica prima un progetto");
        return;
    }
    showPopup("🚧 Upload allegati in arrivo");
}

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

function loadEquipmentChecksStore() {
    const payload = readCachedPayload(EQUIPMENT_CHECKS_KEY);
    if (payload && typeof payload === "object" && !Array.isArray(payload)) {
        return payload;
    }
    return {};
}

function persistEquipmentChecksStore() {
    saveCachedPayload(EQUIPMENT_CHECKS_KEY, equipmentChecksStore);
}

function sanitizeEquipmentChecksPayload(payload) {
    if (!payload || typeof payload !== "object") {
        return {};
    }
    const sanitized = {};
    Object.keys(payload).forEach((itemKey) => {
        const timestamp = Number(payload[itemKey]);
        if (Number.isFinite(timestamp)) {
            sanitized[itemKey] = timestamp;
        }
    });
    return sanitized;
}

function normalizeProjectKey(value) {
    if (value === undefined || value === null) {
        return "DEFAULT";
    }
    const slug = String(value).trim().toUpperCase();
    return slug || "DEFAULT";
}

function replaceEquipmentChecksForProject(projectCode, payload) {
    const fallback = materialsState.project && materialsState.project.code ? materialsState.project.code : "DEFAULT";
    const key = normalizeProjectKey(projectCode || fallback);
    const sanitized = sanitizeEquipmentChecksPayload(payload);
    equipmentChecksStore[key] = sanitized;
    persistEquipmentChecksStore();
    return sanitized;
}

function getMaterialsProjectKey() {
    const raw = materialsState.project && materialsState.project.code ? String(materialsState.project.code) : "";
    return normalizeProjectKey(raw);
}

function getEquipmentCheckContext() {
    const projectKey = getMaterialsProjectKey();
    if (!equipmentChecksStore[projectKey]) {
        equipmentChecksStore[projectKey] = {};
    }
    return { projectKey, checks: equipmentChecksStore[projectKey] };
}

function setEquipmentCheckState(projectKey, itemKey, checked, options) {
    const key = normalizeProjectKey(projectKey);
    if (!equipmentChecksStore[key]) {
        equipmentChecksStore[key] = {};
    }
    let timestamp = null;
    if (checked) {
        const settings = options || {};
        const candidate = Number(settings.timestamp);
        timestamp = Number.isFinite(candidate) ? candidate : Date.now();
        equipmentChecksStore[key][itemKey] = timestamp;
    } else {
        delete equipmentChecksStore[key][itemKey];
    }
    persistEquipmentChecksStore();
    return timestamp;
}

async function persistEquipmentCheckStateOnServer(projectKey, itemKey, checked) {
    if (!itemKey) {
        throw new Error("missing_item_key");
    }
    const payload = {
        item_key: itemKey,
        checked,
    };
    const response = await postJson("/api/project/equipment/checks", payload);
    if (response && response.ok === false) {
        const error = new Error(response.error || "equipment_check_failed");
        error.details = response;
        throw error;
    }
    let timestamp = null;
    if (checked) {
        const serverTimestamp = response && response.timestamp !== undefined ? Number(response.timestamp) : NaN;
        timestamp = Number.isFinite(serverTimestamp) ? serverTimestamp : Date.now();
    }
    return setEquipmentCheckState(projectKey, itemKey, checked, { timestamp });
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

function forceCloseOverlays() {
    // Reset any dimming layers that might linger after navigation/back-forward cache restores.
    closeMenu();
    closeTimeline();
    closeMaterialsModal();
    closeEquipmentModal();
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

// ═══════════════════════════════════════════════════════════════════════════════
//  QR TIMBRATURA MODAL
// ═══════════════════════════════════════════════════════════════════════════════
let qrTimbraturaModalOpen = false;
let qrRefreshInterval = null;
let qrProgressInterval = null;
let qrRefreshSeconds = 10;

function openQrTimbraturaModal() {
    const modal = document.getElementById("qrTimbraturaModal");
    if (!modal || qrTimbraturaModalOpen) return;
    
    qrTimbraturaModalOpen = true;
    modal.classList.add("open");
    markBodyModalOpen();
    
    // Avvia il caricamento del QR
    loadQrTimbratura();
}

function closeQrTimbraturaModal() {
    const modal = document.getElementById("qrTimbraturaModal");
    if (!modal) return;
    
    qrTimbraturaModalOpen = false;
    modal.classList.remove("open");
    releaseBodyModalState();
    
    // Ferma i timer
    if (qrRefreshInterval) {
        clearInterval(qrRefreshInterval);
        qrRefreshInterval = null;
    }
    if (qrProgressInterval) {
        clearInterval(qrProgressInterval);
        qrProgressInterval = null;
    }
}

async function loadQrTimbratura() {
    const container = document.getElementById("qrTimbraturaContainer");
    const loading = document.getElementById("qrTimbraturaLoading");
    const image = document.getElementById("qrTimbraturaImage");
    const timeEl = document.getElementById("qrTimbraturaTime");
    const deviceIdEl = document.getElementById("qrDeviceId");
    const refreshSecondsEl = document.getElementById("qrRefreshSeconds");
    const progressBar = document.getElementById("qrRefreshProgress");
    
    if (!container) return;
    
    // Mostra loading
    if (loading) loading.style.display = "flex";
    if (image) image.classList.remove("loaded");
    
    try {
        const res = await fetch("/api/qr-timbratura");
        if (!res.ok) throw new Error("Errore nel caricamento QR");
        
        const data = await res.json();
        if (!data.ok) throw new Error(data.error || "Errore QR");
        
        // Aggiorna immagine
        if (image) {
            image.src = data.image;
            image.classList.add("loaded");
        }
        if (loading) loading.style.display = "none";
        
        // Aggiorna device ID
        if (deviceIdEl && data.payload) {
            deviceIdEl.textContent = data.payload.dev || "";
        }
        
        // Aggiorna tempo di refresh
        qrRefreshSeconds = data.refresh_seconds || 10;
        if (refreshSecondsEl) {
            refreshSecondsEl.textContent = qrRefreshSeconds;
        }
        
        // Avvia/riavvia i timer
        startQrRefreshTimers(progressBar);
        
    } catch (err) {
        console.error("QR Timbratura error:", err);
        if (loading) {
            loading.innerHTML = `<span style="color:#ef4444;">❌ ${err.message}</span>`;
        }
    }
    
    // Aggiorna orologio
    updateQrClock(timeEl);
}

function startQrRefreshTimers(progressBar) {
    // Ferma timer esistenti
    if (qrRefreshInterval) clearInterval(qrRefreshInterval);
    if (qrProgressInterval) clearInterval(qrProgressInterval);
    
    // Progress bar animation
    let elapsed = 0;
    const interval = 100; // 100ms
    
    if (progressBar) {
        progressBar.style.transition = "none";
        progressBar.style.width = "100%";
        
        qrProgressInterval = setInterval(() => {
            elapsed += interval;
            const remaining = Math.max(0, 100 - (elapsed / (qrRefreshSeconds * 1000)) * 100);
            progressBar.style.width = remaining + "%";
        }, interval);
    }
    
    // Refresh QR ogni N secondi
    qrRefreshInterval = setInterval(() => {
        elapsed = 0;
        if (progressBar) {
            progressBar.style.width = "100%";
        }
        loadQrTimbratura();
    }, qrRefreshSeconds * 1000);
}

function updateQrClock(element) {
    if (!element) return;
    
    const updateTime = () => {
        const now = new Date();
        const time = now.toLocaleTimeString("it-IT", { 
            hour: "2-digit", 
            minute: "2-digit", 
            second: "2-digit" 
        });
        element.textContent = time;
    };
    
    updateTime();
    
    // Aggiorna ogni secondo se modal aperto
    const clockInterval = setInterval(() => {
        if (!qrTimbraturaModalOpen) {
            clearInterval(clockInterval);
            return;
        }
        updateTime();
    }, 1000);
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

    if (!toggle || !statusLabel || !textLabel) {
        return;
    }

    if (!pushState.supported) {
        toggle.disabled = true;
        textLabel.textContent = 'Notifiche Push';
        statusLabel.textContent = 'Non supportato';
        return;
    }

    if (!pushState.configured) {
        toggle.disabled = true;
        textLabel.textContent = 'Notifiche Push';
        statusLabel.textContent = 'Disattivate dal server';
        return;
    }

    if (typeof Notification !== 'undefined' && Notification.permission === 'denied') {
        toggle.disabled = true;
        textLabel.textContent = 'Notifiche Push';
        statusLabel.textContent = 'Permesso negato';
        return;
    }

    toggle.disabled = false;
    if (pushState.subscribed) {
        textLabel.textContent = 'Disattiva notifiche';
        statusLabel.textContent = 'Attive';
    } else {
        textLabel.textContent = 'Attiva notifiche';
        statusLabel.textContent = (typeof Notification !== 'undefined' && Notification.permission === 'granted')
            ? 'Disponibili'
            : 'Richiedono permesso';
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

    updateTeamAddActivityButtonState();

    if (!active) {
        closeNewActivityModal();
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
    const startLabel = formatMemberStartLabel(member);
    
    node.innerHTML = `
        <div class="task-header-row">
            <div class="member-name-block">
                <span class="member-name">${member.member_name}</span>
                ${startLabel ? `<span class="member-start">${startLabel}</span>` : ""}
            </div>
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

function handleTeamAddActivityClick(event) {
    if (event && typeof event.preventDefault === "function") {
        event.preventDefault();
        if (typeof event.stopPropagation === "function") {
            event.stopPropagation();
        }
    }
    if (!projectVisible) {
        showPopup("⚠️ Carica un progetto per aggiungere attività");
        return;
    }
    const opened = openNewActivityModal();
    if (!opened) {
        showPopup("⚠️ Impossibile aprire il modulo. Aggiorna la pagina.");
    }
}

function updateTeamAddActivityButtonState() {
    const btn = document.getElementById("teamAddActivityBtn");
    if (!btn) {
        return;
    }
    btn.disabled = !projectVisible || newActivitySaving;
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
    updateTeamAddActivityButtonState();
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
    attachmentsState.project = null;
    attachmentsState.items = [];
    attachmentsState.lastUpdated = null;
    renderAttachments();
    if (attachmentsModalOpen) {
        closeAttachmentsModal();
    }
    if (materialsModalOpen) {
        closeMaterialsModal();
    }
    materialsState.project = null;
    materialsState.items = [];
    materialsState.loading = false;
    materialsState.lastUpdated = null;
    renderMaterials();
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
    updateTeamAddActivityButtonState();
    allPaused = state.allPaused;
    setProjectVisibility(hasProject);
    setProjectLabel(state.project || null);
    syncAttachmentsProject(state.project || null);
    syncMaterialsProject(state.project || null);
    syncPhotosProject(state.project || null);
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

function resetNewActivityForm() {
    [
        "newActivityLabelInput",
        "newActivityIdInput",
        "newActivityStartInput",
        "newActivityEndInput",
        "newActivityMembersInput",
        "newActivityNotesInput",
    ].forEach((id) => {
        const element = document.getElementById(id);
        if (element) {
            element.value = "";
        }
    });
    syncNewActivitySubmitState();
}

function syncNewActivitySubmitState() {
    const labelInput = document.getElementById("newActivityLabelInput");
    const saveBtn = document.getElementById("newActivitySaveBtn");
    if (!saveBtn) {
        return;
    }
    if (newActivitySaving) {
        saveBtn.disabled = true;
        return;
    }
    saveBtn.disabled = false;
}

function collectNewActivityPayload() {
    const labelInput = document.getElementById("newActivityLabelInput");
    const idInput = document.getElementById("newActivityIdInput");
    const startInput = document.getElementById("newActivityStartInput");
    const endInput = document.getElementById("newActivityEndInput");
    const membersInput = document.getElementById("newActivityMembersInput");
    const notesInput = document.getElementById("newActivityNotesInput");

    const payload = {
        label: labelInput ? labelInput.value.trim() : "",
    };
    if (idInput && idInput.value.trim()) {
        payload.activity_id = idInput.value.trim();
    }
    if (startInput && startInput.value) {
        payload.plan_start = startInput.value;
    }
    if (endInput && endInput.value) {
        payload.plan_end = endInput.value;
    }
    if (membersInput && membersInput.value !== "") {
        const parsed = parseInt(membersInput.value, 10);
        if (!Number.isNaN(parsed)) {
            payload.planned_members = parsed;
        }
    }
    if (notesInput && notesInput.value.trim()) {
        payload.notes = notesInput.value.trim();
    }
    return payload;
}

function openNewActivityModal() {
    const modal = document.getElementById("newActivityModal");
    if (!modal) {
        return false;
    }
    resetNewActivityForm();
    modal.style.display = "flex";
    newActivityModalOpen = true;
    markBodyModalOpen();
    const toolbar = document.getElementById("selectionToolbar");
    newActivityToolbarWasVisible = false;
    if (toolbar) {
        newActivityToolbarWasVisible = !toolbar.classList.contains("hidden");
        toolbar.dataset.modalSuppressed = "true";
        toolbar.classList.add("hidden");
        syncSelectionToolbarOffset(toolbar);
    }
    setTimeout(() => {
        const labelInput = document.getElementById("newActivityLabelInput");
        if (labelInput) {
            labelInput.focus();
        }
    }, 80);
    return true;
}

function closeNewActivityModal() {
    const modal = document.getElementById("newActivityModal");
    if (!modal) {
        newActivityModalOpen = false;
        return;
    }
    if (!newActivityModalOpen) {
        modal.style.display = "none";
        return;
    }
    modal.style.display = "none";
    newActivityModalOpen = false;
    resetNewActivityForm();
    releaseBodyModalState();
    const toolbar = document.getElementById("selectionToolbar");
    if (toolbar && toolbar.dataset.modalSuppressed) {
        delete toolbar.dataset.modalSuppressed;
        if (newActivityToolbarWasVisible) {
            toolbar.classList.remove("hidden");
        }
        syncSelectionToolbarOffset(toolbar);
    }
    newActivityToolbarWasVisible = false;
}

async function submitNewActivity(event) {
    if (event && typeof event.preventDefault === "function") {
        event.preventDefault();
    }
    if (newActivitySaving) {
        return;
    }
    const payload = collectNewActivityPayload();
    if (!payload.label) {
        showPopup("⚠️ Inserisci il nome dell'attività");
        return;
    }
    const saveBtn = document.getElementById("newActivitySaveBtn");
    if (saveBtn && !saveBtn.dataset.label) {
        saveBtn.dataset.label = saveBtn.textContent || "Crea attività";
    }
    newActivitySaving = true;
    updateTeamAddActivityButtonState();
    if (saveBtn) {
        saveBtn.disabled = true;
        saveBtn.textContent = "Creazione...";
    }
    try {
        const hadSelection = getSelectedMemberNodes().length > 0;
        const result = await postJson("/api/activities", payload);
        const created = result && result.activity ? result.activity : null;
        const createdLabel = created && created.label ? created.label : payload.label;
        showPopup(`🆕 Attività "${createdLabel}" creata`);
        closeNewActivityModal();
        let refreshFailed = false;
        try {
            await refreshState();
        } catch (refreshErr) {
            refreshFailed = true;
            console.error("refreshState after create", refreshErr);
            showPopup("⚠️ Attività creata, ricarica se non la vedi");
        }
        if (!refreshFailed && hadSelection) {
            activitySearchTerm = createdLabel || "";
            openActivityModal();
        }
    } catch (err) {
        console.error("submitNewActivity", err);
        const status = err && err.status;
        if (status === 409) {
            showPopup("⚠️ Codice attività già in uso o progetto non disponibile");
        } else if (status === 400) {
            showPopup("⚠️ Dati attività non validi");
        } else {
            showPopup("⚠️ Impossibile creare l'attività");
        }
    } finally {
        newActivitySaving = false;
        updateTeamAddActivityButtonState();
        if (saveBtn) {
            saveBtn.textContent = saveBtn.dataset.label || "Crea attività";
            syncNewActivitySubmitState();
        }
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
        } else if (res.status === 409) {
            let payload = null;
            try {
                payload = await res.json();
            } catch (parseError) {
                console.warn("loadProject payload", parseError);
            }
            const warning =
                (payload && (payload.message || payload.error)) ||
                "⚠️ Impossibile ricaricare: attività in corso";
            showPopup(warning);
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
    const refreshPushNotificationsBtn = document.getElementById("refreshPushNotificationsBtn");
    const notificationsBtn = document.getElementById("notificationsBtn");
    const closePushNotificationsBtn = document.getElementById("closePushNotificationsBtn");
    const pushNotificationsModal = document.getElementById("pushNotificationsModal");
    const teamSelectBtn = document.getElementById("teamSelectBtn");
    const teamAddActivityBtn = document.getElementById("teamAddActivityBtn");
    const teamCollapseBtn = document.getElementById("teamCollapseBtn");
    const newActivityModal = document.getElementById("newActivityModal");
    const newActivityForm = document.getElementById("newActivityForm");
    const newActivityCancelBtn = document.getElementById("newActivityCancelBtn");
    const newActivityLabelInput = document.getElementById("newActivityLabelInput");
    const newActivityIdInput = document.getElementById("newActivityIdInput");
    const newActivityStartInput = document.getElementById("newActivityStartInput");
    const newActivityEndInput = document.getElementById("newActivityEndInput");
    const newActivityMembersInput = document.getElementById("newActivityMembersInput");
    const newActivityNotesInput = document.getElementById("newActivityNotesInput");
    const attachmentsMenuBtn = document.getElementById("attachmentsMenuBtn");
    const attachmentsRefreshBtn = document.getElementById("attachmentsRefreshBtn");
    const attachmentsUploadBtn = document.getElementById("attachmentsUploadBtn");
    const attachmentsCloseBtn = document.getElementById("attachmentsCloseBtn");
    const attachmentsModal = document.getElementById("attachmentsModal");
    const materialsRefreshBtn = document.getElementById("materialsRefreshBtn");
    const materialsMenuBtn = document.getElementById("materialsMenuBtn");
    const materialsCloseBtn = document.getElementById("materialsCloseBtn");
    const materialsCloseBottomBtn = document.getElementById("materialsCloseBottomBtn");
    const materialsScrollTopBtn = document.getElementById("materialsScrollTopBtn");
    const materialsModal = document.getElementById("materialsModal");
    const materialsModalCard = document.getElementById("materialsModalCard");
    const equipmentMenuBtn = document.getElementById("equipmentMenuBtn");
    const equipmentOpenBtn = document.getElementById("equipmentOpenBtn");
    const equipmentRefreshBtn = document.getElementById("equipmentRefreshBtn");
    const equipmentCloseBtn = document.getElementById("equipmentCloseBtn");
    const equipmentCloseBottomBtn = document.getElementById("equipmentCloseBottomBtn");
    const equipmentScrollTopBtn = document.getElementById("equipmentScrollTopBtn");
    const equipmentModal = document.getElementById("equipmentModal");
    const equipmentModalCard = document.getElementById("equipmentModalCard");
    const materialPhotoCloseBtn = document.getElementById("materialPhotoCloseBtn");
    const materialPhotoModal = document.getElementById("materialPhotoModal");
    const photosMenuBtn = document.getElementById("photosMenuBtn");
    const photosRefreshBtn = document.getElementById("photosRefreshBtn");
    const photosCloseBtn = document.getElementById("photosCloseBtn");
    const photosModal = document.getElementById("photosModal");
    const photosFileInput = document.getElementById("photosFileInput");
    const photosCameraBtn = document.getElementById("photosCameraBtn");
    const photosCameraInput = document.getElementById("photosCameraInput");
    const photoPreviewModal = document.getElementById("photoPreviewModal");
    const photoPreviewCloseBtn = document.getElementById("photoPreviewCloseBtn");
    const photoDeleteBtn = document.getElementById("photoDeleteBtn");

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

    // QR Timbratura handlers
    const qrTimbraturaMenuBtn = document.getElementById("qrTimbraturaMenuBtn");
    const qrTimbraturaModal = document.getElementById("qrTimbraturaModal");
    const qrTimbraturaCloseBtn = document.getElementById("qrTimbraturaCloseBtn");
    
    if (qrTimbraturaMenuBtn) {
        qrTimbraturaMenuBtn.addEventListener("click", () => {
            closeMenu();
            openQrTimbraturaModal();
        });
    }
    
    if (qrTimbraturaCloseBtn) {
        qrTimbraturaCloseBtn.addEventListener("click", closeQrTimbraturaModal);
    }
    
    if (qrTimbraturaModal) {
        qrTimbraturaModal.addEventListener("click", (e) => {
            if (e.target === qrTimbraturaModal) closeQrTimbraturaModal();
        });
    }

    if (attachmentsMenuBtn) {
        attachmentsMenuBtn.addEventListener("click", () => {
            closeMenu();
            openAttachmentsModal({ forceToast: false });
        });
    }

    if (materialsMenuBtn) {
        materialsMenuBtn.addEventListener("click", () => {
            closeMenu();
            openMaterialsModal({ forceToast: false });
        });
    }

    if (equipmentMenuBtn) {
        equipmentMenuBtn.addEventListener("click", () => {
            closeMenu();
            openEquipmentModal({ forceToast: false });
        });
    }

    if (attachmentsRefreshBtn) {
        attachmentsRefreshBtn.dataset.label = attachmentsRefreshBtn.textContent || "Aggiorna";
        attachmentsRefreshBtn.addEventListener("click", () => fetchProjectAttachments({ silent: false }));
    }

    if (materialsRefreshBtn) {
        materialsRefreshBtn.dataset.label = materialsRefreshBtn.textContent || "Aggiorna materiali";
        materialsRefreshBtn.addEventListener("click", () => fetchProjectMaterials({ silent: false, refresh: true }));
    }

    if (equipmentRefreshBtn) {
        equipmentRefreshBtn.dataset.label = equipmentRefreshBtn.textContent || "Aggiorna attrezzature";
        equipmentRefreshBtn.addEventListener("click", () => fetchProjectMaterials({ silent: false, refresh: true }));
    }

    if (attachmentsUploadBtn) {
        attachmentsUploadBtn.addEventListener("click", handleAttachmentUpload);
    }

    if (attachmentsCloseBtn) {
        attachmentsCloseBtn.addEventListener("click", closeAttachmentsModal);
    }

    if (materialsCloseBtn) {
        materialsCloseBtn.addEventListener("click", closeMaterialsModal);
    }

    if (materialsCloseBottomBtn) {
        materialsCloseBottomBtn.addEventListener("click", closeMaterialsModal);
    }

    if (materialsScrollTopBtn && materialsModalCard) {
        materialsScrollTopBtn.addEventListener("click", () => {
            materialsModalCard.scrollTo({ top: 0, behavior: "smooth" });
        });
    }

    if (equipmentOpenBtn) {
        equipmentOpenBtn.addEventListener("click", () => {
            openEquipmentModal({ source: "materials" });
        });
    }

    if (equipmentCloseBtn) {
        equipmentCloseBtn.addEventListener("click", closeEquipmentModal);
    }

    if (equipmentCloseBottomBtn) {
        equipmentCloseBottomBtn.addEventListener("click", closeEquipmentModal);
    }

    if (equipmentScrollTopBtn && equipmentModalCard) {
        equipmentScrollTopBtn.addEventListener("click", () => {
            equipmentModalCard.scrollTo({ top: 0, behavior: "smooth" });
        });
    }

    const equipmentAddBtn = document.getElementById("equipmentAddBtn");
    const newEquipmentModal = document.getElementById("newEquipmentModal");
    const newEquipmentForm = document.getElementById("newEquipmentForm");
    const newEquipmentCloseBtn = document.getElementById("newEquipmentCloseBtn");
    const newEquipmentCancelBtn = document.getElementById("newEquipmentCancelBtn");

    function openNewEquipmentModal() {
        if (!newEquipmentModal) return;
        newEquipmentModal.classList.add("open");
        const nameInput = document.getElementById("newEquipmentName");
        if (nameInput) nameInput.focus();
    }

    function closeNewEquipmentModal() {
        if (!newEquipmentModal) return;
        newEquipmentModal.classList.remove("open");
        if (newEquipmentForm) newEquipmentForm.reset();
    }

    if (equipmentAddBtn) {
        equipmentAddBtn.addEventListener("click", openNewEquipmentModal);
    }

    if (newEquipmentCloseBtn) {
        newEquipmentCloseBtn.addEventListener("click", closeNewEquipmentModal);
    }

    if (newEquipmentCancelBtn) {
        newEquipmentCancelBtn.addEventListener("click", closeNewEquipmentModal);
    }

    if (newEquipmentModal) {
        newEquipmentModal.addEventListener("click", (event) => {
            if (event.target === newEquipmentModal) {
                closeNewEquipmentModal();
            }
        });
    }

    if (newEquipmentForm) {
        newEquipmentForm.addEventListener("submit", async (event) => {
            event.preventDefault();
            const nameInput = document.getElementById("newEquipmentName");
            const quantityInput = document.getElementById("newEquipmentQuantity");
            const groupInput = document.getElementById("newEquipmentGroup");
            const notesInput = document.getElementById("newEquipmentNotes");

            const name = (nameInput && nameInput.value || "").trim();
            if (!name) {
                showPopup("⚠️ Inserisci il nome dell'attrezzatura");
                return;
            }

            const quantity = parseInt(quantityInput && quantityInput.value, 10) || 1;
            const groupName = (groupInput && groupInput.value || "").trim() || "Attrezzature extra";
            const notes = (notesInput && notesInput.value || "").trim();

            try {
                const response = await fetch("/api/project/local-equipment", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ name, quantity, group_name: groupName, notes }),
                });
                const data = await response.json();
                if (!response.ok || !data.ok) {
                    showPopup("⚠️ " + (data.error || "Errore durante l'aggiunta"));
                    return;
                }
                showPopup(`✅ Attrezzatura "${name}" aggiunta`);
                closeNewEquipmentModal();
                fetchProjectMaterials({ silent: true, refresh: true });
            } catch (err) {
                console.error("Errore aggiunta attrezzatura locale:", err);
                showPopup("⚠️ Errore di rete");
            }
        });
    }

    if (materialPhotoCloseBtn) {
        materialPhotoCloseBtn.addEventListener("click", closeMaterialPhotoPreview);
    }

    if (materialPhotoModal) {
        materialPhotoModal.addEventListener("click", (event) => {
            if (event.target === materialPhotoModal) {
                closeMaterialPhotoPreview();
            }
        });
    }

    // Foto progetto handlers
    if (photosMenuBtn) {
        photosMenuBtn.addEventListener("click", () => {
            closeMenu();
            openPhotosModal();
        });
    }

    if (photosRefreshBtn) {
        photosRefreshBtn.addEventListener("click", () => fetchPhotos({ silent: false }));
    }

    if (photosCloseBtn) {
        photosCloseBtn.addEventListener("click", closePhotosModal);
    }

    if (photosModal) {
        photosModal.addEventListener("click", (event) => {
            if (event.target === photosModal) {
                closePhotosModal();
            }
        });
    }

    if (photosFileInput) {
        photosFileInput.addEventListener("change", (event) => {
            const files = event.target.files;
            if (files && files.length > 0) {
                Array.from(files).forEach((file) => uploadPhoto(file));
            }
            photosFileInput.value = "";
        });
    }

    // Pulsante fotocamera - usa API MediaDevices
    if (photosCameraBtn) {
        photosCameraBtn.addEventListener("click", openCameraModal);
    }

    // Fallback: input file con capture (usato se API MediaDevices fallisce)
    if (photosCameraInput) {
        photosCameraInput.addEventListener("change", (event) => {
            const files = event.target.files;
            if (files && files.length > 0) {
                uploadPhoto(files[0]);
            }
            photosCameraInput.value = "";
        });
    }

    // Camera modal buttons
    const cameraCloseBtn = document.getElementById("cameraCloseBtn");
    const cameraCaptureBtn = document.getElementById("cameraCaptureBtn");
    const cameraModal = document.getElementById("cameraModal");

    if (cameraCloseBtn) {
        cameraCloseBtn.addEventListener("click", closeCameraModal);
    }

    if (cameraCaptureBtn) {
        cameraCaptureBtn.addEventListener("click", capturePhoto);
    }

    if (cameraModal) {
        cameraModal.addEventListener("click", (event) => {
            if (event.target === cameraModal) {
                closeCameraModal();
            }
        });
    }

    if (photoPreviewCloseBtn) {
        photoPreviewCloseBtn.addEventListener("click", closePhotoPreview);
    }

    if (photoDeleteBtn) {
        photoDeleteBtn.addEventListener("click", deleteCurrentPhoto);
    }

    if (photoPreviewModal) {
        photoPreviewModal.addEventListener("click", (event) => {
            if (event.target === photoPreviewModal) {
                closePhotoPreview();
            }
        });
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

    document.addEventListener("click", (event) => {
        const trigger = event.target.closest("[data-open-new-activity]");
        if (!trigger) {
            return;
        }
        handleTeamAddActivityClick(event);
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

    if (attachmentsModal) {
        attachmentsModal.addEventListener("click", (event) => {
            if (event.target === attachmentsModal) {
                closeAttachmentsModal();
            }
        });
    }

    if (materialsModal) {
        materialsModal.addEventListener("click", (event) => {
            if (event.target === materialsModal) {
                closeMaterialsModal();
            }
        });
    }

    if (equipmentModal) {
        equipmentModal.addEventListener("click", (event) => {
            if (event.target === equipmentModal) {
                closeEquipmentModal();
            }
        });
    }

    if (newActivityModal) {
        newActivityModal.addEventListener("click", (event) => {
            if (event.target === newActivityModal && !newActivitySaving) {
                closeNewActivityModal();
            }
        });
    }

    if (newActivityForm) {
        newActivityForm.addEventListener("submit", submitNewActivity);
    }

    [
        newActivityLabelInput,
        newActivityIdInput,
        newActivityStartInput,
        newActivityEndInput,
        newActivityMembersInput,
        newActivityNotesInput,
    ]
        .filter(Boolean)
        .forEach((input) => {
            input.addEventListener("input", syncNewActivitySubmitState);
        });

    if (newActivityCancelBtn) {
        newActivityCancelBtn.addEventListener("click", () => {
            if (!newActivitySaving) {
                closeNewActivityModal();
            }
        });
    }

    if (teamSelectBtn) {
        teamSelectBtn.addEventListener("click", toggleTeamSelection);
    }

    if (teamAddActivityBtn) {
        teamAddActivityBtn.addEventListener("click", handleTeamAddActivityClick);
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
            if (!newActivitySaving) {
                closeNewActivityModal();
            }
            closeTimeline();
            closeMenu();
            closeExportModal();
            closePushNotificationsModal();
            closeAttachmentsModal();
            closeMaterialsModal();
            closeEquipmentModal();
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
    forceCloseOverlays();
    setTeamCollapsed(true);
    setProjectDefaultDate();
    hydrateInitialContentFromCache();
    renderAttachments();
    renderMaterials();
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
    fetchProjectMaterials({ silent: true, refresh: false });
}

window.addEventListener('online', handleOnlineEvent);
window.addEventListener('offline', handleOfflineEvent);
document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') {
        forceCloseOverlays();
    }
});
window.addEventListener('pageshow', () => {
    forceCloseOverlays();
});
window.addEventListener("DOMContentLoaded", init);
window.addEventListener("resize", () => {
    syncSelectionToolbarOffset();
});
