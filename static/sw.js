const CACHE_NAME = 'joblog-v2026.01.06a';
const STATIC_CACHE = 'joblog-static-v27';
const DYNAMIC_CACHE = 'joblog-dynamic-v27';
const API_CACHE = 'joblog-api-v25';
const BG_SYNC_TAG = 'joblog-sync-queue';
const QUEUE_DB = 'joblog-sync-db';
const QUEUE_STORE = 'requests';
const OFFLINE_QUEUE_PATHS = [
    '/api/move',
    '/api/member/pause',
    '/api/member/resume',
    '/api/member/finish',
    '/api/start_member',
    '/api/start_activity',
    '/api/pause_all',
    '/api/resume_all',
    '/api/finish_all',
    '/api/timbratura',
];

const API_CACHE_PATHS = ['/api/state', '/api/events', '/api/push/notifications'];
const OFFLINE_FALLBACK = '/';

const STATIC_ASSETS = [
    '/',
    '/static/js/app.js',
    '/static/manifest.json',
    '/static/icons/icon-192x192.png',
    '/static/icons/icon-72x72.png',
];

// Install event - cache static assets
self.addEventListener('install', (event) => {
    console.log('[ServiceWorker] Installing...');
    event.waitUntil(
        caches.open(STATIC_CACHE)
            .then((cache) => {
                console.log('[ServiceWorker] Caching static assets');
                return cache.addAll(STATIC_ASSETS);
            })
            .then(async () => {
                //_F Allow immediate activation only on the very first install so updates can stay waiting.
                const hasActiveWorker = Boolean(self.registration && self.registration.active);
                if (!hasActiveWorker) {
                    await self.skipWaiting();
                }
            })
    );
});
//proviamo qui 
// opure qui
// Activate event - cleanup old caches   ++  aaaaa a AUTOAGGIORNAMENTO ++
self.addEventListener('activate', (event) => {
    console.log('[ServiceWorker] Activating...');
    event.waitUntil(
        Promise.all([
            caches.keys().then((cacheNames) => {
                return Promise.all(
                    cacheNames.map((cacheName) => {
                        const keep = [STATIC_CACHE, DYNAMIC_CACHE, API_CACHE];
                        if (!keep.includes(cacheName)) {
                            console.log('[ServiceWorker] Deleting old cache:', cacheName);
                            return caches.delete(cacheName);
                        }
                    })
                );
            }).then(() => self.clients.claim()),
            processQueue().catch((error) => {
                console.warn('[ServiceWorker] Impossibile processare la coda offline all\'attivazione', error);
            }),
        ])
    );
});

// Fetch event - network first, fallback to cache
self.addEventListener('fetch', (event) => {
    const { request } = event;
    const url = new URL(request.url);

    // Skip cross-origin requests
    if (url.origin !== location.origin) {
        return;
    }

    if (request.method === 'POST' && shouldQueueRequest(url.pathname)) {
        event.respondWith(handleMutatingRequest(event));
        return;
    }

    if (request.method !== 'GET') {
        return;
    }

    // App shell / navigation requests
    if (request.mode === 'navigate') {
        event.respondWith(handleNavigationRequest(request));
        return;
    }

    if (isApiCacheTarget(url.pathname)) {
        event.respondWith(handleApiRequest(request));
        return;
    }

    // Static assets - cache first, fallback to network
    if (url.pathname.startsWith('/static/')) {
        event.respondWith(
            caches.match(request).then((cachedResponse) => {
                if (cachedResponse) {
                    return cachedResponse;
                }
                return fetch(request).then((networkResponse) => {
                    return caches.open(DYNAMIC_CACHE).then((cache) => {
                        cache.put(request, networkResponse.clone());
                        return networkResponse;
                    });
                });
            })
        );
        return;
    }

    // Default: try network, fallback to cache
    event.respondWith(
        fetch(request)
            .then((response) => {
                return caches.open(DYNAMIC_CACHE).then((cache) => {
                    cache.put(request, response.clone());
                    return response;
                });
            })
            .catch(() => caches.match(request))
    );
});

self.addEventListener('sync', (event) => {
    if (event.tag === BG_SYNC_TAG) {
        event.waitUntil(processQueue());
    }
});

function isApiCacheTarget(pathname) {
    return API_CACHE_PATHS.some((path) => pathname.startsWith(path));
}

function handleNavigationRequest(request) {
    return fetch(request)
        .then((response) => {
            const copy = response.clone();
            caches.open(DYNAMIC_CACHE).then((cache) => cache.put(request, copy)).catch(() => {});
            return response;
        })
        .catch(() => caches.match(OFFLINE_FALLBACK));
}

function handleApiRequest(request) {
    return fetch(request)
        .then((response) => {
            const copy = response.clone();
            caches.open(API_CACHE).then((cache) => cache.put(request, copy)).catch(() => {});
            return response;
        })
        .catch(() => caches.match(request));
}

function shouldQueueRequest(pathname) {
    return OFFLINE_QUEUE_PATHS.some((path) => pathname.startsWith(path));
}

async function handleMutatingRequest(event) {
    const { request } = event;
    try {
        const response = await fetch(request.clone());
        return response;
    } catch (error) {
        const queueId = await queueRequest(request);
        let pathname = request.url;
        try {
            pathname = new URL(request.url).pathname;
        } catch (err) {
            // noop
        }
        await notifyOfflineQueue({ action: 'queued', id: queueId, url: request.url, pathname });
        await scheduleQueueSync();
        return buildQueuedResponse(queueId);
    }
}

function buildQueuedResponse(queueId) {
    return new Response(
        JSON.stringify({ queued: true, id: queueId }),
        {
            status: 202,
            headers: {
                'Content-Type': 'application/json',
                'X-JobLog-Queued': '1',
            },
        }
    );
}

async function queueRequest(request) {
    const serialized = await serializeRequest(request);
    return addQueueEntry(serialized);
}

async function serializeRequest(request) {
    const headers = {};
    for (const [key, value] of request.headers.entries()) {
        headers[key] = value;
    }
    let body = null;
    if (request.method !== 'GET' && request.method !== 'HEAD') {
        body = await request.clone().text();
    }
    let pathname = request.url;
    try {
        pathname = new URL(request.url).pathname;
    } catch (error) {
        // noop
    }
    return {
        url: request.url,
        pathname,
        method: request.method,
        headers,
        body,
        timestamp: Date.now(),
    };
}

async function addQueueEntry(entry) {
    const db = await openQueueDb();
    try {
        return await new Promise((resolve, reject) => {
            const tx = db.transaction(QUEUE_STORE, 'readwrite');
            const store = tx.objectStore(QUEUE_STORE);
            const addReq = store.add(entry);
            addReq.onsuccess = () => resolve(addReq.result);
            addReq.onerror = () => reject(addReq.error);
        });
    } finally {
        db.close();
    }
}

function openQueueDb() {
    return new Promise((resolve, reject) => {
        const request = indexedDB.open(QUEUE_DB, 1);
        request.onupgradeneeded = () => {
            const db = request.result;
            if (!db.objectStoreNames.contains(QUEUE_STORE)) {
                db.createObjectStore(QUEUE_STORE, { keyPath: 'id', autoIncrement: true });
            }
        };
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
    });
}

async function getAllQueueEntries() {
    const db = await openQueueDb();
    try {
        return await new Promise((resolve, reject) => {
            const tx = db.transaction(QUEUE_STORE, 'readonly');
            const store = tx.objectStore(QUEUE_STORE);
            const getReq = store.getAll();
            getReq.onsuccess = () => resolve(getReq.result || []);
            getReq.onerror = () => reject(getReq.error);
        });
    } finally {
        db.close();
    }
}

async function deleteQueueEntry(id) {
    const db = await openQueueDb();
    try {
        await new Promise((resolve, reject) => {
            const tx = db.transaction(QUEUE_STORE, 'readwrite');
            const store = tx.objectStore(QUEUE_STORE);
            const delReq = store.delete(id);
            delReq.onsuccess = () => resolve();
            delReq.onerror = () => reject(delReq.error);
        });
    } finally {
        db.close();
    }
}

async function scheduleQueueSync() {
    if (self.registration && 'sync' in self.registration) {
        try {
            await self.registration.sync.register(BG_SYNC_TAG);
            return;
        } catch (error) {
            console.warn('[ServiceWorker] sync.register fallita', error);
        }
    }
    await processQueue();
}

async function processQueue() {
    const entries = await getAllQueueEntries();
    if (!entries.length) {
        return;
    }
    entries.sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
    for (const entry of entries) {
        try {
            const response = await replayQueuedRequest(entry);
            // Rimuovi dalla coda anche se 400 (es. "già registrato") - non è un errore di rete
            if (response && (response.ok || response.status === 400)) {
                await deleteQueueEntry(entry.id);
                if (response.ok) {
                    await notifyOfflineQueue({ action: 'delivered', id: entry.id, url: entry.url, pathname: entry.pathname });
                } else {
                    // 400 = richiesta già processata o non valida, rimuovi silenziosamente
                    console.log('[ServiceWorker] Richiesta rimossa (400):', entry.pathname);
                }
            } else if (!response) {
                throw new Error('No response');
            } else {
                throw new Error(`HTTP ${response.status}`);
            }
        } catch (error) {
            console.warn('[ServiceWorker] Errore durante la ripetizione della richiesta', error);
            await notifyOfflineQueue({
                action: 'error',
                id: entry.id,
                url: entry.url,
                pathname: entry.pathname,
                error: error && error.message ? error.message : String(error),
            });
            // Non rilanciare l'errore, continua con le altre richieste
        }
    }
    await notifyOfflineQueue({ action: 'idle' });
}

function replayQueuedRequest(entry) {
    const headers = entry.headers || {};
    return fetch(entry.url, {
        method: entry.method || 'POST',
        headers,
        body: entry.body,
        credentials: 'include',
    });
}

async function notifyOfflineQueue(meta) {
    const clientList = await clients.matchAll({ type: 'window', includeUncontrolled: true });
    const message = { type: 'offline-queue', ...meta };
    clientList.forEach((client) => client.postMessage(message));
}

// Handle background sync (optional - for future offline capabilities)
self.addEventListener('sync', (event) => {
    if (event.tag === 'sync-data') {
        console.log('[ServiceWorker] Background sync triggered');
        // Future: sync pending operations when back online
    }
});

self.addEventListener('push', (event) => {
    const payload = parsePushPayload(event);
    event.waitUntil(handlePushNotification(payload));
});

function parsePushPayload(event) {
    const fallback = { title: 'JobLog', body: 'Nuova notifica', data: {} };
    if (!event.data) {
        return fallback;
    }
    try {
        const parsed = event.data.json();
        if (parsed && typeof parsed === 'object') {
            return {
                title: parsed.title || fallback.title,
                body: parsed.body || fallback.body,
                data: parsed.data || {},
            };
        }
    } catch (err) {
        console.warn('[ServiceWorker] Impossibile leggere il payload push', err);
    }
    return fallback;
}

async function handlePushNotification(payload) {
    console.log('[ServiceWorker] Push ricevuto', payload);
    const options = {
        body: payload.body,
        icon: '/static/icons/icon-192x192.svg',
        badge: '/static/icons/icon-72x72.svg',
        vibrate: [200, 100, 200],
        timestamp: Date.now(),
        data: payload.data || {},
        requireInteraction: true,
    };

    const result = {
        permission: typeof Notification === 'undefined' ? 'unsupported' : Notification.permission,
        delivered: false,
    };

    if (typeof Notification === 'undefined') {
        console.warn('[ServiceWorker] Notification API non disponibile');
        await broadcastPushToClients(payload, result);
        return;
    }

    if (Notification.permission !== 'granted') {
        console.warn('[ServiceWorker] Permesso notifiche:', Notification.permission);
        await broadcastPushToClients(payload, result);
        return;
    }

    try {
        await self.registration.showNotification(payload.title, options);
        result.delivered = true;
    } catch (error) {
        console.error('[ServiceWorker] showNotification fallita', error);
        result.error = error && error.message ? error.message : String(error);
    }

    await broadcastPushToClients(payload, result);
}

async function broadcastPushToClients(payload, meta) {
    const clientList = await clients.matchAll({ type: 'window', includeUncontrolled: true });
    const message = {
        type: 'push-notification',
        payload,
        meta: meta || {},
    };
    clientList.forEach((client) => {
        client.postMessage(message);
    });
}

self.addEventListener('notificationclick', (event) => {
    event.notification.close();
    
    // Determina l'URL di destinazione in base ai dati della notifica
    let targetUrl = '/';
    const notificationData = event.notification.data || {};
    
    if (notificationData.url) {
        targetUrl = notificationData.url;
    } else if (notificationData.type === 'turni_published') {
        targetUrl = '/turni';
    }
    
    event.waitUntil(
        clients.matchAll({ type: 'window', includeUncontrolled: true }).then((windowClients) => {
            // Cerca una finestra esistente con lo stesso URL
            for (const client of windowClients) {
                const clientUrl = new URL(client.url);
                if (clientUrl.pathname === targetUrl && 'focus' in client) {
                    client.postMessage({ type: 'push-notification-click', data: notificationData });
                    return client.focus();
                }
            }
            // Se non trovata, cerca qualsiasi finestra aperta
            for (const client of windowClients) {
                if ('focus' in client) {
                    client.postMessage({ type: 'push-notification-click', data: notificationData });
                    // Naviga alla URL corretta
                    client.navigate(targetUrl);
                    return client.focus();
                }
            }
            // Se nessuna finestra aperta, aprine una nuova
            if (clients.openWindow) {
                return clients.openWindow(targetUrl);
            }
            return undefined;
        })
    );
});

self.addEventListener('message', (event) => {
    if (!event.data || typeof event.data.type !== 'string') {
        return;
    }
    if (event.data.type === 'claim-clients') {
        event.waitUntil(
            self.skipWaiting().then(() => clients.claim())
        );
    } else if (event.data.type === 'flush-offline-queue') {
        event.waitUntil(processQueue());
    }
});
