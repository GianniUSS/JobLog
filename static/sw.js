const CACHE_NAME = 'joblog-v2025.11.14';
const STATIC_CACHE = 'joblog-static-v1';
const DYNAMIC_CACHE = 'joblog-dynamic-v1';

const STATIC_ASSETS = [
    '/',
    '/static/js/app.js',
    '/static/manifest.json',
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
            .then(() => self.skipWaiting())
    );
});

// Activate event - cleanup old caches
self.addEventListener('activate', (event) => {
    console.log('[ServiceWorker] Activating...');
    event.waitUntil(
        caches.keys().then((cacheNames) => {
            return Promise.all(
                cacheNames.map((cacheName) => {
                    if (cacheName !== STATIC_CACHE && cacheName !== DYNAMIC_CACHE) {
                        console.log('[ServiceWorker] Deleting old cache:', cacheName);
                        return caches.delete(cacheName);
                    }
                })
            );
        }).then(() => self.clients.claim())
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

    // API requests - network only (don't cache dynamic data)
    if (url.pathname.startsWith('/api/')) {
        event.respondWith(fetch(request));
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

    // HTML pages - network first, fallback to cache
    event.respondWith(
        fetch(request)
            .then((networkResponse) => {
                return caches.open(DYNAMIC_CACHE).then((cache) => {
                    cache.put(request, networkResponse.clone());
                    return networkResponse;
                });
            })
            .catch(() => {
                return caches.match(request);
            })
    );
});

// Handle background sync (optional - for future offline capabilities)
self.addEventListener('sync', (event) => {
    if (event.tag === 'sync-data') {
        console.log('[ServiceWorker] Background sync triggered');
        // Future: sync pending operations when back online
    }
});

// Handle push notifications (optional - for future)
self.addEventListener('push', (event) => {
    if (event.data) {
        const data = event.data.json();
        const options = {
            body: data.body || 'Nuova notifica da JobLog',
            icon: '/static/icons/icon-192x192.svg',
            badge: '/static/icons/icon-72x72.svg',
            vibrate: [200, 100, 200],
            data: data.data || {}
        };
        event.waitUntil(
            self.registration.showNotification(data.title || 'JobLog', options)
        );
    }
});
