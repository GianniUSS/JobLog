/**
 * Admin Update System
 * Fornisce il sistema di notifica e aggiornamento della PWA per le pagine admin
 */

(function initAdminUpdateSystem() {
    const updateBanner = document.getElementById('updateBanner');
    const refreshAppBtn = document.getElementById('refreshAppBtn');
    const newVersionDisplay = document.getElementById('newVersionDisplay');
    let pendingServiceWorker = null;
    let reloadOnControllerChange = false;

    function showUpdateAvailable() {
        if (!updateBanner) return;
        updateBanner.classList.remove('hidden');
        updateBanner.setAttribute('aria-hidden', 'false');
        // Mostra il numero di versione
        if (newVersionDisplay && window.APP_VERSION) {
            newVersionDisplay.textContent = `(${window.APP_VERSION})`;
        }
    }

    function hideUpdateBanner() {
        if (!updateBanner) return;
        updateBanner.classList.add('hidden');
        updateBanner.setAttribute('aria-hidden', 'true');
    }

    if (refreshAppBtn) {
        refreshAppBtn.addEventListener('click', () => {
            hideUpdateBanner();
            if (pendingServiceWorker) {
                reloadOnControllerChange = true;
                pendingServiceWorker.postMessage({ type: 'claim-clients' });
                pendingServiceWorker = null;
            } else {
                window.location.reload();
            }
        });
    }

    if ('serviceWorker' in navigator) {
        navigator.serviceWorker.addEventListener('controllerchange', () => {
            if (reloadOnControllerChange) {
                reloadOnControllerChange = false;
                window.location.reload();
            }
        });
    }

    function monitorServiceWorkerUpdates(registration) {
        if (!registration || !navigator.serviceWorker.controller) {
            return;
        }

        const handleWaitingWorker = (worker) => {
            if (!worker || worker.state !== 'installed') {
                return;
            }
            pendingServiceWorker = worker;
            showUpdateAvailable();
        };

        if (registration.waiting) {
            handleWaitingWorker(registration.waiting);
        }

        const onUpdateFound = () => {
            const newWorker = registration.installing;
            if (!newWorker) {
                return;
            }
            newWorker.addEventListener('statechange', () => {
                if (newWorker.state === 'installed' && navigator.serviceWorker.controller) {
                    handleWaitingWorker(newWorker);
                }
            });
        };

        if (typeof registration.addEventListener === 'function') {
            registration.addEventListener('updatefound', onUpdateFound);
        } else {
            registration.onupdatefound = onUpdateFound;
        }
    }

    // Schedule periodic service worker checks
    const SW_UPDATE_INTERVAL_MS = 10000;
    let swUpdateIntervalId = null;

    function scheduleServiceWorkerUpdates(registration) {
        const runUpdateCheck = () => {
            if (!registration) {
                return;
            }
            registration.update().catch((error) => {
                console.warn('[SW] Errore aggiornamento:', error);
            });
        };
        runUpdateCheck();
        if (swUpdateIntervalId) {
            clearInterval(swUpdateIntervalId);
        }
        swUpdateIntervalId = setInterval(runUpdateCheck, SW_UPDATE_INTERVAL_MS);
    }

    if ('serviceWorker' in navigator) {
        window.addEventListener('load', () => {
            navigator.serviceWorker.register('/sw.js')
                .then((registration) => {
                    console.log('✓ Service Worker registrato:', registration.scope);
                    if (registration.active && navigator.serviceWorker.controller === null) {
                        registration.active.postMessage({ type: 'claim-clients' });
                    }
                    scheduleServiceWorkerUpdates(registration);
                    monitorServiceWorkerUpdates(registration);
                })
                .catch((error) => {
                    console.log('✗ Service Worker fallito:', error);
                });
        });
    }

    window.addEventListener('beforeunload', () => {
        if (swUpdateIntervalId) {
            clearInterval(swUpdateIntervalId);
        }
    });
})();
