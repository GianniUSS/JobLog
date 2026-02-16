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
    const DISMISSED_SW_KEY = 'joblog-dismissed-sw-version';

    function getSwVersionFromUrl(scriptUrl) {
        // Estrai un identificatore unico dal service worker URL
        if (!scriptUrl) return null;
        try {
            const url = new URL(scriptUrl);
            // Usa il parametro v= se presente, altrimenti usa l'hash dell'URL
            const vParam = url.searchParams.get('v');
            return vParam || url.pathname;
        } catch (e) {
            return scriptUrl;
        }
    }

    function showUpdateAvailable(swVersion) {
        if (!updateBanner) return;
        
        // Controlla se l'utente ha già dismissato questo aggiornamento
        const dismissedVersion = localStorage.getItem(DISMISSED_SW_KEY);
        if (dismissedVersion && dismissedVersion === swVersion) {
            console.log('[Update] Banner già dismissato per versione:', swVersion);
            return;
        }
        
        updateBanner.classList.remove('hidden');
        updateBanner.setAttribute('aria-hidden', 'false');
        // Salva la versione in attesa per riferimento
        updateBanner.dataset.swVersion = swVersion || '';
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
            // Pulisci il localStorage per questa versione quando l'utente aggiorna
            localStorage.removeItem(DISMISSED_SW_KEY);
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
    
    // Aggiungi pulsante per dismissare il banner (chiudere senza aggiornare)
    const dismissBtn = document.getElementById('dismissUpdateBtn');
    if (dismissBtn) {
        dismissBtn.addEventListener('click', () => {
            const swVersion = updateBanner?.dataset?.swVersion;
            if (swVersion) {
                localStorage.setItem(DISMISSED_SW_KEY, swVersion);
                console.log('[Update] Dismissato aggiornamento per versione:', swVersion);
            }
            hideUpdateBanner();
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

        const getCurrentScriptURL = () => {
            const ctrl = navigator.serviceWorker.controller || registration.active || registration.waiting;
            return ctrl ? ctrl.scriptURL : null;
        };

        const handleWaitingWorker = (worker) => {
            if (!worker || worker.state !== 'installed') {
                return;
            }
            const currentUrl = getCurrentScriptURL();
            // Evita banner se lo script è lo stesso (false positive)
            if (currentUrl && worker.scriptURL === currentUrl) {
                return;
            }
            pendingServiceWorker = worker;
            // Passa l'identificatore della versione del SW in waiting
            const swVersion = getSwVersionFromUrl(worker.scriptURL);
            showUpdateAvailable(swVersion);
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
    // Poll meno aggressivo per evitare falsi positivi: 15 minuti
    const SW_UPDATE_INTERVAL_MS = 15 * 60 * 1000;
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
            navigator.serviceWorker.register('/sw.js?v=2026.01.08e')
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
