/* =====================================================================
   admin_external_resources.js
   Vanilla JS logic for the External Resources admin page.
   ===================================================================== */

// --------------- State ---------------
let resources = [];
let currentResourceId = null;
let currentYear = new Date().getFullYear();
let currentMonth = new Date().getMonth() + 1; // 1-based

// --------------- Utility ---------------

function debounce(fn, delay) {
    let timer = null;
    return function () {
        const ctx = this;
        const args = arguments;
        clearTimeout(timer);
        timer = setTimeout(function () {
            fn.apply(ctx, args);
        }, delay);
    };
}

async function apiCall(url, method, body) {
    var opts = {
        method: method || 'GET',
        headers: { 'Content-Type': 'application/json' }
    };
    if (body !== undefined) {
        opts.body = JSON.stringify(body);
    }
    try {
        var resp = await fetch(url, opts);
        var data = await resp.json();
        if (!resp.ok) {
            throw new Error(data.error || data.message || ('HTTP ' + resp.status));
        }
        return data;
    } catch (err) {
        showToast(err.message || 'Errore di rete', 'error');
        throw err;
    }
}

function showToast(message, type) {
    var el = document.getElementById('toast');
    el.textContent = message;
    el.className = 'toast ' + (type || 'success');
    // Force reflow for re-trigger
    void el.offsetWidth;
    el.classList.add('show');
    setTimeout(function () {
        el.classList.remove('show');
    }, 3000);
}

function sanitizePhone(phone) {
    if (!phone) return '';
    return phone.replace(/[\s+\-()]/g, '');
}

function starHtml(rating) {
    var max = 5;
    var filled = Math.round(rating || 0);
    var html = '';
    for (var i = 1; i <= max; i++) {
        if (i <= filled) {
            html += '<span class="star filled">\u2605</span>';
        } else {
            html += '<span class="star">\u2606</span>';
        }
    }
    return html;
}

function escapeHtml(text) {
    if (!text) return '';
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(text));
    return div.innerHTML;
}

// --------------- List View ---------------

async function loadResources() {
    var params = new URLSearchParams();
    var q = document.getElementById('searchInput').value.trim();
    var type = document.getElementById('filterType').value;
    var category = document.getElementById('filterCategory').value;
    var rating = document.getElementById('filterRating').value;

    if (q) params.set('q', q);
    if (type) params.set('resource_type', type);
    if (category) params.set('category', category);
    if (rating) params.set('min_rating', rating);

    var url = '/api/admin/external-resources';
    var qs = params.toString();
    if (qs) url += '?' + qs;

    try {
        var data = await apiCall(url, 'GET');
        resources = data.resources || data.data || data || [];
        if (!Array.isArray(resources)) resources = [];
        renderResourceCards(resources);
    } catch (e) {
        document.getElementById('resourcesGrid').innerHTML =
            '<div class="empty-state"><div class="empty-state-icon">&#9888;</div>' +
            '<div class="empty-state-text">Errore nel caricamento</div></div>';
    }
}

function renderResourceCards(list) {
    var grid = document.getElementById('resourcesGrid');

    if (!list || list.length === 0) {
        grid.innerHTML =
            '<div class="empty-state">' +
            '<div class="empty-state-icon">&#128101;</div>' +
            '<div class="empty-state-text">Nessuna risorsa trovata</div></div>';
        return;
    }

    var html = '';
    for (var i = 0; i < list.length; i++) {
        var r = list[i];
        var phoneClean = sanitizePhone(r.phone);
        var waNumber = sanitizePhone(r.whatsapp || r.phone);

        html += '<div class="resource-card">';

        // Header
        html += '<div class="card-header">';
        html += '<div>';
        html += '<div class="card-name">' + escapeHtml(r.contact_name) + '</div>';
        if (r.company_name) {
            html += '<div class="card-company">' + escapeHtml(r.company_name) + '</div>';
        }
        html += '</div>';
        html += '<div class="card-rating">' + starHtml(r.rating) + '</div>';
        html += '</div>';

        // Meta
        html += '<div class="card-meta">';
        if (r.category) {
            html += '<span class="badge badge-category">' + escapeHtml(r.category) + '</span>';
        }
        if (r.resource_type) {
            html += '<span class="badge badge-type">' + escapeHtml(r.resource_type) + '</span>';
        }
        if (r.city) {
            html += '<span class="card-city">' + escapeHtml(r.city) + '</span>';
        }
        html += '</div>';

        // Phone display
        if (r.phone) {
            html += '<div class="card-phone">' + escapeHtml(r.phone) + '</div>';
        }

        // Actions
        html += '<div class="card-actions">';
        html += '<button class="action-btn edit" title="Modifica" onclick="openEditModal(' + r.id + ')">&#9998;</button>';
        html += '<button class="action-btn detail" title="Dettaglio" onclick="openDetail(' + r.id + ')">&#128196;</button>';
        if (r.phone) {
            html += '<a class="action-btn phone-btn" title="Chiama" href="tel:' + escapeHtml(r.phone) + '">&#128222;</a>';
        }
        if (waNumber) {
            html += '<a class="action-btn wa-btn" title="WhatsApp" href="https://wa.me/' + waNumber + '" target="_blank">&#128172;</a>';
        }
        if (r.email) {
            html += '<a class="action-btn email-btn" title="Email" href="mailto:' + escapeHtml(r.email) + '">&#9993;</a>';
        }
        html += '</div>';

        html += '</div>';
    }

    grid.innerHTML = html;
}

function applyFilters() {
    loadResources();
}

var searchResources = debounce(function () {
    loadResources();
}, 350);

// --------------- Create / Edit Modal ---------------

function resetForm() {
    document.getElementById('formResourceId').value = '';
    document.getElementById('formContactName').value = '';
    document.getElementById('formCompanyName').value = '';
    document.getElementById('formPhone').value = '';
    document.getElementById('formWhatsapp').value = '';
    document.getElementById('formEmail').value = '';
    document.getElementById('formCity').value = '';
    document.getElementById('formAddress').value = '';
    document.getElementById('formCategory').value = 'facchinaggio';
    document.getElementById('formVatNumber').value = '';
    document.getElementById('formHourlyRate').value = '';
    document.getElementById('formDailyRate').value = '';
    document.getElementById('formNotes').value = '';

    // Reset type radio to 'persona'
    var radios = document.querySelectorAll('input[name="resource_type"]');
    for (var i = 0; i < radios.length; i++) {
        radios[i].checked = (radios[i].value === 'persona');
    }

    // Reset rating to 3
    var ratingRadios = document.querySelectorAll('input[name="rating"]');
    for (var j = 0; j < ratingRadios.length; j++) {
        ratingRadios[j].checked = (ratingRadios[j].value === '3');
    }
}

function openCreateModal() {
    currentResourceId = null;
    resetForm();
    document.getElementById('formModalTitle').textContent = 'Nuova Risorsa';
    document.getElementById('formModal').classList.add('open');
}

async function openEditModal(id) {
    try {
        var data = await apiCall('/api/admin/external-resources/' + id, 'GET');
        var r = data.resource || data;
        currentResourceId = r.id;

        document.getElementById('formResourceId').value = r.id;
        document.getElementById('formContactName').value = r.contact_name || '';
        document.getElementById('formCompanyName').value = r.company_name || '';
        document.getElementById('formPhone').value = r.phone || '';
        document.getElementById('formWhatsapp').value = r.whatsapp || '';
        document.getElementById('formEmail').value = r.email || '';
        document.getElementById('formCity').value = r.city || '';
        document.getElementById('formAddress').value = r.address || '';
        document.getElementById('formCategory').value = r.category || 'facchinaggio';
        document.getElementById('formVatNumber').value = r.vat_number || '';
        document.getElementById('formHourlyRate').value = r.hourly_rate || '';
        document.getElementById('formDailyRate').value = r.daily_rate || '';
        document.getElementById('formNotes').value = r.notes || '';

        // Set type radio
        var radios = document.querySelectorAll('input[name="resource_type"]');
        for (var i = 0; i < radios.length; i++) {
            radios[i].checked = (radios[i].value === (r.resource_type || 'persona'));
        }

        // Set rating radio
        var ratingVal = String(r.rating || 3);
        var ratingRadios = document.querySelectorAll('input[name="rating"]');
        for (var j = 0; j < ratingRadios.length; j++) {
            ratingRadios[j].checked = (ratingRadios[j].value === ratingVal);
        }

        document.getElementById('formModalTitle').textContent = 'Modifica Risorsa';
        document.getElementById('formModal').classList.add('open');
    } catch (e) {
        // Error already shown by apiCall
    }
}

function closeFormModal() {
    document.getElementById('formModal').classList.remove('open');
    currentResourceId = null;
}

function getFormData() {
    var typeRadio = document.querySelector('input[name="resource_type"]:checked');
    var ratingRadio = document.querySelector('input[name="rating"]:checked');

    return {
        resource_type: typeRadio ? typeRadio.value : 'persona',
        contact_name: document.getElementById('formContactName').value.trim(),
        company_name: document.getElementById('formCompanyName').value.trim(),
        phone: document.getElementById('formPhone').value.trim(),
        whatsapp: document.getElementById('formWhatsapp').value.trim(),
        email: document.getElementById('formEmail').value.trim(),
        city: document.getElementById('formCity').value.trim(),
        address: document.getElementById('formAddress').value.trim(),
        category: document.getElementById('formCategory').value,
        vat_number: document.getElementById('formVatNumber').value.trim(),
        hourly_rate: parseFloat(document.getElementById('formHourlyRate').value) || null,
        daily_rate: parseFloat(document.getElementById('formDailyRate').value) || null,
        rating: ratingRadio ? parseInt(ratingRadio.value, 10) : 3,
        notes: document.getElementById('formNotes').value.trim()
    };
}

async function saveResource() {
    var body = getFormData();

    if (!body.contact_name) {
        showToast('Il nome contatto e obbligatorio', 'error');
        return;
    }

    try {
        if (currentResourceId) {
            await apiCall('/api/admin/external-resources/' + currentResourceId, 'PUT', body);
            showToast('Risorsa aggiornata', 'success');
        } else {
            await apiCall('/api/admin/external-resources', 'POST', body);
            showToast('Risorsa creata', 'success');
        }
        closeFormModal();
        loadResources();
    } catch (e) {
        // Error already shown by apiCall
    }
}

async function deleteResource(id) {
    if (!confirm('Sei sicuro di voler eliminare questa risorsa?')) return;

    try {
        await apiCall('/api/admin/external-resources/' + id, 'DELETE');
        showToast('Risorsa eliminata', 'success');
        closeDetailModal();
        loadResources();
    } catch (e) {
        // Error already shown by apiCall
    }
}

// --------------- Detail Modal ---------------

async function openDetail(id) {
    try {
        var data = await apiCall('/api/admin/external-resources/' + id, 'GET');
        var r = data.resource || data;
        currentResourceId = r.id;

        document.getElementById('detailTitle').textContent = r.contact_name || 'Dettaglio Risorsa';

        // Reset to Info tab
        switchTab('info');

        // Render all tabs
        renderInfo(r);
        renderSkills(r);

        // Init calendar month
        currentYear = new Date().getFullYear();
        currentMonth = new Date().getMonth() + 1;
        loadAvailability(currentYear, currentMonth);

        // Load available skills for the add-skill select
        loadAvailableSkills();

        document.getElementById('detailModal').classList.add('open');
    } catch (e) {
        // Error already shown by apiCall
    }
}

function closeDetailModal() {
    document.getElementById('detailModal').classList.remove('open');
    currentResourceId = null;
}

function switchTab(tabName, btnEl) {
    // Deactivate all tabs and content
    var tabs = document.querySelectorAll('.detail-tab');
    var contents = document.querySelectorAll('.tab-content');

    for (var i = 0; i < tabs.length; i++) {
        tabs[i].classList.remove('active');
        if (tabs[i].getAttribute('data-tab') === tabName) {
            tabs[i].classList.add('active');
        }
    }

    for (var j = 0; j < contents.length; j++) {
        contents[j].classList.remove('active');
    }

    // Activate the correct tab content
    var tabMap = {
        'info': 'tabInfo',
        'skills': 'tabSkills',
        'disponibilita': 'tabDisponibilita'
    };
    var targetId = tabMap[tabName];
    if (targetId) {
        document.getElementById(targetId).classList.add('active');
    }
}

// --------------- Info Tab ---------------

function renderInfo(r) {
    var grid = document.getElementById('infoGrid');

    var fields = [
        { label: 'Nome contatto', value: escapeHtml(r.contact_name) },
        { label: 'Azienda', value: escapeHtml(r.company_name) },
        { label: 'Tipo', value: escapeHtml(r.resource_type) },
        { label: 'Categoria', value: escapeHtml(r.category) },
        { label: 'Telefono', value: r.phone ? '<a href="tel:' + escapeHtml(r.phone) + '">' + escapeHtml(r.phone) + '</a>' : '-' },
        { label: 'WhatsApp', value: (r.whatsapp || r.phone) ? '<a href="https://wa.me/' + sanitizePhone(r.whatsapp || r.phone) + '" target="_blank">' + escapeHtml(r.whatsapp || r.phone) + '</a>' : '-' },
        { label: 'Email', value: r.email ? '<a href="mailto:' + escapeHtml(r.email) + '">' + escapeHtml(r.email) + '</a>' : '-' },
        { label: 'Citta', value: escapeHtml(r.city) || '-' },
        { label: 'Indirizzo', value: escapeHtml(r.address) || '-' },
        { label: 'P. IVA', value: escapeHtml(r.vat_number) || '-' },
        { label: 'Tariffa oraria', value: r.hourly_rate ? ('\u20AC ' + parseFloat(r.hourly_rate).toFixed(2)) : '-' },
        { label: 'Tariffa giornaliera', value: r.daily_rate ? ('\u20AC ' + parseFloat(r.daily_rate).toFixed(2)) : '-' },
        { label: 'Rating', value: '<div class="card-rating">' + starHtml(r.rating) + '</div>' },
        { label: 'Note', value: escapeHtml(r.notes) || '-' }
    ];

    var html = '';
    for (var i = 0; i < fields.length; i++) {
        html += '<div>';
        html += '<div class="info-label">' + fields[i].label + '</div>';
        html += '<div class="info-value">' + (fields[i].value || '-') + '</div>';
        html += '</div>';
    }

    // Add action buttons row spanning both columns
    html += '<div style="grid-column:1/-1; display:flex; gap:10px; margin-top:8px;">';
    html += '<button class="btn primary" onclick="openEditModal(' + r.id + '); closeDetailModal();">&#9998; Modifica</button>';
    html += '<button class="btn danger" onclick="deleteResource(' + r.id + ')">&#128465; Elimina</button>';
    html += '</div>';

    grid.innerHTML = html;
}

// --------------- Skills Tab ---------------

function renderSkills(r) {
    var list = document.getElementById('skillList');
    var skills = r.skills || [];

    if (skills.length === 0) {
        list.innerHTML = '<div class="empty-state" style="padding:20px;">' +
            '<div class="empty-state-text">Nessuna skill assegnata</div></div>';
        return;
    }

    var html = '';
    for (var i = 0; i < skills.length; i++) {
        var s = skills[i];
        html += '<div class="skill-item">';
        html += '<div>';
        html += '<div class="skill-name">' + escapeHtml(s.name || s.skill_name) + '</div>';
        html += '<div class="skill-level">Livello: ' + escapeHtml(s.level || '-') + '</div>';
        html += '</div>';
        html += '<button class="skill-remove" title="Rimuovi" onclick="removeSkill(' + (s.skill_id || s.id) + ')">&times;</button>';
        html += '</div>';
    }

    list.innerHTML = html;
}

async function loadAvailableSkills() {
    try {
        var data = await apiCall('/api/admin/skills', 'GET');
        var skills = data.skills || data.data || data || [];
        if (!Array.isArray(skills)) skills = [];

        var sel = document.getElementById('addSkillSelect');
        // Keep the first placeholder option
        sel.innerHTML = '<option value="">Seleziona skill...</option>';
        for (var i = 0; i < skills.length; i++) {
            var opt = document.createElement('option');
            opt.value = skills[i].id;
            opt.textContent = skills[i].name || skills[i].skill_name || ('Skill #' + skills[i].id);
            sel.appendChild(opt);
        }
    } catch (e) {
        // Error already shown by apiCall
    }
}

async function addSkill() {
    if (!currentResourceId) return;

    var skillId = document.getElementById('addSkillSelect').value;
    var level = document.getElementById('addSkillLevel').value;

    if (!skillId) {
        showToast('Seleziona una skill', 'error');
        return;
    }

    try {
        await apiCall('/api/admin/external-resources/' + currentResourceId + '/skills', 'POST', {
            skill_id: parseInt(skillId, 10),
            level: level,
            notes: ''
        });
        showToast('Skill aggiunta', 'success');

        // Reload resource detail to refresh skills
        var data = await apiCall('/api/admin/external-resources/' + currentResourceId, 'GET');
        var r = data.resource || data;
        renderSkills(r);

        // Reset select
        document.getElementById('addSkillSelect').value = '';
    } catch (e) {
        // Error already shown by apiCall
    }
}

async function removeSkill(skillId) {
    if (!currentResourceId) return;

    try {
        await apiCall('/api/admin/external-resources/' + currentResourceId + '/skills/' + skillId, 'DELETE');
        showToast('Skill rimossa', 'success');

        // Reload resource detail to refresh skills
        var data = await apiCall('/api/admin/external-resources/' + currentResourceId, 'GET');
        var r = data.resource || data;
        renderSkills(r);
    } catch (e) {
        // Error already shown by apiCall
    }
}

// --------------- Availability Tab ---------------

async function loadAvailability(year, month) {
    if (!currentResourceId) return;

    try {
        var data = await apiCall(
            '/api/admin/external-resources/' + currentResourceId +
            '/availability?year=' + year + '&month=' + month, 'GET'
        );
        var entries = data.availability || data.entries || data.data || [];
        if (!Array.isArray(entries)) entries = [];
        renderCalendar(year, month, entries);
    } catch (e) {
        renderCalendar(year, month, []);
    }
}

function renderCalendar(year, month, availability) {
    var monthNames = [
        'Gennaio', 'Febbraio', 'Marzo', 'Aprile', 'Maggio', 'Giugno',
        'Luglio', 'Agosto', 'Settembre', 'Ottobre', 'Novembre', 'Dicembre'
    ];
    var dayHeaders = ['Lun', 'Mar', 'Mer', 'Gio', 'Ven', 'Sab', 'Dom'];

    document.getElementById('calMonth').textContent = monthNames[month - 1] + ' ' + year;

    // Build availability lookup by date string YYYY-MM-DD
    var avMap = {};
    for (var a = 0; a < availability.length; a++) {
        var entry = availability[a];
        avMap[entry.date] = entry.status;
    }

    // First day of month (0=Sun, 1=Mon, ..., 6=Sat) - adjust to Monday-first
    var firstDate = new Date(year, month - 1, 1);
    var firstDow = firstDate.getDay(); // 0=Sun
    var startOffset = (firstDow === 0) ? 6 : (firstDow - 1); // Monday=0

    // Days in month
    var daysInMonth = new Date(year, month, 0).getDate();

    var html = '';

    // Day headers
    for (var h = 0; h < dayHeaders.length; h++) {
        html += '<div class="cal-head">' + dayHeaders[h] + '</div>';
    }

    // Empty cells before first day
    for (var e = 0; e < startOffset; e++) {
        html += '<div class="cal-day empty"></div>';
    }

    // Day cells
    for (var d = 1; d <= daysInMonth; d++) {
        var dateStr = year + '-' + String(month).padStart(2, '0') + '-' + String(d).padStart(2, '0');
        var status = avMap[dateStr] || '';
        var statusClass = status ? (' ' + status) : '';

        html += '<div class="cal-day' + statusClass + '" data-date="' + dateStr + '" data-status="' + status + '" onclick="toggleDayStatus(\'' + dateStr + '\', \'' + status + '\')">';
        html += d;
        html += '</div>';
    }

    // Fill remaining cells to complete the grid row
    var totalCells = startOffset + daysInMonth;
    var remainder = totalCells % 7;
    if (remainder > 0) {
        for (var f = 0; f < (7 - remainder); f++) {
            html += '<div class="cal-day empty"></div>';
        }
    }

    document.getElementById('calGrid').innerHTML = html;
}

function changeMonth(delta) {
    currentMonth += delta;
    if (currentMonth > 12) {
        currentMonth = 1;
        currentYear++;
    } else if (currentMonth < 1) {
        currentMonth = 12;
        currentYear--;
    }
    loadAvailability(currentYear, currentMonth);
}

async function toggleDayStatus(date, currentStatus) {
    if (!currentResourceId) return;

    // Cycle: '' -> available -> unavailable -> tentative -> available
    var cycle = {
        '': 'available',
        'available': 'unavailable',
        'unavailable': 'tentative',
        'tentative': 'available'
    };
    var newStatus = cycle[currentStatus] || 'available';

    try {
        await apiCall('/api/admin/external-resources/' + currentResourceId + '/availability', 'POST', {
            entries: [{ date: date, status: newStatus, notes: '' }]
        });
        loadAvailability(currentYear, currentMonth);
    } catch (e) {
        // Error already shown by apiCall
    }
}

// --------------- Event Listeners ---------------

document.addEventListener('DOMContentLoaded', function () {
    // Initial load
    loadResources();

    // Search debounced input
    var searchInput = document.getElementById('searchInput');
    if (searchInput) {
        searchInput.addEventListener('input', searchResources);
    }

    // Filter selects
    var filterIds = ['filterType', 'filterCategory', 'filterRating'];
    for (var i = 0; i < filterIds.length; i++) {
        var el = document.getElementById(filterIds[i]);
        if (el) {
            el.addEventListener('change', applyFilters);
        }
    }

    // Close modals on overlay click
    document.getElementById('formModal').addEventListener('click', function (e) {
        if (e.target === this) closeFormModal();
    });
    document.getElementById('detailModal').addEventListener('click', function (e) {
        if (e.target === this) closeDetailModal();
    });

    // Close modals on Escape key
    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') {
            if (document.getElementById('detailModal').classList.contains('open')) {
                closeDetailModal();
            } else if (document.getElementById('formModal').classList.contains('open')) {
                closeFormModal();
            }
        }
    });
});
