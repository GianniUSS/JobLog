// admin_skills.js — Skills management: categories, skills, operator matrix
// Vanilla JS, no frameworks. All functions in global scope for onclick="" usage.

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let allCategories = [];
let allSkills = [];
let matrixData = null;
let toastTimer = null;

// ---------------------------------------------------------------------------
// Utility: API wrapper
// ---------------------------------------------------------------------------
async function apiCall(url, method, body) {
    const opts = {
        method: method || 'GET',
        headers: {}
    };
    if (body !== undefined && body !== null) {
        opts.headers['Content-Type'] = 'application/json';
        opts.body = JSON.stringify(body);
    }
    try {
        const res = await fetch(url, opts);
        const data = await res.json();
        if (!res.ok) {
            throw new Error(data.error || data.message || ('HTTP ' + res.status));
        }
        if (data.ok === false) {
            throw new Error(data.error || 'Operazione fallita');
        }
        return data;
    } catch (err) {
        throw err;
    }
}

// ---------------------------------------------------------------------------
// Utility: Toast
// ---------------------------------------------------------------------------
function showToast(message, type) {
    var el = document.getElementById('toast');
    if (!el) return;
    el.textContent = message;
    el.className = 'toast ' + (type || 'success');
    // Force reflow so transition re-triggers if already visible
    void el.offsetWidth;
    el.classList.add('show');
    if (toastTimer) clearTimeout(toastTimer);
    toastTimer = setTimeout(function () {
        el.classList.remove('show');
    }, 3000);
}

// ---------------------------------------------------------------------------
// Tab management
// ---------------------------------------------------------------------------
function switchTab(tabName) {
    // Toggle buttons
    var buttons = document.querySelectorAll('.tab-btn');
    buttons.forEach(function (btn) {
        if (btn.getAttribute('data-tab') === tabName) {
            btn.classList.add('active');
        } else {
            btn.classList.remove('active');
        }
    });
    // Toggle panels
    var panels = document.querySelectorAll('.tab-panel');
    panels.forEach(function (panel) {
        if (panel.id === 'tab-' + tabName) {
            panel.classList.add('active');
        } else {
            panel.classList.remove('active');
        }
    });
    // Lazy-load matrix when first opened
    if (tabName === 'matrix' && !matrixData) {
        loadMatrix();
    }
}

// ===================================================================
// CATEGORIES
// ===================================================================

async function loadCategories() {
    try {
        var data = await apiCall('/api/admin/skills/categories');
        allCategories = data.categories || data || [];
        renderCategoriesTable(allCategories);
        // Also refresh category dropdowns used in skills tab
        populateCategoryFilter();
        populateSkillCategorySelect();
    } catch (err) {
        showToast('Errore caricamento categorie: ' + err.message, 'error');
        allCategories = [];
        renderCategoriesTable([]);
    }
}

function renderCategoriesTable(categories) {
    var tbody = document.getElementById('categoriesTableBody');
    if (!tbody) return;
    if (!categories || categories.length === 0) {
        tbody.innerHTML =
            '<tr><td colspan="4">' +
            '<div class="empty-state"><div class="empty-state-icon">&#128193;</div>' +
            '<h3>Nessuna categoria</h3><p>Crea la prima categoria di skills</p></div>' +
            '</td></tr>';
        return;
    }
    var html = '';
    categories.forEach(function (cat) {
        html +=
            '<tr data-category-id="' + cat.id + '">' +
            '<td style="font-size:24px;text-align:center;width:60px">' + escapeHtml(cat.icon || '') + '</td>' +
            '<td><strong>' + escapeHtml(cat.name) + '</strong></td>' +
            '<td>' + (cat.sort_order != null ? cat.sort_order : 0) + '</td>' +
            '<td class="action-btns">' +
            '<button class="icon-btn edit" title="Modifica" onclick="openCategoryModal(' + cat.id + ')">&#9998;</button>' +
            '<button class="icon-btn delete" title="Elimina" onclick="deleteCategory(' + cat.id + ')">&#128465;</button>' +
            '</td>' +
            '</tr>';
    });
    tbody.innerHTML = html;
}

function filterCategories() {
    var query = (document.getElementById('searchCategories').value || '').toLowerCase().trim();
    if (!query) {
        renderCategoriesTable(allCategories);
        return;
    }
    var filtered = allCategories.filter(function (cat) {
        return (cat.name || '').toLowerCase().indexOf(query) !== -1;
    });
    renderCategoriesTable(filtered);
}

function openCategoryModal(id) {
    var overlay = document.getElementById('categoryModalOverlay');
    var title = document.getElementById('categoryModalTitle');
    var idField = document.getElementById('categoryId');
    var nameField = document.getElementById('categoryName');
    var iconField = document.getElementById('categoryIcon');
    var sortField = document.getElementById('categorySortOrder');

    // Reset form
    document.getElementById('categoryForm').reset();
    idField.value = '';
    sortField.value = '0';

    if (id) {
        title.textContent = 'Modifica Categoria';
        // Find category in local cache
        var cat = allCategories.find(function (c) { return c.id === id; });
        if (cat) {
            idField.value = cat.id;
            nameField.value = cat.name || '';
            iconField.value = cat.icon || '';
            sortField.value = cat.sort_order != null ? cat.sort_order : 0;
        }
    } else {
        title.textContent = 'Nuova Categoria';
    }
    overlay.classList.add('open');
    nameField.focus();
}

function closeCategoryModal() {
    var overlay = document.getElementById('categoryModalOverlay');
    overlay.classList.remove('open');
}

async function saveCategory(event) {
    event.preventDefault();
    var idField = document.getElementById('categoryId');
    var nameField = document.getElementById('categoryName');
    var iconField = document.getElementById('categoryIcon');
    var sortField = document.getElementById('categorySortOrder');

    var payload = {
        name: nameField.value.trim(),
        icon: iconField.value.trim(),
        sort_order: parseInt(sortField.value, 10) || 0
    };
    if (!payload.name) {
        showToast('Il nome categoria è obbligatorio', 'error');
        return;
    }

    try {
        if (idField.value) {
            // Update
            await apiCall('/api/admin/skills/categories/' + idField.value, 'PUT', payload);
            showToast('Categoria aggiornata');
        } else {
            // Create
            await apiCall('/api/admin/skills/categories', 'POST', payload);
            showToast('Categoria creata');
        }
        closeCategoryModal();
        await loadCategories();
    } catch (err) {
        showToast('Errore: ' + err.message, 'error');
    }
}

async function deleteCategory(id) {
    if (!confirm('Eliminare questa categoria? Le skills associate non verranno eliminate ma resteranno senza categoria.')) {
        return;
    }
    try {
        await apiCall('/api/admin/skills/categories/' + id, 'DELETE');
        showToast('Categoria eliminata');
        await loadCategories();
    } catch (err) {
        showToast('Errore: ' + err.message, 'error');
    }
}

// ===================================================================
// SKILLS
// ===================================================================

async function loadSkills(categoryId) {
    try {
        var url = '/api/admin/skills';
        if (categoryId) {
            url += '?category_id=' + encodeURIComponent(categoryId);
        }
        var data = await apiCall(url);
        allSkills = data.skills || data || [];
        renderSkillsTable(allSkills);
    } catch (err) {
        showToast('Errore caricamento skills: ' + err.message, 'error');
        allSkills = [];
        renderSkillsTable([]);
    }
}

function renderSkillsTable(skills) {
    var tbody = document.getElementById('skillsTableBody');
    if (!tbody) return;
    if (!skills || skills.length === 0) {
        tbody.innerHTML =
            '<tr><td colspan="5">' +
            '<div class="empty-state"><div class="empty-state-icon">&#127919;</div>' +
            '<h3>Nessuna skill</h3><p>Crea la prima skill</p></div>' +
            '</td></tr>';
        return;
    }
    var html = '';
    skills.forEach(function (skill) {
        var catDisplay = '';
        var cat = allCategories.find(function (c) { return c.id === skill.category_id; });
        if (cat) {
            catDisplay = (cat.icon ? cat.icon + ' ' : '') + escapeHtml(cat.name);
        } else if (skill.category_name) {
            catDisplay = escapeHtml(skill.category_name);
        } else {
            catDisplay = '<span style="color:var(--text-muted)">—</span>';
        }

        var certBadge = skill.requires_certification
            ? '<span class="badge badge-warning">&#128220; Richiesta</span>'
            : '<span class="badge badge-success">Non richiesta</span>';

        html +=
            '<tr data-skill-id="' + skill.id + '">' +
            '<td><strong>' + escapeHtml(skill.name) + '</strong></td>' +
            '<td>' + catDisplay + '</td>' +
            '<td>' + escapeHtml(skill.description || '—') + '</td>' +
            '<td>' + certBadge + '</td>' +
            '<td class="action-btns">' +
            '<button class="icon-btn edit" title="Modifica" onclick="openSkillModal(' + skill.id + ')">&#9998;</button>' +
            '<button class="icon-btn delete" title="Disattiva" onclick="deleteSkill(' + skill.id + ')">&#128465;</button>' +
            '</td>' +
            '</tr>';
    });
    tbody.innerHTML = html;
}

function filterSkills() {
    var query = (document.getElementById('searchSkills').value || '').toLowerCase().trim();
    var catFilter = document.getElementById('filterCategory').value;

    var filtered = allSkills.filter(function (skill) {
        var matchesSearch = !query || (skill.name || '').toLowerCase().indexOf(query) !== -1 ||
            (skill.description || '').toLowerCase().indexOf(query) !== -1;
        var matchesCat = !catFilter || String(skill.category_id) === String(catFilter);
        return matchesSearch && matchesCat;
    });
    renderSkillsTable(filtered);
}

function populateCategoryFilter() {
    var select = document.getElementById('filterCategory');
    if (!select) return;
    // Preserve current value
    var current = select.value;
    // Keep the "all" option, remove the rest
    select.innerHTML = '<option value="">Tutte le categorie</option>';
    allCategories.forEach(function (cat) {
        var opt = document.createElement('option');
        opt.value = cat.id;
        opt.textContent = (cat.icon ? cat.icon + ' ' : '') + cat.name;
        select.appendChild(opt);
    });
    // Restore previous selection if still valid
    if (current) select.value = current;
}

function populateSkillCategorySelect() {
    var select = document.getElementById('skillCategory');
    if (!select) return;
    var current = select.value;
    select.innerHTML = '<option value="">-- Seleziona categoria --</option>';
    allCategories.forEach(function (cat) {
        var opt = document.createElement('option');
        opt.value = cat.id;
        opt.textContent = (cat.icon ? cat.icon + ' ' : '') + cat.name;
        select.appendChild(opt);
    });
    if (current) select.value = current;
}

function openSkillModal(id) {
    var overlay = document.getElementById('skillModalOverlay');
    var title = document.getElementById('skillModalTitle');
    var idField = document.getElementById('skillId');
    var nameField = document.getElementById('skillName');
    var categoryField = document.getElementById('skillCategory');
    var descField = document.getElementById('skillDescription');
    var certField = document.getElementById('skillRequiresCertification');

    // Reset form
    document.getElementById('skillForm').reset();
    idField.value = '';

    // Make sure category dropdown is populated
    populateSkillCategorySelect();

    if (id) {
        title.textContent = 'Modifica Skill';
        var skill = allSkills.find(function (s) { return s.id === id; });
        if (skill) {
            idField.value = skill.id;
            nameField.value = skill.name || '';
            categoryField.value = skill.category_id || '';
            descField.value = skill.description || '';
            certField.checked = !!skill.requires_certification;
        }
    } else {
        title.textContent = 'Nuova Skill';
    }
    overlay.classList.add('open');
    nameField.focus();
}

function closeSkillModal() {
    var overlay = document.getElementById('skillModalOverlay');
    overlay.classList.remove('open');
}

async function saveSkill(event) {
    event.preventDefault();
    var idField = document.getElementById('skillId');
    var nameField = document.getElementById('skillName');
    var categoryField = document.getElementById('skillCategory');
    var descField = document.getElementById('skillDescription');
    var certField = document.getElementById('skillRequiresCertification');

    var payload = {
        name: nameField.value.trim(),
        category_id: parseInt(categoryField.value, 10) || null,
        description: descField.value.trim(),
        requires_certification: certField.checked
    };
    if (!payload.name) {
        showToast('Il nome skill è obbligatorio', 'error');
        return;
    }
    if (!payload.category_id) {
        showToast('Seleziona una categoria', 'error');
        return;
    }

    try {
        if (idField.value) {
            await apiCall('/api/admin/skills/' + idField.value, 'PUT', payload);
            showToast('Skill aggiornata');
        } else {
            await apiCall('/api/admin/skills', 'POST', payload);
            showToast('Skill creata');
        }
        closeSkillModal();
        await loadSkills();
    } catch (err) {
        showToast('Errore: ' + err.message, 'error');
    }
}

async function deleteSkill(id) {
    if (!confirm('Disattivare questa skill? Potrà essere riattivata in seguito.')) {
        return;
    }
    try {
        // Soft-delete: set active = false
        await apiCall('/api/admin/skills/' + id, 'PUT', { active: false });
        showToast('Skill disattivata');
        await loadSkills();
    } catch (err) {
        showToast('Errore: ' + err.message, 'error');
    }
}

// ===================================================================
// MATRIX
// ===================================================================

async function loadMatrix() {
    var container = document.getElementById('matrixContainer');
    if (!container) return;
    container.innerHTML =
        '<div class="empty-state"><div class="empty-state-icon">&#9203;</div>' +
        '<h3>Caricamento matrice...</h3></div>';
    try {
        var data = await apiCall('/api/admin/skills/matrix');
        matrixData = data;
        renderMatrix(data);
    } catch (err) {
        showToast('Errore caricamento matrice: ' + err.message, 'error');
        container.innerHTML =
            '<div class="empty-state"><div class="empty-state-icon">&#9888;</div>' +
            '<h3>Errore</h3><p>' + escapeHtml(err.message) + '</p></div>';
    }
}

function renderMatrix(data) {
    var container = document.getElementById('matrixContainer');
    if (!container) return;

    var operators = data.operators || [];
    var categories = data.categories || [];
    var skills = data.skills || [];
    var assignments = data.assignments || {};

    if (operators.length === 0 || skills.length === 0) {
        container.innerHTML =
            '<div class="empty-state"><div class="empty-state-icon">&#128202;</div>' +
            '<h3>Matrice vuota</h3><p>Servono operatori e skills per popolare la matrice</p></div>';
        return;
    }

    // Group skills by category for header rendering
    var skillsByCategory = {};
    var orderedCategoryIds = [];
    skills.forEach(function (s) {
        var cid = s.category_id || 0;
        if (!skillsByCategory[cid]) {
            skillsByCategory[cid] = [];
            orderedCategoryIds.push(cid);
        }
        skillsByCategory[cid].push(s);
    });

    // Build header rows
    // Row 1: category groups spanning columns
    var categoryHeaderRow = '<th rowspan="2" style="position:sticky;left:0;z-index:3;background:var(--bg)">Operatore</th>';
    orderedCategoryIds.forEach(function (cid) {
        var cat = categories.find(function (c) { return c.id === cid; });
        var catName = cat ? ((cat.icon ? cat.icon + ' ' : '') + cat.name) : 'Senza categoria';
        var count = skillsByCategory[cid].length;
        categoryHeaderRow += '<th class="category-header" colspan="' + count + '">' + escapeHtml(catName) + '</th>';
    });

    // Row 2: individual skill names
    var skillHeaderRow = '';
    var skillOrder = []; // flat ordered list of skill ids
    orderedCategoryIds.forEach(function (cid) {
        skillsByCategory[cid].forEach(function (s) {
            skillHeaderRow += '<th class="skill-header" title="' + escapeHtml(s.name) + '">' + escapeHtml(s.name) + '</th>';
            skillOrder.push(s);
        });
    });

    // Build body rows
    var bodyRows = '';
    operators.forEach(function (op) {
        var username = op.username || op.name || '';
        var displayName = op.display_name || op.full_name || username;
        bodyRows += '<tr data-operator="' + escapeHtml(username) + '">';
        bodyRows += '<td class="operator-name">' + escapeHtml(displayName) + '</td>';
        skillOrder.forEach(function (skill) {
            var key = username + ':' + skill.id;
            var assignment = assignments[key] || null;
            var isChecked = !!assignment;
            var level = assignment ? (assignment.level || '') : '';
            var assignmentId = assignment ? (assignment.id || '') : '';

            bodyRows += '<td class="matrix-cell">';
            bodyRows +=
                '<input type="checkbox" class="matrix-check" ' +
                (isChecked ? 'checked ' : '') +
                'data-username="' + escapeHtml(username) + '" ' +
                'data-skill-id="' + skill.id + '" ' +
                'data-assignment-id="' + assignmentId + '" ' +
                'onchange="toggleSkillAssignment(\'' + escapeHtml(username) + '\',' + skill.id + ',this.checked,' + (assignmentId || 'null') + ')">';

            if (isChecked) {
                bodyRows +=
                    '<br><select class="level-select" ' +
                    'data-username="' + escapeHtml(username) + '" ' +
                    'data-skill-id="' + skill.id + '" ' +
                    'onchange="changeSkillLevel(\'' + escapeHtml(username) + '\',' + skill.id + ',this.value)">' +
                    '<option value=""' + (!level ? ' selected' : '') + '>—</option>' +
                    '<option value="base"' + (level === 'base' ? ' selected' : '') + '>Base</option>' +
                    '<option value="intermedio"' + (level === 'intermedio' ? ' selected' : '') + '>Intermedio</option>' +
                    '<option value="avanzato"' + (level === 'avanzato' ? ' selected' : '') + '>Avanzato</option>' +
                    '<option value="esperto"' + (level === 'esperto' ? ' selected' : '') + '>Esperto</option>' +
                    '</select>';
            }

            bodyRows += '</td>';
        });
        bodyRows += '</tr>';
    });

    var html =
        '<table>' +
        '<thead>' +
        '<tr>' + categoryHeaderRow + '</tr>' +
        '<tr>' + skillHeaderRow + '</tr>' +
        '</thead>' +
        '<tbody id="matrixTableBody">' + bodyRows + '</tbody>' +
        '</table>';

    container.innerHTML = html;
}

function filterMatrix() {
    var query = (document.getElementById('searchMatrix').value || '').toLowerCase().trim();
    var rows = document.querySelectorAll('#matrixTableBody tr');
    rows.forEach(function (row) {
        var name = (row.getAttribute('data-operator') || '').toLowerCase();
        var nameCell = row.querySelector('.operator-name');
        var displayText = nameCell ? nameCell.textContent.toLowerCase() : '';
        if (!query || name.indexOf(query) !== -1 || displayText.indexOf(query) !== -1) {
            row.style.display = '';
        } else {
            row.style.display = 'none';
        }
    });
}

async function toggleSkillAssignment(username, skillId, isChecked, assignmentId) {
    try {
        if (isChecked) {
            // Assign skill
            await apiCall('/api/admin/operators/' + encodeURIComponent(username) + '/skills', 'POST', {
                skill_id: skillId,
                level: ''
            });
            showToast('Skill assegnata');
        } else {
            // Remove assignment
            if (assignmentId) {
                await apiCall(
                    '/api/admin/operators/' + encodeURIComponent(username) + '/skills/' + assignmentId,
                    'DELETE'
                );
                showToast('Skill rimossa');
            }
        }
        // Reload matrix to refresh UI (including level selects)
        await loadMatrix();
    } catch (err) {
        showToast('Errore: ' + err.message, 'error');
        // Reload to revert checkbox state
        await loadMatrix();
    }
}

async function changeSkillLevel(username, skillId, level) {
    try {
        await apiCall('/api/admin/operators/' + encodeURIComponent(username) + '/skills', 'POST', {
            skill_id: skillId,
            level: level
        });
        showToast('Livello aggiornato');
    } catch (err) {
        showToast('Errore: ' + err.message, 'error');
        await loadMatrix();
    }
}

// ===================================================================
// Helpers
// ===================================================================

function escapeHtml(str) {
    if (!str) return '';
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(String(str)));
    return div.innerHTML;
}

// Close modals on overlay click
function setupModalOverlayClose() {
    var overlays = document.querySelectorAll('.modal-overlay');
    overlays.forEach(function (overlay) {
        overlay.addEventListener('click', function (e) {
            if (e.target === overlay) {
                overlay.classList.remove('open');
            }
        });
    });
}

// Close modals on Escape key
function setupEscapeClose() {
    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') {
            var openOverlay = document.querySelector('.modal-overlay.open');
            if (openOverlay) {
                openOverlay.classList.remove('open');
            }
        }
    });
}

// ===================================================================
// Init
// ===================================================================

document.addEventListener('DOMContentLoaded', function () {
    setupModalOverlayClose();
    setupEscapeClose();
    // Initial data load
    loadCategories();
    loadSkills();
});
