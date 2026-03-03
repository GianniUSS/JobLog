"""
AI Crew Planner — Blueprint Fase 1
Route API per gestione skills, risorse esterne, booking e disponibilita.
Separato da app.py per iniziare il processo di modularizzazione.
"""
from __future__ import annotations

import json
from datetime import datetime
from functools import wraps
from typing import Any

from flask import (
    Blueprint,
    abort,
    g,
    jsonify,
    render_template,
    request,
    session,
)

ai_planner = Blueprint("ai_planner", __name__)

# ---------------------------------------------------------------------------
# Helper: lazy import per evitare import circolari con app.py
# ---------------------------------------------------------------------------

def _get_db():
    """Restituisce la connessione DB dal contesto Flask (lazy import)."""
    from app import get_db
    return get_db()


def _db_vendor():
    """Restituisce il vendor DB ('mysql' o 'sqlite')."""
    from app import DB_VENDOR
    return DB_VENDOR


def _ph():
    """Placeholder parametrico: %s per MySQL, ? per SQLite."""
    return "%s" if _db_vendor() == "mysql" else "?"


def _login_required(f):
    """Replica il decoratore login_required di app.py."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user" not in session:
            if request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required"}), 401
            from flask import redirect, url_for
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


def _admin_required(f):
    """Decoratore: richiede login + ruolo admin."""
    @wraps(f)
    @_login_required
    def decorated(*args, **kwargs):
        if not session.get("is_admin"):
            return jsonify({"error": "forbidden"}), 403
        return f(*args, **kwargs)
    return decorated


def _row_to_dict(row, columns: list[str]) -> dict:
    """Converte una riga DB (dict o tuple) in dizionario."""
    if isinstance(row, dict):
        return {c: row.get(c) for c in columns}
    # sqlite3.Row o tuple
    if hasattr(row, "keys"):
        return {c: row[c] for c in columns}
    return {c: row[i] for i, c in enumerate(columns)}


# ═══════════════════════════════════════════════════════════════════════════
# PAGINE ADMIN (HTML)
# ═══════════════════════════════════════════════════════════════════════════

@ai_planner.route("/admin/skills")
@_login_required
def admin_skills():
    if not session.get("is_admin"):
        abort(403)
    return render_template("admin_skills.html", is_admin=True, active_page="skills")


@ai_planner.route("/admin/external-resources")
@_login_required
def admin_external_resources():
    if not session.get("is_admin"):
        abort(403)
    return render_template("admin_external_resources.html", is_admin=True, active_page="external-resources")


# ═══════════════════════════════════════════════════════════════════════════
# API SKILLS — Categorie
# ═══════════════════════════════════════════════════════════════════════════

@ai_planner.get("/api/admin/skills/categories")
@_admin_required
def api_skill_categories_list():
    """Lista categorie con conteggio skills."""
    db = _get_db()
    ph = _ph()
    rows = db.execute(
        "SELECT sc.id, sc.name, sc.icon, sc.sort_order, sc.active, "
        "COUNT(s.id) AS skill_count "
        "FROM skill_categories sc "
        "LEFT JOIN skills s ON s.category_id = sc.id AND s.active = 1 "
        "WHERE sc.active = 1 "
        "GROUP BY sc.id "
        "ORDER BY sc.sort_order, sc.name"
    ).fetchall()

    categories = []
    for r in rows:
        categories.append(_row_to_dict(r, [
            "id", "name", "icon", "sort_order", "active", "skill_count"
        ]))
    return jsonify({"ok": True, "categories": categories})


@ai_planner.post("/api/admin/skills/categories")
@_admin_required
def api_skill_categories_create():
    """Crea una nuova categoria skill."""
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Il nome della categoria e' obbligatorio"}), 400

    icon = (data.get("icon") or "").strip() or None
    sort_order = int(data.get("sort_order", 0))

    db = _get_db()
    ph = _ph()
    db.execute(
        f"INSERT INTO skill_categories (name, icon, sort_order) VALUES ({ph}, {ph}, {ph})",
        (name, icon, sort_order)
    )
    db.commit()
    return jsonify({"ok": True, "message": "Categoria creata"}), 201


@ai_planner.put("/api/admin/skills/categories/<int:cat_id>")
@_admin_required
def api_skill_categories_update(cat_id: int):
    """Modifica una categoria skill."""
    data = request.get_json(silent=True) or {}
    db = _get_db()
    ph = _ph()

    existing = db.execute(
        f"SELECT id FROM skill_categories WHERE id = {ph}", (cat_id,)
    ).fetchone()
    if not existing:
        return jsonify({"error": "Categoria non trovata"}), 404

    updates = []
    params = []
    for field in ("name", "icon"):
        val = data.get(field)
        if val is not None:
            updates.append(f"{field} = {ph}")
            params.append(val.strip() if isinstance(val, str) else val)
    if "sort_order" in data:
        updates.append(f"sort_order = {ph}")
        params.append(int(data["sort_order"]))
    if "active" in data:
        updates.append(f"active = {ph}")
        params.append(1 if data["active"] else 0)

    if not updates:
        return jsonify({"error": "Nessun campo da aggiornare"}), 400

    params.append(cat_id)
    db.execute(
        f"UPDATE skill_categories SET {', '.join(updates)} WHERE id = {ph}",
        tuple(params)
    )
    db.commit()
    return jsonify({"ok": True, "message": "Categoria aggiornata"})


@ai_planner.delete("/api/admin/skills/categories/<int:cat_id>")
@_admin_required
def api_skill_categories_delete(cat_id: int):
    """Disattiva una categoria (soft delete)."""
    db = _get_db()
    ph = _ph()
    db.execute(
        f"UPDATE skill_categories SET active = 0 WHERE id = {ph}", (cat_id,)
    )
    db.commit()
    return jsonify({"ok": True, "message": "Categoria disattivata"})


# ═══════════════════════════════════════════════════════════════════════════
# API SKILLS — Skills
# ═══════════════════════════════════════════════════════════════════════════

@ai_planner.get("/api/admin/skills")
@_admin_required
def api_skills_list():
    """Lista skills con filtro opzionale per categoria."""
    db = _get_db()
    ph = _ph()

    category_id = request.args.get("category_id", type=int)
    where = "WHERE s.active = 1"
    params: list = []
    if category_id:
        where += f" AND s.category_id = {ph}"
        params.append(category_id)

    rows = db.execute(
        f"SELECT s.id, s.name, s.description, s.requires_certification, "
        f"s.category_id, sc.name AS category_name, sc.icon AS category_icon "
        f"FROM skills s "
        f"JOIN skill_categories sc ON sc.id = s.category_id "
        f"{where} "
        f"ORDER BY sc.sort_order, s.name",
        tuple(params)
    ).fetchall()

    skills = []
    for r in rows:
        skills.append(_row_to_dict(r, [
            "id", "name", "description", "requires_certification",
            "category_id", "category_name", "category_icon"
        ]))
    return jsonify({"ok": True, "skills": skills})


@ai_planner.post("/api/admin/skills")
@_admin_required
def api_skills_create():
    """Crea una nuova skill."""
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    category_id = data.get("category_id")
    if not name or not category_id:
        return jsonify({"error": "Nome e categoria sono obbligatori"}), 400

    description = (data.get("description") or "").strip() or None
    requires_cert = 1 if data.get("requires_certification") else 0

    db = _get_db()
    ph = _ph()
    db.execute(
        f"INSERT INTO skills (category_id, name, description, requires_certification) "
        f"VALUES ({ph}, {ph}, {ph}, {ph})",
        (int(category_id), name, description, requires_cert)
    )
    db.commit()
    return jsonify({"ok": True, "message": "Skill creata"}), 201


@ai_planner.put("/api/admin/skills/<int:skill_id>")
@_admin_required
def api_skills_update(skill_id: int):
    """Modifica una skill."""
    data = request.get_json(silent=True) or {}
    db = _get_db()
    ph = _ph()

    existing = db.execute(
        f"SELECT id FROM skills WHERE id = {ph}", (skill_id,)
    ).fetchone()
    if not existing:
        return jsonify({"error": "Skill non trovata"}), 404

    updates = []
    params = []
    for field in ("name", "description"):
        val = data.get(field)
        if val is not None:
            updates.append(f"{field} = {ph}")
            params.append(val.strip() if isinstance(val, str) else val)
    if "category_id" in data:
        updates.append(f"category_id = {ph}")
        params.append(int(data["category_id"]))
    if "requires_certification" in data:
        updates.append(f"requires_certification = {ph}")
        params.append(1 if data["requires_certification"] else 0)
    if "active" in data:
        updates.append(f"active = {ph}")
        params.append(1 if data["active"] else 0)

    if not updates:
        return jsonify({"error": "Nessun campo da aggiornare"}), 400

    params.append(skill_id)
    db.execute(
        f"UPDATE skills SET {', '.join(updates)} WHERE id = {ph}",
        tuple(params)
    )
    db.commit()
    return jsonify({"ok": True, "message": "Skill aggiornata"})


# ═══════════════════════════════════════════════════════════════════════════
# API SKILLS — Assegnazione Operatori
# ═══════════════════════════════════════════════════════════════════════════

@ai_planner.get("/api/admin/operators/<username>/skills")
@_admin_required
def api_operator_skills_list(username: str):
    """Skills assegnate a un operatore."""
    db = _get_db()
    ph = _ph()
    rows = db.execute(
        f"SELECT os.id, os.skill_id, s.name AS skill_name, sc.name AS category_name, "
        f"sc.icon AS category_icon, os.level, os.certification_number, "
        f"os.certification_expiry, os.notes, os.assigned_by, os.assigned_at "
        f"FROM operator_skills os "
        f"JOIN skills s ON s.id = os.skill_id "
        f"JOIN skill_categories sc ON sc.id = s.category_id "
        f"WHERE os.username = {ph} "
        f"ORDER BY sc.sort_order, s.name",
        (username,)
    ).fetchall()

    skills = []
    for r in rows:
        d = _row_to_dict(r, [
            "id", "skill_id", "skill_name", "category_name", "category_icon",
            "level", "certification_number", "certification_expiry",
            "notes", "assigned_by", "assigned_at"
        ])
        # Serializza date per JSON
        for k in ("certification_expiry", "assigned_at"):
            if d.get(k) and hasattr(d[k], "isoformat"):
                d[k] = d[k].isoformat()
        skills.append(d)
    return jsonify({"ok": True, "skills": skills})


@ai_planner.post("/api/admin/operators/<username>/skills")
@_admin_required
def api_operator_skills_assign(username: str):
    """Assegna una skill a un operatore (upsert: aggiorna livello se esiste gia)."""
    data = request.get_json(silent=True) or {}
    skill_id = data.get("skill_id")
    if not skill_id:
        return jsonify({"error": "skill_id obbligatorio"}), 400

    level = data.get("level", "base")
    valid_levels = ("base", "intermedio", "avanzato", "esperto")
    if level not in valid_levels:
        level = "base"

    cert_number = (data.get("certification_number") or "").strip() or None
    cert_expiry = data.get("certification_expiry") or None
    notes = (data.get("notes") or "").strip() or None
    assigned_by = session.get("username") or session.get("user")

    db = _get_db()
    ph = _ph()

    # Verifica che l'utente esista
    user_row = db.execute(
        f"SELECT username FROM app_users WHERE username = {ph}", (username,)
    ).fetchone()
    if not user_row:
        return jsonify({"error": "Utente non trovato"}), 404

    # Verifica che la skill esista
    skill_row = db.execute(
        f"SELECT id FROM skills WHERE id = {ph} AND active = 1", (int(skill_id),)
    ).fetchone()
    if not skill_row:
        return jsonify({"error": "Skill non trovata"}), 404

    # Upsert: se esiste gia, aggiorna il livello; altrimenti inserisci
    existing = db.execute(
        f"SELECT id FROM operator_skills WHERE username = {ph} AND skill_id = {ph}",
        (username, int(skill_id))
    ).fetchone()

    if existing:
        # Aggiorna livello (e opzionalmente certificazione/note)
        eid = existing["id"] if isinstance(existing, dict) else existing[0]
        updates = [f"level = {ph}"]
        params: list = [level]
        if cert_number is not None:
            updates.append(f"certification_number = {ph}")
            params.append(cert_number)
        if cert_expiry is not None:
            updates.append(f"certification_expiry = {ph}")
            params.append(cert_expiry)
        if notes is not None:
            updates.append(f"notes = {ph}")
            params.append(notes)
        params.append(eid)
        db.execute(
            f"UPDATE operator_skills SET {', '.join(updates)} WHERE id = {ph}",
            tuple(params)
        )
        db.commit()
        return jsonify({"ok": True, "message": "Livello aggiornato"})
    else:
        # Inserisci nuova assegnazione
        db.execute(
            f"INSERT INTO operator_skills (username, skill_id, level, certification_number, "
            f"certification_expiry, notes, assigned_by) "
            f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})",
            (username, int(skill_id), level, cert_number, cert_expiry, notes, assigned_by)
        )
        db.commit()
        return jsonify({"ok": True, "message": "Skill assegnata"}), 201


@ai_planner.delete("/api/admin/operators/<username>/skills/<int:assignment_id>")
@_admin_required
def api_operator_skills_remove(username: str, assignment_id: int):
    """Rimuovi una skill da un operatore."""
    db = _get_db()
    ph = _ph()
    result = db.execute(
        f"DELETE FROM operator_skills WHERE id = {ph} AND username = {ph}",
        (assignment_id, username)
    )
    db.commit()
    # Controlla righe effettivamente cancellate
    affected = getattr(result, "rowcount", 1)
    if affected == 0:
        return jsonify({"error": "Assegnazione non trovata"}), 404
    return jsonify({"ok": True, "message": "Skill rimossa"})


@ai_planner.get("/api/admin/skills/matrix")
@_admin_required
def api_skills_matrix():
    """Matrice operatori x skills: per ogni utente le skills assegnate."""
    db = _get_db()
    ph = _ph()

    # Filtro per gruppo (default: Produzione, group_id dalla tabella user_groups)
    group_filter = request.args.get("group")  # "all" per tutti, altrimenti filtra produzione
    if group_filter == "all":
        users = db.execute(
            "SELECT u.username, u.display_name, u.full_name, ug.name AS group_name "
            "FROM app_users u "
            "LEFT JOIN user_groups ug ON ug.id = u.group_id "
            "WHERE u.is_active = 1 ORDER BY u.display_name, u.username"
        ).fetchall()
    else:
        # Default: solo Produzione (group_name = 'Produzione')
        users = db.execute(
            f"SELECT u.username, u.display_name, u.full_name, ug.name AS group_name "
            f"FROM app_users u "
            f"JOIN user_groups ug ON ug.id = u.group_id "
            f"WHERE u.is_active = 1 AND LOWER(ug.name) = {ph} "
            f"ORDER BY u.display_name, u.username",
            (group_filter.lower() if group_filter else "produzione",)
        ).fetchall()

    # Tutte le categorie attive
    categories_rows = db.execute(
        "SELECT id, name, icon, sort_order FROM skill_categories "
        "WHERE active = 1 ORDER BY sort_order, name"
    ).fetchall()

    # Tutte le skills attive (con category_id per il JS)
    skills = db.execute(
        "SELECT s.id, s.name, s.category_id, sc.name AS category_name, sc.icon "
        "FROM skills s JOIN skill_categories sc ON sc.id = s.category_id "
        "WHERE s.active = 1 AND sc.active = 1 "
        "ORDER BY sc.sort_order, s.name"
    ).fetchall()

    # Tutte le assegnazioni (con id per poter cancellare)
    assignment_rows = db.execute(
        "SELECT id, username, skill_id, level FROM operator_skills"
    ).fetchall()

    # Costruisci mappa assignments: "username:skill_id" -> {id, level}
    assignments_map: dict[str, dict] = {}
    for a in assignment_rows:
        aid = a["id"] if isinstance(a, dict) else a[0]
        u = a["username"] if isinstance(a, dict) else a[1]
        sid = a["skill_id"] if isinstance(a, dict) else a[2]
        lvl = a["level"] if isinstance(a, dict) else a[3]
        key = f"{u}:{sid}"
        assignments_map[key] = {"id": aid, "level": lvl or ""}

    # Operators list
    operators_list = []
    for u in users:
        username = u["username"] if isinstance(u, dict) else u[0]
        display = u["display_name"] if isinstance(u, dict) else u[1]
        full = u["full_name"] if isinstance(u, dict) else u[2]
        group_name = u["group_name"] if isinstance(u, dict) else (u[3] if len(u) > 3 else "")
        operators_list.append({
            "username": username,
            "display_name": display or full or username,
            "group_name": group_name or "",
        })

    # Categories list
    categories_list = []
    for c in categories_rows:
        categories_list.append(_row_to_dict(c, ["id", "name", "icon", "sort_order"]))

    # Skills list (include category_id)
    skills_list = []
    for s in skills:
        skills_list.append(_row_to_dict(s, ["id", "name", "category_id", "category_name", "icon"]))

    return jsonify({
        "ok": True,
        "operators": operators_list,
        "categories": categories_list,
        "skills": skills_list,
        "assignments": assignments_map,
    })


# ═══════════════════════════════════════════════════════════════════════════
# API RISORSE ESTERNE — CRUD
# ═══════════════════════════════════════════════════════════════════════════

_EXT_FIELDS = [
    "id", "resource_type", "company_name", "contact_name", "phone", "whatsapp",
    "email", "city", "address", "latitude", "longitude", "category",
    "hourly_rate", "daily_rate", "vat_number", "rating", "total_engagements",
    "notes", "active", "created_at", "updated_at"
]


@ai_planner.get("/api/admin/external-resources")
@_admin_required
def api_external_resources_list():
    """Lista risorse esterne con filtri opzionali."""
    db = _get_db()
    ph = _ph()

    where_clauses = ["er.active = 1"]
    params: list = []

    # Filtri
    resource_type = request.args.get("resource_type")
    if resource_type:
        where_clauses.append(f"er.resource_type = {ph}")
        params.append(resource_type)

    category = request.args.get("category")
    if category:
        where_clauses.append(f"er.category = {ph}")
        params.append(category)

    city = request.args.get("city")
    if city:
        where_clauses.append(f"er.city LIKE {ph}")
        params.append(f"%{city}%")

    min_rating = request.args.get("min_rating", type=int)
    if min_rating:
        where_clauses.append(f"er.rating >= {ph}")
        params.append(min_rating)

    search = request.args.get("q")
    if search:
        where_clauses.append(
            f"(er.contact_name LIKE {ph} OR er.company_name LIKE {ph} OR er.city LIKE {ph})"
        )
        like_val = f"%{search}%"
        params.extend([like_val, like_val, like_val])

    where = " AND ".join(where_clauses)

    rows = db.execute(
        f"SELECT er.* FROM external_resources er WHERE {where} "
        f"ORDER BY er.contact_name",
        tuple(params)
    ).fetchall()

    resources = []
    for r in rows:
        d = _row_to_dict(r, _EXT_FIELDS)
        # Serializza Decimal e datetime per JSON
        for k in ("hourly_rate", "daily_rate", "latitude", "longitude"):
            if d.get(k) is not None:
                d[k] = float(d[k])
        for k in ("created_at", "updated_at"):
            if d.get(k) and hasattr(d[k], "isoformat"):
                d[k] = d[k].isoformat()
        resources.append(d)

    return jsonify({"ok": True, "resources": resources})


@ai_planner.post("/api/admin/external-resources")
@_admin_required
def api_external_resources_create():
    """Crea una nuova risorsa esterna."""
    data = request.get_json(silent=True) or {}
    contact_name = (data.get("contact_name") or "").strip()
    if not contact_name:
        return jsonify({"error": "Nome contatto obbligatorio"}), 400

    db = _get_db()
    ph = _ph()

    fields = [
        "resource_type", "company_name", "contact_name", "phone", "whatsapp",
        "email", "city", "address", "latitude", "longitude", "category",
        "hourly_rate", "daily_rate", "vat_number", "rating", "notes"
    ]
    values = []
    for f in fields:
        val = data.get(f)
        if isinstance(val, str):
            val = val.strip() or None
        values.append(val)

    placeholders = ", ".join([ph] * len(fields))
    col_names = ", ".join(fields)

    db.execute(
        f"INSERT INTO external_resources ({col_names}) VALUES ({placeholders})",
        tuple(values)
    )
    db.commit()
    return jsonify({"ok": True, "message": "Risorsa creata"}), 201


@ai_planner.get("/api/admin/external-resources/<int:res_id>")
@_admin_required
def api_external_resources_detail(res_id: int):
    """Dettaglio risorsa con skills e storico booking."""
    db = _get_db()
    ph = _ph()

    row = db.execute(
        f"SELECT * FROM external_resources WHERE id = {ph}", (res_id,)
    ).fetchone()
    if not row:
        return jsonify({"error": "Risorsa non trovata"}), 404

    resource = _row_to_dict(row, _EXT_FIELDS)
    for k in ("hourly_rate", "daily_rate", "latitude", "longitude"):
        if resource.get(k) is not None:
            resource[k] = float(resource[k])
    for k in ("created_at", "updated_at"):
        if resource.get(k) and hasattr(resource[k], "isoformat"):
            resource[k] = resource[k].isoformat()

    # Skills della risorsa
    skill_rows = db.execute(
        f"SELECT ers.id, ers.skill_id, s.name AS skill_name, sc.name AS category_name, "
        f"sc.icon, ers.level, ers.notes "
        f"FROM external_resource_skills ers "
        f"JOIN skills s ON s.id = ers.skill_id "
        f"JOIN skill_categories sc ON sc.id = s.category_id "
        f"WHERE ers.resource_id = {ph} "
        f"ORDER BY sc.sort_order, s.name",
        (res_id,)
    ).fetchall()
    resource["skills"] = [
        _row_to_dict(s, ["id", "skill_id", "skill_name", "category_name", "icon", "level", "notes"])
        for s in skill_rows
    ]

    # Storico booking
    booking_rows = db.execute(
        f"SELECT id, project_code, project_name, function_name, date, "
        f"start_time, end_time, status, created_at "
        f"FROM resource_bookings "
        f"WHERE resource_type = 'external' AND external_resource_id = {ph} "
        f"ORDER BY date DESC LIMIT 50",
        (res_id,)
    ).fetchall()
    bookings = []
    for b in booking_rows:
        bd = _row_to_dict(b, [
            "id", "project_code", "project_name", "function_name",
            "date", "start_time", "end_time", "status", "created_at"
        ])
        for k in ("date", "start_time", "end_time", "created_at"):
            if bd.get(k) and hasattr(bd[k], "isoformat"):
                bd[k] = bd[k].isoformat()
        bookings.append(bd)
    resource["bookings"] = bookings

    return jsonify({"ok": True, "resource": resource})


@ai_planner.put("/api/admin/external-resources/<int:res_id>")
@_admin_required
def api_external_resources_update(res_id: int):
    """Modifica una risorsa esterna."""
    data = request.get_json(silent=True) or {}
    db = _get_db()
    ph = _ph()

    existing = db.execute(
        f"SELECT id FROM external_resources WHERE id = {ph}", (res_id,)
    ).fetchone()
    if not existing:
        return jsonify({"error": "Risorsa non trovata"}), 404

    updatable = [
        "resource_type", "company_name", "contact_name", "phone", "whatsapp",
        "email", "city", "address", "latitude", "longitude", "category",
        "hourly_rate", "daily_rate", "vat_number", "rating", "notes"
    ]
    updates = []
    params = []
    for field in updatable:
        val = data.get(field)
        if val is not None:
            updates.append(f"{field} = {ph}")
            params.append(val.strip() if isinstance(val, str) else val)

    if not updates:
        return jsonify({"error": "Nessun campo da aggiornare"}), 400

    params.append(res_id)
    db.execute(
        f"UPDATE external_resources SET {', '.join(updates)} WHERE id = {ph}",
        tuple(params)
    )
    db.commit()
    return jsonify({"ok": True, "message": "Risorsa aggiornata"})


@ai_planner.delete("/api/admin/external-resources/<int:res_id>")
@_admin_required
def api_external_resources_delete(res_id: int):
    """Disattiva una risorsa esterna (soft delete)."""
    db = _get_db()
    ph = _ph()
    db.execute(
        f"UPDATE external_resources SET active = 0 WHERE id = {ph}", (res_id,)
    )
    db.commit()
    return jsonify({"ok": True, "message": "Risorsa disattivata"})


# ═══════════════════════════════════════════════════════════════════════════
# API RISORSE ESTERNE — Skills
# ═══════════════════════════════════════════════════════════════════════════

@ai_planner.post("/api/admin/external-resources/<int:res_id>/skills")
@_admin_required
def api_external_resource_skills_assign(res_id: int):
    """Assegna una skill a una risorsa esterna."""
    data = request.get_json(silent=True) or {}
    skill_id = data.get("skill_id")
    if not skill_id:
        return jsonify({"error": "skill_id obbligatorio"}), 400

    level = data.get("level", "base")
    if level not in ("base", "intermedio", "esperto"):
        level = "base"
    notes = (data.get("notes") or "").strip() or None

    db = _get_db()
    ph = _ph()

    # Verifica risorsa
    res = db.execute(
        f"SELECT id FROM external_resources WHERE id = {ph} AND active = 1", (res_id,)
    ).fetchone()
    if not res:
        return jsonify({"error": "Risorsa non trovata"}), 404

    # Verifica duplicato
    dup = db.execute(
        f"SELECT id FROM external_resource_skills WHERE resource_id = {ph} AND skill_id = {ph}",
        (res_id, int(skill_id))
    ).fetchone()
    if dup:
        return jsonify({"error": "Skill gia assegnata a questa risorsa"}), 409

    db.execute(
        f"INSERT INTO external_resource_skills (resource_id, skill_id, level, notes) "
        f"VALUES ({ph}, {ph}, {ph}, {ph})",
        (res_id, int(skill_id), level, notes)
    )
    db.commit()
    return jsonify({"ok": True, "message": "Skill assegnata"}), 201


@ai_planner.delete("/api/admin/external-resources/<int:res_id>/skills/<int:skill_id>")
@_admin_required
def api_external_resource_skills_remove(res_id: int, skill_id: int):
    """Rimuovi una skill da una risorsa esterna."""
    db = _get_db()
    ph = _ph()
    db.execute(
        f"DELETE FROM external_resource_skills WHERE resource_id = {ph} AND skill_id = {ph}",
        (res_id, skill_id)
    )
    db.commit()
    return jsonify({"ok": True, "message": "Skill rimossa"})


# ═══════════════════════════════════════════════════════════════════════════
# API RISORSE ESTERNE — Disponibilita
# ═══════════════════════════════════════════════════════════════════════════

@ai_planner.get("/api/admin/external-resources/<int:res_id>/availability")
@_admin_required
def api_external_availability_get(res_id: int):
    """Disponibilita mensile di una risorsa esterna."""
    db = _get_db()
    ph = _ph()

    year = request.args.get("year", type=int) or datetime.now().year
    month = request.args.get("month", type=int) or datetime.now().month

    # Range del mese
    date_start = f"{year:04d}-{month:02d}-01"
    if month == 12:
        date_end = f"{year + 1:04d}-01-01"
    else:
        date_end = f"{year:04d}-{month + 1:02d}-01"

    rows = db.execute(
        f"SELECT id, date, status, notes FROM external_availability "
        f"WHERE resource_id = {ph} AND date >= {ph} AND date < {ph} "
        f"ORDER BY date",
        (res_id, date_start, date_end)
    ).fetchall()

    availability = []
    for r in rows:
        d = _row_to_dict(r, ["id", "date", "status", "notes"])
        if d.get("date") and hasattr(d["date"], "isoformat"):
            d["date"] = d["date"].isoformat()
        availability.append(d)

    return jsonify({"ok": True, "availability": availability, "year": year, "month": month})


@ai_planner.post("/api/admin/external-resources/<int:res_id>/availability")
@_admin_required
def api_external_availability_set(res_id: int):
    """Imposta disponibilita per una o piu date."""
    data = request.get_json(silent=True) or {}
    entries = data.get("entries", [])
    if not entries:
        # Singola data
        date_val = data.get("date")
        status = data.get("status", "available")
        notes = data.get("notes")
        if not date_val:
            return jsonify({"error": "Data obbligatoria"}), 400
        entries = [{"date": date_val, "status": status, "notes": notes}]

    db = _get_db()
    ph = _ph()
    vendor = _db_vendor()

    for entry in entries:
        date_val = entry.get("date")
        status = entry.get("status", "available")
        notes = (entry.get("notes") or "").strip() or None

        if status not in ("available", "unavailable", "tentative"):
            status = "available"

        if vendor == "mysql":
            db.execute(
                "INSERT INTO external_availability (resource_id, date, status, notes) "
                "VALUES (%s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE status = VALUES(status), notes = VALUES(notes)",
                (res_id, date_val, status, notes)
            )
        else:
            db.execute(
                "INSERT INTO external_availability (resource_id, date, status, notes) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(resource_id, date) DO UPDATE SET status = excluded.status, notes = excluded.notes",
                (res_id, date_val, status, notes)
            )

    db.commit()
    return jsonify({"ok": True, "message": f"{len(entries)} date aggiornate"})


# ═══════════════════════════════════════════════════════════════════════════
# API AI CREW PLANNER — Fase 2
# ═══════════════════════════════════════════════════════════════════════════

@ai_planner.post("/api/admin/ai/propose-crew")
@_admin_required
def api_ai_propose_crew():
    """Chiede all'AI di proporre assegnazioni crew per un progetto."""
    data = request.get_json(silent=True) or {}
    project_code = (data.get("project_code") or "").strip()
    planning_date = (data.get("date") or "").strip()

    if not project_code:
        return jsonify({"ok": False, "error": "project_code obbligatorio"}), 400
    if not planning_date:
        return jsonify({"ok": False, "error": "date obbligatoria (YYYY-MM-DD)"}), 400

    from flask import current_app
    from ai_planner_engine import propose_crew

    db = _get_db()
    result = propose_crew(project_code, planning_date, db, current_app)

    if not result.get("ok"):
        return jsonify(result), 500 if "API" in result.get("error", "") else 400

    return jsonify(result)


@ai_planner.post("/api/admin/ai/chat")
@_admin_required
def api_ai_chat():
    """Chat interattiva per raffinare le proposte AI."""
    data = request.get_json(silent=True) or {}
    session_id = data.get("session_id")
    message = (data.get("message") or "").strip()
    chat_history = data.get("chat_history", [])
    current_proposals = data.get("current_proposals", [])
    original_summary = data.get("original_summary", "")

    if not session_id:
        return jsonify({"ok": False, "error": "session_id obbligatorio"}), 400
    if not message:
        return jsonify({"ok": False, "error": "Messaggio vuoto"}), 400

    from flask import current_app
    from ai_planner_engine import chat_with_ai

    db = _get_db()
    result = chat_with_ai(
        session_id=session_id,
        user_message=message,
        chat_history=chat_history,
        current_proposals=current_proposals,
        original_context_summary=original_summary,
        db=db,
        app=current_app,
    )

    if not result.get("ok"):
        return jsonify(result), 500

    return jsonify(result)


@ai_planner.post("/api/admin/ai/confirm-proposals")
@_admin_required
def api_ai_confirm_proposals():
    """Conferma/rifiuta le proposte AI e crea i booking."""
    data = request.get_json(silent=True) or {}
    session_id = data.get("session_id")
    accepted = data.get("accepted", [])
    rejected = data.get("rejected", [])

    if not session_id:
        return jsonify({"ok": False, "error": "session_id obbligatorio"}), 400
    if not accepted:
        return jsonify({"ok": False, "error": "Nessuna proposta selezionata"}), 400

    db = _get_db()
    ph = _ph()
    vendor = _db_vendor()
    confirmed_by = session.get("username") or session.get("user")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    bookings_created = 0
    errors = []

    # Inserisci le proposte accettate come booking
    for proposal in accepted:
        resource_type = proposal.get("resource_type", "internal")
        identifier = str(proposal.get("identifier", "")).strip()
        project_code = (proposal.get("project_code") or "").strip()
        project_name = (proposal.get("project_name") or "").strip()
        function_name = (proposal.get("function") or "").strip()
        planning_date = (proposal.get("date") or "").strip()
        score = proposal.get("score", 0)
        reasoning = proposal.get("reasoning", "")

        # Validazione data
        if not planning_date:
            errors.append(f"Data mancante per {proposal.get('name', 'N/D')}")
            continue

        # Validazione project_code
        if not project_code:
            errors.append(f"Codice progetto mancante per {proposal.get('name', 'N/D')}")
            continue

        username = identifier if resource_type == "internal" and identifier else None
        ext_id = int(identifier) if resource_type == "external" and identifier.isdigit() else None

        try:
            db.execute(
                f"INSERT INTO resource_bookings "
                f"(project_code, project_name, resource_type, username, external_resource_id, "
                f"function_name, date, status, proposed_by_ai, ai_score, ai_reasoning, "
                f"confirmed_by, confirmed_at) "
                f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})",
                (project_code, project_name, resource_type, username, ext_id,
                 function_name, planning_date, "optioned", 1, score, reasoning,
                 confirmed_by, now)
            )
            bookings_created += 1
        except Exception as e:
            errors.append(f"Errore inserimento {proposal.get('name', 'N/D')}: {str(e)}")

    # Aggiorna la sessione AI con le scelte
    try:
        db.execute(
            f"UPDATE ai_planning_sessions SET "
            f"accepted_proposals = {ph}, rejected_proposals = {ph} "
            f"WHERE id = {ph}",
            (
                json.dumps(accepted, ensure_ascii=False),
                json.dumps(rejected, ensure_ascii=False),
                session_id,
            )
        )
    except Exception as e:
        errors.append(f"Errore aggiornamento sessione: {str(e)}")

    db.commit()

    result = {
        "ok": bookings_created > 0,
        "bookings_created": bookings_created,
        "message": f"{bookings_created} assegnazioni create",
    }
    if errors:
        result["errors"] = errors
        result["message"] += f" ({len(errors)} errori)"

    return jsonify(result)


@ai_planner.get("/api/admin/ai/sessions")
@_admin_required
def api_ai_sessions_list():
    """Lista sessioni AI, opzionalmente filtrate per progetto."""
    db = _get_db()
    ph = _ph()

    project_code = request.args.get("project_code")
    if project_code:
        rows = db.execute(
            f"SELECT id, project_code, requested_by, ai_model, tokens_input, tokens_output, "
            f"cost_estimate_eur, duration_ms, created_at "
            f"FROM ai_planning_sessions WHERE project_code = {ph} "
            f"ORDER BY created_at DESC LIMIT 50",
            (project_code,)
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT id, project_code, requested_by, ai_model, tokens_input, tokens_output, "
            "cost_estimate_eur, duration_ms, created_at "
            "FROM ai_planning_sessions "
            "ORDER BY created_at DESC LIMIT 50"
        ).fetchall()

    sessions = []
    for r in rows:
        d = _row_to_dict(r, [
            "id", "project_code", "requested_by", "ai_model",
            "tokens_input", "tokens_output", "cost_estimate_eur",
            "duration_ms", "created_at"
        ])
        if d.get("cost_estimate_eur") is not None:
            d["cost_estimate_eur"] = float(d["cost_estimate_eur"])
        if d.get("created_at") and hasattr(d["created_at"], "isoformat"):
            d["created_at"] = d["created_at"].isoformat()
        sessions.append(d)

    return jsonify({"ok": True, "sessions": sessions})
