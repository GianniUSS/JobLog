"""
AI Crew Planner Engine — Fase 2
Motore per la proposta automatica di assegnazioni crew ai progetti.
Usa Claude (Anthropic API) per generare suggerimenti intelligenti
basati su skills, disponibilita, merce e contesto progetto.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, date as date_type
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# COSTANTI
# ═══════════════════════════════════════════════════════════════════════════

# Costo stimato per 1M token (input/output) — Claude Sonnet 4
_COST_PER_1M_INPUT = 3.00   # USD → EUR ~2.80
_COST_PER_1M_OUTPUT = 15.00  # USD → EUR ~14.00
_EUR_PER_USD = 0.93

DEFAULT_MODEL = "claude-sonnet-4-20250514"
DEFAULT_MAX_TOKENS = 4096


# ═══════════════════════════════════════════════════════════════════════════
# RACCOLTA CONTESTO
# ═══════════════════════════════════════════════════════════════════════════

def gather_planning_context(project_code: str, planning_date: str, db, app) -> Dict[str, Any]:
    """
    Raccoglie TUTTO il contesto necessario all'AI per proporre crew.

    Args:
        project_code: Codice progetto Rentman
        planning_date: Data in formato YYYY-MM-DD
        db: Connessione database (da get_db())
        app: Flask app (per accedere a get_rentman_client e load_config)

    Returns:
        Dict con: project, equipment, functions, phases,
                  internal_crew, external_resources, existing_assignments
    """
    context: Dict[str, Any] = {
        "project_code": project_code,
        "date": planning_date,
        "project": None,
        "equipment": [],
        "functions": [],
        "phases": [],
        "existing_crew": [],
        "internal_available": [],
        "external_available": [],
    }

    # --- 1. Dati progetto da Rentman ---
    try:
        from app import get_rentman_client
        client = get_rentman_client()
        if client:
            project = client.find_project(project_code)
            if project:
                context["project"] = {
                    "id": project.get("id"),
                    "name": project.get("name") or project.get("displayname", ""),
                    "code": project_code,
                    "location": project.get("location_name", ""),
                    "period_start": project.get("planperiod_start") or project.get("period_start", ""),
                    "period_end": project.get("planperiod_end") or project.get("period_end", ""),
                    "manager": project.get("project_manager_name", ""),
                }
                pid = project["id"]

                # Merce/Equipment
                try:
                    equip_list = client.get_project_planned_equipment(pid)
                    for eq in equip_list:
                        context["equipment"].append({
                            "name": eq.get("name") or eq.get("displayname", ""),
                            "quantity": eq.get("quantity", 1),
                            "group": eq.get("equipment_group_name", ""),
                            "weight": eq.get("weight"),
                        })
                except Exception as e:
                    logger.warning("AI Engine: errore merce Rentman: %s", e)

                # Funzioni (attivita/ruoli)
                try:
                    funcs = client.get_project_functions(pid)
                    for f in funcs:
                        context["functions"].append({
                            "id": f.get("id"),
                            "name": f.get("name") or f.get("displayname", ""),
                            "group": f.get("planperiod_start", ""),
                        })
                except Exception as e:
                    logger.warning("AI Engine: errore funzioni Rentman: %s", e)

                # Fasi (function groups)
                try:
                    groups = client.get_project_function_groups(pid)
                    for g in groups:
                        context["phases"].append({
                            "id": g.get("id"),
                            "name": g.get("name") or g.get("displayname", ""),
                        })
                except Exception as e:
                    logger.warning("AI Engine: errore fasi Rentman: %s", e)

                # Crew gia assegnata su Rentman
                try:
                    crew = client.get_project_crew(pid)
                    for c in crew:
                        context["existing_crew"].append({
                            "crew_id": c.get("crew"),
                            "name": c.get("crew_name", ""),
                            "function": c.get("function_name", ""),
                            "start": c.get("planperiod_start", ""),
                            "end": c.get("planperiod_end", ""),
                        })
                except Exception as e:
                    logger.warning("AI Engine: errore crew Rentman: %s", e)
    except Exception as e:
        logger.error("AI Engine: errore connessione Rentman: %s", e)

    # --- 2. Operatori interni disponibili (da DB) ---
    try:
        from app import DB_VENDOR
        ph = "%s" if DB_VENDOR == "mysql" else "?"

        # Tutti gli utenti attivi con le loro skills
        users = db.execute(
            "SELECT u.username, u.display_name, u.full_name, u.gruppo "
            "FROM app_users u WHERE u.is_active = 1 "
            "ORDER BY u.display_name"
        ).fetchall()

        # Skills per operatore (batch)
        all_skills = db.execute(
            "SELECT os.username, s.name AS skill_name, sc.name AS category, os.level "
            "FROM operator_skills os "
            "JOIN skills s ON s.id = os.skill_id "
            "JOIN skill_categories sc ON sc.id = s.category_id "
            "ORDER BY os.username"
        ).fetchall()

        skills_map: Dict[str, List[Dict]] = {}
        for row in all_skills:
            u = row["username"] if isinstance(row, dict) else row[0]
            skills_map.setdefault(u, []).append({
                "skill": row["skill_name"] if isinstance(row, dict) else row[1],
                "category": row["category"] if isinstance(row, dict) else row[2],
                "level": row["level"] if isinstance(row, dict) else row[3],
            })

        # Utenti in ferie/permesso il giorno richiesto
        on_leave = set()
        leave_rows = db.execute(
            f"SELECT username FROM user_requests "
            f"WHERE status = 'approved' "
            f"AND date_from <= {ph} AND COALESCE(date_to, date_from) >= {ph}",
            (planning_date, planning_date)
        ).fetchall()
        for row in leave_rows:
            on_leave.add(row["username"] if isinstance(row, dict) else row[0])

        # Utenti gia assegnati ad altri progetti il giorno richiesto
        already_booked = set()
        try:
            booked_rows = db.execute(
                f"SELECT DISTINCT crew_name FROM rentman_plannings "
                f"WHERE DATE(plan_start) <= {ph} AND DATE(plan_end) >= {ph} "
                f"AND project_code != {ph}",
                (planning_date, planning_date, project_code)
            ).fetchall()
            for row in booked_rows:
                name = row["crew_name"] if isinstance(row, dict) else row[0]
                already_booked.add(name)
        except Exception:
            pass  # Tabella potrebbe non avere dati

        for u in users:
            username = u["username"] if isinstance(u, dict) else u[0]
            display = u["display_name"] if isinstance(u, dict) else u[1]
            full = u["full_name"] if isinstance(u, dict) else u[2]
            gruppo = u["gruppo"] if isinstance(u, dict) else u[3]
            name = display or full or username

            status = "disponibile"
            if username in on_leave:
                status = "in_ferie"
            elif name in already_booked:
                status = "assegnato_altrove"

            context["internal_available"].append({
                "username": username,
                "name": name,
                "group": gruppo or "",
                "skills": skills_map.get(username, []),
                "status": status,
            })

    except Exception as e:
        logger.error("AI Engine: errore caricamento interni: %s", e)

    # --- 3. Risorse esterne disponibili ---
    try:
        from app import DB_VENDOR
        ph = "%s" if DB_VENDOR == "mysql" else "?"

        ext_rows = db.execute(
            "SELECT er.id, er.contact_name, er.company_name, er.category, "
            "er.city, er.hourly_rate, er.daily_rate, er.rating "
            "FROM external_resources er WHERE er.active = 1 "
            "ORDER BY er.rating DESC, er.contact_name"
        ).fetchall()

        # Skills esterne
        ext_skills = db.execute(
            "SELECT ers.resource_id, s.name AS skill_name, sc.name AS category, ers.level "
            "FROM external_resource_skills ers "
            "JOIN skills s ON s.id = ers.skill_id "
            "JOIN skill_categories sc ON sc.id = s.category_id"
        ).fetchall()

        ext_skills_map: Dict[int, List[Dict]] = {}
        for row in ext_skills:
            rid = row["resource_id"] if isinstance(row, dict) else row[0]
            ext_skills_map.setdefault(rid, []).append({
                "skill": row["skill_name"] if isinstance(row, dict) else row[1],
                "category": row["category"] if isinstance(row, dict) else row[2],
                "level": row["level"] if isinstance(row, dict) else row[3],
            })

        # Disponibilita esterne per la data
        avail_rows = db.execute(
            f"SELECT resource_id, status FROM external_availability "
            f"WHERE date = {ph}",
            (planning_date,)
        ).fetchall()
        avail_map = {}
        for row in avail_rows:
            rid = row["resource_id"] if isinstance(row, dict) else row[0]
            st = row["status"] if isinstance(row, dict) else row[1]
            avail_map[rid] = st

        for er in ext_rows:
            rid = er["id"] if isinstance(er, dict) else er[0]
            avail_status = avail_map.get(rid, "unknown")

            # Salta chi e' esplicitamente non disponibile
            if avail_status == "unavailable":
                continue

            hourly = er["hourly_rate"] if isinstance(er, dict) else er[5]
            daily = er["daily_rate"] if isinstance(er, dict) else er[6]
            rating = er["rating"] if isinstance(er, dict) else er[7]

            context["external_available"].append({
                "id": rid,
                "name": er["contact_name"] if isinstance(er, dict) else er[1],
                "company": er["company_name"] if isinstance(er, dict) else er[2],
                "category": er["category"] if isinstance(er, dict) else er[3],
                "city": er["city"] if isinstance(er, dict) else er[4],
                "hourly_rate": float(hourly) if hourly else None,
                "daily_rate": float(daily) if daily else None,
                "rating": int(rating) if rating else 3,
                "skills": ext_skills_map.get(rid, []),
                "availability": avail_status,
            })

    except Exception as e:
        logger.error("AI Engine: errore caricamento esterni: %s", e)

    return context


# ═══════════════════════════════════════════════════════════════════════════
# COSTRUZIONE PROMPT
# ═══════════════════════════════════════════════════════════════════════════

def build_system_prompt() -> str:
    """Prompt di sistema per l'AI — definisce ruolo e regole."""
    return """Sei un pianificatore esperto di eventi nel settore audiovisivo e noleggio attrezzature (AV rental).
Il tuo compito e' proporre l'assegnazione ottimale di tecnici e operatori a un progetto evento.

REGOLE DI ASSEGNAZIONE (in ordine di priorita):
1. DISPONIBILITA: Proponi SOLO chi ha status "disponibile". MAI proporre chi e' "in_ferie" o "assegnato_altrove".
2. PRIORITA INTERNI: Le risorse interne (dipendenti) hanno sempre priorita sulle esterne (costo zero vs costo orario/giornaliero).
3. ANALISI MERCE → SKILLS: Dalla lista merce/attrezzature DEDUCI le competenze necessarie:
   - Mixer, casse, microfoni, amplificatori, radiomicrofoni → tecnici AUDIO
   - Teste mobili, LED, par, follow spot, dimmer, consolle luci → tecnici LUCI
   - LED wall, processori video, telecamere, regia, proiettori → tecnici VIDEO
   - Americane, truss, motori, chain hoist → RIGGER (se certificazione richiesta, segnalalo)
   - Palco, pedane, strutture → CARPENTIERI
   - Quadri elettrici, cavi di potenza → ELETTRICISTI
   - Se la merce e' molta/pesante → aggiungi FACCHINI per montaggio/smontaggio
4. SKILL MATCHING: Se gli operatori hanno skills registrate, abbina le competenze alla merce. Se NON hanno skills registrate, usa il "gruppo" dell'operatore come indicazione del reparto (es: gruppo "audio" → tecnico audio).
5. STIMA PERSONE: Stima quante persone servono in base alla QUANTITA e COMPLESSITA della merce. Non proporre troppi o troppo pochi.
6. LEADER: Se servono piu di 3 persone, proponi un CAPO SQUADRA tra gli interni piu esperti.
7. ESTERNI: Solo se gli interni non bastano. Preferisci rating alto e costo basso.
8. REASONING: Per ogni proposta, spiega brevemente il motivo in italiano, citando quale merce/attrezzatura ha richiesto quella competenza.
9. SCORE: Punteggio 0-100 basato su: skill match (40%), disponibilita/gruppo (30%), esperienza (20%), costo (10%).

FORMATO RISPOSTA — Rispondi SOLO con un JSON valido, senza testo prima o dopo:
{
  "summary": "Breve riepilogo della proposta in italiano (es: Servono 4 tecnici: 2 audio, 1 luci, 1 facchino)",
  "proposals": [
    {
      "resource_type": "internal" oppure "external",
      "identifier": "username (stringa) per interni, oppure id (numero come stringa) per esterni",
      "name": "Nome e cognome della persona",
      "function": "Ruolo proposto (es: Tecnico audio FOH, Tecnico luci, Rigger, Facchino, Capo squadra)",
      "phase": "montaggio oppure evento oppure smontaggio oppure completo",
      "is_leader": false,
      "score": 85,
      "reasoning": "Motivazione in italiano che cita la merce/attrezzatura specifica"
    }
  ],
  "notes": "Eventuali note, avvertimenti o skills mancanti nel team"
}"""


def build_user_prompt(context: Dict[str, Any]) -> str:
    """Costruisce il prompt utente con tutto il contesto raccolto."""
    lines = []

    # Progetto
    proj = context.get("project") or {}
    lines.append(f"=== PROGETTO ===")
    lines.append(f"Nome: {proj.get('name', 'N/D')}")
    lines.append(f"Codice: {context['project_code']}")
    lines.append(f"Data pianificazione: {context['date']}")
    lines.append(f"Location: {proj.get('location', 'N/D')}")
    lines.append(f"Periodo: {proj.get('period_start', '')} → {proj.get('period_end', '')}")
    lines.append(f"Project Manager: {proj.get('manager', 'N/D')}")
    lines.append("")

    # Fasi
    phases = context.get("phases", [])
    if phases:
        lines.append("=== FASI DEL PROGETTO ===")
        for p in phases:
            lines.append(f"- {p.get('name', 'N/D')}")
        lines.append("")

    # Funzioni/Attivita
    functions = context.get("functions", [])
    if functions:
        lines.append("=== FUNZIONI/ATTIVITA ===")
        for f in functions:
            lines.append(f"- {f.get('name', 'N/D')}")
        lines.append("")

    # Merce/Equipment → qui l'AI deve inferire le skills necessarie
    equipment = context.get("equipment", [])
    if equipment:
        lines.append("=== MERCE/ATTREZZATURE (da queste deduci le competenze necessarie) ===")
        for eq in equipment:
            qty = eq.get("quantity", 1)
            name = eq.get("name", "N/D")
            group = eq.get("group", "")
            weight = eq.get("weight")
            weight_str = f" ({weight}kg)" if weight else ""
            lines.append(f"- {qty}x {name} [{group}]{weight_str}")
        lines.append("")

    # Crew gia assegnata
    existing = context.get("existing_crew", [])
    if existing:
        lines.append("=== CREW GIA ASSEGNATA SU RENTMAN ===")
        for c in existing:
            lines.append(f"- {c.get('name', 'N/D')} → {c.get('function', 'N/D')}")
        lines.append("")

    # Risorse interne disponibili
    internals = context.get("internal_available", [])
    available_internals = [i for i in internals if i.get("status") == "disponibile"]
    unavailable_internals = [i for i in internals if i.get("status") != "disponibile"]

    # Conta quanti hanno skills registrate
    with_skills = sum(1 for i in available_internals if i.get("skills"))
    total_avail = len(available_internals)

    lines.append(f"=== RISORSE INTERNE DISPONIBILI ({total_avail}) ===")
    if with_skills == 0 and total_avail > 0:
        lines.append("NOTA: Nessun operatore ha skills formalmente registrate nel sistema.")
        lines.append("USA IL CAMPO 'gruppo' COME INDICAZIONE DEL REPARTO DI COMPETENZA dell'operatore.")
        lines.append("Esempio: gruppo='audio' → probabilmente tecnico audio, gruppo='luci' → tecnico luci, ecc.")
        lines.append("")

    for i in available_internals:
        skills_str = ", ".join(
            f"{s['skill']} ({s['level']})" for s in i.get("skills", [])
        )
        gruppo = i.get('group', '')
        if skills_str:
            lines.append(f"- {i['name']} (username: {i['username']}, gruppo: {gruppo or 'N/D'})")
            lines.append(f"  Skills: {skills_str}")
        else:
            lines.append(f"- {i['name']} (username: {i['username']}, gruppo: {gruppo or 'N/D'}) — skills non registrate, usa il gruppo come riferimento")
    lines.append("")

    if unavailable_internals:
        lines.append(f"=== RISORSE INTERNE NON DISPONIBILI ({len(unavailable_internals)}) — NON PROPORRE QUESTE ===")
        for i in unavailable_internals:
            lines.append(f"- {i['name']} → {i['status']}")
        lines.append("")

    # Risorse esterne
    externals = context.get("external_available", [])
    if externals:
        lines.append(f"=== RISORSE ESTERNE DISPONIBILI ({len(externals)}) ===")
        for e in externals:
            skills_str = ", ".join(
                f"{s['skill']} ({s['level']})" for s in e.get("skills", [])
            ) or "nessuna skill"
            rate_str = ""
            if e.get("daily_rate"):
                rate_str = f"tariffa: {e['daily_rate']}EUR/giorno"
            elif e.get("hourly_rate"):
                rate_str = f"tariffa: {e['hourly_rate']}EUR/ora"
            lines.append(
                f"- {e['name']} (id: {e['id']}, {e.get('category', '')}, "
                f"citta: {e.get('city', 'N/D')}, rating: {'*' * e.get('rating', 3)}, {rate_str})"
            )
            lines.append(f"  Skills: {skills_str}")
        lines.append("")
    else:
        lines.append("=== NESSUNA RISORSA ESTERNA DISPONIBILE ===")
        lines.append("")

    lines.append("Proponi l'assegnazione ottimale della crew per questo progetto. Rispondi SOLO con il JSON.")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# CHIAMATA ANTHROPIC
# ═══════════════════════════════════════════════════════════════════════════

def call_anthropic(
    system_prompt: str,
    user_prompt: str,
    api_key: str,
    model: str = DEFAULT_MODEL,
    max_tokens: int = DEFAULT_MAX_TOKENS,
) -> Dict[str, Any]:
    """
    Chiama l'API Anthropic e ritorna risposta + metadata.

    Returns:
        {text, model, tokens_input, tokens_output, cost_eur, duration_ms}
    """
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)

    start_ms = int(time.time() * 1000)

    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )

    duration_ms = int(time.time() * 1000) - start_ms

    text = ""
    for block in response.content:
        if hasattr(block, "text"):
            text += block.text

    tokens_in = response.usage.input_tokens
    tokens_out = response.usage.output_tokens

    cost_eur = (
        (tokens_in / 1_000_000) * _COST_PER_1M_INPUT +
        (tokens_out / 1_000_000) * _COST_PER_1M_OUTPUT
    ) * _EUR_PER_USD

    return {
        "text": text,
        "model": response.model,
        "tokens_input": tokens_in,
        "tokens_output": tokens_out,
        "cost_eur": round(cost_eur, 4),
        "duration_ms": duration_ms,
    }


# ═══════════════════════════════════════════════════════════════════════════
# PARSING RISPOSTA AI
# ═══════════════════════════════════════════════════════════════════════════

def parse_ai_proposals(ai_text: str) -> Dict[str, Any]:
    """
    Estrae il JSON strutturato dalla risposta AI.
    Gestisce risposte con ```json ... ``` wrapper o testo extra.

    Returns:
        {summary, proposals: [...], notes} oppure {error}
    """
    text = ai_text.strip()

    # Rimuovi eventuale wrapper markdown
    if "```json" in text:
        start = text.index("```json") + 7
        end = text.index("```", start)
        text = text[start:end].strip()
    elif "```" in text:
        start = text.index("```") + 3
        end = text.index("```", start)
        text = text[start:end].strip()

    # Prova a trovare il JSON se c'e' testo prima/dopo
    if not text.startswith("{"):
        brace_start = text.find("{")
        if brace_start >= 0:
            text = text[brace_start:]
    if not text.endswith("}"):
        brace_end = text.rfind("}")
        if brace_end >= 0:
            text = text[:brace_end + 1]

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        logger.error("AI Engine: errore parsing JSON: %s\nTesto: %s", e, text[:500])
        return {"error": f"Errore parsing risposta AI: {e}", "raw_text": ai_text}

    # Valida struttura
    proposals = data.get("proposals", [])
    validated = []
    for p in proposals:
        validated.append({
            "resource_type": p.get("resource_type", "internal"),
            "identifier": str(p.get("identifier", "")),
            "name": p.get("name", "N/D"),
            "function": p.get("function", "N/D"),
            "phase": p.get("phase", "completo"),
            "is_leader": bool(p.get("is_leader", False)),
            "score": min(100, max(0, int(p.get("score", 50)))),
            "reasoning": p.get("reasoning", ""),
        })

    return {
        "summary": data.get("summary", ""),
        "proposals": validated,
        "notes": data.get("notes", ""),
    }


# ═══════════════════════════════════════════════════════════════════════════
# ORCHESTRATORE PRINCIPALE
# ═══════════════════════════════════════════════════════════════════════════

def propose_crew(
    project_code: str,
    planning_date: str,
    db,
    app,
) -> Dict[str, Any]:
    """
    Orchestratore principale: raccoglie contesto, chiama AI, parsa risposta.

    Args:
        project_code: Codice progetto Rentman
        planning_date: Data YYYY-MM-DD
        db: Connessione DB
        app: Flask app

    Returns:
        {ok, proposals, session_id, stats} oppure {ok: False, error}
    """
    # Carica config Anthropic
    from app import load_config, DB_VENDOR
    config = load_config()
    anthropic_cfg = config.get("anthropic", {})
    api_key = (anthropic_cfg.get("api_key") or "").strip()

    if not api_key:
        return {"ok": False, "error": "API key Anthropic non configurata in config.json"}

    model = anthropic_cfg.get("model", DEFAULT_MODEL)

    # 1. Raccolta contesto
    logger.info("[AI-PROPOSE] Raccolta contesto per %s del %s", project_code, planning_date)
    context = gather_planning_context(project_code, planning_date, db, app)

    if not context.get("project"):
        return {"ok": False, "error": f"Progetto '{project_code}' non trovato su Rentman"}

    # 2. Costruzione prompt
    system_prompt = build_system_prompt()
    user_prompt = build_user_prompt(context)
    logger.info("[AI-PROPOSE] Prompt costruito (%d chars), invio a %s", len(user_prompt), model)

    # 3. Chiamata AI
    try:
        ai_result = call_anthropic(system_prompt, user_prompt, api_key, model)
    except Exception as e:
        logger.error("[AI-PROPOSE] Errore chiamata Anthropic: %s", e)
        return {"ok": False, "error": f"Errore chiamata AI: {str(e)}"}

    # 4. Parsing risposta
    parsed = parse_ai_proposals(ai_result["text"])
    if "error" in parsed:
        # Salva sessione anche in caso di errore parsing
        _save_session(db, project_code, planning_date, context, ai_result, parsed, DB_VENDOR)
        return {"ok": False, "error": parsed["error"], "raw_response": ai_result["text"]}

    # 5. Salva sessione
    session_id = _save_session(db, project_code, planning_date, context, ai_result, parsed, DB_VENDOR)

    return {
        "ok": True,
        "proposals": parsed["proposals"],
        "summary": parsed.get("summary", ""),
        "notes": parsed.get("notes", ""),
        "session_id": session_id,
        "stats": {
            "model": ai_result["model"],
            "tokens_input": ai_result["tokens_input"],
            "tokens_output": ai_result["tokens_output"],
            "cost_eur": ai_result["cost_eur"],
            "duration_ms": ai_result["duration_ms"],
            "internal_available": len([i for i in context["internal_available"] if i["status"] == "disponibile"]),
            "external_available": len(context["external_available"]),
            "equipment_items": len(context["equipment"]),
        },
    }


def chat_with_ai(
    session_id: int,
    user_message: str,
    chat_history: List[Dict[str, str]],
    current_proposals: List[Dict],
    original_context_summary: str,
    db,
    app,
) -> Dict[str, Any]:
    """
    Chat interattiva: l'admin puo' raffinare le proposte AI via messaggio.

    Args:
        session_id: ID sessione AI
        user_message: Messaggio dell'admin
        chat_history: Lista di {role, content} precedenti
        current_proposals: Proposte attualmente visualizzate
        original_context_summary: Riassunto del contesto originale (summary dalla prima proposta)
        db: Connessione DB
        app: Flask app

    Returns:
        {ok, proposals, summary, notes, assistant_message, chat_history, stats}
    """
    from app import load_config, DB_VENDOR
    config = load_config()
    anthropic_cfg = config.get("anthropic", {})
    api_key = (anthropic_cfg.get("api_key") or "").strip()
    if not api_key:
        return {"ok": False, "error": "API key Anthropic non configurata"}

    model = anthropic_cfg.get("model", DEFAULT_MODEL)

    system_prompt = build_system_prompt()
    system_prompt += """

CONTESTO CHAT:
Stai conversando con l'admin che vuole raffinare le proposte di assegnazione crew.
L'admin puo' chiedere di:
- Aggiungere o rimuovere persone
- Cambiare ruoli/funzioni
- Sostituire un operatore con un altro
- Cambiare fasi (montaggio/evento/smontaggio)
- Qualsiasi altra modifica

Quando rispondi:
1. Applica le modifiche richieste alle proposte
2. Rispondi con il JSON aggiornato completo (TUTTE le proposte, non solo quelle modificate)
3. Aggiungi un campo "chat_response" con un breve messaggio in italiano che spiega cosa hai modificato

FORMATO RISPOSTA:
{
  "chat_response": "Breve risposta in italiano (es: Ho sostituito Mario con Luigi come tecnico audio)",
  "summary": "Riepilogo aggiornato",
  "proposals": [... lista completa aggiornata ...],
  "notes": "Note aggiornate"
}"""

    # Costruisci la conversazione
    messages = []

    # Primo messaggio: contesto originale (sintetico per risparmiare token)
    context_msg = f"Proposte attuali per il progetto:\n{json.dumps(current_proposals, ensure_ascii=False, indent=2)}"
    if original_context_summary:
        context_msg = f"Riepilogo: {original_context_summary}\n\n{context_msg}"
    messages.append({"role": "user", "content": context_msg})
    messages.append({"role": "assistant", "content": json.dumps({
        "chat_response": "Ecco le proposte attuali. Come posso modificarle?",
        "summary": original_context_summary,
        "proposals": current_proposals,
        "notes": ""
    }, ensure_ascii=False)})

    # Aggiungi la chat history precedente
    for msg in chat_history:
        messages.append({"role": msg["role"], "content": msg["content"]})

    # Aggiungi il nuovo messaggio dell'utente
    messages.append({"role": "user", "content": user_message})

    # Chiama API
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        start_ms = int(time.time() * 1000)

        response = client.messages.create(
            model=model,
            max_tokens=DEFAULT_MAX_TOKENS,
            system=system_prompt,
            messages=messages,
        )

        duration_ms = int(time.time() * 1000) - start_ms
        text = ""
        for block in response.content:
            if hasattr(block, "text"):
                text += block.text

        tokens_in = response.usage.input_tokens
        tokens_out = response.usage.output_tokens
        cost_eur = (
            (tokens_in / 1_000_000) * _COST_PER_1M_INPUT +
            (tokens_out / 1_000_000) * _COST_PER_1M_OUTPUT
        ) * _EUR_PER_USD

    except Exception as e:
        logger.error("[AI-CHAT] Errore chiamata Anthropic: %s", e)
        return {"ok": False, "error": f"Errore chiamata AI: {str(e)}"}

    # Parsing
    parsed = parse_ai_proposals(text)
    if "error" in parsed:
        return {"ok": False, "error": parsed["error"]}

    chat_response = ""
    # Prova a estrarre chat_response dal JSON raw
    try:
        raw_json = text.strip()
        if "```json" in raw_json:
            start = raw_json.index("```json") + 7
            end = raw_json.index("```", start)
            raw_json = raw_json[start:end].strip()
        elif "```" in raw_json:
            start = raw_json.index("```") + 3
            end = raw_json.index("```", start)
            raw_json = raw_json[start:end].strip()
        if not raw_json.startswith("{"):
            idx = raw_json.find("{")
            if idx >= 0:
                raw_json = raw_json[idx:]
        data = json.loads(raw_json[:raw_json.rfind("}") + 1] if not raw_json.endswith("}") else raw_json)
        chat_response = data.get("chat_response", "")
    except Exception:
        pass

    # Aggiorna chat_history nella sessione DB
    ph = "%s" if DB_VENDOR == "mysql" else "?"
    new_history = list(chat_history) + [
        {"role": "user", "content": user_message},
        {"role": "assistant", "content": text},
    ]
    try:
        db.execute(
            f"UPDATE ai_planning_sessions SET chat_history = {ph}, "
            f"ai_response = {ph} WHERE id = {ph}",
            (json.dumps(new_history, ensure_ascii=False),
             json.dumps(parsed, ensure_ascii=False),
             session_id)
        )
        db.commit()
    except Exception as e:
        logger.error("[AI-CHAT] Errore salvataggio chat: %s", e)

    return {
        "ok": True,
        "proposals": parsed["proposals"],
        "summary": parsed.get("summary", ""),
        "notes": parsed.get("notes", ""),
        "chat_response": chat_response,
        "chat_history": new_history,
        "stats": {
            "model": model,
            "tokens_input": tokens_in,
            "tokens_output": tokens_out,
            "cost_eur": round(cost_eur, 4),
            "duration_ms": duration_ms,
        },
    }


def _save_session(
    db, project_code: str, planning_date: str,
    context: Dict, ai_result: Dict, parsed: Dict,
    db_vendor: str,
) -> Optional[int]:
    """Salva la sessione AI nel database."""
    ph = "%s" if db_vendor == "mysql" else "?"
    try:
        cursor = db.execute(
            f"INSERT INTO ai_planning_sessions "
            f"(project_code, requested_by, request_context, ai_response, ai_model, "
            f"tokens_input, tokens_output, cost_estimate_eur, duration_ms) "
            f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})",
            (
                project_code,
                "admin",  # sara' sostituito con session['username'] nella route
                json.dumps({"date": planning_date, "equipment_count": len(context.get("equipment", []))}, ensure_ascii=False),
                json.dumps(parsed, ensure_ascii=False),
                ai_result.get("model", ""),
                ai_result.get("tokens_input", 0),
                ai_result.get("tokens_output", 0),
                ai_result.get("cost_eur", 0),
                ai_result.get("duration_ms", 0),
            )
        )
        db.commit()
        # Recupera last insert id
        if db_vendor == "mysql":
            row = db.execute("SELECT LAST_INSERT_ID() AS id").fetchone()
            return row["id"] if isinstance(row, dict) else row[0]
        else:
            return cursor.lastrowid
    except Exception as e:
        logger.error("[AI-PROPOSE] Errore salvataggio sessione: %s", e)
        return None
