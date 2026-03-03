-- ═══════════════════════════════════════════════════════════════════════════
-- AI CREW PLANNER — Migrazione Fase 1
-- Tabelle per skills, risorse esterne, booking e sessioni AI
-- Data: 2026-03-03
-- ═══════════════════════════════════════════════════════════════════════════

-- -----------------------------------------------------------------------
-- 1. SKILL CATEGORIES — Categorie di competenze (Audio, Luci, Video...)
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS skill_categories (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    icon VARCHAR(50) DEFAULT NULL,
    sort_order INT DEFAULT 0,
    active TINYINT(1) DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------
-- 2. SKILLS — Competenze specifiche (FOH, GrandMA, Rigger...)
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS skills (
    id INT AUTO_INCREMENT PRIMARY KEY,
    category_id INT NOT NULL,
    name VARCHAR(150) NOT NULL,
    description TEXT,
    requires_certification TINYINT(1) DEFAULT 0,
    active TINYINT(1) DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES skill_categories(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------
-- 3. OPERATOR_SKILLS — Associazione operatore ↔ skill (con livello)
--    FIX: aggiunto UNIQUE per evitare duplicati
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS operator_skills (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(190) NOT NULL,
    skill_id INT NOT NULL,
    level ENUM('base','intermedio','esperto') DEFAULT 'base',
    certification_number VARCHAR(100),
    certification_expiry DATE,
    notes TEXT,
    assigned_by VARCHAR(50),
    assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (skill_id) REFERENCES skills(id),
    FOREIGN KEY (username) REFERENCES app_users(username),
    UNIQUE KEY unique_user_skill (username, skill_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------
-- 4. EXTERNAL_RESOURCES — Risorse esterne (cooperative, freelance, tecnici)
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS external_resources (
    id INT AUTO_INCREMENT PRIMARY KEY,
    resource_type ENUM('persona','azienda') DEFAULT 'persona',
    company_name VARCHAR(200),
    contact_name VARCHAR(150) NOT NULL,
    phone VARCHAR(30),
    whatsapp VARCHAR(30),
    email VARCHAR(150),
    city VARCHAR(100),
    address TEXT,
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    category ENUM('facchinaggio','artigiano','tecnico','autista','altro') DEFAULT 'altro',
    hourly_rate DECIMAL(8,2),
    daily_rate DECIMAL(8,2),
    vat_number VARCHAR(30),
    rating TINYINT UNSIGNED DEFAULT 3,
    total_engagements INT DEFAULT 0,
    notes TEXT,
    active TINYINT(1) DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------
-- 5. EXTERNAL_RESOURCE_SKILLS — Skill delle risorse esterne
--    FIX: aggiunto UNIQUE per evitare duplicati
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS external_resource_skills (
    id INT AUTO_INCREMENT PRIMARY KEY,
    resource_id INT NOT NULL,
    skill_id INT NOT NULL,
    level ENUM('base','intermedio','esperto') DEFAULT 'base',
    notes TEXT,
    FOREIGN KEY (resource_id) REFERENCES external_resources(id) ON DELETE CASCADE,
    FOREIGN KEY (skill_id) REFERENCES skills(id),
    UNIQUE KEY unique_resource_skill (resource_id, skill_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------
-- 6. EXTERNAL_AVAILABILITY — Disponibilita risorse esterne per data
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS external_availability (
    id INT AUTO_INCREMENT PRIMARY KEY,
    resource_id INT NOT NULL,
    date DATE NOT NULL,
    status ENUM('available','unavailable','tentative') DEFAULT 'available',
    notes TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (resource_id) REFERENCES external_resources(id) ON DELETE CASCADE,
    UNIQUE KEY unique_resource_date (resource_id, date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------
-- 7. RESOURCE_BOOKINGS — Prenotazioni risorse (interne ed esterne)
--    FIX: aggiunto constraint per evitare doppio booking stesso slot
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS resource_bookings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    project_code VARCHAR(50) NOT NULL,
    project_name VARCHAR(200),
    resource_type ENUM('internal','external') NOT NULL,
    username VARCHAR(50),
    external_resource_id INT,
    function_name VARCHAR(200),
    date DATE NOT NULL,
    start_time TIME,
    end_time TIME,
    status ENUM('optioned','confirmed','cancelled') DEFAULT 'optioned',
    proposed_by_ai TINYINT(1) DEFAULT 0,
    ai_score DECIMAL(5,2),
    ai_reasoning TEXT,
    confirmed_by VARCHAR(50),
    confirmed_at DATETIME,
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_booking_project (project_code),
    INDEX idx_booking_date (date),
    INDEX idx_booking_status (status),
    INDEX idx_booking_username (username),
    INDEX idx_booking_external (external_resource_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------
-- 8. AI_PLANNING_SESSIONS — Log sessioni AI per tracciamento e costi
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ai_planning_sessions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    project_code VARCHAR(50) NOT NULL,
    requested_by VARCHAR(50),
    request_context JSON,
    ai_response JSON,
    ai_model VARCHAR(50),
    tokens_input INT,
    tokens_output INT,
    cost_estimate_eur DECIMAL(6,4),
    accepted_proposals JSON,
    rejected_proposals JSON,
    chat_history JSON,
    duration_ms INT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ai_session_project (project_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ═══════════════════════════════════════════════════════════════════════════
-- DATI SEED — Categorie e Skills per il settore eventi/AV
-- ═══════════════════════════════════════════════════════════════════════════

INSERT INTO skill_categories (name, icon, sort_order) VALUES
('Audio', '🔊', 1),
('Luci', '💡', 2),
('Video', '🎬', 3),
('Rigging', '🏗️', 4),
('Facchinaggio', '📦', 5),
('Elettrico', '⚡', 6),
('Carpenteria', '🔨', 7),
('Trasporto', '🚛', 8);

INSERT INTO skills (category_id, name, requires_certification) VALUES
-- Audio (category_id = 1)
(1, 'FOH (Front of House)', 0),
(1, 'Monitor', 0),
(1, 'RF / Microfonista', 0),
(1, 'Programmazione audio digitale', 0),
-- Luci (category_id = 2)
(2, 'Tecnico luci generico', 0),
(2, 'Programmazione GrandMA', 0),
(2, 'Teste mobili / LED', 0),
(2, 'Follow spot', 0),
-- Video (category_id = 3)
(3, 'Regia video', 0),
(3, 'LED Wall / Processori', 0),
(3, 'Streaming / VMIX', 0),
(3, 'Operatore camera', 0),
-- Rigging (category_id = 4)
(4, 'Rigger certificato', 1),
(4, 'Montaggio americane', 0),
(4, 'Motori / Chain hoist', 1),
-- Facchinaggio (category_id = 5)
(5, 'Facchino generico', 0),
(5, 'Capo squadra', 0),
-- Elettrico (category_id = 6)
(6, 'Elettricista certificato', 1),
(6, 'Quadri elettrici spettacolo', 1),
-- Carpenteria (category_id = 7)
(7, 'Carpentiere palchi', 0),
(7, 'Carpentiere scenografo', 0),
-- Trasporto (category_id = 8)
(8, 'Autista patente B', 0),
(8, 'Autista patente C', 1),
(8, 'Autista patente CE + ADR', 1);
