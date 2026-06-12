const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const pixgg = require('./pixgg');
const exchange = require('./exchange');

const JWT_SECRET = process.env.JWT_SECRET || '';
const ADMIN_USER = process.env.ADMIN_USER || '';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '';

const app = express();

// ── Timezone helper (GMT-3 São Paulo) ──
function getBrazilDate() {
  return new Date().toLocaleDateString('en-CA', { timeZone: 'America/Sao_Paulo' });
}
function getBrazilTimestamp() {
  return new Date().toLocaleString('sv-SE', { timeZone: 'America/Sao_Paulo' }).replace(' ', 'T') + '-03:00';
}

// ── Log geral diário (rotaciona à meia-noite GMT-3) ──
const GENERAL_LOG_DIR = path.join(__dirname, 'data', 'logs', 'general');
if (!fs.existsSync(GENERAL_LOG_DIR)) fs.mkdirSync(GENERAL_LOG_DIR, { recursive: true });

let currentLogDate = getBrazilDate();
let generalLogStream = fs.createWriteStream(
  path.join(GENERAL_LOG_DIR, `${currentLogDate}.log`),
  { flags: 'a' }
);

function writeGeneralLog(level, args) {
  const today = getBrazilDate();
  // Rotação de dia — se passou de meia-noite GMT-3, criar novo arquivo
  if (today !== currentLogDate) {
    generalLogStream.end();
    currentLogDate = today;
    generalLogStream = fs.createWriteStream(
      path.join(GENERAL_LOG_DIR, `${currentLogDate}.log`),
      { flags: 'a' }
    );
  }
  const timestamp = getBrazilTimestamp();
  const message = args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' ');
  generalLogStream.write(`[${timestamp}] [${level}] ${message}\n`);
}

// Sobrescrever console.log/warn/error para escrever no log geral também
const _origLog = console.log.bind(console);
const _origWarn = console.warn.bind(console);
const _origError = console.error.bind(console);

console.log = (...args) => { _origLog(...args); writeGeneralLog('INFO', args); };
console.warn = (...args) => { _origWarn(...args); writeGeneralLog('WARN', args); };
console.error = (...args) => { _origError(...args); writeGeneralLog('ERROR', args); };

// ── Logger por live (um arquivo por live por streamer) ──
const LIVES_LOG_DIR = path.join(__dirname, 'data', 'logs', 'lives');
if (!fs.existsSync(LIVES_LOG_DIR)) fs.mkdirSync(LIVES_LOG_DIR, { recursive: true });

function createLiveLogPath(idStreamer) {
  const date = getBrazilDate();
  const prefix = `${date}_${idStreamer}_`;
  const existing = fs.readdirSync(LIVES_LOG_DIR).filter(f => f.startsWith(prefix) && f.endsWith('.log'));
  const n = existing.length + 1;
  return path.join(LIVES_LOG_DIR, `${prefix}${n}.log`);
}

function logToLive(idStreamer, level, ...args) {
  const live = activeLives[idStreamer];
  if (!live || !live.logPath) return;
  const timestamp = getBrazilTimestamp();
  const message = args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' ');
  const line = `[${timestamp}] [${level}] ${message}\n`;
  try {
    fs.appendFileSync(live.logPath, line);
  } catch (e) { /* ignore */ }
}

const logger = {
  info: (...args) => console.log(...args),
  warn: (...args) => console.warn(...args),
  error: (...args) => console.error(...args),
  live: (idStreamer, level, ...args) => {
    console[level === 'ERROR' ? 'error' : level === 'WARN' ? 'warn' : 'log'](...args);
    logToLive(idStreamer, level, ...args);
  },
};

// ── API Key secreta (só a extensão conhece) ──
const API_KEY = process.env.API_KEY;

// ── CDN domain para montar stream URLs ──
const CDN_DOMAIN = process.env.CDN_DOMAIN || 'live.udhyogstream.stream';

// ── ID da extensão na Chrome Web Store ──
const EXTENSION_ID = process.env.EXTENSION_ID || 'cgpdaogbcjjfmnoeacopegocjpcfcikf';

// ── CORS (extensão, squareweb, kick.com e twitch.tv para o bookmarklet) ──
app.use(cors({
  origin: (origin, callback) => {
    const allowedOrigins = [
      `chrome-extension://${EXTENSION_ID}`,
    ];
    if (!origin || allowedOrigins.includes(origin)
        || (origin.endsWith('.squareweb.app') || origin === 'https://squareweb.app')
        || (origin.endsWith('.kick.com') || origin === 'https://kick.com')
        || (origin.endsWith('.twitch.tv') || origin === 'https://twitch.tv')) {
      callback(null, true);
    } else {
      console.warn(`[CORS] Bloqueado: ${origin}`);
      callback(new Error('Origem não permitida'));
    }
  }
}));

app.use(express.json());

// ── Servir arquivos estaticos (overlay.js do bookmarklet) ──
app.use('/public', express.static(path.join(__dirname, 'public')));

// ── Rate limiting simples (por IP) ──
const rateLimit = {};
const RATE_LIMIT_WINDOW = 60 * 1000;
const RATE_LIMIT_MAX = 60; // aumentado pra 60 por causa dos heartbeats

function rateLimiter(req, res, next) {
  // Bypass para stress test (só funciona se STRESS_TEST_KEY estiver definida no .env)
  var stressKey = process.env.STRESS_TEST_KEY;
  if (stressKey && req.headers['x-stress-test'] === stressKey) return next();

  const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
  const now = Date.now();

  if (!rateLimit[ip]) {
    rateLimit[ip] = { count: 1, start: now };
  } else if (now - rateLimit[ip].start > RATE_LIMIT_WINDOW) {
    rateLimit[ip] = { count: 1, start: now };
  } else {
    rateLimit[ip].count++;
  }

  if (rateLimit[ip].count > RATE_LIMIT_MAX) {
    console.warn(`[RATE] Limite excedido para IP: ${ip}`);
    return res.status(429).json({ message: 'Muitas requisicoes. Tente novamente em 1 minuto.' });
  }

  next();
}

setInterval(() => {
  const now = Date.now();
  for (const ip in rateLimit) {
    if (now - rateLimit[ip].start > RATE_LIMIT_WINDOW) {
      delete rateLimit[ip];
    }
  }
}, 60 * 1000);

app.use(rateLimiter);

// ── Middleware de autenticação por API Key + Origin ──
function requireApiKey(req, res, next) {
  const key = req.headers['x-api-key'];
  const origin = req.headers['origin'] || '';

  if (key !== API_KEY) {
    console.warn(`[AUTH] API Key invalida de ${req.headers['x-forwarded-for'] || req.connection.remoteAddress}`);
    return res.status(403).json({ message: 'Acesso negado' });
  }

  if (origin && !origin.includes(EXTENSION_ID)
      && !(origin.endsWith('.squareweb.app') || origin === 'https://squareweb.app')
      && !(origin.endsWith('.kick.com') || origin === 'https://kick.com')
      && !(origin.endsWith('.twitch.tv') || origin === 'https://twitch.tv')) {
    console.warn(`[AUTH] Origin invalida: ${origin}`);
    return res.status(403).json({ message: 'Acesso negado' });
  }

  next();
}

// ── Middleware de autenticação por JWT ──
function requireAuth(req, res, next) {
  const authHeader = req.headers['authorization'] || '';
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  if (!match) {
    return res.status(401).json({ message: 'Token não fornecido' });
  }
  try {
    const decoded = jwt.verify(match[1], JWT_SECRET);
    req.auth = decoded;
    next();
  } catch (e) {
    return res.status(401).json({ message: 'Token inválido ou expirado' });
  }
}

function requireAdmin(req, res, next) {
  requireAuth(req, res, () => {
    if (req.auth.role !== 'admin') {
      return res.status(403).json({ message: 'Acesso restrito a administradores' });
    }
    next();
  });
}

// Aceita API Key OU JWT (backwards compatible com overlay/VPS)
function requireApiKeyOrAuth(req, res, next) {
  if (req.headers['authorization'] && req.headers['authorization'].startsWith('Bearer ')) {
    return requireAuth(req, res, next);
  }
  return requireApiKey(req, res, next);
}

// ── Logger middleware ──
app.use((req, res, next) => {
  const start = Date.now();
  const timestamp = new Date().toISOString();

  res.on('finish', () => {
    const duration = Date.now() - start;
    // Não logar heartbeats pra não poluir
    if (req.originalUrl.includes('/heartbeat')) return;

    const log = `[${timestamp}] ${req.method} ${req.originalUrl} -> ${res.statusCode} (${duration}ms)`;
    if (res.statusCode >= 500) {
      console.error(`[ERROR] ${log}`);
    } else if (res.statusCode >= 400) {
      console.warn(`[WARN] ${log}`);
    } else {
      console.log(`[OK] ${log}`);
    }

    if (['POST', 'PUT', 'PATCH'].includes(req.method) && req.body) {
      console.log(`   Body: ${JSON.stringify(req.body)}`);
    }
  });

  next();
});

// ══════════════════════════════════════════════
// ── SISTEMA DE CONTAGEM DE VIEWERS (em memória) ──
// ══════════════════════════════════════════════

// Estrutura: { "id_streamer": { "viewer_uid": timestamp_ultimo_heartbeat } }
const activeViewers = {};

// Tempo máximo sem heartbeat antes de remover (60 segundos)
const HEARTBEAT_TIMEOUT = 60 * 1000;

// Limpar viewers inativos a cada 30 segundos
setInterval(() => {
  const now = Date.now();
  let totalRemoved = 0;

  for (const streamerId in activeViewers) {
    for (const viewerUid in activeViewers[streamerId]) {
      if (now - activeViewers[streamerId][viewerUid] > HEARTBEAT_TIMEOUT) {
        delete activeViewers[streamerId][viewerUid];
        totalRemoved++;
      }
    }
    // Remover streamer se não tem mais viewers
    if (Object.keys(activeViewers[streamerId]).length === 0) {
      delete activeViewers[streamerId];
    }
  }

  if (totalRemoved > 0) {
    console.log(`[CLEANUP] Removidos ${totalRemoved} viewers inativos`);
  }
}, 30 * 1000);

// Contar viewers ativos de um streamer
function getViewerCount(idStreamer) {
  const key = idStreamer.toLowerCase();
  if (!activeViewers[key]) return 0;
  return Object.keys(activeViewers[key]).length;
}

// Registrar/atualizar heartbeat de um viewer
function registerViewer(idStreamer, viewerUid) {
  const key = idStreamer.toLowerCase();
  if (!activeViewers[key]) {
    activeViewers[key] = {};
  }
  activeViewers[key][viewerUid] = Date.now();
}

// Registrar viewer com check atômico de max_spectators (previne race condition)
// Retorna { allowed: true } ou { allowed: false, current, max }
function registerViewerIfAllowed(idStreamer, viewerUid, maxSpectators) {
  const key = idStreamer.toLowerCase();
  if (!activeViewers[key]) activeViewers[key] = {};
  // Se ilimitado ou viewer já está registrado, sempre permite
  if (maxSpectators <= 0 || activeViewers[key][viewerUid]) {
    activeViewers[key][viewerUid] = Date.now();
    return { allowed: true };
  }
  // Check + register atômico (single-threaded JS garante atomicidade)
  const current = Object.keys(activeViewers[key]).length;
  if (current >= maxSpectators) {
    return { allowed: false, current, max: maxSpectators };
  }
  activeViewers[key][viewerUid] = Date.now();
  return { allowed: true };
}

// Remover viewer
function removeViewer(idStreamer, viewerUid) {
  const key = idStreamer.toLowerCase();
  if (activeViewers[key]) {
    delete activeViewers[key][viewerUid];
    if (Object.keys(activeViewers[key]).length === 0) {
      delete activeViewers[key];
    }
  }
}

// ══════════════════════════════════════════════
// ── FILA DE ENTRADA (proteção contra raids) ──
// ══════════════════════════════════════════════

// Estrutura: { "id_streamer": { processing: 0, queue: [{uid, addedAt}], ready: {uid: ticket} } }
const entryQueues = {};

// Tickets válidos: { "ticket_string": { streamer: "id", expiresAt: timestamp } }
const validTickets = {};

const QUEUE_CONFIG = {
  MAX_CONCURRENT: 10,       // máx validate simultâneos por streamer
  MAX_QUEUE_SIZE: 10000,    // máx viewers na fila
  QUEUE_TIMEOUT_MS: 60000,  // expira da fila após 60s
  POLL_RETRY_MS: 2000,      // cliente polls a cada 2s
  TICKET_TTL_MS: 15000,     // ticket válido por 15s
};

function getOrCreateQueue(idStreamer) {
  const key = idStreamer.toLowerCase();
  if (!entryQueues[key]) {
    entryQueues[key] = { processing: 0, queue: [], ready: {} };
  }
  return entryQueues[key];
}

function enqueueViewer(idStreamer, viewerUid) {
  const q = getOrCreateQueue(idStreamer);

  // Deduplica por uid — se já está na fila, retorna posição atual
  const existing = q.queue.findIndex(e => e.uid === viewerUid);
  if (existing !== -1) {
    return { position: existing + 1, total: q.queue.length };
  }

  if (q.queue.length >= QUEUE_CONFIG.MAX_QUEUE_SIZE) {
    return null; // fila cheia
  }

  q.queue.push({ uid: viewerUid, addedAt: Date.now() });
  return { position: q.queue.length, total: q.queue.length };
}

function generateTicket(idStreamer) {
  const ticket = Math.random().toString(36).substring(2) + Date.now().toString(36);
  validTickets[ticket] = {
    streamer: idStreamer.toLowerCase(),
    expiresAt: Date.now() + QUEUE_CONFIG.TICKET_TTL_MS,
  };
  return ticket;
}

function consumeTicket(ticket, idStreamer) {
  const t = validTickets[ticket];
  if (!t) return false;
  if (t.streamer !== idStreamer.toLowerCase()) return false;
  if (Date.now() > t.expiresAt) {
    delete validTickets[ticket];
    return false;
  }
  delete validTickets[ticket];
  return true;
}

function dequeueNext(idStreamer) {
  const key = idStreamer.toLowerCase();
  const q = entryQueues[key];
  if (!q || q.queue.length === 0) return;

  const next = q.queue.shift();
  const ticket = generateTicket(idStreamer);
  q.ready[next.uid] = ticket;
}

function getQueuePosition(idStreamer, viewerUid) {
  const key = idStreamer.toLowerCase();
  const q = entryQueues[key];
  if (!q) return null;

  // Checar se já está pronto (tem ticket)
  if (q.ready[viewerUid]) {
    const ticket = q.ready[viewerUid];
    if (validTickets[ticket]) {
      delete q.ready[viewerUid];
      return { status: 'ready', ticket };
    }
    delete q.ready[viewerUid];
  }

  // Checar posição na fila
  const idx = q.queue.findIndex(e => e.uid === viewerUid);
  if (idx === -1) return null;

  return { status: 'queued', position: idx + 1, total: q.queue.length };
}

function releaseProcessing(idStreamer) {
  const key = idStreamer.toLowerCase();
  const q = entryQueues[key];
  if (!q) return;

  q.processing = Math.max(0, q.processing - 1);

  // Promover próximo da fila
  if (q.queue.length > 0 && q.processing < QUEUE_CONFIG.MAX_CONCURRENT) {
    dequeueNext(idStreamer);
  }
}

// Cleanup da fila a cada 10 segundos
setInterval(() => {
  const now = Date.now();

  // Limpar tickets expirados
  for (const ticket in validTickets) {
    if (now > validTickets[ticket].expiresAt) {
      delete validTickets[ticket];
    }
  }

  for (const key in entryQueues) {
    const q = entryQueues[key];

    // Remover viewers que esperaram demais
    const before = q.queue.length;
    q.queue = q.queue.filter(e => now - e.addedAt < QUEUE_CONFIG.QUEUE_TIMEOUT_MS);
    const removed = before - q.queue.length;
    if (removed > 0) {
      console.log(`[QUEUE] Removidos ${removed} viewers expirados da fila de "${key}"`);
    }

    // Limpar ready expirados
    for (const uid in q.ready) {
      if (!validTickets[q.ready[uid]]) {
        delete q.ready[uid];
      }
    }

    // Promover viewers se tem slots livres
    while (q.queue.length > 0 && q.processing < QUEUE_CONFIG.MAX_CONCURRENT) {
      dequeueNext(key);
    }

    // Remover fila vazia
    if (q.queue.length === 0 && q.processing === 0 && Object.keys(q.ready).length === 0) {
      delete entryQueues[key];
    }
  }
}, 10 * 1000);

// ══════════════════════════════════════════════

// ── Conexão PostgreSQL com SSL ──
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: true,
    ca: fs.readFileSync(path.join(__dirname, 'certs', 'ca-certificate.crt')).toString(),
    cert: fs.readFileSync(path.join(__dirname, 'certs', 'certificate.pem')).toString(),
    key: fs.readFileSync(path.join(__dirname, 'certs', 'private-key.key')).toString(),
  },
  max: 5,                   // Máximo 5 conexões no pool
  idleTimeoutMillis: 60000, // Fecha conexão ociosa após 60s
  connectionTimeoutMillis: 10000, // Timeout pra conectar: 10s
});

let dbConnected = false;
pool.on('connect', () => {
  if (!dbConnected) {
    console.log('[DB] Conectado ao PostgreSQL');
    dbConnected = true;
  }
});

pool.on('error', (err) => {
  console.error('[DB] Erro no pool PostgreSQL:', err.message);
});

// ── Inicializar tabela ──
async function initDB() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS streamer (
        id SERIAL PRIMARY KEY,
        "user" VARCHAR(255) NOT NULL,
        link VARCHAR(255),
        id_streamer VARCHAR(255) NOT NULL UNIQUE,
        max_spectators INTEGER NOT NULL DEFAULT 0
      )
    `);

    // Adicionar colunas se não existirem (pra não quebrar banco existente)
    await pool.query(`
      ALTER TABLE streamer ADD COLUMN IF NOT EXISTS max_spectators INTEGER NOT NULL DEFAULT 0
    `);
    await pool.query(`
      ALTER TABLE streamer ADD COLUMN IF NOT EXISTS id_mediamtx VARCHAR(255)
    `);
    await pool.query(`
      ALTER TABLE streamer ADD COLUMN IF NOT EXISTS commission REAL DEFAULT 0
    `);
    await pool.query(`
      ALTER TABLE streamer ADD COLUMN IF NOT EXISTS login VARCHAR(255) UNIQUE
    `);
    await pool.query(`
      ALTER TABLE streamer ADD COLUMN IF NOT EXISTS senha VARCHAR(255)
    `);
    await pool.query(`
      ALTER TABLE streamer ADD COLUMN IF NOT EXISTS value_per_view_hour REAL DEFAULT 0
    `);
    // Remover link_vps se existir (deprecated)
    await pool.query(`
      ALTER TABLE streamer DROP COLUMN IF EXISTS link_vps
    `);

    console.log('[DB] Tabela streamer pronta');

    // Tabela de lives (cada sessão de transmissão)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS lives (
        id SERIAL PRIMARY KEY,
        streamer VARCHAR(255) NOT NULL,
        id_streamer VARCHAR(255) NOT NULL,
        started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        ended_at TIMESTAMP WITH TIME ZONE,
        duration_seconds INTEGER DEFAULT 0,
        peak_viewers INTEGER DEFAULT 0,
        total_unique_viewers INTEGER DEFAULT 0,
        avg_viewers REAL DEFAULT 0,
        status VARCHAR(20) DEFAULT 'active'
      )
    `);

    // Adicionar colunas se não existirem (banco existente)
    await pool.query(`ALTER TABLE lives ADD COLUMN IF NOT EXISTS unique_mobile INTEGER DEFAULT 0`);
    await pool.query(`ALTER TABLE lives ADD COLUMN IF NOT EXISTS unique_desktop INTEGER DEFAULT 0`);
    // Adicionar avg_viewers se não existir (banco existente)
    await pool.query(`ALTER TABLE lives ADD COLUMN IF NOT EXISTS avg_viewers REAL DEFAULT 0`);
    await pool.query(`ALTER TABLE lives ADD COLUMN IF NOT EXISTS avg_viewers_mobile REAL DEFAULT 0`);
    await pool.query(`ALTER TABLE lives ADD COLUMN IF NOT EXISTS avg_viewers_desktop REAL DEFAULT 0`);
    await pool.query(`ALTER TABLE lives ADD COLUMN IF NOT EXISTS stream_uuid VARCHAR(255)`);

    // Tabela de sessões de viewer por live
    await pool.query(`
      CREATE TABLE IF NOT EXISTS live_viewer_sessions (
        id SERIAL PRIMARY KEY,
        live_id INTEGER REFERENCES lives(id) ON DELETE CASCADE,
        ip VARCHAR(100) NOT NULL,
        username VARCHAR(255) DEFAULT '',
        platform VARCHAR(50) DEFAULT 'unknown',
        os VARCHAR(50) DEFAULT 'unknown',
        os_version VARCHAR(50) DEFAULT '',
        device_model VARCHAR(100) DEFAULT '',
        browser VARCHAR(50) DEFAULT 'unknown',
        browser_version VARCHAR(50) DEFAULT '',
        user_agent TEXT DEFAULT '',
        is_mobile BOOLEAN DEFAULT false,
        joined_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        last_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        total_seconds INTEGER DEFAULT 0,
        segments_loaded INTEGER DEFAULT 0,
        estimated_mb REAL DEFAULT 0,
        quality_history JSONB DEFAULT '[]',
        player_health JSONB DEFAULT '{}',
        viewer_uid VARCHAR(100) NOT NULL,
        UNIQUE(live_id, viewer_uid)
      )
    `);

    // Tabela de cobranças semanais
    await pool.query(`
      CREATE TABLE IF NOT EXISTS invoices (
        id SERIAL PRIMARY KEY,
        id_streamer VARCHAR(255) NOT NULL,
        period_start DATE NOT NULL,
        period_end DATE NOT NULL,
        total_hours REAL DEFAULT 0,
        total_revenue REAL DEFAULT 0,
        amount_due REAL DEFAULT 0,
        status VARCHAR(20) DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT NOW(),
        paid_at TIMESTAMP,
        confirmed_at TIMESTAMP,
        confirmed_by VARCHAR(255)
      )
    `);
    // Cotação USD→BRL congelada no momento do pagamento (valor em real da cobrança paga
    // não muda depois; só as em aberto variam com a cotação atual).
    await pool.query(`ALTER TABLE invoices ADD COLUMN IF NOT EXISTS paid_usd_brl_rate REAL`);
    // Backfill (uma vez): cobranças JÁ pagas sem cotação salva congelam na cotação ATUAL agora.
    // Não temos o dólar histórico da época, então usamos o de hoje pra elas pararem de variar.
    try {
      const rate = await exchange.getUsdBrlRate();
      if (rate) {
        const bf = await pool.query(
          `UPDATE invoices SET paid_usd_brl_rate = $1
           WHERE paid_usd_brl_rate IS NULL AND status IN ('paid', 'confirmed')`,
          [rate]
        );
        if (bf.rowCount > 0) console.log(`[DB] Backfill cotação em ${bf.rowCount} cobrança(s) paga(s) @ ${rate}`);
      }
    } catch (e) { console.error('[DB] Backfill cotação falhou:', e.message); }

    // Coluna de bloqueio no streamer
    await pool.query(`ALTER TABLE streamer ADD COLUMN IF NOT EXISTS is_blocked BOOLEAN DEFAULT false`);
    await pool.query(`ALTER TABLE streamer ADD COLUMN IF NOT EXISTS billing_type VARCHAR(20) DEFAULT 'view_hours'`);
    await pool.query(`ALTER TABLE streamer ADD COLUMN IF NOT EXISTS fixed_weekly_value REAL DEFAULT 0`);

    // Restream pra Rumble (consumido pelo app rumble-relay): servidor RTMP + chave + on/off por streamer
    await pool.query(`ALTER TABLE streamer ADD COLUMN IF NOT EXISTS rumble_server VARCHAR(500)`);
    await pool.query(`ALTER TABLE streamer ADD COLUMN IF NOT EXISTS rumble_key VARCHAR(500)`);
    await pool.query(`ALTER TABLE streamer ADD COLUMN IF NOT EXISTS rumble_enabled BOOLEAN DEFAULT false`);
    await pool.query(`ALTER TABLE streamer ADD COLUMN IF NOT EXISTS iframe_rumble TEXT`);
    await pool.query(`ALTER TABLE streamer ALTER COLUMN iframe_rumble TYPE TEXT`);
    await pool.query(`ALTER TABLE streamer ADD COLUMN IF NOT EXISTS rumble_api_url TEXT`);

    // Multi-plataforma: link Twitch opcional + métricas separadas por plataforma
    // Coluna 'platform' já existia em live_viewer_sessions (guarda OS), por isso usamos 'stream_platform'
    await pool.query(`ALTER TABLE streamer ADD COLUMN IF NOT EXISTS new_plataform VARCHAR(255)`);
    await pool.query(`ALTER TABLE live_viewer_sessions ADD COLUMN IF NOT EXISTS stream_platform VARCHAR(20) DEFAULT 'kick'`);
    await pool.query(`ALTER TABLE lives ADD COLUMN IF NOT EXISTS unique_kick INTEGER DEFAULT 0`);
    await pool.query(`ALTER TABLE lives ADD COLUMN IF NOT EXISTS unique_twitch INTEGER DEFAULT 0`);
    await pool.query(`ALTER TABLE lives ADD COLUMN IF NOT EXISTS avg_viewers_kick REAL DEFAULT 0`);
    await pool.query(`ALTER TABLE lives ADD COLUMN IF NOT EXISTS avg_viewers_twitch REAL DEFAULT 0`);


    // Renomear kick_username → username (idempotente — só roda se kick_username ainda existe e username não)
    await pool.query(`
      DO $$
      BEGIN
        IF EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'live_viewer_sessions' AND column_name = 'kick_username')
           AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                           WHERE table_name = 'live_viewer_sessions' AND column_name = 'username') THEN
          ALTER TABLE live_viewer_sessions RENAME COLUMN kick_username TO username;
        END IF;
      END $$;
    `);
    // Garantir que a coluna username existe (caso nem kick_username nem username existissem)
    await pool.query(`ALTER TABLE live_viewer_sessions ADD COLUMN IF NOT EXISTS username VARCHAR(255) DEFAULT ''`);

    // Índices para queries rápidas
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_lives_streamer ON lives(id_streamer)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_lives_status ON lives(status)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_live_viewer_sessions_live ON live_viewer_sessions(live_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_live_viewer_sessions_ip ON live_viewer_sessions(ip)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_invoices_streamer ON invoices(id_streamer)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_invoices_status ON invoices(status)`);

    // PixGG — anti-reuso de donate: cada donate.id so paga uma cobranca (PK garante)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS pixgg_donations_used (
        donate_id BIGINT PRIMARY KEY,
        id_streamer VARCHAR(255),
        invoice_id INTEGER,
        total_amount REAL,
        donator_nickname VARCHAR(255),
        used_at TIMESTAMP DEFAULT NOW()
      )
    `);
    // Flag: streamer estava AO VIVO quando a cobranca foi gerada → bloqueia ao fim da live
    await pool.query(`ALTER TABLE streamer ADD COLUMN IF NOT EXISTS pending_block BOOLEAN DEFAULT false`);

    // Version gate — bloqueia .exe desatualizado no startup do streamer-app.
    // Consultada por /api/app/version-check. Bumpar required_version força update.
    await pool.query(`
      CREATE TABLE IF NOT EXISTS app_versions (
        app_name         VARCHAR(50)  PRIMARY KEY,
        required_version VARCHAR(20)  NOT NULL,
        download_url     TEXT,
        message          TEXT,
        updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
      )
    `);
    // Seed inicial: registra streamer_app com a versao corrente (0.1.0).
    // Pra forcar update: UPDATE app_versions SET required_version='0.2.0' WHERE app_name='streamer_app';
    await pool.query(`
      INSERT INTO app_versions (app_name, required_version, download_url, message)
      VALUES ('streamer_app', '0.1.0',
              'https://discord.gg/SEU_INVITE_AQUI',
              'Por favor atualize seu aplicativo. Link disponível no Discord.')
      ON CONFLICT (app_name) DO NOTHING
    `);

    console.log('[DB] Tabelas streamer, lives, live_viewer_sessions, invoices, app_versions prontas');

    const count = await pool.query('SELECT COUNT(*) FROM streamer');
    console.log(`[DB] Streamers cadastrados: ${count.rows[0].count}`);
  } catch (err) {
    console.error('[DB] Erro ao criar tabela:', err.message);
  }
}

// ══════════════════════════════════════════════
// ── ROTAS ──
// ══════════════════════════════════════════════

// ── GET /api/app/version-check ──
// Gate de versao usado pelo streamer-app no startup. Compara versao instalada
// com app_versions.required_version. Semantica: app_version >= required = OK.
// 200 = ok, 426 Upgrade Required = bloqueio (UI mostra modal "Atualize via Discord").
// Fail-open no app: erro 5xx/timeout permite startup (nao brica streamers se API cair).
app.get('/api/app/version-check', requireApiKey, async (req, res) => {
  try {
    const { app: appName, version } = req.query;
    if (!appName || !version) {
      return res.status(400).json({ message: 'Parametros app e version obrigatorios' });
    }

    const result = await pool.query(
      'SELECT required_version, download_url, message FROM app_versions WHERE app_name = $1',
      [appName]
    );

    // App nao registrado na tabela = sem gate (nao bloqueia)
    if (result.rows.length === 0) {
      return res.json({ status: 'ok', registered: false });
    }

    const { required_version, download_url, message } = result.rows[0];

    if (versionGte(version, required_version)) {
      return res.json({ status: 'ok', required: required_version });
    }

    // 426 Upgrade Required — RFC 7231
    return res.status(426).json({
      status: 'outdated',
      required: required_version,
      current: version,
      message: message || 'Por favor atualize seu aplicativo.',
      download_url: download_url || null,
    });
  } catch (e) {
    console.error('[VERSION-CHECK]', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// Compara duas versoes 'X.Y.Z'. Retorna true se a >= b. Trata strings nao-numericas como 0.
function versionGte(a, b) {
  const parse = v => String(v).split('.').map(n => parseInt(n, 10) || 0);
  const ap = parse(a);
  const bp = parse(b);
  const len = Math.max(ap.length, bp.length);
  for (let i = 0; i < len; i++) {
    const av = ap[i] || 0;
    const bv = bp[i] || 0;
    if (av > bv) return true;
    if (av < bv) return false;
  }
  return true; // igual = aceita
}

// ── GET /api/auth/validate — valida se JWT ainda eh valido (usado pelo streamer-app no startup) ──
// requireAuth ja faz toda a validacao: assina + expiracao. Se passar, 200; se nao, 401.
app.get('/api/auth/validate', requireAuth, (req, res) => {
  res.json({
    valid: true,
    role: req.auth.role,
    id_streamer: req.auth.id_streamer || null,
    login: req.auth.login || null,
  });
});

// ── POST /api/auth/login ──
app.post('/api/auth/login', async (req, res) => {
  try {
    const { login, senha } = req.body;
    if (!login || !senha) {
      return res.status(400).json({ message: 'Login e senha são obrigatórios' });
    }

    // Check admin credentials (env vars)
    if (ADMIN_USER && login === ADMIN_USER && senha === ADMIN_PASSWORD) {
      const token = jwt.sign({ role: 'admin', login }, JWT_SECRET, { expiresIn: '7d' });
      return res.json({ token, role: 'admin', user: 'Admin', id_streamer: null });
    }

    // Check streamer credentials (DB)
    const result = await pool.query(
      'SELECT id, "user", id_streamer, login, senha FROM streamer WHERE login = $1',
      [login]
    );
    if (result.rows.length === 0) {
      return res.status(401).json({ message: 'Login ou senha incorretos' });
    }

    const streamer = result.rows[0];
    if (!streamer.senha) {
      return res.status(401).json({ message: 'Senha não configurada para este streamer' });
    }

    const valid = await bcrypt.compare(senha, streamer.senha);
    if (!valid) {
      return res.status(401).json({ message: 'Login ou senha incorretos' });
    }

    const token = jwt.sign(
      { role: 'streamer', id_streamer: streamer.id_streamer, login: streamer.login },
      JWT_SECRET,
      { expiresIn: '7d' }
    );
    return res.json({ token, role: 'streamer', user: streamer.user, id_streamer: streamer.id_streamer });
  } catch (e) {
    console.error('[AUTH] Erro no login:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// ── Streamer endpoints (área do streamer autenticado) ──

// GET /api/streamer/me/lives — lives do streamer autenticado
app.get('/api/streamer/me/lives', requireAuth, async (req, res) => {
  try {
    if (req.auth.role !== 'streamer' || !req.auth.id_streamer) {
      return res.status(403).json({ message: 'Apenas streamers podem acessar' });
    }
    const limit = parseInt(req.query.limit) || 50;
    const result = await pool.query(
      'SELECT * FROM lives WHERE LOWER(id_streamer) = LOWER($1) ORDER BY started_at DESC LIMIT $2',
      [req.auth.id_streamer, limit]
    );
    return res.json({ lives: result.rows });
  } catch (e) {
    console.error('[STREAMER] Erro lives:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// GET /api/streamer/me/lives/:live_id — detalhe de live sem dados sensíveis
app.get('/api/streamer/me/lives/:live_id', requireAuth, async (req, res) => {
  try {
    if (req.auth.role !== 'streamer' || !req.auth.id_streamer) {
      return res.status(403).json({ message: 'Apenas streamers podem acessar' });
    }
    const { live_id } = req.params;
    const liveResult = await pool.query(
      'SELECT * FROM lives WHERE id = $1 AND LOWER(id_streamer) = LOWER($2)',
      [live_id, req.auth.id_streamer]
    );
    if (liveResult.rows.length === 0) {
      return res.status(404).json({ message: 'Live não encontrada' });
    }

    const viewersResult = await pool.query(
      `SELECT username, platform, browser, is_mobile, total_seconds, segments_loaded, estimated_mb,
              quality_history, joined_at, last_seen, stream_platform
       FROM live_viewer_sessions WHERE live_id = $1 ORDER BY joined_at ASC`,
      [live_id]
    );

    return res.json({
      live: liveResult.rows[0],
      viewers: viewersResult.rows.map(v => ({ ...v, stream_platform: v.stream_platform || 'kick' })),
    });
  } catch (e) {
    console.error('[STREAMER] Erro live detail:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// GET /api/streamer/me/payment — cálculo de pagamento do streamer
app.get('/api/streamer/me/payment', requireAuth, async (req, res) => {
  try {
    if (req.auth.role !== 'streamer' || !req.auth.id_streamer) {
      return res.status(403).json({ message: 'Apenas streamers podem acessar' });
    }

    const { start_date, end_date, platform_filter } = req.query;
    if (!start_date || !end_date) {
      return res.status(400).json({ message: 'start_date e end_date obrigatórios' });
    }

    const pf = platform_filter || 'all';

    // Buscar value_per_view_hour e commission do streamer
    const streamerResult = await pool.query(
      'SELECT value_per_view_hour, commission FROM streamer WHERE LOWER(id_streamer) = LOWER($1)',
      [req.auth.id_streamer]
    );
    if (streamerResult.rows.length === 0) {
      return res.status(404).json({ message: 'Streamer não encontrado' });
    }
    const vpvh = streamerResult.rows[0].value_per_view_hour || 0;
    const commissionPct = streamerResult.rows[0].commission || 0;

    // Buscar lives no período
    const livesResult = await pool.query(
      `SELECT l.id, l.started_at, l.ended_at, l.duration_seconds,
              l.peak_viewers, l.total_unique_viewers, l.avg_viewers,
              l.avg_viewers_mobile, l.avg_viewers_desktop, l.status,
              COALESCE(SUM(CASE WHEN vs.is_mobile = true THEN vs.total_seconds ELSE 0 END), 0)::INTEGER as mobile_seconds,
              COALESCE(SUM(CASE WHEN vs.is_mobile = false THEN vs.total_seconds ELSE 0 END), 0)::INTEGER as desktop_seconds,
              COALESCE(SUM(vs.total_seconds), 0)::INTEGER as all_seconds
       FROM lives l
       LEFT JOIN live_viewer_sessions vs ON vs.live_id = l.id
       WHERE LOWER(l.id_streamer) = LOWER($1) AND l.started_at >= $2 AND l.started_at <= $3
       GROUP BY l.id
       ORDER BY l.started_at ASC`,
      [req.auth.id_streamer, start_date, end_date + 'T23:59:59']
    );

    let totalRevenue = 0;
    let totalHours = 0;
    const lives = [];

    for (const live of livesResult.rows) {
      const durationSeconds = live.duration_seconds || 0;
      const durationHours = durationSeconds / 3600;
      const avgAll = durationSeconds > 0 ? Math.ceil((live.all_seconds / durationSeconds) * 10) / 10 : (live.avg_viewers || 0);
      const avgMobile = durationSeconds > 0 ? Math.ceil((live.mobile_seconds / durationSeconds) * 10) / 10 : 0;
      const avgDesktop = durationSeconds > 0 ? Math.ceil((live.desktop_seconds / durationSeconds) * 10) / 10 : 0;

      let avgForRevenue;
      if (pf === 'mobile') avgForRevenue = avgMobile;
      else if (pf === 'desktop') avgForRevenue = avgDesktop;
      else avgForRevenue = avgAll;

      const vpvs = vpvh / 3600; // value per view/second
      const revenue = avgForRevenue * durationSeconds * vpvs;

      lives.push({
        live_id: live.id,
        started_at: live.started_at,
        ended_at: live.ended_at,
        duration_seconds: durationSeconds,
        duration_hours: Math.round(durationHours * 100) / 100,
        avg_viewers: Math.ceil(avgAll),
        avg_viewers_mobile: Math.ceil(avgMobile),
        avg_viewers_desktop: Math.ceil(avgDesktop),
        peak_viewers: live.peak_viewers,
        total_unique_viewers: live.total_unique_viewers,
        revenue: Math.round(revenue * 100) / 100,
        status: live.status,
      });

      totalHours += durationHours;
      totalRevenue += revenue;
    }

    const totalCommission = totalRevenue * (commissionPct / 100);

    return res.json({
      value_per_view_hour: vpvh,
      commission_pct: commissionPct,
      period: { start: start_date, end: end_date },
      platform_filter: pf,
      total_hours: Math.round(totalHours * 100) / 100,
      total_revenue: Math.round(totalRevenue * 100) / 100,
      total_commission: Math.round(totalCommission * 100) / 100,
      your_earnings: Math.round(totalCommission * 100) / 100,
      lives,
    });
  } catch (e) {
    console.error('[STREAMER] Erro payment:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// ── POST /api/streamer/me/log-upload — Streamer envia chunk do log do app ──
// Recebe bytes raw do log (Content-Type: application/octet-stream).
// Headers:
//   X-Log-Filename: '2026-05-15.log' (validado anti path-traversal)
//   X-Log-Mode: 'append' | 'replace'
// Escreve em log-app/{id_streamer}/{filename}. Permite visibilidade real-time
// do que ta acontecendo no app do streamer (atraso ate LOG_UPLOAD_INTERVAL).
const LOG_APP_DIR = path.join(__dirname, 'log-app');
const LOG_UPLOAD_MAX_BYTES = 50 * 1024 * 1024; // 50MB por request

app.post(
  '/api/streamer/me/log-upload',
  requireAuth,
  express.raw({ type: 'application/octet-stream', limit: LOG_UPLOAD_MAX_BYTES }),
  async (req, res) => {
    try {
      if (req.auth.role !== 'streamer') {
        return res.status(403).json({ message: 'Apenas streamers' });
      }

      const filename = String(req.headers['x-log-filename'] || '').trim();
      const mode = String(req.headers['x-log-mode'] || 'append').toLowerCase();

      // Anti path-traversal: aceita só YYYY-MM-DD.log ou simlar (alfanum + . - _)
      if (!/^[A-Za-z0-9_.\-]+\.log$/.test(filename)) {
        return res.status(400).json({ message: 'Filename invalido (esperado YYYY-MM-DD.log)' });
      }
      if (mode !== 'append' && mode !== 'replace') {
        return res.status(400).json({ message: 'X-Log-Mode deve ser append ou replace' });
      }

      const body = req.body;
      if (!Buffer.isBuffer(body) || body.length === 0) {
        return res.status(400).json({ message: 'Body vazio ou invalido (Content-Type deve ser application/octet-stream)' });
      }

      const idStreamer = req.auth.id_streamer;
      const dir = path.join(LOG_APP_DIR, idStreamer);
      await fs.promises.mkdir(dir, { recursive: true });

      const filePath = path.join(dir, filename);
      if (mode === 'replace') {
        await fs.promises.writeFile(filePath, body);
      } else {
        await fs.promises.appendFile(filePath, body);
      }

      return res.json({ ok: true, bytes: body.length, mode, path: `log-app/${idStreamer}/${filename}` });
    } catch (e) {
      console.error('[LOG-UPLOAD]', e.message);
      return res.status(500).json({ message: e.message });
    }
  }
);

// ── PUT /api/streamer/me/password — Streamer troca a própria senha ──
app.put('/api/streamer/me/password', requireAuth, async (req, res) => {
  try {
    if (req.auth.role !== 'streamer') {
      return res.status(403).json({ message: 'Apenas streamers podem trocar a senha por aqui' });
    }

    const { senha_atual, nova_senha } = req.body;
    if (!senha_atual || !nova_senha) {
      return res.status(400).json({ message: 'Campos "senha_atual" e "nova_senha" são obrigatórios' });
    }
    if (nova_senha.length < 6) {
      return res.status(400).json({ message: 'A nova senha deve ter no mínimo 6 caracteres' });
    }

    const result = await pool.query(
      'SELECT senha FROM streamer WHERE LOWER(id_streamer) = LOWER($1)',
      [req.auth.id_streamer]
    );
    if (result.rows.length === 0 || !result.rows[0].senha) {
      return res.status(404).json({ message: 'Streamer não encontrado ou sem senha configurada' });
    }

    const valid = await bcrypt.compare(senha_atual, result.rows[0].senha);
    if (!valid) {
      return res.status(401).json({ message: 'Senha atual incorreta' });
    }

    const hashed = await bcrypt.hash(nova_senha, 10);
    await pool.query(
      'UPDATE streamer SET senha = $1 WHERE LOWER(id_streamer) = LOWER($2)',
      [hashed, req.auth.id_streamer]
    );

    return res.json({ updated: true });
  } catch (e) {
    console.error('[STREAMER] Erro ao trocar senha:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// ══════════════════════════════════════════════
// ── SISTEMA DE COBRANÇA SEMANAL (QUINTA A QUINTA) ──
// ══════════════════════════════════════════════

// Gera cobranças automáticas pra todos os streamers
// Gera cobrança pra um streamer específico num período
async function generateInvoiceForStreamer(streamer, periodStart, periodEnd) {
  try {
    // Checar duplicata pra esse streamer+período
    const dup = await pool.query(
      'SELECT id FROM invoices WHERE LOWER(id_streamer) = LOWER($1) AND period_start = $2 AND period_end = $3 LIMIT 1',
      [streamer.id_streamer, periodStart, periodEnd]
    );
    if (dup.rows.length > 0) return false;

    const billingType = streamer.billing_type || 'view_hours';
    let totalRevenue = 0;
    let totalHours = 0;
    let amountDue = 0;

    if (billingType === 'fixed') {
      // Valor fixo semanal — não precisa calcular view hours
      amountDue = streamer.fixed_weekly_value || 0;
      if (amountDue <= 0) return false;

      // Buscar horas apenas pra informação (não afeta o valor)
      const livesResult = await pool.query(
        `SELECT COALESCE(SUM(l.duration_seconds), 0)::INTEGER as total_seconds
         FROM lives l
         WHERE LOWER(l.id_streamer) = LOWER($1)
           AND l.started_at >= $2 AND l.started_at <= $3`,
        [streamer.id_streamer, periodStart, periodEnd + 'T23:59:59']
      );
      totalHours = Math.round((livesResult.rows[0]?.total_seconds || 0) / 3600 * 100) / 100;
      totalRevenue = amountDue;
    } else {
      // Cálculo por view hours (padrão)
      const vpvh = streamer.value_per_view_hour || 0;
      if (vpvh <= 0) return false;

      const livesResult = await pool.query(
        `SELECT l.id, l.duration_seconds,
                COALESCE(SUM(vs.total_seconds), 0)::INTEGER as all_seconds
         FROM lives l
         LEFT JOIN live_viewer_sessions vs ON vs.live_id = l.id
         WHERE LOWER(l.id_streamer) = LOWER($1)
           AND l.started_at >= $2 AND l.started_at <= $3
         GROUP BY l.id`,
        [streamer.id_streamer, periodStart, periodEnd + 'T23:59:59']
      );

      for (const live of livesResult.rows) {
        const dur = live.duration_seconds || 0;
        if (dur <= 0) continue;
        const avgViewers = Math.ceil((live.all_seconds / dur) * 10) / 10;
        totalRevenue += avgViewers * dur * (vpvh / 3600);
        totalHours += dur / 3600;
      }

      totalRevenue = Math.round(totalRevenue * 100) / 100;
      totalHours = Math.round(totalHours * 100) / 100;
      const commissionPct = streamer.commission || 0;
      amountDue = Math.round(totalRevenue * (commissionPct / 100) * 100) / 100;
    }

    await pool.query(
      `INSERT INTO invoices (id_streamer, period_start, period_end, total_hours, total_revenue, amount_due, status)
       VALUES ($1, $2, $3, $4, $5, $6, 'pending')`,
      [streamer.id_streamer.toLowerCase(), periodStart, periodEnd, totalHours, totalRevenue, amountDue]
    );
    console.log(`[BILLING] Cobrança gerada (${billingType}): ${streamer.user} | ${periodStart}→${periodEnd} | Due: $${amountDue}`);
    return true;
  } catch (e) {
    console.error(`[BILLING] Erro gerando cobrança para ${streamer.id_streamer}:`, e.message);
    return false;
  }
}

// Gera cobranças automáticas pra todos os streamers
async function generateWeeklyInvoices() {
  try {
    // Calcular periodo: sexta passada → quinta (roda na sexta 00h, fecha a semana sex→qui).
    // Ex: roda sexta 10/04 → periodo 03/04 (sexta) ate 09/04 (quinta) = 7 dias.
    // Antes era startDate = endDate - 7 (qui→qui, 8 dias inclusivos) — fazia toda quinta
    // entrar em DOIS invoices consecutivos (double-billing). Fix: usar - 6 pra startDate
    // cair em SEXTA, garantindo periodos contiguos sem overlap nem buraco.
    const now = new Date();
    const brNow = new Date(now.toLocaleString('en-US', { timeZone: 'America/Sao_Paulo' }));
    // endDate = quinta mais recente (ontem se hoje é sexta)
    const dayOfWeek = brNow.getDay(); // 0=dom, 5=sex
    const endDate = new Date(brNow);
    // Voltar pro dia da semana 4 (quinta) mais recente
    const daysBack = (dayOfWeek + 3) % 7; // sex=1, sab=2, dom=3, seg=4, ter=5, qua=6, qui=0
    endDate.setDate(endDate.getDate() - daysBack);
    endDate.setHours(0, 0, 0, 0);
    // startDate = 6 dias antes do endDate → cai na SEXTA (periodo sex→qui = 7 dias)
    const startDate = new Date(endDate);
    startDate.setDate(startDate.getDate() - 6);

    const periodStart = startDate.toISOString().split('T')[0];
    const periodEnd = endDate.toISOString().split('T')[0];

    // Checar se já gerou pra esse período
    const existing = await pool.query(
      'SELECT id FROM invoices WHERE period_start = $1 AND period_end = $2 LIMIT 1',
      [periodStart, periodEnd]
    );
    if (existing.rows.length > 0) {
      console.log(`[BILLING] Cobranças já geradas para ${periodStart} → ${periodEnd}, pulando.`);
      return;
    }

    // Buscar todos os streamers e gerar cobrança pra cada um
    const streamers = await pool.query('SELECT id, "user", id_streamer, value_per_view_hour, commission, billing_type, fixed_weekly_value FROM streamer');
    let generated = 0;
    let blocked = 0;
    for (const s of streamers.rows) {
      const ok = await generateInvoiceForStreamer(s, periodStart, periodEnd);
      if (!ok) continue;
      generated++;
      // Bloqueio automatico: offline bloqueia ja; ao vivo agenda bloqueio pro fim da live
      const isLive = !!activeLives[s.id_streamer.toLowerCase()];
      if (isLive) {
        await pool.query('UPDATE streamer SET pending_block = true WHERE LOWER(id_streamer) = LOWER($1)', [s.id_streamer]);
        console.log(`[BILLING] ${s.user} ao vivo — bloqueio agendado pro fim da live`);
      } else {
        await pool.query('UPDATE streamer SET is_blocked = true WHERE LOWER(id_streamer) = LOWER($1)', [s.id_streamer]);
        blocked++;
      }
    }
    invalidateStreamerCache();
    console.log(`[BILLING] ${generated} cobranças geradas para ${periodStart} → ${periodEnd} | ${blocked} bloqueados (offline)`);
  } catch (e) {
    console.error('[BILLING] Erro ao gerar cobranças:', e.message);
  }
}

// Cron interno: checar a cada minuto se é sexta-feira 00:00 BRT
// Gera cobranças referentes à semana sexta→quinta (7 dias) que acabou de fechar
let _lastBillingCheck = '';
setInterval(() => {
  const brNow = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/Sao_Paulo' }));
  const day = brNow.getDay(); // 5 = sexta
  const hour = brNow.getHours();
  const minute = brNow.getMinutes();
  const key = `${brNow.getFullYear()}-${brNow.getMonth()}-${brNow.getDate()}`;
  if (day === 5 && hour === 0 && minute < 2 && _lastBillingCheck !== key) {
    _lastBillingCheck = key;
    generateWeeklyInvoices();
  }
}, 60000);

// ── GET /api/admin/invoices — Listar cobranças (admin) ──
app.get('/api/admin/invoices', requireApiKey, async (req, res) => {
  try {
    const { status, id_streamer } = req.query;
    let q = `SELECT i.*, s."user" as streamer_name, COALESCE(s.is_blocked, false) as is_blocked
             FROM invoices i
             LEFT JOIN streamer s ON LOWER(s.id_streamer) = LOWER(i.id_streamer)`;
    const conditions = [];
    const values = [];
    if (status) { conditions.push(`i.status = $${values.length + 1}`); values.push(status); }
    if (id_streamer) { conditions.push(`LOWER(i.id_streamer) = LOWER($${values.length + 1})`); values.push(id_streamer); }
    if (conditions.length) q += ' WHERE ' + conditions.join(' AND ');
    q += ' ORDER BY i.created_at DESC';
    const result = await pool.query(q, values);
    // Dias ate o token do PixGG expirar — pro admin renovar antes de quebrar o desbloqueio
    const pixggTokenDaysLeft = pixgg.getTokenDaysLeft(process.env.PIXGG_ACCESS_TOKEN || '');
    const usdBrlRate = await exchange.getUsdBrlRate();
    return res.json({ total: result.rows.length, invoices: result.rows, pixggTokenDaysLeft, usd_brl_rate: usdBrlRate });
  } catch (e) {
    return res.status(500).json({ message: e.message });
  }
});

// ── POST /api/admin/invoices/generate — Gerar cobrança manual pra um streamer (admin) ──
app.post('/api/admin/invoices/generate', requireApiKey, async (req, res) => {
  try {
    const { id_streamer, start_date, end_date } = req.body;
    if (!id_streamer || !start_date || !end_date) {
      return res.status(400).json({ message: 'Campos obrigatórios: id_streamer, start_date, end_date' });
    }
    const streamer = await pool.query(
      'SELECT id, "user", id_streamer, value_per_view_hour, commission, billing_type, fixed_weekly_value FROM streamer WHERE LOWER(id_streamer) = LOWER($1)',
      [id_streamer]
    );
    if (streamer.rows.length === 0) return res.status(404).json({ message: 'Streamer não encontrado' });
    const ok = await generateInvoiceForStreamer(streamer.rows[0], start_date, end_date);
    if (!ok) return res.status(409).json({ message: 'Cobrança já existe para esse período ou streamer sem rate configurado' });
    return res.json({ generated: true });
  } catch (e) {
    return res.status(500).json({ message: e.message });
  }
});

// ── POST /api/admin/invoices/:id/confirm — Confirmar pagamento (admin) ──
app.post('/api/admin/invoices/:id/confirm', requireApiKey, async (req, res) => {
  try {
    const result = await pool.query(
      `UPDATE invoices SET status = 'confirmed', confirmed_at = NOW(), confirmed_by = 'admin'
       WHERE id = $1 AND status = 'paid' RETURNING id`,
      [req.params.id]
    );
    if (result.rows.length === 0) return res.status(404).json({ message: 'Cobrança não encontrada ou não está marcada como paga' });
    return res.json({ confirmed: true, id: result.rows[0].id });
  } catch (e) {
    return res.status(500).json({ message: e.message });
  }
});

// ── POST /api/admin/invoices/:id/overdue — Marcar como atrasada (admin) ──
app.post('/api/admin/invoices/:id/overdue', requireApiKey, async (req, res) => {
  try {
    const result = await pool.query(
      `UPDATE invoices SET status = 'overdue' WHERE id = $1 AND status = 'pending' RETURNING id`,
      [req.params.id]
    );
    if (result.rows.length === 0) return res.status(404).json({ message: 'Cobrança não encontrada ou não está pendente' });
    return res.json({ overdue: true, id: result.rows[0].id });
  } catch (e) {
    return res.status(500).json({ message: e.message });
  }
});

// ── POST /api/admin/invoices/:id/mark-paid — Admin marca pago DIRETO (pagamento fora do PixGG) ──
// Pra quando o streamer paga direto pro admin. Vai pra 'confirmed' (quitado) + desbloqueia.
// Atualiza os dois lados: admin vê "Confirmado", streamer vê "Pago" e some o bloqueio.
app.post('/api/admin/invoices/:id/mark-paid', requireApiKey, async (req, res) => {
  try {
    const payRate = await exchange.getUsdBrlRate();
    const result = await pool.query(
      `UPDATE invoices SET status = 'confirmed', paid_at = COALESCE(paid_at, NOW()), confirmed_at = NOW(),
              confirmed_by = 'admin', paid_usd_brl_rate = COALESCE(paid_usd_brl_rate, $2)
       WHERE id = $1 AND status IN ('pending', 'overdue') RETURNING id, id_streamer`,
      [req.params.id, payRate]
    );
    if (result.rows.length === 0) return res.status(404).json({ message: 'Cobrança não encontrada ou já paga' });
    const idStreamer = result.rows[0].id_streamer;
    // Pagou → desbloqueia o streamer (limpa bloqueio imediato e o agendado)
    await pool.query('UPDATE streamer SET is_blocked = false, pending_block = false WHERE LOWER(id_streamer) = LOWER($1)', [idStreamer]);
    invalidateStreamerCache();
    console.log(`[ADMIN] Invoice #${result.rows[0].id} marcada como paga (direto) + ${idStreamer} desbloqueado`);
    return res.json({ paid: true, id: result.rows[0].id, id_streamer: idStreamer });
  } catch (e) {
    return res.status(500).json({ message: e.message });
  }
});

// ── POST /api/admin/streamer/:id_streamer/block — Bloquear streamer (admin) ──
app.post('/api/admin/streamer/:id_streamer/block', requireApiKey, async (req, res) => {
  try {
    await pool.query('UPDATE streamer SET is_blocked = true WHERE LOWER(id_streamer) = LOWER($1)', [req.params.id_streamer]);
    invalidateStreamerCache();
    console.log(`[ADMIN] Streamer ${req.params.id_streamer} BLOQUEADO`);
    return res.json({ blocked: true });
  } catch (e) {
    return res.status(500).json({ message: e.message });
  }
});

// ── POST /api/admin/streamer/:id_streamer/unblock — Desbloquear streamer (admin) ──
app.post('/api/admin/streamer/:id_streamer/unblock', requireApiKey, async (req, res) => {
  try {
    await pool.query('UPDATE streamer SET is_blocked = false WHERE LOWER(id_streamer) = LOWER($1)', [req.params.id_streamer]);
    invalidateStreamerCache();
    console.log(`[ADMIN] Streamer ${req.params.id_streamer} DESBLOQUEADO`);
    return res.json({ unblocked: true });
  } catch (e) {
    return res.status(500).json({ message: e.message });
  }
});

// ── GET /api/admin/pixgg-token — Dias restantes do token PixGG (leve: só decodifica o JWT) ──
// Usado pelo banner do dashboard admin. NAO chama o PixGG (rápido, pode ser chamado sempre).
app.get('/api/admin/pixgg-token', requireApiKey, (req, res) => {
  try {
    const token = process.env.PIXGG_ACCESS_TOKEN || '';
    return res.json({
      configured: !!token,
      daysLeft: pixgg.getTokenDaysLeft(token),
      expiresAt: pixgg.getTokenExpiry(token),
    });
  } catch (e) {
    return res.status(500).json({ message: e.message });
  }
});

// ── GET /api/admin/pixgg-test — Testa se o servidor passa pelo WAF do PixGG ──
// TEMPORARIO: valida o bloqueante (WAF + token) antes de construir o fluxo de
// desbloqueio automatico. Remover apos confirmar.
app.get('/api/admin/pixgg-test', requireApiKey, async (req, res) => {
  try {
    const daysLeft = pixgg.getTokenDaysLeft(process.env.PIXGG_ACCESS_TOKEN || '');
    const result = await pixgg.getDonations({ pageSize: 3 });
    return res.json({
      tokenDaysLeft: daysLeft,           // dias ate o token expirar (null = nao configurado/invalido)
      passouWAF: result.ok,              // true = servidor conseguiu falar com o PixGG
      httpStatus: result.status,         // 200 ok | 401 token | 403 WAF
      error: result.error,
      sampleCount: result.donations.length,
      sample: result.donations.slice(0, 2).map(d => ({
        nick: d.donatorNickname, total: d.totalAmount, status: d.status, date: d.approvedDate,
      })),
    });
  } catch (e) {
    return res.status(500).json({ message: e.message });
  }
});

// ── GET /api/streamer/me/invoices — Listar cobranças do streamer autenticado ──
app.get('/api/streamer/me/invoices', requireAuth, async (req, res) => {
  try {
    if (req.auth.role !== 'streamer') return res.status(403).json({ message: 'Apenas streamers' });
    const result = await pool.query(
      'SELECT * FROM invoices WHERE LOWER(id_streamer) = LOWER($1) ORDER BY created_at DESC',
      [req.auth.id_streamer]
    );
    // Estado de bloqueio do proprio streamer (pro banner de status no dashboard)
    const st = await pool.query(
      'SELECT COALESCE(is_blocked, false) as is_blocked FROM streamer WHERE LOWER(id_streamer) = LOWER($1) LIMIT 1',
      [req.auth.id_streamer]
    );
    const isBlocked = st.rows[0]?.is_blocked || false;
    // Cotação USD→BRL pra mostrar os valores em real (cache 1h)
    const usdBrlRate = await exchange.getUsdBrlRate();
    return res.json({ invoices: result.rows, is_blocked: isBlocked, usd_brl_rate: usdBrlRate });
  } catch (e) {
    return res.status(500).json({ message: e.message });
  }
});

// ── POST /api/streamer/me/invoices/:id/pay — Streamer confirma pagamento (valida no PixGG) ──
app.post('/api/streamer/me/invoices/:id/pay', requireAuth, async (req, res) => {
  try {
    if (req.auth.role !== 'streamer') return res.status(403).json({ message: 'Apenas streamers' });
    const idStreamer = req.auth.id_streamer;

    // 1. Busca a cobranca pendente do proprio streamer
    const inv = await pool.query(
      `SELECT id, created_at FROM invoices
       WHERE id = $1 AND LOWER(id_streamer) = LOWER($2) AND status IN ('pending', 'overdue') LIMIT 1`,
      [req.params.id, idStreamer]
    );
    if (inv.rows.length === 0) {
      return res.status(404).json({ message: 'Cobrança não encontrada ou já paga' });
    }
    const invoice = inv.rows[0];

    // 2. Procura donate elegivel no PixGG (nick == id, > R$50, aprovado, apos a geracao).
    //    Margem de 24h pra cobrir diferenca de fuso entre PixGG e servidor.
    const afterDate = new Date(new Date(invoice.created_at).getTime() - 24 * 3600 * 1000);
    const pg = await pixgg.findEligibleDonations({ idStreamer, minAmount: 50, afterDate });

    if (!pg.ok) {
      // Falha de infra (token expirado / WAF / rede) — NAO marca pago, pede retry
      console.error(`[PIXGG] Validacao falhou pra ${idStreamer}: ${pg.error} (status ${pg.status})`);
      return res.status(502).json({
        message: 'Não conseguimos validar seu pagamento agora. Tente novamente em alguns minutos.',
      });
    }

    if (pg.eligible.length === 0) {
      return res.status(400).json({
        notFound: true,
        message: `Não encontramos seu pagamento. Confirme: doação de mais de R$50 no PixGG com o apelido igual ao seu ID de streamer ("${idStreamer}").`,
      });
    }

    // 3. Anti-reuso: consome o primeiro donate ainda nao usado. O INSERT com PK
    //    (donate_id) ON CONFLICT serve de trava final contra clique duplo/race.
    let consumed = null;
    for (const d of pg.eligible) {
      const ins = await pool.query(
        `INSERT INTO pixgg_donations_used (donate_id, id_streamer, invoice_id, total_amount, donator_nickname)
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT (donate_id) DO NOTHING RETURNING donate_id`,
        [d.id, idStreamer, invoice.id, d.totalAmount, d.donatorNickname]
      );
      if (ins.rows.length > 0) { consumed = d; break; }
    }

    if (!consumed) {
      return res.status(400).json({
        notFound: true,
        message: 'Esse pagamento já foi usado em uma cobrança anterior. Faça uma nova doação para esta cobrança.',
      });
    }

    // 4. Marca pago + congela a cotação USD→BRL do momento + DESBLOQUEIA
    const payRate = await exchange.getUsdBrlRate();
    await pool.query(
      `UPDATE invoices SET status = 'paid', paid_at = NOW(), paid_usd_brl_rate = $2 WHERE id = $1`,
      [invoice.id, payRate]
    );
    await pool.query(
      'UPDATE streamer SET is_blocked = false, pending_block = false WHERE LOWER(id_streamer) = LOWER($1)',
      [idStreamer]
    );
    invalidateStreamerCache();
    console.log(`[PIXGG] ${idStreamer} pagou invoice #${invoice.id} via donate #${consumed.id} (R$${consumed.totalAmount}) — DESBLOQUEADO`);

    return res.json({
      paid: true,
      unblocked: true,
      id: invoice.id,
      donate: { nick: consumed.donatorNickname, amount: consumed.totalAmount },
    });
  } catch (e) {
    console.error('[PIXGG] Erro no pay:', e.message);
    return res.status(500).json({ message: e.message });
  }
});

// ── GET /health ──
app.get('/health', (req, res) => {
  const stats = {};
  for (const streamerId in activeViewers) {
    stats[streamerId] = Object.keys(activeViewers[streamerId]).length;
  }
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    activeViewers: stats,
    activeLives: Object.keys(activeLives),
  });
});

// ── Monta stream URL a partir do UUID em memória (activeLives) ──
function getStreamUrl(idStreamer, streamer) {
  const live = activeLives[idStreamer];
  if (!live || !live.streamUuid) return '';
  const mediamtxPath = streamer.id_mediamtx || streamer.id_streamer;
  return `https://${CDN_DOMAIN}/${live.streamUuid}/${mediamtxPath}/master.m3u8`;
}

// ── GET /api/queue/status/:id_streamer — Posição na fila (zero DB) ──
app.get('/api/queue/status/:id_streamer', requireApiKey, (req, res) => {
  const id_streamer = req.params.id_streamer.toLowerCase();
  const viewer_uid = req.query.viewer_uid;

  if (!viewer_uid) {
    return res.status(400).json({ message: 'viewer_uid obrigatorio' });
  }

  const result = getQueuePosition(id_streamer, viewer_uid);

  if (!result) {
    return res.json({ status: 'not_found' });
  }

  if (result.status === 'ready') {
    return res.json({ status: 'ready', ticket: result.ticket });
  }

  return res.json({
    status: 'queued',
    position: result.position,
    total: result.total,
    retry_ms: QUEUE_CONFIG.POLL_RETRY_MS,
  });
});

// ── GET /api/streamer/validate/:id_streamer ──
// Query params:
//   - platform=kick|twitch  → plataforma onde o overlay está rodando
// O :id_streamer SEMPRE é o id_streamer do banco (o código que o viewer digita no overlay).
// Se a plataforma não tem o link correspondente cadastrado (link pra Kick, new_plataform
// pra Twitch), retorna erro específico `streamer_not_on_platform`.
// A validação da URL (kick.com/xxx vs twitch.tv/yyy) é feita no próprio overlay,
// comparando com data.streamer.link ou data.streamer.new_plataform.
app.get('/api/streamer/validate/:id_streamer', requireApiKey, async (req, res) => {
  const id_streamer = req.params.id_streamer.toLowerCase();
  const platform = (req.query.platform || 'kick').toLowerCase();

  // Verificar se o streamer tem link da plataforma atual cadastrado.
  // Sem isso, o overlay não deve rodar nessa plataforma pra esse streamer.
  if (platform === 'kick' || platform === 'twitch') {
    const streamerCheck = await getCachedStreamer(id_streamer);
    if (streamerCheck) {
      const linkField = platform === 'twitch' ? 'new_plataform' : 'link';
      const linkValue = streamerCheck[linkField];
      if (!linkValue || !String(linkValue).trim()) {
        console.log(`[VALIDATE] ${platform} bloqueado: streamer "${id_streamer}" sem ${linkField} cadastrado`);
        return res.status(404).json({
          valid: false,
          error: 'streamer_not_on_platform',
          message: 'Streamer não disponível nesta plataforma',
          platform: platform
        });
      }
    }
    // Se streamerCheck for null (id_streamer inexistente), cai no fluxo normal
    // que retorna 404 "Streamer nao encontrado" no processValidate
  }

  // ── Bypass 1: Path check de viewer já conectado (não enfileira) ──
  if (req.query.mode === 'pathcheck') {
    const pcUid = req.query.viewer_uid;
    if (pcUid && activeViewers[id_streamer] && activeViewers[id_streamer][pcUid]) {
      // Viewer já está conectado, processar direto sem fila
      return processValidate(id_streamer, req, res);
    }
  }

  // ── Bypass 2: Viewer com ticket válido (já passou pela fila) ──
  if (req.query.ticket) {
    if (consumeTicket(req.query.ticket, id_streamer)) {
      return processValidate(id_streamer, req, res);
    }
    // Ticket inválido/expirado — cai no fluxo normal da fila
  }

  // ── Gate da fila ──
  const q = getOrCreateQueue(id_streamer);
  if (q.processing < QUEUE_CONFIG.MAX_CONCURRENT) {
    // Tem slot livre — processar agora
    q.processing++;
    try {
      await processValidate(id_streamer, req, res);
    } finally {
      releaseProcessing(id_streamer);
    }
    return;
  }

  // Sem slot — enfileirar o viewer
  const viewerUid = req.query.viewer_uid || 'anon_' + (req.headers['x-forwarded-for'] || req.connection.remoteAddress);
  const pos = enqueueViewer(id_streamer, viewerUid);

  if (!pos) {
    // Fila cheia
    return res.status(503).json({ message: 'Fila cheia, tente novamente em instantes' });
  }

  console.log(`[QUEUE] Viewer ${viewerUid.substring(0, 8)}... enfileirado em "${id_streamer}" | Posição: ${pos.position}/${pos.total}`);
  return res.status(202).json({
    queued: true,
    position: pos.position,
    total: pos.total,
    retry_ms: QUEUE_CONFIG.POLL_RETRY_MS,
  });
});

// ── Cache de streamers do banco (carregado no boot, invalidado apenas em escrita) ──
// Streamers quase nunca mudam e sempre com streamer offline, então cache permanente em memória.
// Invalidado por: POST/PUT/DELETE /api/streamer
const streamerCache = {}; // { id_streamer_lower: streamer_row }
let streamerCacheLoaded = false;

async function loadStreamerCache() {
  const result = await pool.query(
    'SELECT id, "user", link, id_streamer, max_spectators, id_mediamtx, new_plataform, is_blocked, COALESCE(rumble_enabled, false) AS rumble_enabled, iframe_rumble FROM streamer'
  );
  // Limpar cache antes de recarregar
  for (const key in streamerCache) delete streamerCache[key];
  for (const row of result.rows) {
    streamerCache[row.id_streamer.toLowerCase()] = row;
  }
  streamerCacheLoaded = true;
  console.log(`[CACHE] Streamers carregados em memória: ${result.rows.length}`);
}

function invalidateStreamerCache() {
  streamerCacheLoaded = false;
}

async function getCachedStreamer(id_streamer) {
  if (!streamerCacheLoaded) await loadStreamerCache();
  return streamerCache[id_streamer.toLowerCase()] || null;
}

// ── Lógica real do validate (extraída para reuso) ──
async function processValidate(id_streamer, req, res) {
  try {
    console.log(`[VALIDATE] Validando streamer: "${id_streamer}"`);

    const streamer = await getCachedStreamer(id_streamer);

    if (!streamer) {
      console.log(`[VALIDATE] Streamer "${id_streamer}" nao encontrado`);
      return res.status(404).json({
        valid: false,
        message: 'Streamer não encontrado no sistema'
      });
    }

    if (streamer.is_blocked) {
      console.log(`[VALIDATE] Streamer "${id_streamer}" bloqueado`);
      // O app do streamer se identifica com viewer_uid=streamer-app-validate.
      // Pra ELE mostramos o motivo real (inadimplência); pro VIEWER, msg neutra
      // (não expõe publicamente que o streamer está devendo).
      const isStreamerApp = req.query.viewer_uid === 'streamer-app-validate';
      return res.status(403).json({
        valid: false,
        message: isStreamerApp
          ? 'Conta bloqueada por inadimplência. Regularize o pagamento para voltar a transmitir.'
          : 'Streamer não possibilitado de realizar live no momento'
      });
    }

    const currentViewers = getViewerCount(id_streamer);

    console.log(`[VALIDATE] Streamer encontrado: ${streamer.user} | Viewers: ${currentViewers}/${streamer.max_spectators}`);

    // Stream URL vem da memória (activeLives) — sem KV
    const stream_url = getStreamUrl(id_streamer, streamer);

    updateLivePeak(id_streamer);

    const streamEnded = endedStreamers[id_streamer.toLowerCase()] || false;

    // Rumble: viewer assiste pelo embed da Rumble (injetado sobre o Kick) em vez do R2.
    // SÓ libera quando a live está ON — isLive vem do getStreamUrl, que só retorna URL
    // se há live ativa em activeLives (registrada pelo notify do app no início). Offline
    // => sem iframe + stream_url vazio => cai no gate normal de "stream offline" do
    // overlay. A checagem de URL/canal do overlay continua acontecendo antes disso.
    const rumbleEnabled = !!(streamer.rumble_enabled && streamer.iframe_rumble);
    const isLive = !!stream_url && !streamEnded;
    const rumbleActive = rumbleEnabled && isLive;

    // O id do vídeo da Rumble muda a cada live → resolve o id atual via a Live Stream API
    // (cacheado, server-side) e troca no embed. is_live NÃO muda (continua o activeLives).
    let rumbleEmbed = null;
    if (rumbleActive) {
      rumbleEmbed = streamer.iframe_rumble;
      try {
        const freshId = await getRumbleVideoId(id_streamer);
        if (freshId) rumbleEmbed = patchEmbedVideoId(rumbleEmbed, freshId);
      } catch (e) { /* mantém o embed armazenado */ }
    }

    return res.json({
      valid: true,
      stream_ended: streamEnded,
      streamer: {
        ...streamer,
        current_viewers: currentViewers,
        stream_url: (rumbleEnabled || streamEnded) ? '' : stream_url,
        rumble_active: rumbleActive,
        rumble_iframe: rumbleEmbed
      }
    });
  } catch (err) {
    console.error(`[VALIDATE] Erro:`, err.message);
    return res.status(500).json({ valid: false, message: 'Erro interno do servidor' });
  }
}

// ── POST /api/viewer/join — Viewer entra na sala ──
app.post('/api/viewer/join', requireApiKey, async (req, res) => {
  try {
    const id_streamer = (req.body.id_streamer || '').toLowerCase();
    const viewer_uid = req.body.viewer_uid;

    if (!id_streamer || !viewer_uid) {
      return res.status(400).json({ message: 'Campos "id_streamer" e "viewer_uid" sao obrigatorios' });
    }

    // Buscar streamer no banco pra pegar max_spectators
    const result = await pool.query(
      'SELECT max_spectators FROM streamer WHERE LOWER(id_streamer) = LOWER($1)',
      [id_streamer]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: 'Streamer nao encontrado' });
    }

    const maxSpectators = result.rows[0].max_spectators;

    // Check + register atômico (previne race condition em requests concorrentes)
    const joinResult = registerViewerIfAllowed(id_streamer, viewer_uid, maxSpectators);
    if (!joinResult.allowed) {
      logger.live(id_streamer, 'WARN', `[JOIN] Sala cheia para "${id_streamer}": ${joinResult.current}/${joinResult.max}`);
      return res.status(403).json({
        message: 'Sala cheia',
        current_viewers: joinResult.current,
        max_spectators: joinResult.max
      });
    }

    updateLivePeak(id_streamer);
    const newCount = getViewerCount(id_streamer);

    logger.live(id_streamer, 'INFO', `[JOIN] Viewer ${viewer_uid.substring(0, 8)}... entrou em "${id_streamer}" | Viewers: ${newCount}/${maxSpectators || 'ilimitado'}`);

    return res.json({
      joined: true,
      current_viewers: newCount,
      max_spectators: maxSpectators
    });
  } catch (err) {
    logger.live(id_streamer, 'ERROR', '[JOIN] Erro:', err.message);
    return res.status(500).json({ message: 'Erro interno do servidor' });
  }
});

// ── POST /api/viewer/heartbeat — Viewer ainda está assistindo ──
app.post('/api/viewer/heartbeat', requireApiKey, async (req, res) => {
  try {
    const id_streamer = (req.body.id_streamer || '').toLowerCase();
    const viewer_uid = req.body.viewer_uid;

    if (!id_streamer || !viewer_uid) {
      return res.status(400).json({ message: 'Campos obrigatorios faltando' });
    }

    registerViewer(id_streamer, viewer_uid);
    const currentViewers = getViewerCount(id_streamer);

    // Stream URL da memória (sem KV)
    let stream_url = null;
    if (activeLives[id_streamer]) {
      const streamer = await getCachedStreamer(id_streamer);
      if (streamer) stream_url = getStreamUrl(id_streamer, streamer);
    }

    const stream_ended = endedStreamers[id_streamer] || false;

    return res.json({
      active: true,
      current_viewers: currentViewers,
      stream_url,
      stream_ended,
    });
  } catch (err) {
    logger.live(id_streamer, 'ERROR', '[HEARTBEAT] Erro:', err.message);
    return res.status(500).json({ message: 'Erro interno do servidor' });
  }
});

// ── POST /api/viewer/leave — Viewer saiu ──
app.post('/api/viewer/leave', requireApiKey, async (req, res) => {
  try {
    const id_streamer = (req.body.id_streamer || '').toLowerCase();
    const viewer_uid = req.body.viewer_uid;

    if (!id_streamer || !viewer_uid) {
      return res.status(400).json({ message: 'Campos obrigatorios faltando' });
    }

    removeViewer(id_streamer, viewer_uid);
    const currentViewers = getViewerCount(id_streamer);

    logger.live(id_streamer, 'INFO', `[LEAVE] Viewer ${viewer_uid.substring(0, 8)}... saiu de "${id_streamer}" | Viewers: ${currentViewers}`);

    return res.json({
      left: true,
      current_viewers: currentViewers
    });
  } catch (err) {
    logger.live(id_streamer, 'ERROR', '[LEAVE] Erro:', err.message);
    return res.status(500).json({ message: 'Erro interno do servidor' });
  }
});

// ── GET /api/viewer/count/:id_streamer — Ver contagem atual ──
app.get('/api/viewer/count/:id_streamer', requireApiKey, async (req, res) => {
  const { id_streamer } = req.params;
  const key = id_streamer.toLowerCase();
  const currentViewers = getViewerCount(id_streamer);

  // Contar viewers ativos por plataforma (cruzando activeViewers com activeLives)
  let currentMobile = 0;
  let currentDesktop = 0;
  const liveData = activeLives[key];
  const viewers = activeViewers[key];
  if (liveData && viewers) {
    for (const uid of Object.keys(viewers)) {
      const session = liveData.viewerSessions[uid];
      if (session?.is_mobile) currentMobile++;
      else currentDesktop++;
    }
  }

  const result = await pool.query(
    'SELECT max_spectators FROM streamer WHERE LOWER(id_streamer) = LOWER($1)',
    [id_streamer]
  );

  const maxSpectators = result.rows.length > 0 ? result.rows[0].max_spectators : 0;

  return res.json({
    id_streamer,
    current_viewers: currentViewers,
    current_mobile: currentMobile,
    current_desktop: currentDesktop,
    max_spectators: maxSpectators
  });
});

// ── GET /api/streamers (admin) ──
app.get('/api/streamers', requireApiKeyOrAuth, async (req, res) => {
  try {
    console.log('[LIST] Listando todos os streamers');
    const result = await pool.query('SELECT id, "user", link, id_streamer, max_spectators, id_mediamtx, commission, login, value_per_view_hour, new_plataform FROM streamer ORDER BY id');

    // Adicionar contagem de viewers ativos a cada streamer
    const streamers = result.rows.map(s => ({
      ...s,
      current_viewers: getViewerCount(s.id_streamer)
    }));

    console.log(`[LIST] Total: ${result.rows.length} streamers`);
    return res.json(streamers);
  } catch (err) {
    console.error('[LIST] Erro:', err.message);
    return res.status(500).json({ message: 'Erro interno do servidor' });
  }
});

// ── POST /api/streamer (admin) ──
app.post('/api/streamer', requireApiKeyOrAuth, async (req, res) => {
  try {
    const { user, link, id_streamer, max_spectators, id_mediamtx, login, senha, value_per_view_hour, new_plataform } = req.body;
    console.log(`[CREATE] Cadastrando streamer: user="${user}", id_streamer="${id_streamer}", max=${max_spectators || 0}, mediamtx="${id_mediamtx || ''}", login="${login || ''}", twitch="${new_plataform || ''}"`);

    if (!user || !id_streamer) {
      return res.status(400).json({ message: 'Campos "user" e "id_streamer" sao obrigatorios' });
    }

    const hashedSenha = senha ? await bcrypt.hash(senha, 10) : null;

    const result = await pool.query(
      'INSERT INTO streamer ("user", link, id_streamer, max_spectators, id_mediamtx, login, senha, value_per_view_hour, new_plataform) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *',
      [user, link || null, id_streamer, max_spectators || 0, id_mediamtx || null, login || null, hashedSenha, value_per_view_hour || 0, new_plataform || null]
    );

    console.log(`[CREATE] Streamer cadastrado com sucesso: ID ${result.rows[0].id}`);
    invalidateStreamerCache();
    return res.status(201).json(result.rows[0]);
  } catch (err) {
    if (err.code === '23505') {
      console.warn(`[CREATE] Streamer "${req.body.id_streamer}" ja existe`);
      return res.status(409).json({ message: 'Streamer ja cadastrado' });
    }
    console.error('[CREATE] Erro:', err.message);
    return res.status(500).json({ message: 'Erro interno do servidor' });
  }
});

// ── PUT /api/streamer/:id_streamer (admin — atualizar streamer) ──
app.put('/api/streamer/:id_streamer', requireApiKeyOrAuth, async (req, res) => {
  try {
    const { id_streamer } = req.params;
    const { user, link, max_spectators, id_mediamtx, login, senha, value_per_view_hour, billing_type, fixed_weekly_value, new_plataform, rumble_server, rumble_key, rumble_enabled, iframe_rumble, rumble_api_url } = req.body;

    console.log(`[UPDATE] Atualizando streamer: "${id_streamer}" body:`, JSON.stringify(req.body));

    // Build dynamic SET clause — only update fields that were sent
    const fields = [];
    const values = [];
    let idx = 1;

    const new_id_streamer = req.body.id_streamer;
    if (new_id_streamer !== undefined) { fields.push(`id_streamer = $${idx++}`);     values.push(new_id_streamer); }
    if (user !== undefined)            { fields.push(`"user" = $${idx++}`);          values.push(user); }
    if (link !== undefined)            { fields.push(`link = $${idx++}`);            values.push(link); }
    if (max_spectators !== undefined)  { fields.push(`max_spectators = $${idx++}`);  values.push(max_spectators); }
    if (id_mediamtx !== undefined)     { fields.push(`id_mediamtx = $${idx++}`);     values.push(id_mediamtx); }
    if (login !== undefined)           { fields.push(`login = $${idx++}`);           values.push(login); }
    if (senha !== undefined && senha)  { fields.push(`senha = $${idx++}`);           values.push(await bcrypt.hash(senha, 10)); }
    if (value_per_view_hour !== undefined)  { fields.push(`value_per_view_hour = $${idx++}`);  values.push(value_per_view_hour); }
    if (billing_type !== undefined)         { fields.push(`billing_type = $${idx++}`);         values.push(billing_type); }
    if (fixed_weekly_value !== undefined)   { fields.push(`fixed_weekly_value = $${idx++}`);   values.push(fixed_weekly_value); }
    if (new_plataform !== undefined)        { fields.push(`new_plataform = $${idx++}`);        values.push(new_plataform || null); }
    if (rumble_server !== undefined)        { fields.push(`rumble_server = $${idx++}`);       values.push(rumble_server || null); }
    if (rumble_key !== undefined)           { fields.push(`rumble_key = $${idx++}`);          values.push(rumble_key || null); }
    if (rumble_enabled !== undefined)       { fields.push(`rumble_enabled = $${idx++}`);      values.push(!!rumble_enabled); }
    if (iframe_rumble !== undefined)        { fields.push(`iframe_rumble = $${idx++}`);       values.push(iframe_rumble || null); }
    if (rumble_api_url !== undefined)       { fields.push(`rumble_api_url = $${idx++}`);      values.push(rumble_api_url || null); }

    if (fields.length === 0) {
      return res.status(400).json({ message: 'Nenhum campo para atualizar' });
    }

    values.push(id_streamer);
    const result = await pool.query(
      `UPDATE streamer SET ${fields.join(', ')} WHERE LOWER(id_streamer) = LOWER($${idx}) RETURNING *`,
      values
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: 'Streamer nao encontrado' });
    }

    console.log(`[UPDATE] Streamer atualizado: ${JSON.stringify(result.rows[0])}`);
    invalidateStreamerCache();
    return res.json(result.rows[0]);
  } catch (err) {
    console.error('[UPDATE] Erro:', err.message);
    return res.status(500).json({ message: 'Erro interno do servidor' });
  }
});

// ── GET /api/streamer/:id_streamer/rumble-config (consumido pelo app rumble-relay) ──
// Retorna a config de restream pro Rumble. Protegido por X-Api-Key (server-side only).
app.get('/api/streamer/:id_streamer/rumble-config', requireApiKey, async (req, res) => {
  try {
    const { id_streamer } = req.params;
    const r = await pool.query(
      'SELECT COALESCE(rumble_enabled, false) AS enabled, rumble_server AS server, rumble_key AS key FROM streamer WHERE LOWER(id_streamer) = LOWER($1) LIMIT 1',
      [id_streamer]
    );
    if (r.rows.length === 0) return res.status(404).json({ message: 'Streamer nao encontrado' });
    return res.json(r.rows[0]);
  } catch (err) {
    console.error('[RUMBLE-CONFIG] Erro:', err.message);
    return res.status(500).json({ message: 'Erro interno do servidor' });
  }
});

// Status ao-vivo do Rumble por streamer (em memoria) — reportado pelo app rumble-relay.
const rumbleLiveStatus = {};

// ── Resolução do id do vídeo da Rumble (muda a cada live) via a Live Stream API ──
// O id do embed da Rumble muda a cada live. Guardamos a URL da Live Stream API do streamer
// (secret, coluna rumble_api_url — NUNCA no streamerCache/validate) e buscamos o id da
// live atual (livestreams[0].id), com cache + stale-while-revalidate (1 fetch em voo por
// streamer). is_live continua vindo do activeLives — isto é SÓ pro id do vídeo.
const rumbleVideoCache = {};   // { id_streamer_lower: { videoId, at } }
const _rumbleRefreshing = {};  // dedupe de fetch em voo por streamer
const RUMBLE_VIDEO_TTL = 30000;

function refreshRumbleVideoId(key) {
  if (_rumbleRefreshing[key]) return _rumbleRefreshing[key];
  const fallback = () => (rumbleVideoCache[key] ? rumbleVideoCache[key].videoId : null);
  const p = (async () => {
    let timer;
    try {
      const r = await pool.query(
        'SELECT rumble_api_url FROM streamer WHERE LOWER(id_streamer) = LOWER($1) LIMIT 1', [key]
      );
      const apiUrl = r.rows[0] && r.rows[0].rumble_api_url;
      if (!apiUrl || !/^https:\/\/(www\.)?rumble\.com\//i.test(apiUrl)) return fallback();
      const ctrl = new AbortController();
      timer = setTimeout(() => ctrl.abort(), 5000);
      const resp = await fetch(apiUrl, { signal: ctrl.signal });
      const data = await resp.json();
      const ls = data && data.livestreams && data.livestreams[0];
      const rawId = ls && ls.id;
      if (rawId) {
        // A Live Stream API retorna o id SEM o "v" inicial (ex: "7905aa"); o embed usa o
        // slug COM "v" (ex: "v7905aa"). Prefixa o "v" se faltar.
        const id = String(rawId);
        const videoId = id.charAt(0) === 'v' ? id : 'v' + id;
        rumbleVideoCache[key] = { videoId: videoId, at: Date.now() };
        return videoId;
      }
      return fallback();
    } catch (e) {
      console.error('[RUMBLE-API] fetch do id falhou:', e.message);
      return fallback();
    } finally {
      if (timer) clearTimeout(timer);
      delete _rumbleRefreshing[key];
    }
  })();
  _rumbleRefreshing[key] = p;
  return p;
}

// id atual: fresh do cache, ou stale enquanto revalida; só bloqueia na 1a vez (sem cache).
async function getRumbleVideoId(idStreamer) {
  const key = idStreamer.toLowerCase();
  const c = rumbleVideoCache[key];
  if (c && c.videoId && Date.now() - c.at < RUMBLE_VIDEO_TTL) return c.videoId;
  const p = refreshRumbleVideoId(key);
  if (c && c.videoId) return c.videoId;  // stale-while-revalidate: não bloqueia
  return await p;                         // primeira vez: espera o fetch
}

// Troca o id do vídeo no embed da Rumble (aparece no div + na chamada Rumble("play",...)).
function patchEmbedVideoId(template, freshId) {
  if (!template || !freshId) return template;
  const m = template.match(/"video"\s*:\s*"([a-zA-Z0-9]+)"/);
  const oldId = m && m[1];
  if (!oldId || oldId === freshId) return template;
  return template.split(oldId).join(freshId);
}

// ── POST /api/streamer/:id_streamer/rumble-status (reportado pelo rumble-relay) ──
app.post('/api/streamer/:id_streamer/rumble-status', requireApiKey, (req, res) => {
  const { id_streamer } = req.params;
  const live = !!(req.body && req.body.live);
  rumbleLiveStatus[id_streamer.toLowerCase()] = { live, at: Date.now() };
  return res.json({ ok: true });
});

// ── GET /api/admin/rumble-status (dashboard) ──
app.get('/api/admin/rumble-status', requireApiKeyOrAuth, (req, res) => {
  // "ao vivo" só se reportou nos últimos 60s (heartbeat de 30s) — evita status preso se o relay morrer.
  const now = Date.now();
  const out = {};
  for (const [k, v] of Object.entries(rumbleLiveStatus)) {
    out[k] = { live: v.live && (now - v.at < 60000), at: v.at };
  }
  return res.json(out);
});

// ── DELETE /api/streamer/:id_streamer (admin) ──
app.delete('/api/streamer/:id_streamer', requireApiKeyOrAuth, async (req, res) => {
  try {
    const { id_streamer } = req.params;
    console.log(`[DELETE] Removendo streamer: "${id_streamer}"`);

    const result = await pool.query(
      'DELETE FROM streamer WHERE LOWER(id_streamer) = LOWER($1) RETURNING *',
      [id_streamer]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: 'Streamer nao encontrado' });
    }

    // Remover viewers ativos desse streamer
    const key = id_streamer.toLowerCase();
    delete activeViewers[key];

    console.log(`[DELETE] Streamer removido: ${JSON.stringify(result.rows[0])}`);
    invalidateStreamerCache();
    return res.json({ message: 'Streamer removido', streamer: result.rows[0] });
  } catch (err) {
    console.error('[DELETE] Erro:', err.message);
    return res.status(500).json({ message: 'Erro interno do servidor' });
  }
});

// ══════════════════════════════════════════════
// ── LIVES TRACKING (in-memory + flush to DB) ──
// ══════════════════════════════════════════════

const FLUSH_INTERVAL = 30000; // 30s

// Em memória: lives ativas { id_streamer: { liveId, peakViewers, viewerSessions: {} } }
const activeLives = {};

// Mutex para onLiveStart — evita race condition com múltiplos validates simultâneos
const startingLives = new Set();

// Flag de streams encerrados (para forçar refresh no overlay)
const endedStreamers = {};

// POST /api/live/start — VPS chama quando stream começa
app.post('/api/live/start', async (req, res) => {
  try {
    const { id_mediamtx, api_key, uuid } = req.body;
    if (!id_mediamtx || api_key !== API_KEY) {
      return res.status(403).json({ message: 'Forbidden' });
    }

    // Buscar streamer pelo id_mediamtx
    const result = await pool.query(
      'SELECT id, "user", id_streamer FROM streamer WHERE LOWER(id_mediamtx) = LOWER($1)', [id_mediamtx]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ message: `Streamer com id_mediamtx "${id_mediamtx}" nao encontrado` });
    }

    const streamer = result.rows[0];
    const idStreamer = streamer.id_streamer.toLowerCase();

    // Limpar flag de ended SEMPRE (antes do early return)
    delete endedStreamers[idStreamer];
    delete endedStreamers[idStreamer.toLowerCase()];

    // Proteção: se já tem live ativa, atualizar UUID se veio novo
    if (activeLives[idStreamer]) {
      if (uuid && activeLives[idStreamer].streamUuid !== uuid) {
        activeLives[idStreamer].streamUuid = uuid;
        await pool.query('UPDATE lives SET stream_uuid = $1 WHERE id = $2', [uuid, activeLives[idStreamer].liveId]);
        logger.info(`[LIVE] UUID atualizado para live ativa #${activeLives[idStreamer].liveId}: ${uuid.substring(0, 8)}`);
      }
      return res.json({ started: false, reason: 'already_active', live_id: activeLives[idStreamer].liveId });
    }

    await onLiveStart(idStreamer, streamer.user, uuid);
    return res.json({ started: true, live_id: activeLives[idStreamer]?.liveId });
  } catch (e) {
    logger.error('[LIVE] Erro start:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// POST /api/live/end — VPS chama quando stream termina
app.post('/api/live/end', async (req, res) => {
  try {
    const { id_mediamtx, api_key } = req.body;
    if (!id_mediamtx || api_key !== API_KEY) {
      return res.status(403).json({ message: 'Forbidden' });
    }

    // Buscar streamer pelo id_mediamtx
    const result = await pool.query(
      'SELECT id, "user", id_streamer FROM streamer WHERE LOWER(id_mediamtx) = LOWER($1)', [id_mediamtx]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ message: `Streamer com id_mediamtx "${id_mediamtx}" nao encontrado` });
    }

    const streamer = result.rows[0];
    const idStreamer = streamer.id_streamer.toLowerCase();

    // Proteção: se não tem live ativa, ignorar
    if (!activeLives[idStreamer]) {
      logger.info(`[LIVE] End ignorado: ${idStreamer} nao tem live ativa`);
      return res.json({ ended: false, reason: 'no_active_live' });
    }

    const liveId = activeLives[idStreamer].liveId;

    // Setar flag de ended (overlay vai detectar e forçar refresh)
    endedStreamers[idStreamer.toLowerCase()] = true;

    // Encerrar a live
    await onLiveEnd(idStreamer);

    // Limpar flag após 5 minutos (tempo suficiente pra todos os overlays detectarem)
    setTimeout(() => { delete endedStreamers[idStreamer.toLowerCase()]; }, 300000);

    return res.json({ ended: true, live_id: liveId });
  } catch (e) {
    logger.error('[LIVE] Erro end:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// POST /api/admin/live/force-end/:id_streamer — Forçar fim de live manualmente
app.post('/api/admin/live/force-end/:id_streamer', requireApiKey, async (req, res) => {
  const idStreamer = req.params.id_streamer.toLowerCase();
  try {
    const inMemory = !!activeLives[idStreamer];

    // Setar flag de ended para o overlay detectar e ejetar os viewers
    endedStreamers[idStreamer] = true;
    setTimeout(() => { delete endedStreamers[idStreamer]; }, 300000);

    if (inMemory) {
      // Live ainda está em memória → encerrar normalmente (salva stats + flush sessões)
      await onLiveEnd(idStreamer);
      return res.json({ ended: true, source: 'memory' });
    }

    // Live não está em memória mas pode estar presa no banco como 'active'
    const result = await pool.query(
      "UPDATE lives SET ended_at = NOW(), status = 'ended', duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))::int WHERE id_streamer = $1 AND status = 'active' RETURNING id",
      [idStreamer]
    );

    if (result.rows.length === 0) {
      return res.json({ ended: false, reason: 'no_active_live_found' });
    }

    return res.json({ ended: true, source: 'db_only', live_id: result.rows[0].id });
  } catch (e) {
    logger.error('[ADMIN] Erro force-end:', e.message);
    return res.status(500).json({ message: 'Erro interno', error: e.message });
  }
});

// DELETE /api/admin/live/:live_id — Excluir live e todas as sessões vinculadas
app.delete('/api/admin/live/:live_id', requireApiKey, async (req, res) => {
  const liveId = parseInt(req.params.live_id);
  if (!liveId || isNaN(liveId)) {
    return res.status(400).json({ message: 'live_id inválido' });
  }
  try {
    // Não permitir excluir live ativa
    const check = await pool.query('SELECT id, id_streamer, status FROM lives WHERE id = $1', [liveId]);
    if (check.rows.length === 0) {
      return res.status(404).json({ message: 'Live não encontrada' });
    }
    if (check.rows[0].status === 'active') {
      return res.status(400).json({ message: 'Não é possível excluir uma live ativa. Encerre primeiro.' });
    }

    // ON DELETE CASCADE remove live_viewer_sessions automaticamente
    await pool.query('DELETE FROM lives WHERE id = $1', [liveId]);
    logger.info(`[ADMIN] Live #${liveId} excluída (streamer: ${check.rows[0].id_streamer})`);
    return res.json({ deleted: true, live_id: liveId });
  } catch (e) {
    logger.error('[ADMIN] Erro ao excluir live:', e.message);
    return res.status(500).json({ message: 'Erro interno', error: e.message });
  }
});

// Iniciar live (chamado internamente)
async function onLiveStart(idStreamer, streamerName, streamUuid) {
  idStreamer = idStreamer.toLowerCase();
  if (activeLives[idStreamer] || startingLives.has(idStreamer)) return;
  startingLives.add(idStreamer);
  try {
    // Checar no banco antes de inserir — previne registro duplicado se API reiniciou
    const existing = await pool.query(
      "SELECT id, stream_uuid FROM lives WHERE id_streamer = $1 AND status = 'active' ORDER BY started_at DESC LIMIT 1",
      [idStreamer]
    );
    if (existing.rows.length > 0) {
      const liveId = existing.rows[0].id;
      const existingUuid = streamUuid || existing.rows[0].stream_uuid;
      const logPath = createLiveLogPath(idStreamer);
      activeLives[idStreamer] = { liveId, peakViewers: 0, viewerSessions: {}, logPath, streamUuid: existingUuid };
      // Atualizar UUID no banco se veio um novo
      if (streamUuid && streamUuid !== existing.rows[0].stream_uuid) {
        await pool.query('UPDATE lives SET stream_uuid = $1 WHERE id = $2', [streamUuid, liveId]);
      }
      logger.live(idStreamer, 'INFO', `[LIVE] Live já ativa no DB, restaurada em memória: live #${liveId} | UUID: ${existingUuid?.substring(0, 8) || '?'}`);
      return;
    }

    const result = await pool.query(
      `INSERT INTO lives (streamer, id_streamer, started_at, status, stream_uuid) VALUES ($1, $2, NOW(), 'active', $3) RETURNING id`,
      [streamerName, idStreamer, streamUuid || null]
    );
    const liveId = result.rows[0].id;
    const logPath = createLiveLogPath(idStreamer);
    activeLives[idStreamer] = { liveId, peakViewers: 0, viewerSessions: {}, logPath, streamUuid: streamUuid || null };
    logger.live(idStreamer, 'INFO', `[LIVE] Iniciada: ${streamerName} (${idStreamer}) → live #${liveId} | UUID: ${streamUuid?.substring(0, 8) || '?'}`);
  } catch (e) {
    console.error('[LIVE] Erro ao iniciar:', e.message);
  } finally {
    startingLives.delete(idStreamer);
  }
}

// Detectar fim de live
async function onLiveEnd(idStreamer) {
  idStreamer = idStreamer.toLowerCase();
  const live = activeLives[idStreamer];
  if (!live) return;
  try {
    const uniqueIPs = new Set(Object.values(live.viewerSessions).map(s => s.ip));

    // Buscar duração da live
    const liveRow = await pool.query('SELECT started_at FROM lives WHERE id = $1', [live.liveId]);
    const durationSeconds = liveRow.rows.length > 0
      ? Math.round((Date.now() - new Date(liveRow.rows[0].started_at).getTime()) / 1000)
      : 0;

    // Calcular média de viewers (CCV): soma total_seconds / duração
    const sessions = Object.values(live.viewerSessions);
    const totalWatchSeconds = sessions.reduce((sum, s) => sum + (s.total_seconds || 0), 0);
    const totalMobileSeconds = sessions.filter(s => s.is_mobile).reduce((sum, s) => sum + (s.total_seconds || 0), 0);
    const totalDesktopSeconds = totalWatchSeconds - totalMobileSeconds;

    // Soma de watch seconds por plataforma (kick | twitch)
    const totalKickSeconds = sessions.filter(s => (s.stream_platform || 'kick') === 'kick').reduce((sum, s) => sum + (s.total_seconds || 0), 0);
    const totalTwitchSeconds = sessions.filter(s => s.stream_platform === 'twitch').reduce((sum, s) => sum + (s.total_seconds || 0), 0);

    const avgViewers = durationSeconds > 0
      ? Math.ceil((totalWatchSeconds / durationSeconds) * 10) / 10
      : 0;
    const avgViewersMobile = durationSeconds > 0
      ? Math.ceil((totalMobileSeconds / durationSeconds) * 10) / 10
      : 0;
    const avgViewersDesktop = durationSeconds > 0
      ? Math.ceil((totalDesktopSeconds / durationSeconds) * 10) / 10
      : 0;
    const avgViewersKick = durationSeconds > 0
      ? Math.ceil((totalKickSeconds / durationSeconds) * 10) / 10
      : 0;
    const avgViewersTwitch = durationSeconds > 0
      ? Math.ceil((totalTwitchSeconds / durationSeconds) * 10) / 10
      : 0;

    // Contar viewers únicos mobile vs desktop (por IP) e por plataforma (kick vs twitch)
    const mobileIPs = new Set();
    const desktopIPs = new Set();
    const kickIPs = new Set();
    const twitchIPs = new Set();
    for (const s of Object.values(live.viewerSessions)) {
      if (s.is_mobile) mobileIPs.add(s.ip);
      else desktopIPs.add(s.ip);
      // stream_platform default 'kick' (compatibilidade c/ sessions antigas sem o campo)
      if (s.stream_platform === 'twitch') twitchIPs.add(s.ip);
      else kickIPs.add(s.ip);
    }

    await pool.query(
      `UPDATE lives SET ended_at = NOW(), duration_seconds = $1,
       peak_viewers = $2, total_unique_viewers = $3, avg_viewers = $4,
       unique_mobile = $5, unique_desktop = $6,
       avg_viewers_mobile = $7, avg_viewers_desktop = $8,
       unique_kick = $9, unique_twitch = $10,
       avg_viewers_kick = $11, avg_viewers_twitch = $12,
       status = 'ended' WHERE id = $13`,
      [durationSeconds, live.peakViewers, uniqueIPs.size, avgViewers,
       mobileIPs.size, desktopIPs.size, avgViewersMobile, avgViewersDesktop,
       kickIPs.size, twitchIPs.size, avgViewersKick, avgViewersTwitch,
       live.liveId]
    );
    await flushLiveViewerSessions(live);
    logger.live(idStreamer, 'INFO', `[LIVE] Encerrada: ${idStreamer} → live #${live.liveId} | Peak: ${live.peakViewers} | Média: ${avgViewers} (M:${avgViewersMobile} D:${avgViewersDesktop} K:${avgViewersKick} T:${avgViewersTwitch}) | Únicos: ${uniqueIPs.size} (M:${mobileIPs.size} D:${desktopIPs.size} K:${kickIPs.size} T:${twitchIPs.size})`);
  } catch (e) {
    logger.live(idStreamer, 'ERROR', '[LIVE] Erro ao encerrar:', e.message);
  }

  // Bloqueio diferido: cobranca foi gerada enquanto o streamer estava AO VIVO →
  // bloqueia agora que a live encerrou (nao derrubou a transmissao no meio).
  try {
    const pb = await pool.query('SELECT pending_block FROM streamer WHERE LOWER(id_streamer) = LOWER($1)', [idStreamer]);
    if (pb.rows[0]?.pending_block) {
      await pool.query('UPDATE streamer SET is_blocked = true, pending_block = false WHERE LOWER(id_streamer) = LOWER($1)', [idStreamer]);
      invalidateStreamerCache();
      logger.live(idStreamer, 'INFO', '[BILLING] Bloqueado ao fim da live (cobrança pendente)');
    }
  } catch (e) {
    logger.live(idStreamer, 'ERROR', '[BILLING] Erro no bloqueio diferido:', e.message);
  }

  delete activeLives[idStreamer];
  delete activeViewers[idStreamer];
  delete rumbleVideoCache[(idStreamer || '').toLowerCase()];  // próxima live re-busca o id do vídeo
}

// Flush sessões de viewer pro banco
// Executa em lotes paralelos de FLUSH_BATCH_SIZE para ser rápido no SIGTERM
// sem sobrecarregar o pool (max: 5 conexões).
const FLUSH_BATCH_SIZE = 5;

async function flushLiveViewerSessions(live) {
  const entries = Object.entries(live.viewerSessions);
  if (entries.length === 0) return;

  let count = 0;
  let errors = 0;

  // Processar em lotes de FLUSH_BATCH_SIZE (respeita pool.max = 5)
  for (let i = 0; i < entries.length; i += FLUSH_BATCH_SIZE) {
    const batch = entries.slice(i, i + FLUSH_BATCH_SIZE);

    // Tentar até 2x por batch (retry com 1s de delay se falhar)
    for (let attempt = 1; attempt <= 2; attempt++) {
      const results = await Promise.allSettled(
        batch.map(([viewerUid, s]) =>
          pool.query(`
            INSERT INTO live_viewer_sessions (live_id, ip, username, platform, os, os_version, device_model, browser, browser_version, user_agent, is_mobile, joined_at, last_seen, total_seconds, segments_loaded, estimated_mb, quality_history, player_health, viewer_uid, stream_platform)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
            ON CONFLICT (live_id, viewer_uid) DO UPDATE SET
              username = EXCLUDED.username, last_seen = EXCLUDED.last_seen,
              total_seconds = EXCLUDED.total_seconds, segments_loaded = EXCLUDED.segments_loaded,
              estimated_mb = EXCLUDED.estimated_mb, quality_history = EXCLUDED.quality_history,
              player_health = EXCLUDED.player_health, stream_platform = EXCLUDED.stream_platform
          `, [
            live.liveId, s.ip, s.username || '', s.platform || 'unknown', s.os || 'unknown',
            s.os_version || '', s.device_model || '', s.browser || 'unknown', s.browser_version || '',
            s.user_agent || '', s.is_mobile || false, s.joined_at, s.last_seen,
            s.total_seconds || 0, s.segments_loaded || 0, s.estimated_mb || 0,
            JSON.stringify(s.quality_history || []), JSON.stringify(s.player_health || {}), viewerUid,
            s.stream_platform || 'kick',
          ])
        )
      );
      let batchErrors = 0;
      for (const r of results) {
        if (r.status === 'fulfilled') count++;
        else batchErrors++;
      }
      if (batchErrors === 0) break;
      errors += batchErrors;
      if (attempt < 2) {
        logger.warn(`[FLUSH] Batch live #${live.liveId} falhou (${batchErrors} erros), retentando em 1s...`);
        await new Promise(r => setTimeout(r, 1000));
        count -= (batch.length - batchErrors); // descontar os que já contou como OK
      } else {
        logger.warn(`[FLUSH] Batch live #${live.liveId}: ${batchErrors} erros após retry`);
      }
    }
  }

  if (count > 0 || errors > 0) {
    console.log(`[FLUSH] Live #${live.liveId}: ${count} viewers salvos${errors ? `, ${errors} erros` : ''}`);
  }
}

// Registrar viewer na live ativa
function trackLiveViewer(idStreamer, viewerUid, deviceInfo, ip) {
  idStreamer = (idStreamer || '').toLowerCase();
  const live = activeLives[idStreamer];
  if (!live) return;
  if (!live.viewerSessions[viewerUid]) {
    const now = getBrazilTimestamp();
    // stream_platform: kick | twitch (vem do overlay; fallback 'kick' por compat)
    const sp = String(deviceInfo?.stream_platform || 'kick').toLowerCase();
    live.viewerSessions[viewerUid] = {
      // Aceita 'username' (novo) com fallback pra 'kick_username' (compat com overlays antigos em cache)
      ip, username: deviceInfo?.username || deviceInfo?.kick_username || '',
      platform: deviceInfo?.platform || 'unknown', os: deviceInfo?.os || 'unknown',
      os_version: deviceInfo?.os_version || '', device_model: deviceInfo?.device_model || '',
      browser: deviceInfo?.browser || 'unknown', browser_version: deviceInfo?.browser_version || '',
      user_agent: deviceInfo?.user_agent || '', is_mobile: deviceInfo?.is_mobile || false,
      stream_platform: (sp === 'twitch' ? 'twitch' : 'kick'),
      joined_at: now, last_seen: now, _lastSeenMs: Date.now(),
      total_seconds: 0, segments_loaded: 0, estimated_mb: 0,
      quality_history: [], player_health: {},
    };
  }
}

// Atualizar peak viewers
function updateLivePeak(idStreamer) {
  idStreamer = (idStreamer || '').toLowerCase();
  const live = activeLives[idStreamer];
  if (!live) return;
  // Usar o maior entre: viewer count do heartbeat e sessões ativas na live
  const heartbeatCount = getViewerCount(idStreamer);
  const sessionCount = Object.keys(live.viewerSessions).length;
  const currentViewers = Math.max(heartbeatCount, sessionCount);
  if (currentViewers > live.peakViewers) live.peakViewers = currentViewers;
}

// Flush sessions das lives ativas a cada 30s
async function flushActiveLives() {
  for (const [idStreamer, live] of Object.entries(activeLives)) {
    updateLivePeak(idStreamer);
    await flushLiveViewerSessions(live);
    // Atualizar peak no banco também
    try {
      await pool.query('UPDATE lives SET peak_viewers = $1 WHERE id = $2', [live.peakViewers, live.liveId]);
    } catch (e) { /* ignore */ }
  }
}

setInterval(async () => {
  try {
    await flushActiveLives();
  } catch (e) {
    console.error('[FLUSH] Erro no flush periódico:', e.message);
  }
}, FLUSH_INTERVAL);

// Graceful shutdown — SIGTERM é enviado pelo squarecloud no deploy
// Apenas salvar sessões em andamento; NÃO encerrar as lives no banco.
// Assim, ao reiniciar, restoreActiveLives() retoma de onde parou.
process.on('SIGTERM', async () => {
  console.log('[SIGTERM] Recebido — salvando sessões antes de reiniciar...');
  try { await flushActiveLives(); } catch (e) { console.error('[SIGTERM] Erro flush:', e.message); }
  process.exit(0);
});
// SIGINT (Ctrl+C manual) — aí sim encerrar as lives corretamente
process.on('SIGINT', async () => {
  console.log('[SIGINT] Recebido — encerrando lives...');
  for (const id of Object.keys(activeLives)) await onLiveEnd(id);
  process.exit(0);
});

// POST /api/metrics/join — viewer registra entrada
app.post('/api/metrics/join', requireApiKey, (req, res) => {
  try {
    const id_streamer = (req.body.id_streamer || '').toLowerCase();
    const { viewer_uid, device_info } = req.body;
    if (!id_streamer || !viewer_uid) {
      return res.status(400).json({ message: 'Campos obrigatorios: id_streamer, viewer_uid' });
    }
    const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim()
      || req.headers['x-real-ip'] || req.socket.remoteAddress || 'unknown';
    trackLiveViewer(id_streamer, viewer_uid, device_info, ip);
    updateLivePeak(id_streamer);
    logger.live(id_streamer, 'INFO', `[METRICS] Viewer ${ip} (${device_info?.username || device_info?.kick_username || '?'}) entrou em ${id_streamer}`);
    return res.json({ tracked: true });
  } catch (e) {
    logger.live(id_streamer, 'ERROR', '[METRICS] Erro join:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// POST /api/metrics/update — viewer envia consumo
app.post('/api/metrics/update', requireApiKey, (req, res) => {
  try {
    const id_streamer = (req.body.id_streamer || '').toLowerCase();
    const { viewer_uid, segments_loaded, current_quality, player_health } = req.body;
    if (!id_streamer || !viewer_uid) {
      return res.status(400).json({ message: 'Campos obrigatorios' });
    }
    const live = activeLives[id_streamer];
    if (live && live.viewerSessions[viewer_uid]) {
      const session = live.viewerSessions[viewer_uid];
      const now = Date.now();
      const lastSeen = session._lastSeenMs || now;
      const diff = Math.round((now - lastSeen) / 1000);
      if (diff > 0 && diff < 120) session.total_seconds = (session.total_seconds || 0) + diff;
      session._lastSeenMs = now;
      session.last_seen = getBrazilTimestamp();
      if (segments_loaded !== undefined) {
        session.segments_loaded = segments_loaded;
        session.estimated_mb = Math.round(segments_loaded * 1.2 * 10) / 10;
      }
      if (current_quality) {
        const qh = session.quality_history || [];
        if (qh.length === 0 || qh[qh.length - 1].q !== current_quality) {
          qh.push({ q: current_quality, at: getBrazilTimestamp() });
          session.quality_history = qh;
        }
      }
      if (player_health) session.player_health = player_health;
    }
    updateLivePeak(id_streamer);
    return res.json({ tracked: true });
  } catch (e) {
    logger.live(id_streamer, 'ERROR', '[METRICS] Erro update:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

function formatTime(seconds) {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

// GET /api/lives/:id_streamer — listar lives
app.get('/api/lives/:id_streamer', requireApiKey, async (req, res) => {
  try {
    const { id_streamer } = req.params;
    const limit = parseInt(req.query.limit) || 20;
    const result = await pool.query(
      `SELECT id, streamer, id_streamer, started_at, ended_at, duration_seconds,
              peak_viewers, total_unique_viewers, avg_viewers, unique_mobile, unique_desktop,
              unique_kick, unique_twitch, avg_viewers_kick, avg_viewers_twitch, status
       FROM lives WHERE LOWER(id_streamer) = LOWER($1)
       ORDER BY started_at DESC LIMIT $2`, [id_streamer, limit]
    );
    const lives = result.rows.map(r => ({
      id: r.id, streamer: r.streamer, started_at: r.started_at, ended_at: r.ended_at,
      duration: formatTime(r.duration_seconds || 0), duration_seconds: r.duration_seconds,
      peak_viewers: r.peak_viewers, total_unique_viewers: r.total_unique_viewers,
      avg_viewers: r.avg_viewers || 0, unique_mobile: r.unique_mobile || 0,
      unique_desktop: r.unique_desktop || 0,
      unique_kick: r.unique_kick || 0, unique_twitch: r.unique_twitch || 0,
      avg_viewers_kick: r.avg_viewers_kick || 0, avg_viewers_twitch: r.avg_viewers_twitch || 0,
      status: r.status,
    }));
    return res.json({ streamer: id_streamer, total: lives.length, lives });
  } catch (e) {
    logger.live(id_streamer, 'ERROR', '[LIVES] Erro list:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// GET /api/lives/:id_streamer/:live_id — detalhes de uma live
app.get('/api/lives/:id_streamer/:live_id', requireApiKey, async (req, res) => {
  try {
    const { id_streamer, live_id } = req.params;
    const liveResult = await pool.query(
      'SELECT * FROM lives WHERE id = $1 AND LOWER(id_streamer) = LOWER($2)', [live_id, id_streamer]
    );
    if (liveResult.rows.length === 0) return res.status(404).json({ message: 'Live nao encontrada' });
    const live = liveResult.rows[0];
    const viewersResult = await pool.query(
      `SELECT ip, username, platform, os, os_version, device_model, browser, browser_version,
              user_agent, is_mobile, joined_at, last_seen, total_seconds, segments_loaded, estimated_mb,
              quality_history, player_health, stream_platform
       FROM live_viewer_sessions WHERE live_id = $1 ORDER BY joined_at ASC`, [live_id]
    );
    const viewers = viewersResult.rows.map(v => ({
      ip: v.ip, username: v.username, platform: v.platform, os: v.os,
      device_model: v.device_model, browser: v.browser, is_mobile: v.is_mobile,
      stream_platform: v.stream_platform || 'kick',
      joined_at: v.joined_at, last_seen: v.last_seen,
      time_formatted: formatTime(v.total_seconds || 0), total_seconds: v.total_seconds,
      segments_loaded: v.segments_loaded, estimated_mb: v.estimated_mb,
      quality_history: v.quality_history, player_health: v.player_health,
    }));
    return res.json({
      live: {
        id: live.id, streamer: live.streamer, started_at: live.started_at, ended_at: live.ended_at,
        duration: formatTime(live.duration_seconds || 0), duration_seconds: live.duration_seconds,
        peak_viewers: live.peak_viewers, total_unique_viewers: live.total_unique_viewers,
        avg_viewers: live.avg_viewers || 0, unique_mobile: live.unique_mobile || 0,
        unique_desktop: live.unique_desktop || 0,
        unique_kick: live.unique_kick || 0, unique_twitch: live.unique_twitch || 0,
        avg_viewers_kick: live.avg_viewers_kick || 0, avg_viewers_twitch: live.avg_viewers_twitch || 0,
        status: live.status,
      },
      viewers: {
        total: viewers.length,
        mobile: viewers.filter(v => v.is_mobile).length,
        desktop: viewers.filter(v => !v.is_mobile).length,
        list: viewers,
      },
    });
  } catch (e) {
    logger.live(id_streamer, 'ERROR', '[LIVES] Erro detail:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// Restaurar lives ativas do banco (se API reiniciou durante uma live)
async function restoreActiveLives() {
  try {
    const result = await pool.query("SELECT * FROM lives WHERE status = 'active'");
    for (const row of result.rows) {
      const restoredKey = row.id_streamer.toLowerCase();
      const logPath = createLiveLogPath(restoredKey);
      activeLives[restoredKey] = { liveId: row.id, peakViewers: row.peak_viewers || 0, viewerSessions: {}, logPath, streamUuid: row.stream_uuid || null };
      const sessions = await pool.query('SELECT * FROM live_viewer_sessions WHERE live_id = $1', [row.id]);
      for (const s of sessions.rows) {
        activeLives[restoredKey].viewerSessions[s.viewer_uid] = {
          ip: s.ip, username: s.username, platform: s.platform, os: s.os,
          os_version: s.os_version, device_model: s.device_model, browser: s.browser,
          browser_version: s.browser_version, user_agent: s.user_agent, is_mobile: s.is_mobile,
          stream_platform: s.stream_platform || 'kick',
          joined_at: s.joined_at, last_seen: s.last_seen, total_seconds: s.total_seconds,
          segments_loaded: s.segments_loaded, estimated_mb: s.estimated_mb,
          quality_history: s.quality_history || [], player_health: s.player_health || {},
          _lastSeenMs: Date.now(),
        };
      }
      logger.live(restoredKey, 'INFO', `[LIVE] Restaurada: ${row.streamer} (${row.id_streamer}) → live #${row.id} | UUID: ${row.stream_uuid?.substring(0, 8) || '?'} | ${sessions.rows.length} viewers`);
    }
  } catch (e) {
    console.warn('[LIVE] Erro ao restaurar lives:', e.message);
  }
}

// ── POST /api/revenue — Cálculo de receita e comissão ──
app.post('/api/revenue', requireApiKeyOrAuth, async (req, res) => {
  try {
    const { value_per_view_hour, start_date, end_date, platform_filter } = req.body;
    if (!start_date || !end_date) {
      return res.status(400).json({ message: 'Campos obrigatórios: start_date, end_date' });
    }

    const pf = platform_filter || 'all'; // 'all' | 'mobile' | 'desktop'
    const globalVpvh = value_per_view_hour ? parseFloat(value_per_view_hour) : null;

    // Buscar todos os streamers com comissão, value_per_view_hour e billing_type
    const streamersResult = await pool.query(
      'SELECT id, "user", id_streamer, commission, value_per_view_hour, billing_type, fixed_weekly_value FROM streamer ORDER BY id'
    );

    // Buscar lives no período com watch-seconds por plataforma (via JOIN)
    const livesResult = await pool.query(
      `SELECT l.id, l.streamer, l.id_streamer, l.started_at, l.ended_at, l.duration_seconds,
              l.peak_viewers, l.total_unique_viewers, l.avg_viewers,
              l.avg_viewers_mobile, l.avg_viewers_desktop, l.status,
              COALESCE(SUM(CASE WHEN vs.is_mobile = true THEN vs.total_seconds ELSE 0 END), 0)::INTEGER as mobile_seconds,
              COALESCE(SUM(CASE WHEN vs.is_mobile = false THEN vs.total_seconds ELSE 0 END), 0)::INTEGER as desktop_seconds,
              COALESCE(SUM(vs.total_seconds), 0)::INTEGER as all_seconds
       FROM lives l
       LEFT JOIN live_viewer_sessions vs ON vs.live_id = l.id
       WHERE l.started_at >= $1 AND l.started_at <= $2
       GROUP BY l.id
       ORDER BY l.started_at ASC`,
      [start_date, end_date + 'T23:59:59']
    );

    let totalRevenue = 0;
    let totalCommission = 0;

    // Agrupar lives por streamer (usa value_per_view_hour individual ou global)
    const streamerData = {};
    for (const s of streamersResult.rows) {
      const vpvh = globalVpvh ?? s.value_per_view_hour ?? 0;
      streamerData[s.id_streamer.toLowerCase()] = {
        user: s.user,
        id_streamer: s.id_streamer,
        commission_pct: s.commission || 0,
        value_per_view_hour: vpvh,
        billing_type: s.billing_type || 'view_hours',
        fixed_weekly_value: s.fixed_weekly_value || 0,
        lives: [],
        total_hours: 0,
        total_revenue: 0,
        total_commission: 0,
      };
    }

    for (const live of livesResult.rows) {
      const key = live.id_streamer.toLowerCase();
      if (!streamerData[key]) continue;

      const durationSeconds = live.duration_seconds || 0;
      const durationHours = durationSeconds / 3600;

      // Calcular avg_viewers por plataforma a partir dos seconds (funciona pra dados historicos)
      const avgAll = durationSeconds > 0 ? Math.ceil((live.all_seconds / durationSeconds) * 10) / 10 : (live.avg_viewers || 0);
      const avgMobile = durationSeconds > 0 ? Math.ceil((live.mobile_seconds / durationSeconds) * 10) / 10 : 0;
      const avgDesktop = durationSeconds > 0 ? Math.ceil((live.desktop_seconds / durationSeconds) * 10) / 10 : 0;

      // Escolher avg baseado no filtro de plataforma
      let avgForRevenue;
      if (pf === 'mobile') avgForRevenue = avgMobile;
      else if (pf === 'desktop') avgForRevenue = avgDesktop;
      else avgForRevenue = avgAll;

      const streamerVpvh = streamerData[key].value_per_view_hour;
      const streamerVpvs = streamerVpvh / 3600; // value per view/second
      const revenue = avgForRevenue * durationSeconds * streamerVpvs;

      streamerData[key].lives.push({
        live_id: live.id,
        date: live.started_at,
        started_at: live.started_at,
        ended_at: live.ended_at,
        duration_hours: Math.round(durationHours * 100) / 100,
        duration_formatted: formatTime(durationSeconds),
        avg_viewers: Math.ceil(avgAll),
        avg_viewers_mobile: Math.ceil(avgMobile),
        avg_viewers_desktop: Math.ceil(avgDesktop),
        peak_viewers: live.peak_viewers,
        total_unique_viewers: live.total_unique_viewers,
        revenue: Math.round(revenue * 100) / 100,
        status: live.status,
      });

      streamerData[key].total_hours += durationHours;
      streamerData[key].total_revenue += revenue;
    }

    // Calcular comissão por streamer (diferencia view_hours vs fixed)
    const streamers = [];
    for (const key of Object.keys(streamerData)) {
      const s = streamerData[key];
      s.total_hours = Math.round(s.total_hours * 100) / 100;
      s.total_revenue = Math.round(s.total_revenue * 100) / 100;
      if (s.billing_type === 'fixed') {
        // Fixo: comissão = valor fixo semanal (não depende de view hours)
        s.total_commission = s.fixed_weekly_value;
      } else {
        // View hours: comissão = % sobre receita
        s.total_commission = Math.round(s.total_revenue * (s.commission_pct / 100) * 100) / 100;
      }
      totalRevenue += s.total_revenue;
      totalCommission += s.total_commission;
      if (s.lives.length > 0 || s.commission_pct > 0 || s.billing_type === 'fixed') {
        streamers.push(s);
      }
    }

    return res.json({
      period: { start: start_date, end: end_date },
      value_per_view_hour: globalVpvh ?? 'individual',
      platform_filter: pf,
      total_revenue: Math.round(totalRevenue * 100) / 100,
      total_commission: Math.round(totalCommission * 100) / 100,
      net_profit: Math.round((totalRevenue - totalCommission) * 100) / 100,
      streamers: streamers,
    });
  } catch (e) {
    console.error('[REVENUE] Erro:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// ── PUT /api/streamer/:id_streamer/commission — Atualizar comissão ──
app.put('/api/streamer/:id_streamer/commission', requireApiKeyOrAuth, async (req, res) => {
  try {
    const { id_streamer } = req.params;
    const { commission } = req.body;
    if (commission === undefined || commission < 0 || commission > 100) {
      return res.status(400).json({ message: 'commission deve ser entre 0 e 100' });
    }
    const result = await pool.query(
      'UPDATE streamer SET commission = $1 WHERE LOWER(id_streamer) = LOWER($2) RETURNING *',
      [commission, id_streamer]
    );
    if (result.rows.length === 0) return res.status(404).json({ message: 'Streamer não encontrado' });
    invalidateStreamerCache();
    return res.json({ updated: true, streamer: result.rows[0].user, commission: commission });
  } catch (e) {
    console.error('[COMMISSION] Erro:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// GET /api/logs — ler logs de uma live específica
app.get('/api/logs', requireApiKey, (req, res) => {
  try {
    const streamer = (req.query.streamer || '').toLowerCase();
    if (!streamer) return res.status(400).json({ message: 'Query "streamer" obrigatória' });

    const date = req.query.date || getBrazilDate();
    const prefix = `${date}_${streamer}_`;

    if (!fs.existsSync(LIVES_LOG_DIR)) return res.json({ date, streamer, lines: [] });

    // Se live não informado, pegar o último
    const files = fs.readdirSync(LIVES_LOG_DIR).filter(f => f.startsWith(prefix) && f.endsWith('.log')).sort();
    if (files.length === 0) return res.json({ date, streamer, lines: [] });

    const liveNum = parseInt(req.query.live) || files.length;
    const targetFile = `${prefix}${liveNum}.log`;
    const logPath = path.join(LIVES_LOG_DIR, targetFile);

    if (!fs.existsSync(logPath)) return res.json({ date, streamer, live: liveNum, lines: [] });

    const content = fs.readFileSync(logPath, 'utf8');
    const lines = content.split('\n').filter(Boolean);
    const filter = req.query.filter;
    const filtered = filter ? lines.filter(l => l.includes(filter)) : lines;
    const tail = parseInt(req.query.tail) || 100;

    return res.json({ date, streamer, live: liveNum, total_lives: files.length, total: filtered.length, lines: filtered.slice(-tail) });
  } catch (e) {
    return res.status(500).json({ message: e.message });
  }
});

// GET /api/logs/dates — listar datas e lives disponíveis por streamer
app.get('/api/logs/dates', requireApiKey, (req, res) => {
  try {
    if (!fs.existsSync(LIVES_LOG_DIR)) return res.json({ dates: [] });
    const streamer = (req.query.streamer || '').toLowerCase();
    const files = fs.readdirSync(LIVES_LOG_DIR).filter(f => f.endsWith('.log'));

    const result = {};
    for (const f of files) {
      // Formato: YYYY-MM-DD_streamer_N.log
      const match = f.match(/^(\d{4}-\d{2}-\d{2})_(.+)_(\d+)\.log$/);
      if (!match) continue;
      const [, fDate, fStreamer, fNum] = match;
      if (streamer && fStreamer !== streamer) continue;
      if (!result[fDate]) result[fDate] = {};
      if (!result[fDate][fStreamer]) result[fDate][fStreamer] = [];
      result[fDate][fStreamer].push(parseInt(fNum));
    }

    // Ordenar
    const dates = Object.keys(result).sort().reverse();
    for (const d of dates) {
      for (const s of Object.keys(result[d])) {
        result[d][s].sort((a, b) => a - b);
      }
    }

    return res.json({ dates, lives: result });
  } catch (e) {
    return res.status(500).json({ message: e.message });
  }
});

// GET /api/logs/general — ler log geral do dia
// Query: ?date=2026-03-28&filter=ERROR&tail=200
app.get('/api/logs/general', requireApiKey, (req, res) => {
  try {
    const date = req.query.date || getBrazilDate();
    const logPath = path.join(GENERAL_LOG_DIR, `${date}.log`);

    if (!fs.existsSync(logPath)) return res.json({ date, total: 0, lines: [] });

    const content = fs.readFileSync(logPath, 'utf8');
    const lines = content.split('\n').filter(Boolean);
    const filter = req.query.filter;
    const filtered = filter ? lines.filter(l => l.includes(filter)) : lines;
    const tail = parseInt(req.query.tail) || 200;

    return res.json({ date, total: filtered.length, lines: filtered.slice(-tail) });
  } catch (e) {
    return res.status(500).json({ message: e.message });
  }
});

// GET /api/logs/general/dates — listar datas disponíveis
app.get('/api/logs/general/dates', requireApiKey, (req, res) => {
  try {
    if (!fs.existsSync(GENERAL_LOG_DIR)) return res.json({ dates: [] });
    const files = fs.readdirSync(GENERAL_LOG_DIR)
      .filter(f => f.endsWith('.log'))
      .map(f => f.replace('.log', ''))
      .sort()
      .reverse();
    return res.json({ dates: files });
  } catch (e) {
    return res.status(500).json({ message: e.message });
  }
});

// ── Start ──
const PORT = process.env.PORT || 3000;

initDB().then(() => loadStreamerCache()).then(() => restoreActiveLives()).then(() => {
  app.listen(PORT, () => {
    console.log(`[SERVER] API rodando na porta ${PORT}`);
    console.log(`[SERVER] Endpoints disponíveis:`);
    console.log(`   GET    /health`);
    console.log(`   GET    /api/streamer/validate/:id_streamer`);
    console.log(`   GET    /api/streamers`);
    console.log(`   POST   /api/streamer`);
    console.log(`   PUT    /api/streamer/:id_streamer`);
    console.log(`   DELETE /api/streamer/:id_streamer`);
    console.log(`   GET    /api/queue/status/:id_streamer`);
    console.log(`   POST   /api/viewer/join`);
    console.log(`   POST   /api/viewer/heartbeat`);
    console.log(`   POST   /api/viewer/leave`);
    console.log(`   GET    /api/viewer/count/:id_streamer`);
    console.log(`   POST   /api/metrics/join`);
    console.log(`   POST   /api/metrics/update`);
    console.log(`   GET    /api/lives/:id_streamer`);
    console.log(`   GET    /api/lives/:id_streamer/:live_id`);
  });
});
