const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

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

// ── Cloudflare KV (para buscar UUID rotativo do stream) ──
const CF_API_TOKEN = process.env.CF_API_TOKEN || '';
const CF_ACCOUNT_ID = process.env.CF_ACCOUNT_ID || '';
const CF_KV_NAMESPACE_STREAM_PATHS = process.env.CF_KV_NAMESPACE_STREAM_PATHS || '';
const CDN_DOMAIN = process.env.CDN_DOMAIN || 'live.vody.gg';

// ── ID da extensão na Chrome Web Store ──
const EXTENSION_ID = process.env.EXTENSION_ID || 'cgpdaogbcjjfmnoeacopegocjpcfcikf';

// ── CORS (extensão, squareweb e kick.com para o bookmarklet) ──
app.use(cors({
  origin: (origin, callback) => {
    const allowedOrigins = [
      `chrome-extension://${EXTENSION_ID}`,
    ];
    if (!origin || allowedOrigins.includes(origin)
        || (origin.endsWith('.squareweb.app') || origin === 'https://squareweb.app')
        || (origin.endsWith('.kick.com') || origin === 'https://kick.com')) {
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

  if (origin && !origin.includes(EXTENSION_ID) && !(origin.endsWith('.squareweb.app') || origin === 'https://squareweb.app') && !(origin.endsWith('.kick.com') || origin === 'https://kick.com')) {
    console.warn(`[AUTH] Origin invalida: ${origin}`);
    return res.status(403).json({ message: 'Acesso negado' });
  }

  next();
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
      ALTER TABLE streamer ADD COLUMN IF NOT EXISTS link_vps VARCHAR(500)
    `);
    await pool.query(`
      ALTER TABLE streamer ADD COLUMN IF NOT EXISTS id_mediamtx VARCHAR(255)
    `);
    await pool.query(`
      ALTER TABLE streamer ADD COLUMN IF NOT EXISTS commission REAL DEFAULT 0
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

    // Tabela de sessões de viewer por live
    await pool.query(`
      CREATE TABLE IF NOT EXISTS live_viewer_sessions (
        id SERIAL PRIMARY KEY,
        live_id INTEGER REFERENCES lives(id) ON DELETE CASCADE,
        ip VARCHAR(100) NOT NULL,
        kick_username VARCHAR(255) DEFAULT '',
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

    // Índices para queries rápidas
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_lives_streamer ON lives(id_streamer)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_lives_status ON lives(status)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_live_viewer_sessions_live ON live_viewer_sessions(live_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_live_viewer_sessions_ip ON live_viewer_sessions(ip)`);

    console.log('[DB] Tabelas streamer, lives, live_viewer_sessions prontas');

    const count = await pool.query('SELECT COUNT(*) FROM streamer');
    console.log(`[DB] Streamers cadastrados: ${count.rows[0].count}`);
  } catch (err) {
    console.error('[DB] Erro ao criar tabela:', err.message);
  }
}

// ══════════════════════════════════════════════
// ── ROTAS ──
// ══════════════════════════════════════════════

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

// ── Stream URL cache (evita rate limit da Cloudflare KV API) ──
const streamUrlCache = {}; // { mediamtxPath: { url, timestamp } }
const CACHE_TTL_MS = 30000; // 30s — garante que novo UUID é entregue com folga antes do overlap terminar
const pendingKvRequests = {}; // { mediamtxPath: Promise } — deduplicação de requests

async function getCachedStreamUrl(mediamtxPath) {
  const now = Date.now();
  const cached = streamUrlCache[mediamtxPath];

  // Retorna cache se ainda válido
  if (cached && (now - cached.timestamp) < CACHE_TTL_MS) {
    return cached.url;
  }

  // Se já tem uma request em andamento pro mesmo path, reusar a Promise
  if (pendingKvRequests[mediamtxPath]) {
    return pendingKvRequests[mediamtxPath];
  }

  // Buscar do KV (uma única request, compartilhada entre todos os viewers)
  const kvPromise = (async () => {
    try {
      const kvResp = await fetch(
        `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/storage/kv/namespaces/${CF_KV_NAMESPACE_STREAM_PATHS}/values/path:${mediamtxPath}`,
        { headers: { 'Authorization': `Bearer ${CF_API_TOKEN}` }, signal: AbortSignal.timeout(5000) }
      );
      if (kvResp.ok) {
        const pathData = JSON.parse(await kvResp.text());
        const url = `https://${CDN_DOMAIN}/${pathData.uuid}/${mediamtxPath}/master.m3u8`;
        streamUrlCache[mediamtxPath] = { url, timestamp: Date.now() };
        console.log(`[CACHE] Stream URL atualizado: ${mediamtxPath} → ${url}`);
        return url;
      } else {
        console.warn(`[CACHE] KV retornou ${kvResp.status} para ${mediamtxPath}`);
        if (cached) return cached.url;
        return '';
      }
    } catch (err) {
      console.warn(`[CACHE] Erro KV:`, err.message);
      if (cached) return cached.url;
      return '';
    } finally {
      delete pendingKvRequests[mediamtxPath];
    }
  })();

  pendingKvRequests[mediamtxPath] = kvPromise;
  return kvPromise;
}

// ── Cache de live status via KV ──
const liveStatusCache = {}; // { mediamtxPath: { status, timestamp } }
const LIVE_STATUS_CACHE_TTL = 30000; // 30s

async function getCachedLiveStatus(mediamtxPath) {
  const now = Date.now();
  const cached = liveStatusCache[mediamtxPath];

  if (cached && (now - cached.timestamp) < LIVE_STATUS_CACHE_TTL) {
    return cached.status;
  }

  try {
    const kvResp = await fetch(
      `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/storage/kv/namespaces/${CF_KV_NAMESPACE_STREAM_PATHS}/values/live:${mediamtxPath}`,
      { headers: { 'Authorization': `Bearer ${CF_API_TOKEN}` } }
    );
    if (kvResp.ok) {
      const data = JSON.parse(await kvResp.text());
      liveStatusCache[mediamtxPath] = { status: data.status, timestamp: now };
      return data.status;
    } else {
      liveStatusCache[mediamtxPath] = { status: null, timestamp: now };
      return null;
    }
  } catch (err) {
    console.warn('[LIVE-STATUS] Erro KV:', err.message);
    if (cached) return cached.status;
    return null;
  }
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
app.get('/api/streamer/validate/:id_streamer', requireApiKey, async (req, res) => {
  const id_streamer = req.params.id_streamer.toLowerCase();

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
    'SELECT id, "user", link, id_streamer, max_spectators, link_vps, id_mediamtx FROM streamer'
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
        message: 'Streamer nao encontrado'
      });
    }

    const currentViewers = getViewerCount(id_streamer);

    console.log(`[VALIDATE] Streamer encontrado: ${streamer.user} | Viewers: ${currentViewers}/${streamer.max_spectators}`);

    // Buscar UUID rotativo no KV do Cloudflare pra montar stream URL via CDN
    let stream_url = '';
    const mediamtxPath = streamer.id_mediamtx || streamer.id_streamer;
    if (CF_API_TOKEN && CF_ACCOUNT_ID && CF_KV_NAMESPACE_STREAM_PATHS) {
      stream_url = await getCachedStreamUrl(mediamtxPath);
    }

    // Detectar início/fim de live via KV (live:{mediamtxPath})
    const liveStatus = await getCachedLiveStatus(mediamtxPath);

    // Se status = "active" e não tem live ativa → iniciar live
    if (liveStatus === 'active' && stream_url && !activeLives[id_streamer]) {
      await onLiveStart(id_streamer, streamer.user);
    }

    // Se status = "ended" e tem live ativa → encerrar live
    if (liveStatus === 'ended' && activeLives[id_streamer]) {
      endedStreamers[id_streamer.toLowerCase()] = true;
      await onLiveEnd(id_streamer);
      setTimeout(() => { delete endedStreamers[id_streamer.toLowerCase()]; }, 300000);
    }

    updateLivePeak(id_streamer);

    const streamEnded = endedStreamers[id_streamer.toLowerCase()] || false;

    return res.json({
      valid: true,
      stream_ended: streamEnded,
      streamer: {
        ...streamer,
        current_viewers: currentViewers,
        stream_url: streamEnded ? '' : stream_url
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

    // Incluir stream_url atual no response para o overlay detectar rotação de UUID
    // Só busca se houver live ativa — getCachedStreamUrl usa cache em memória (2min TTL)
    let stream_url = null;
    if (activeLives[id_streamer] && CF_API_TOKEN && CF_ACCOUNT_ID && CF_KV_NAMESPACE_STREAM_PATHS) {
      const streamer = await getCachedStreamer(id_streamer);
      if (streamer) {
        const mediamtxPath = streamer.id_mediamtx || streamer.id_streamer;
        stream_url = await getCachedStreamUrl(mediamtxPath);
      }
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
app.get('/api/streamers', requireApiKey, async (req, res) => {
  try {
    console.log('[LIST] Listando todos os streamers');
    const result = await pool.query('SELECT id, "user", link, id_streamer, max_spectators, link_vps, id_mediamtx, commission FROM streamer ORDER BY id');

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
app.post('/api/streamer', requireApiKey, async (req, res) => {
  try {
    const { user, link, id_streamer, max_spectators, link_vps, id_mediamtx } = req.body;
    console.log(`[CREATE] Cadastrando streamer: user="${user}", id_streamer="${id_streamer}", max=${max_spectators || 0}, mediamtx="${id_mediamtx || ''}"`);

    if (!user || !id_streamer) {
      return res.status(400).json({ message: 'Campos "user" e "id_streamer" sao obrigatorios' });
    }

    const result = await pool.query(
      'INSERT INTO streamer ("user", link, id_streamer, max_spectators, link_vps, id_mediamtx) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *',
      [user, link || null, id_streamer, max_spectators || 0, link_vps || null, id_mediamtx || null]
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
app.put('/api/streamer/:id_streamer', requireApiKey, async (req, res) => {
  try {
    const { id_streamer } = req.params;
    const { user, link, max_spectators, link_vps, id_mediamtx } = req.body;

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
    if (link_vps !== undefined)        { fields.push(`link_vps = $${idx++}`);        values.push(link_vps); }
    if (id_mediamtx !== undefined)     { fields.push(`id_mediamtx = $${idx++}`);     values.push(id_mediamtx); }

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

// ── DELETE /api/streamer/:id_streamer (admin) ──
app.delete('/api/streamer/:id_streamer', requireApiKey, async (req, res) => {
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

    // Proteção: se já tem live ativa, ignorar
    if (activeLives[idStreamer]) {
      logger.info(`[LIVE] Start ignorado: ${idStreamer} já tem live ativa #${activeLives[idStreamer].liveId}`);
      return res.json({ started: false, reason: 'already_active', live_id: activeLives[idStreamer].liveId });
    }

    // Limpar flag de ended (todas as variações de case)
    delete endedStreamers[idStreamer];
    delete endedStreamers[idStreamer.toLowerCase()];

    // Invalidar cache do KV e forçar status active (evita validate re-setar o flag)
    const mediamtxPath = id_mediamtx.toLowerCase();
    delete streamUrlCache[mediamtxPath];
    liveStatusCache[mediamtxPath] = { status: 'active', timestamp: Date.now() };

    await onLiveStart(idStreamer, streamer.user);
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

// Iniciar live (chamado internamente)
async function onLiveStart(idStreamer, streamerName) {
  idStreamer = idStreamer.toLowerCase();
  if (activeLives[idStreamer] || startingLives.has(idStreamer)) return;
  startingLives.add(idStreamer);
  try {
    // Checar no banco antes de inserir — previne registro duplicado se API reiniciou
    // durante uma live (race condition entre restoreActiveLives e primeira chamada de validate)
    const existing = await pool.query(
      "SELECT id FROM lives WHERE id_streamer = $1 AND status = 'active' ORDER BY started_at DESC LIMIT 1",
      [idStreamer]
    );
    if (existing.rows.length > 0) {
      const liveId = existing.rows[0].id;
      const logPath = createLiveLogPath(idStreamer);
      activeLives[idStreamer] = { liveId, peakViewers: 0, viewerSessions: {}, logPath };
      logger.live(idStreamer, 'INFO', `[LIVE] Live já ativa no DB, restaurada em memória: live #${liveId}`);
      return;
    }

    const result = await pool.query(
      `INSERT INTO lives (streamer, id_streamer, started_at, status) VALUES ($1, $2, NOW(), 'active') RETURNING id`,
      [streamerName, idStreamer]
    );
    const liveId = result.rows[0].id;
    const logPath = createLiveLogPath(idStreamer);
    activeLives[idStreamer] = { liveId, peakViewers: 0, viewerSessions: {}, logPath };
    logger.live(idStreamer, 'INFO', `[LIVE] Iniciada: ${streamerName} (${idStreamer}) → live #${liveId}`);
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

    const avgViewers = durationSeconds > 0
      ? Math.round((totalWatchSeconds / durationSeconds) * 10) / 10
      : 0;
    const avgViewersMobile = durationSeconds > 0
      ? Math.round((totalMobileSeconds / durationSeconds) * 10) / 10
      : 0;
    const avgViewersDesktop = durationSeconds > 0
      ? Math.round((totalDesktopSeconds / durationSeconds) * 10) / 10
      : 0;

    // Contar viewers únicos mobile vs desktop (por IP)
    const mobileIPs = new Set();
    const desktopIPs = new Set();
    for (const s of Object.values(live.viewerSessions)) {
      if (s.is_mobile) mobileIPs.add(s.ip);
      else desktopIPs.add(s.ip);
    }

    await pool.query(
      `UPDATE lives SET ended_at = NOW(), duration_seconds = $1,
       peak_viewers = $2, total_unique_viewers = $3, avg_viewers = $4,
       unique_mobile = $5, unique_desktop = $6,
       avg_viewers_mobile = $7, avg_viewers_desktop = $8,
       status = 'ended' WHERE id = $9`,
      [durationSeconds, live.peakViewers, uniqueIPs.size, avgViewers, mobileIPs.size, desktopIPs.size, avgViewersMobile, avgViewersDesktop, live.liveId]
    );
    await flushLiveViewerSessions(live);
    logger.live(idStreamer, 'INFO', `[LIVE] Encerrada: ${idStreamer} → live #${live.liveId} | Peak: ${live.peakViewers} | Média: ${avgViewers} (M:${avgViewersMobile} D:${avgViewersDesktop}) | Únicos: ${uniqueIPs.size} (M:${mobileIPs.size} D:${desktopIPs.size})`);
  } catch (e) {
    logger.live(idStreamer, 'ERROR', '[LIVE] Erro ao encerrar:', e.message);
  }
  delete activeLives[idStreamer];
  delete activeViewers[idStreamer];
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
            INSERT INTO live_viewer_sessions (live_id, ip, kick_username, platform, os, os_version, device_model, browser, browser_version, user_agent, is_mobile, joined_at, last_seen, total_seconds, segments_loaded, estimated_mb, quality_history, player_health, viewer_uid)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
            ON CONFLICT (live_id, viewer_uid) DO UPDATE SET
              kick_username = EXCLUDED.kick_username, last_seen = EXCLUDED.last_seen,
              total_seconds = EXCLUDED.total_seconds, segments_loaded = EXCLUDED.segments_loaded,
              estimated_mb = EXCLUDED.estimated_mb, quality_history = EXCLUDED.quality_history,
              player_health = EXCLUDED.player_health
          `, [
            live.liveId, s.ip, s.kick_username || '', s.platform || 'unknown', s.os || 'unknown',
            s.os_version || '', s.device_model || '', s.browser || 'unknown', s.browser_version || '',
            s.user_agent || '', s.is_mobile || false, s.joined_at, s.last_seen,
            s.total_seconds || 0, s.segments_loaded || 0, s.estimated_mb || 0,
            JSON.stringify(s.quality_history || []), JSON.stringify(s.player_health || {}), viewerUid,
          ])
        )
      );
      let batchErrors = 0;
      for (const r of results) {
        if (r.status === 'fulfilled') count++;
        else batchErrors++;
      }
      if (batchErrors === 0) break; // batch inteiro OK, prosseguir
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
    live.viewerSessions[viewerUid] = {
      ip, kick_username: deviceInfo?.kick_username || '',
      platform: deviceInfo?.platform || 'unknown', os: deviceInfo?.os || 'unknown',
      os_version: deviceInfo?.os_version || '', device_model: deviceInfo?.device_model || '',
      browser: deviceInfo?.browser || 'unknown', browser_version: deviceInfo?.browser_version || '',
      user_agent: deviceInfo?.user_agent || '', is_mobile: deviceInfo?.is_mobile || false,
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
    logger.live(id_streamer, 'INFO', `[METRICS] Viewer ${ip} (${device_info?.kick_username || '?'}) entrou em ${id_streamer}`);
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
              peak_viewers, total_unique_viewers, avg_viewers, unique_mobile, unique_desktop, status
       FROM lives WHERE LOWER(id_streamer) = LOWER($1)
       ORDER BY started_at DESC LIMIT $2`, [id_streamer, limit]
    );
    const lives = result.rows.map(r => ({
      id: r.id, streamer: r.streamer, started_at: r.started_at, ended_at: r.ended_at,
      duration: formatTime(r.duration_seconds || 0), duration_seconds: r.duration_seconds,
      peak_viewers: r.peak_viewers, total_unique_viewers: r.total_unique_viewers,
      avg_viewers: r.avg_viewers || 0, unique_mobile: r.unique_mobile || 0,
      unique_desktop: r.unique_desktop || 0, status: r.status,
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
      `SELECT ip, kick_username, platform, os, os_version, device_model, browser, browser_version,
              user_agent, is_mobile, joined_at, last_seen, total_seconds, segments_loaded, estimated_mb,
              quality_history, player_health
       FROM live_viewer_sessions WHERE live_id = $1 ORDER BY joined_at ASC`, [live_id]
    );
    const viewers = viewersResult.rows.map(v => ({
      ip: v.ip, kick_username: v.kick_username, platform: v.platform, os: v.os,
      device_model: v.device_model, browser: v.browser, is_mobile: v.is_mobile,
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
        unique_desktop: live.unique_desktop || 0, status: live.status,
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
      activeLives[restoredKey] = { liveId: row.id, peakViewers: row.peak_viewers || 0, viewerSessions: {}, logPath };
      const sessions = await pool.query('SELECT * FROM live_viewer_sessions WHERE live_id = $1', [row.id]);
      for (const s of sessions.rows) {
        activeLives[restoredKey].viewerSessions[s.viewer_uid] = {
          ip: s.ip, kick_username: s.kick_username, platform: s.platform, os: s.os,
          os_version: s.os_version, device_model: s.device_model, browser: s.browser,
          browser_version: s.browser_version, user_agent: s.user_agent, is_mobile: s.is_mobile,
          joined_at: s.joined_at, last_seen: s.last_seen, total_seconds: s.total_seconds,
          segments_loaded: s.segments_loaded, estimated_mb: s.estimated_mb,
          quality_history: s.quality_history || [], player_health: s.player_health || {},
          _lastSeenMs: Date.now(),
        };
      }
      logger.live(restoredKey, 'INFO', `[LIVE] Restaurada: ${row.streamer} (${row.id_streamer}) → live #${row.id} | ${sessions.rows.length} viewers`);
    }
  } catch (e) {
    console.warn('[LIVE] Erro ao restaurar lives:', e.message);
  }
}

// ── POST /api/revenue — Cálculo de receita e comissão ──
app.post('/api/revenue', requireApiKey, async (req, res) => {
  try {
    const { value_per_view_hour, start_date, end_date, platform_filter } = req.body;
    if (!value_per_view_hour || !start_date || !end_date) {
      return res.status(400).json({ message: 'Campos obrigatórios: value_per_view_hour, start_date, end_date' });
    }

    const pf = platform_filter || 'all'; // 'all' | 'mobile' | 'desktop'

    // Buscar todos os streamers com comissão
    const streamersResult = await pool.query(
      'SELECT id, "user", id_streamer, commission FROM streamer ORDER BY id'
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

    const vpvh = parseFloat(value_per_view_hour);
    let totalRevenue = 0;
    let totalCommission = 0;

    // Agrupar lives por streamer
    const streamerData = {};
    for (const s of streamersResult.rows) {
      streamerData[s.id_streamer.toLowerCase()] = {
        user: s.user,
        id_streamer: s.id_streamer,
        commission_pct: s.commission || 0,
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
      const avgAll = durationSeconds > 0 ? Math.round((live.all_seconds / durationSeconds) * 10) / 10 : (live.avg_viewers || 0);
      const avgMobile = durationSeconds > 0 ? Math.round((live.mobile_seconds / durationSeconds) * 10) / 10 : 0;
      const avgDesktop = durationSeconds > 0 ? Math.round((live.desktop_seconds / durationSeconds) * 10) / 10 : 0;

      // Escolher avg baseado no filtro de plataforma
      let avgForRevenue;
      if (pf === 'mobile') avgForRevenue = avgMobile;
      else if (pf === 'desktop') avgForRevenue = avgDesktop;
      else avgForRevenue = avgAll;

      const revenue = avgForRevenue * durationHours * vpvh;

      streamerData[key].lives.push({
        live_id: live.id,
        date: live.started_at,
        started_at: live.started_at,
        ended_at: live.ended_at,
        duration_hours: Math.round(durationHours * 100) / 100,
        duration_formatted: formatTime(durationSeconds),
        avg_viewers: Math.round(avgAll),
        avg_viewers_mobile: Math.round(avgMobile),
        avg_viewers_desktop: Math.round(avgDesktop),
        peak_viewers: live.peak_viewers,
        total_unique_viewers: live.total_unique_viewers,
        revenue: Math.round(revenue * 100) / 100,
        status: live.status,
      });

      streamerData[key].total_hours += durationHours;
      streamerData[key].total_revenue += revenue;
    }

    // Calcular comissão por streamer
    const streamers = [];
    for (const key of Object.keys(streamerData)) {
      const s = streamerData[key];
      s.total_hours = Math.round(s.total_hours * 100) / 100;
      s.total_revenue = Math.round(s.total_revenue * 100) / 100;
      s.total_commission = Math.round(s.total_revenue * (s.commission_pct / 100) * 100) / 100;
      totalRevenue += s.total_revenue;
      totalCommission += s.total_commission;
      if (s.lives.length > 0 || s.commission_pct > 0) {
        streamers.push(s);
      }
    }

    return res.json({
      period: { start: start_date, end: end_date },
      value_per_view_hour: vpvh,
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
app.put('/api/streamer/:id_streamer/commission', requireApiKey, async (req, res) => {
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
// Query: ?streamer=beli21&date=2026-03-26&live=1&filter=JOIN&tail=100
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
// Query: ?streamer=beli21 (opcional)
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
