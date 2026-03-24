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

// ── API Key secreta (só a extensão conhece) ──
const API_KEY = process.env.API_KEY || 'vdo-overlay-k8x2m9p4q7w1';

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
        || origin.includes('squareweb.app')
        || origin.includes('kick.com')) {
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
    if (now - rateLimit[ip].start > RATE_LIMIT_WINDOW * 2) {
      delete rateLimit[ip];
    }
  }
}, 5 * 60 * 1000);

app.use(rateLimiter);

// ── Middleware de autenticação por API Key + Origin ──
function requireApiKey(req, res, next) {
  const key = req.headers['x-api-key'];
  const origin = req.headers['origin'] || '';

  if (key !== API_KEY) {
    console.warn(`[AUTH] API Key invalida de ${req.headers['x-forwarded-for'] || req.connection.remoteAddress}`);
    return res.status(403).json({ message: 'Acesso negado' });
  }

  if (origin && !origin.includes(EXTENSION_ID) && !origin.includes('squareweb.app') && !origin.includes('kick.com')) {
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

// ── Conexão PostgreSQL com SSL ──
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://squarecloud:CnGtU7gqRieRr5PqzGN8Ck6R@square-cloud-db-63b65448d06b4c6ab2b3db9b54bfe0d6.squareweb.app:7162/squarecloud',
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

    console.log('[DB] Tabela streamer pronta');

    // Tabela de métricas por viewer/conexão
    await pool.query(`
      CREATE TABLE IF NOT EXISTS live_metrics (
        id SERIAL PRIMARY KEY,
        streamer VARCHAR(255) NOT NULL,
        date VARCHAR(10) NOT NULL,
        ip VARCHAR(100) NOT NULL,
        kick_username VARCHAR(255) DEFAULT '',
        viewer_uid VARCHAR(100) NOT NULL,
        platform VARCHAR(50) DEFAULT 'unknown',
        os VARCHAR(50) DEFAULT 'unknown',
        os_version VARCHAR(50) DEFAULT '',
        device_model VARCHAR(100) DEFAULT '',
        browser VARCHAR(50) DEFAULT 'unknown',
        browser_version VARCHAR(50) DEFAULT '',
        user_agent TEXT DEFAULT '',
        is_mobile BOOLEAN DEFAULT false,
        joined_at VARCHAR(50),
        last_seen VARCHAR(50),
        segments_loaded INTEGER DEFAULT 0,
        estimated_mb REAL DEFAULT 0,
        quality_history JSONB DEFAULT '[]',
        player_health JSONB DEFAULT '{}',
        UNIQUE(streamer, date, viewer_uid)
      )
    `);

    // Tabela de resumo diário (viewers únicos)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS daily_viewers (
        id SERIAL PRIMARY KEY,
        streamer VARCHAR(255) NOT NULL,
        date VARCHAR(10) NOT NULL,
        ip VARCHAR(100) NOT NULL,
        kick_username VARCHAR(255) DEFAULT '',
        first_seen VARCHAR(50),
        last_seen VARCHAR(50),
        total_seconds INTEGER DEFAULT 0,
        sessions INTEGER DEFAULT 1,
        current_uid VARCHAR(100) DEFAULT '',
        UNIQUE(streamer, date, ip)
      )
    `);

    // Índices para queries rápidas
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_metrics_streamer_date ON live_metrics(streamer, date)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_daily_streamer_date ON daily_viewers(streamer, date)`);

    console.log('[DB] Tabelas streamer, live_metrics, daily_viewers prontas');

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
    activeViewers: stats
  });
});

// ── Stream URL cache (evita rate limit da Cloudflare KV API) ──
const streamUrlCache = {}; // { mediamtxPath: { url, timestamp } }
const CACHE_TTL_MS = 30000; // 30 segundos

async function getCachedStreamUrl(mediamtxPath) {
  const now = Date.now();
  const cached = streamUrlCache[mediamtxPath];

  // Retorna cache se ainda válido
  if (cached && (now - cached.timestamp) < CACHE_TTL_MS) {
    return cached.url;
  }

  // Buscar do KV
  try {
    const kvResp = await fetch(
      `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/storage/kv/namespaces/${CF_KV_NAMESPACE_STREAM_PATHS}/values/path:${mediamtxPath}`,
      { headers: { 'Authorization': `Bearer ${CF_API_TOKEN}` } }
    );
    if (kvResp.ok) {
      const pathData = JSON.parse(await kvResp.text());
      const url = `https://${CDN_DOMAIN}/${pathData.uuid}/${mediamtxPath}/master.m3u8`;
      streamUrlCache[mediamtxPath] = { url, timestamp: now };
      console.log(`[CACHE] Stream URL atualizado: ${mediamtxPath} → ${url}`);
      return url;
    } else {
      console.warn(`[CACHE] KV retornou ${kvResp.status} para ${mediamtxPath}`);
      // Se tem cache expirado, retorna ele em vez de vazio (graceful degradation)
      if (cached) return cached.url;
      return '';
    }
  } catch (err) {
    console.warn(`[CACHE] Erro KV:`, err.message);
    if (cached) return cached.url;
    return '';
  }
}

// ── GET /api/streamer/validate/:id_streamer ──
app.get('/api/streamer/validate/:id_streamer', requireApiKey, async (req, res) => {
  try {
    const { id_streamer } = req.params;
    console.log(`[VALIDATE] Validando streamer: "${id_streamer}"`);

    const result = await pool.query(
      'SELECT id, "user", link, id_streamer, max_spectators, link_vps, id_mediamtx FROM streamer WHERE LOWER(id_streamer) = LOWER($1)',
      [id_streamer]
    );

    if (result.rows.length === 0) {
      console.log(`[VALIDATE] Streamer "${id_streamer}" nao encontrado`);
      return res.status(404).json({
        valid: false,
        message: 'Streamer nao encontrado'
      });
    }

    const streamer = result.rows[0];
    const currentViewers = getViewerCount(id_streamer);

    console.log(`[VALIDATE] Streamer encontrado: ${streamer.user} | Viewers: ${currentViewers}/${streamer.max_spectators}`);

    // Buscar UUID rotativo no KV do Cloudflare pra montar stream URL via CDN
    let stream_url = '';
    const mediamtxPath = streamer.id_mediamtx || streamer.id_streamer;
    if (CF_API_TOKEN && CF_ACCOUNT_ID && CF_KV_NAMESPACE_STREAM_PATHS) {
      stream_url = await getCachedStreamUrl(mediamtxPath);
    }

    return res.json({
      valid: true,
      streamer: {
        ...streamer,
        current_viewers: currentViewers,
        stream_url: stream_url
      }
    });
  } catch (err) {
    console.error(`[VALIDATE] Erro:`, err.message);
    return res.status(500).json({ valid: false, message: 'Erro interno do servidor' });
  }
});

// ── POST /api/viewer/join — Viewer entra na sala ──
app.post('/api/viewer/join', requireApiKey, async (req, res) => {
  try {
    const { id_streamer, viewer_uid } = req.body;

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
    const currentViewers = getViewerCount(id_streamer);

    // max_spectators = 0 significa sem limite
    if (maxSpectators > 0 && currentViewers >= maxSpectators) {
      console.log(`[JOIN] Sala cheia para "${id_streamer}": ${currentViewers}/${maxSpectators}`);
      return res.status(403).json({
        message: 'Sala cheia',
        current_viewers: currentViewers,
        max_spectators: maxSpectators
      });
    }

    // Registrar viewer
    registerViewer(id_streamer, viewer_uid);
    const newCount = getViewerCount(id_streamer);

    console.log(`[JOIN] Viewer ${viewer_uid.substring(0, 8)}... entrou em "${id_streamer}" | Viewers: ${newCount}/${maxSpectators || 'ilimitado'}`);

    return res.json({
      joined: true,
      current_viewers: newCount,
      max_spectators: maxSpectators
    });
  } catch (err) {
    console.error('[JOIN] Erro:', err.message);
    return res.status(500).json({ message: 'Erro interno do servidor' });
  }
});

// ── POST /api/viewer/heartbeat — Viewer ainda está assistindo ──
app.post('/api/viewer/heartbeat', requireApiKey, async (req, res) => {
  try {
    const { id_streamer, viewer_uid } = req.body;

    if (!id_streamer || !viewer_uid) {
      return res.status(400).json({ message: 'Campos obrigatorios faltando' });
    }

    registerViewer(id_streamer, viewer_uid);
    const currentViewers = getViewerCount(id_streamer);

    return res.json({
      active: true,
      current_viewers: currentViewers
    });
  } catch (err) {
    console.error('[HEARTBEAT] Erro:', err.message);
    return res.status(500).json({ message: 'Erro interno do servidor' });
  }
});

// ── POST /api/viewer/leave — Viewer saiu ──
app.post('/api/viewer/leave', requireApiKey, async (req, res) => {
  try {
    const { id_streamer, viewer_uid } = req.body;

    if (!id_streamer || !viewer_uid) {
      return res.status(400).json({ message: 'Campos obrigatorios faltando' });
    }

    removeViewer(id_streamer, viewer_uid);
    const currentViewers = getViewerCount(id_streamer);

    console.log(`[LEAVE] Viewer ${viewer_uid.substring(0, 8)}... saiu de "${id_streamer}" | Viewers: ${currentViewers}`);

    return res.json({
      left: true,
      current_viewers: currentViewers
    });
  } catch (err) {
    console.error('[LEAVE] Erro:', err.message);
    return res.status(500).json({ message: 'Erro interno do servidor' });
  }
});

// ── GET /api/viewer/count/:id_streamer — Ver contagem atual ──
app.get('/api/viewer/count/:id_streamer', requireApiKey, async (req, res) => {
  const { id_streamer } = req.params;
  const currentViewers = getViewerCount(id_streamer);

  const result = await pool.query(
    'SELECT max_spectators FROM streamer WHERE LOWER(id_streamer) = LOWER($1)',
    [id_streamer]
  );

  const maxSpectators = result.rows.length > 0 ? result.rows[0].max_spectators : 0;

  return res.json({
    id_streamer,
    current_viewers: currentViewers,
    max_spectators: maxSpectators
  });
});

// ── GET /api/streamers (admin) ──
app.get('/api/streamers', requireApiKey, async (req, res) => {
  try {
    console.log('[LIST] Listando todos os streamers');
    const result = await pool.query('SELECT id, "user", link, id_streamer, max_spectators, link_vps, id_mediamtx FROM streamer ORDER BY id');

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

// ── PUT /api/streamer/:id_streamer (admin — atualizar max_spectators) ──
app.put('/api/streamer/:id_streamer', requireApiKey, async (req, res) => {
  try {
    const { id_streamer } = req.params;
    const { user, link, max_spectators, link_vps, id_mediamtx } = req.body;

    console.log(`[UPDATE] Atualizando streamer: "${id_streamer}"`);

    const result = await pool.query(
      `UPDATE streamer SET
        "user" = COALESCE($1, "user"),
        link = COALESCE($2, link),
        max_spectators = COALESCE($3, max_spectators),
        link_vps = COALESCE($4, link_vps),
        id_mediamtx = COALESCE($5, id_mediamtx)
      WHERE LOWER(id_streamer) = LOWER($6) RETURNING *`,
      [user || null, link || null, max_spectators !== undefined ? max_spectators : null, link_vps !== undefined ? link_vps : null, id_mediamtx !== undefined ? id_mediamtx : null, id_streamer]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: 'Streamer nao encontrado' });
    }

    console.log(`[UPDATE] Streamer atualizado: ${JSON.stringify(result.rows[0])}`);
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
    return res.json({ message: 'Streamer removido', streamer: result.rows[0] });
  } catch (err) {
    console.error('[DELETE] Erro:', err.message);
    return res.status(500).json({ message: 'Erro interno do servidor' });
  }
});

// ── Metrics + Daily (in-memory + flush periódico pro PostgreSQL) ──
const FLUSH_INTERVAL = 30000; // Flush pro banco a cada 30s

// Cache em memória — operações instantâneas, sem I/O
const metricsMemory = {}; // { "streamer_date": { viewers: { ip: { ... } } } }
const dailyMemory = {};   // { "streamer_date": { unique_viewers: [...] } }
const dirtyMetrics = new Set();
const dirtyDaily = new Set();

function memKey(idStreamer, date) {
  return `${idStreamer.toLowerCase()}_${date || getBrazilDate()}`;
}

function getMetricsMem(idStreamer, date) {
  const key = memKey(idStreamer, date);
  if (!metricsMemory[key]) {
    metricsMemory[key] = { streamer: idStreamer, date: date || getBrazilDate(), viewers: {} };
  }
  return metricsMemory[key];
}

function getDailyMem(idStreamer, date) {
  const key = memKey(idStreamer, date);
  if (!dailyMemory[key]) {
    dailyMemory[key] = { streamer: idStreamer, date: date || getBrazilDate(), unique_viewers: [] };
  }
  return dailyMemory[key];
}

// Carregar dados do banco na memória (chamado na inicialização)
async function loadMetricsFromDB() {
  try {
    const today = getBrazilDate();

    // Carregar métricas do dia
    const metricsRows = await pool.query(
      'SELECT * FROM live_metrics WHERE date = $1', [today]
    );
    for (const row of metricsRows.rows) {
      const mem = getMetricsMem(row.streamer, row.date);
      if (!mem.viewers[row.ip]) {
        mem.viewers[row.ip] = { kick_username: row.kick_username, join_count: 0, connections: [] };
      }
      mem.viewers[row.ip].join_count++;
      if (row.kick_username) mem.viewers[row.ip].kick_username = row.kick_username;
      mem.viewers[row.ip].connections.push({
        viewer_uid: row.viewer_uid,
        platform: row.platform,
        os: row.os,
        os_version: row.os_version,
        device_model: row.device_model,
        browser: row.browser,
        browser_version: row.browser_version,
        user_agent: row.user_agent,
        is_mobile: row.is_mobile,
        joined_at: row.joined_at,
        last_seen: row.last_seen,
        segments_loaded: row.segments_loaded,
        estimated_mb: row.estimated_mb,
        quality_history: row.quality_history || [],
        player_health: row.player_health || {},
      });
    }

    // Carregar daily do dia
    const dailyRows = await pool.query(
      'SELECT * FROM daily_viewers WHERE date = $1', [today]
    );
    for (const row of dailyRows.rows) {
      const mem = getDailyMem(row.streamer, row.date);
      mem.unique_viewers.push({
        ip: row.ip,
        kick_username: row.kick_username,
        first_seen: row.first_seen,
        last_seen: row.last_seen,
        total_seconds: row.total_seconds,
        sessions: row.sessions,
        current_uid: row.current_uid,
      });
    }

    console.log(`[DB] Carregados ${metricsRows.rows.length} metrics e ${dailyRows.rows.length} daily do dia ${today}`);
  } catch (e) {
    console.warn('[DB] Erro ao carregar metrics/daily:', e.message);
  }
}

// Flush periódico — salva dados modificados no PostgreSQL
async function flushToDB() {
  let metricsCount = 0;
  let dailyCount = 0;

  // Flush metrics
  for (const key of dirtyMetrics) {
    const data = metricsMemory[key];
    if (!data) continue;
    try {
      for (const [ip, viewer] of Object.entries(data.viewers)) {
        for (const conn of viewer.connections) {
          await pool.query(`
            INSERT INTO live_metrics (streamer, date, ip, kick_username, viewer_uid, platform, os, os_version, device_model, browser, browser_version, user_agent, is_mobile, joined_at, last_seen, segments_loaded, estimated_mb, quality_history, player_health)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
            ON CONFLICT (streamer, date, viewer_uid) DO UPDATE SET
              kick_username = EXCLUDED.kick_username,
              last_seen = EXCLUDED.last_seen,
              segments_loaded = EXCLUDED.segments_loaded,
              estimated_mb = EXCLUDED.estimated_mb,
              quality_history = EXCLUDED.quality_history,
              player_health = EXCLUDED.player_health
          `, [
            data.streamer, data.date, ip, viewer.kick_username || '', conn.viewer_uid,
            conn.platform, conn.os, conn.os_version, conn.device_model,
            conn.browser, conn.browser_version, conn.user_agent, conn.is_mobile,
            conn.joined_at, conn.last_seen, conn.segments_loaded, conn.estimated_mb,
            JSON.stringify(conn.quality_history || []),
            JSON.stringify(conn.player_health || {}),
          ]);
          metricsCount++;
        }
      }
    } catch (e) {
      console.warn(`[FLUSH] Erro metrics ${key}:`, e.message);
    }
  }
  dirtyMetrics.clear();

  // Flush daily
  for (const key of dirtyDaily) {
    const data = dailyMemory[key];
    if (!data) continue;
    try {
      for (const v of data.unique_viewers) {
        await pool.query(`
          INSERT INTO daily_viewers (streamer, date, ip, kick_username, first_seen, last_seen, total_seconds, sessions, current_uid)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
          ON CONFLICT (streamer, date, ip) DO UPDATE SET
            kick_username = EXCLUDED.kick_username,
            last_seen = EXCLUDED.last_seen,
            total_seconds = EXCLUDED.total_seconds,
            sessions = EXCLUDED.sessions,
            current_uid = EXCLUDED.current_uid
        `, [
          data.streamer, data.date, v.ip, v.kick_username,
          v.first_seen, v.last_seen, v.total_seconds, v.sessions, v.current_uid,
        ]);
        dailyCount++;
      }
    } catch (e) {
      console.warn(`[FLUSH] Erro daily ${key}:`, e.message);
    }
  }
  dirtyDaily.clear();

  if (metricsCount > 0 || dailyCount > 0) {
    console.log(`[FLUSH] DB: ${metricsCount} metrics, ${dailyCount} daily`);
  }
}

// Flush a cada 30s
setInterval(flushToDB, FLUSH_INTERVAL);

// Flush ao encerrar
process.on('SIGTERM', async () => { await flushToDB(); process.exit(0); });
process.on('SIGINT', async () => { await flushToDB(); process.exit(0); });

// POST /api/metrics/join — viewer registra entrada com device info
app.post('/api/metrics/join', requireApiKey, (req, res) => {
  try {
    const { id_streamer, viewer_uid, device_info } = req.body;
    if (!id_streamer || !viewer_uid) {
      return res.status(400).json({ message: 'Campos obrigatorios: id_streamer, viewer_uid' });
    }

    const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim()
      || req.headers['x-real-ip']
      || req.socket.remoteAddress
      || 'unknown';

    const metrics = getMetricsMem(id_streamer);
    const now = getBrazilTimestamp();

    if (!metrics.viewers[ip]) {
      metrics.viewers[ip] = { kick_username: device_info?.kick_username || '', join_count: 1, connections: [] };
    } else {
      metrics.viewers[ip].join_count = (metrics.viewers[ip].join_count || 1) + 1;
      if (device_info?.kick_username) metrics.viewers[ip].kick_username = device_info.kick_username;
    }

    metrics.viewers[ip].connections.unshift({
      viewer_uid, platform: device_info?.platform || 'unknown',
      os: device_info?.os || 'unknown', os_version: device_info?.os_version || '',
      device_model: device_info?.device_model || '', browser: device_info?.browser || 'unknown',
      browser_version: device_info?.browser_version || '', user_agent: device_info?.user_agent || '',
      is_mobile: device_info?.is_mobile || false,
      joined_at: now, last_seen: now, segments_loaded: 0, estimated_mb: 0, quality_history: [],
    });

    console.log(`[METRICS] Viewer ${ip} (${device_info?.kick_username || '?'}) — conexão #${metrics.viewers[ip].join_count}`);
    dirtyMetrics.add(memKey(id_streamer));

    // Daily
    trackDailyViewer(id_streamer, ip, device_info?.kick_username, viewer_uid);

    return res.json({ tracked: true });
  } catch (e) {
    console.error('[METRICS] Erro join:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// POST /api/metrics/update — viewer envia consumo periodicamente
app.post('/api/metrics/update', requireApiKey, (req, res) => {
  try {
    const { id_streamer, viewer_uid, segments_loaded, current_quality, player_health } = req.body;
    if (!id_streamer || !viewer_uid) {
      return res.status(400).json({ message: 'Campos obrigatorios' });
    }

    const metrics = getMetricsMem(id_streamer);

    let connection = null;
    for (const [, viewer] of Object.entries(metrics.viewers)) {
      if (viewer.connections) {
        connection = viewer.connections.find(c => c.viewer_uid === viewer_uid);
        if (connection) break;
      }
    }
    if (!connection) return res.json({ tracked: false });

    connection.last_seen = getBrazilTimestamp();
    if (segments_loaded !== undefined) {
      connection.segments_loaded = segments_loaded;
      connection.estimated_mb = Math.round(segments_loaded * 1.2 * 10) / 10;
    }
    if (current_quality && (connection.quality_history.length === 0 || connection.quality_history[connection.quality_history.length - 1].q !== current_quality)) {
      connection.quality_history.push({ q: current_quality, at: getBrazilTimestamp() });
    }
    if (player_health) connection.player_health = player_health;

    dirtyMetrics.add(memKey(id_streamer));
    updateDailyTime(id_streamer, viewer_uid);

    return res.json({ tracked: true });
  } catch (e) {
    console.error('[METRICS] Erro update:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// GET /api/metrics/:id_streamer — consultar métricas
app.get('/api/metrics/:id_streamer', requireApiKey, async (req, res) => {
  try {
    const { id_streamer } = req.params;
    const date = req.query.date || getBrazilDate();

    // Se é o dia atual, usa memória. Se é histórico, busca no banco.
    let viewers;
    if (date === getBrazilDate()) {
      const metrics = getMetricsMem(id_streamer, date);
      viewers = metrics.viewers;
    } else {
      const rows = await pool.query(
        'SELECT * FROM live_metrics WHERE LOWER(streamer) = LOWER($1) AND date = $2', [id_streamer, date]
      );
      viewers = {};
      for (const row of rows.rows) {
        if (!viewers[row.ip]) {
          viewers[row.ip] = { kick_username: row.kick_username, join_count: 0, connections: [] };
        }
        viewers[row.ip].join_count++;
        if (row.kick_username) viewers[row.ip].kick_username = row.kick_username;
        viewers[row.ip].connections.push({
          viewer_uid: row.viewer_uid, platform: row.platform, os: row.os,
          os_version: row.os_version, device_model: row.device_model,
          browser: row.browser, browser_version: row.browser_version,
          user_agent: row.user_agent, is_mobile: row.is_mobile,
          joined_at: row.joined_at, last_seen: row.last_seen,
          segments_loaded: row.segments_loaded, estimated_mb: row.estimated_mb,
          quality_history: row.quality_history || [], player_health: row.player_health || {},
        });
      }
    }

    const viewerEntries = Object.entries(viewers);
    const totalJoins = viewerEntries.reduce((sum, [, v]) => sum + (v.join_count || 1), 0);
    const allConnections = viewerEntries.flatMap(([, v]) => v.connections || []);

    return res.json({
      streamer: id_streamer, date,
      unique_viewers: viewerEntries.length,
      total_joins: totalJoins,
      total_connections: allConnections.length,
      mobile: allConnections.filter(c => c.is_mobile).length,
      desktop: allConnections.filter(c => !c.is_mobile).length,
      total_estimated_mb: Math.round(allConnections.reduce((sum, c) => sum + (c.estimated_mb || 0), 0) * 10) / 10,
      viewers,
    });
  } catch (e) {
    console.error('[METRICS] Erro get:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// GET /api/metrics — listar datas disponíveis
app.get('/api/metrics', requireApiKey, async (req, res) => {
  try {
    const { streamer } = req.query;
    let query = 'SELECT DISTINCT date FROM live_metrics';
    const params = [];
    if (streamer) {
      query += ' WHERE LOWER(streamer) = LOWER($1)';
      params.push(streamer);
    }
    query += ' ORDER BY date DESC';
    const result = await pool.query(query, params);
    return res.json({ streamer: streamer || 'all', dates: result.rows.map(r => r.date) });
  } catch (e) {
    console.error('[METRICS] Erro list:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// ── Daily helpers ──

function trackDailyViewer(idStreamer, ip, kickUsername, viewerUid) {
  const daily = getDailyMem(idStreamer);

  const exists = daily.unique_viewers.find(v =>
    v.ip === ip || (kickUsername && v.kick_username && v.kick_username.toLowerCase() === kickUsername.toLowerCase())
  );

  if (exists) {
    exists.last_seen = getBrazilTimestamp();
    exists.sessions = (exists.sessions || 1) + 1;
    if (kickUsername) exists.kick_username = kickUsername;
    exists.current_uid = viewerUid;
  } else {
    daily.unique_viewers.push({
      ip, kick_username: kickUsername || '',
      first_seen: getBrazilTimestamp(), last_seen: getBrazilTimestamp(),
      total_seconds: 0, sessions: 1, current_uid: viewerUid,
    });
  }

  dirtyDaily.add(memKey(idStreamer));
}

function updateDailyTime(idStreamer, viewerUid) {
  const daily = getDailyMem(idStreamer);
  const viewer = daily.unique_viewers.find(v => v.current_uid === viewerUid);
  if (viewer) {
    const now = new Date();
    const lastSeen = new Date(viewer.last_seen);
    const diff = Math.round((now - lastSeen) / 1000);
    if (diff > 0 && diff < 120) {
      viewer.total_seconds = (viewer.total_seconds || 0) + diff;
    }
    viewer.last_seen = getBrazilTimestamp();
    dirtyDaily.add(memKey(idStreamer));
  }
}

// GET /api/daily/:id_streamer — resumo diário
app.get('/api/daily/:id_streamer', requireApiKey, async (req, res) => {
  try {
    const { id_streamer } = req.params;
    const date = req.query.date || getBrazilDate();

    let uniqueViewers;
    if (date === getBrazilDate()) {
      const daily = getDailyMem(id_streamer, date);
      uniqueViewers = daily.unique_viewers;
    } else {
      const rows = await pool.query(
        'SELECT * FROM daily_viewers WHERE LOWER(streamer) = LOWER($1) AND date = $2', [id_streamer, date]
      );
      uniqueViewers = rows.rows.map(r => ({
        ip: r.ip, kick_username: r.kick_username,
        first_seen: r.first_seen, last_seen: r.last_seen,
        total_seconds: r.total_seconds, sessions: r.sessions,
      }));
    }

    const viewers = uniqueViewers.map(v => ({
      ip: v.ip, kick_username: v.kick_username,
      first_seen: v.first_seen, last_seen: v.last_seen,
      sessions: v.sessions,
      time_minutes: Math.round((v.total_seconds || 0) / 60 * 10) / 10,
      time_formatted: formatTime(v.total_seconds || 0),
    }));

    return res.json({
      streamer: id_streamer, date,
      total_unique_viewers: uniqueViewers.length,
      viewers,
    });
  } catch (e) {
    console.error('[DAILY] Erro get:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// GET /api/daily — listar datas disponíveis
app.get('/api/daily', requireApiKey, async (req, res) => {
  try {
    const { streamer } = req.query;
    let query = 'SELECT DISTINCT date FROM daily_viewers';
    const params = [];
    if (streamer) {
      query += ' WHERE LOWER(streamer) = LOWER($1)';
      params.push(streamer);
    }
    query += ' ORDER BY date DESC';
    const result = await pool.query(query, params);
    return res.json({ streamer: streamer || 'all', dates: result.rows.map(r => r.date) });
  } catch (e) {
    console.error('[DAILY] Erro list:', e.message);
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

// ── Start ──
const PORT = process.env.PORT || 3000;

initDB().then(() => loadMetricsFromDB()).then(() => {
  app.listen(PORT, () => {
    console.log(`[SERVER] API rodando na porta ${PORT}`);
    console.log(`[SERVER] Endpoints disponíveis:`);
    console.log(`   GET    /health`);
    console.log(`   GET    /api/streamer/validate/:id_streamer`);
    console.log(`   GET    /api/streamers`);
    console.log(`   POST   /api/streamer`);
    console.log(`   PUT    /api/streamer/:id_streamer`);
    console.log(`   DELETE /api/streamer/:id_streamer`);
    console.log(`   POST   /api/viewer/join`);
    console.log(`   POST   /api/viewer/heartbeat`);
    console.log(`   POST   /api/viewer/leave`);
    console.log(`   GET    /api/viewer/count/:id_streamer`);
  });
});
