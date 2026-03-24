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

    // Adicionar avg_viewers se não existir (banco existente)
    await pool.query(`ALTER TABLE lives ADD COLUMN IF NOT EXISTS avg_viewers REAL DEFAULT 0`);

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

    // Detectar início de live
    if (stream_url && !activeLives[id_streamer]) {
      await onLiveStart(id_streamer, streamer.user);
    }
    updateLivePeak(id_streamer, currentViewers);

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

// ══════════════════════════════════════════════
// ── LIVES TRACKING (in-memory + flush to DB) ──
// ══════════════════════════════════════════════

const FLUSH_INTERVAL = 30000; // 30s

// Em memória: lives ativas { id_streamer: { liveId, peakViewers, viewerSessions: {} } }
const activeLives = {};

// Detectar início de live
async function onLiveStart(idStreamer, streamerName) {
  if (activeLives[idStreamer]) return;
  try {
    const result = await pool.query(
      `INSERT INTO lives (streamer, id_streamer, started_at, status) VALUES ($1, $2, NOW(), 'active') RETURNING id`,
      [streamerName, idStreamer]
    );
    const liveId = result.rows[0].id;
    activeLives[idStreamer] = { liveId, peakViewers: 0, viewerSessions: {} };
    console.log(`[LIVE] Iniciada: ${streamerName} (${idStreamer}) → live #${liveId}`);
  } catch (e) {
    console.error('[LIVE] Erro ao iniciar:', e.message);
  }
}

// Detectar fim de live
async function onLiveEnd(idStreamer) {
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
    const totalWatchSeconds = Object.values(live.viewerSessions)
      .reduce((sum, s) => sum + (s.total_seconds || 0), 0);
    const avgViewers = durationSeconds > 0
      ? Math.round((totalWatchSeconds / durationSeconds) * 10) / 10
      : 0;

    await pool.query(
      `UPDATE lives SET ended_at = NOW(), duration_seconds = $1,
       peak_viewers = $2, total_unique_viewers = $3, avg_viewers = $4, status = 'ended' WHERE id = $5`,
      [durationSeconds, live.peakViewers, uniqueIPs.size, avgViewers, live.liveId]
    );
    await flushLiveViewerSessions(live);
    console.log(`[LIVE] Encerrada: ${idStreamer} → live #${live.liveId} | Peak: ${live.peakViewers} | Média: ${avgViewers} | Únicos: ${uniqueIPs.size}`);
  } catch (e) {
    console.error('[LIVE] Erro ao encerrar:', e.message);
  }
  delete activeLives[idStreamer];
}

// Flush sessões de viewer pro banco
async function flushLiveViewerSessions(live) {
  let count = 0;
  for (const [viewerUid, s] of Object.entries(live.viewerSessions)) {
    try {
      await pool.query(`
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
      ]);
      count++;
    } catch (e) {
      console.warn(`[LIVE] Erro flush viewer ${viewerUid}:`, e.message);
    }
  }
  if (count > 0) console.log(`[FLUSH] Live #${live.liveId}: ${count} viewers`);
}

// Registrar viewer na live ativa
function trackLiveViewer(idStreamer, viewerUid, deviceInfo, ip) {
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
      joined_at: now, last_seen: now, total_seconds: 0, segments_loaded: 0, estimated_mb: 0,
      quality_history: [], player_health: {},
    };
  }
}

// Atualizar peak viewers
function updateLivePeak(idStreamer, currentViewers) {
  const live = activeLives[idStreamer];
  if (live && currentViewers > live.peakViewers) live.peakViewers = currentViewers;
}

// Verificar status das lives a cada 30s
async function checkLiveStatus() {
  for (const idStreamer of Object.keys(activeLives)) {
    try {
      const result = await pool.query(
        'SELECT id_mediamtx FROM streamer WHERE LOWER(id_streamer) = LOWER($1)', [idStreamer]
      );
      if (result.rows.length === 0) continue;
      const mediamtxPath = result.rows[0].id_mediamtx;
      if (!mediamtxPath) continue;
      const url = await getCachedStreamUrl(mediamtxPath);
      if (!url) await onLiveEnd(idStreamer);
    } catch (e) { /* ignore */ }
  }
  // Flush sessions das lives ativas
  for (const live of Object.values(activeLives)) {
    await flushLiveViewerSessions(live);
  }
}

// Flush a cada 30s + check live status
setInterval(async () => {
  await checkLiveStatus();
}, FLUSH_INTERVAL);

// Encerrar lives ativas ao desligar
process.on('SIGTERM', async () => {
  for (const id of Object.keys(activeLives)) await onLiveEnd(id);
  process.exit(0);
});
process.on('SIGINT', async () => {
  for (const id of Object.keys(activeLives)) await onLiveEnd(id);
  process.exit(0);
});

// POST /api/metrics/join — viewer registra entrada
app.post('/api/metrics/join', requireApiKey, (req, res) => {
  try {
    const { id_streamer, viewer_uid, device_info } = req.body;
    if (!id_streamer || !viewer_uid) {
      return res.status(400).json({ message: 'Campos obrigatorios: id_streamer, viewer_uid' });
    }
    const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim()
      || req.headers['x-real-ip'] || req.socket.remoteAddress || 'unknown';
    trackLiveViewer(id_streamer, viewer_uid, device_info, ip);
    console.log(`[METRICS] Viewer ${ip} (${device_info?.kick_username || '?'}) entrou em ${id_streamer}`);
    return res.json({ tracked: true });
  } catch (e) {
    console.error('[METRICS] Erro join:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// POST /api/metrics/update — viewer envia consumo
app.post('/api/metrics/update', requireApiKey, (req, res) => {
  try {
    const { id_streamer, viewer_uid, segments_loaded, current_quality, player_health } = req.body;
    if (!id_streamer || !viewer_uid) {
      return res.status(400).json({ message: 'Campos obrigatorios' });
    }
    const live = activeLives[id_streamer];
    if (live && live.viewerSessions[viewer_uid]) {
      const session = live.viewerSessions[viewer_uid];
      const now = new Date();
      const lastSeen = new Date(session.last_seen);
      const diff = Math.round((now - lastSeen) / 1000);
      if (diff > 0 && diff < 120) session.total_seconds = (session.total_seconds || 0) + diff;
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
    updateLivePeak(id_streamer, getViewerCount(id_streamer));
    return res.json({ tracked: true });
  } catch (e) {
    console.error('[METRICS] Erro update:', e.message);
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
              peak_viewers, total_unique_viewers, avg_viewers, status
       FROM lives WHERE LOWER(id_streamer) = LOWER($1)
       ORDER BY started_at DESC LIMIT $2`, [id_streamer, limit]
    );
    const lives = result.rows.map(r => ({
      id: r.id, streamer: r.streamer, started_at: r.started_at, ended_at: r.ended_at,
      duration: formatTime(r.duration_seconds || 0), duration_seconds: r.duration_seconds,
      peak_viewers: r.peak_viewers, total_unique_viewers: r.total_unique_viewers,
      avg_viewers: r.avg_viewers || 0, status: r.status,
    }));
    return res.json({ streamer: id_streamer, total: lives.length, lives });
  } catch (e) {
    console.error('[LIVES] Erro list:', e.message);
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
        avg_viewers: live.avg_viewers || 0, status: live.status,
      },
      viewers: {
        total: viewers.length,
        mobile: viewers.filter(v => v.is_mobile).length,
        desktop: viewers.filter(v => !v.is_mobile).length,
        list: viewers,
      },
    });
  } catch (e) {
    console.error('[LIVES] Erro detail:', e.message);
    return res.status(500).json({ message: 'Erro interno' });
  }
});

// Restaurar lives ativas do banco (se API reiniciou durante uma live)
async function restoreActiveLives() {
  try {
    const result = await pool.query("SELECT * FROM lives WHERE status = 'active'");
    for (const row of result.rows) {
      activeLives[row.id_streamer] = { liveId: row.id, peakViewers: row.peak_viewers || 0, viewerSessions: {} };
      const sessions = await pool.query('SELECT * FROM live_viewer_sessions WHERE live_id = $1', [row.id]);
      for (const s of sessions.rows) {
        activeLives[row.id_streamer].viewerSessions[s.viewer_uid] = {
          ip: s.ip, kick_username: s.kick_username, platform: s.platform, os: s.os,
          os_version: s.os_version, device_model: s.device_model, browser: s.browser,
          browser_version: s.browser_version, user_agent: s.user_agent, is_mobile: s.is_mobile,
          joined_at: s.joined_at, last_seen: s.last_seen, total_seconds: s.total_seconds,
          segments_loaded: s.segments_loaded, estimated_mb: s.estimated_mb,
          quality_history: s.quality_history || [], player_health: s.player_health || {},
        };
      }
      console.log(`[LIVE] Restaurada: ${row.streamer} (${row.id_streamer}) → live #${row.id} | ${sessions.rows.length} viewers`);
    }
  } catch (e) {
    console.warn('[LIVE] Erro ao restaurar lives:', e.message);
  }
}

// ── Start ──
const PORT = process.env.PORT || 3000;

initDB().then(() => restoreActiveLives()).then(() => {
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
    console.log(`   POST   /api/metrics/join`);
    console.log(`   POST   /api/metrics/update`);
    console.log(`   GET    /api/lives/:id_streamer`);
    console.log(`   GET    /api/lives/:id_streamer/:live_id`);
  });
});
