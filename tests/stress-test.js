/**
 * Stress Test — Simula viewers simultâneos na API
 *
 * Uso:
 *   node stress-test.js                        # 100 viewers, streamer tteuw21
 *   node stress-test.js 600                    # 600 viewers
 *   node stress-test.js 600 beli21             # 600 viewers no beli21
 *   node stress-test.js 600 tteuw21 burst      # todos ao mesmo tempo (burst)
 *   node stress-test.js 600 tteuw21 ramp       # gradual em 10s (ramp)
 *
 * O teste simula o fluxo real do overlay:
 *   1. GET /validate (buscar stream_url)
 *   2. POST /join (entrar na sala)
 *   3. POST /heartbeat (manter vivo)
 *   4. POST /leave (sair)
 */

const API_URL = 'https://extensao-api.squareweb.app';
const API_KEY = 'vdo-overlay-k8x2m9p4q7w1';

const TOTAL_VIEWERS = parseInt(process.argv[2]) || 100;
const STREAMER = process.argv[3] || 'tteuw21';
const MODE = process.argv[4] || 'burst'; // burst | ramp

// Métricas globais
const metrics = {
  validate: { total: 0, ok: 0, fail: 0, empty_url: 0, times: [] },
  join: { total: 0, ok: 0, fail: 0, times: [] },
  heartbeat: { total: 0, ok: 0, fail: 0, times: [] },
  leave: { total: 0, ok: 0, fail: 0, times: [] },
};

function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
    const r = Math.random() * 16 | 0;
    return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
  });
}

async function timedFetch(url, options = {}) {
  const start = Date.now();
  try {
    const resp = await fetch(url, {
      ...options,
      headers: { 'X-Api-Key': API_KEY, 'Content-Type': 'application/json', 'X-Stress-Test': 'vdo-stress-2026', ...options.headers },
      signal: AbortSignal.timeout(15000),
    });
    const ms = Date.now() - start;
    const data = await resp.json().catch(() => ({}));
    return { ok: resp.ok, status: resp.status, ms, data };
  } catch (err) {
    return { ok: false, status: 0, ms: Date.now() - start, data: {}, error: err.message };
  }
}

async function simulateViewer(viewerId, viewerUid) {
  // 1. VALIDATE
  const validate = await timedFetch(
    `${API_URL}/api/streamer/validate/${encodeURIComponent(STREAMER)}?viewer_uid=${viewerUid}`
  );
  metrics.validate.total++;
  metrics.validate.times.push(validate.ms);

  if (!validate.ok) {
    metrics.validate.fail++;
    if (validate.status === 429) {
      metrics.validate.rate_limited = (metrics.validate.rate_limited || 0) + 1;
    }
    return { viewerId, step: 'validate', error: validate.status, ms: validate.ms };
  }

  const streamUrl = validate.data?.streamer?.stream_url || '';
  if (!streamUrl) {
    metrics.validate.empty_url++;
    metrics.validate.ok++;
    return { viewerId, step: 'validate', result: 'empty_url', ms: validate.ms };
  }
  metrics.validate.ok++;

  // 2. JOIN
  const join = await timedFetch(`${API_URL}/api/viewer/join`, {
    method: 'POST',
    body: JSON.stringify({ id_streamer: STREAMER, viewer_uid: viewerUid }),
  });
  metrics.join.total++;
  metrics.join.times.push(join.ms);

  if (!join.ok) {
    metrics.join.fail++;
    return { viewerId, step: 'join', error: join.status, ms: join.ms };
  }
  metrics.join.ok++;

  // 3. HEARTBEAT (1 vez)
  await new Promise(r => setTimeout(r, 1000 + Math.random() * 2000));

  const hb = await timedFetch(`${API_URL}/api/viewer/heartbeat`, {
    method: 'POST',
    body: JSON.stringify({ id_streamer: STREAMER, viewer_uid: viewerUid }),
  });
  metrics.heartbeat.total++;
  metrics.heartbeat.times.push(hb.ms);
  if (hb.ok) metrics.heartbeat.ok++; else metrics.heartbeat.fail++;

  // 4. LEAVE
  await new Promise(r => setTimeout(r, 500));

  const leave = await timedFetch(`${API_URL}/api/viewer/leave`, {
    method: 'POST',
    body: JSON.stringify({ id_streamer: STREAMER, viewer_uid: viewerUid }),
  });
  metrics.leave.total++;
  metrics.leave.times.push(leave.ms);
  if (leave.ok) metrics.leave.ok++; else metrics.leave.fail++;

  return { viewerId, step: 'complete', validateMs: validate.ms, joinMs: join.ms };
}

function percentile(arr, p) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.ceil(sorted.length * p / 100) - 1;
  return sorted[Math.max(0, idx)];
}

function printMetrics() {
  console.log('\n' + '='.repeat(70));
  console.log('  RESULTADO DO STRESS TEST');
  console.log('='.repeat(70));
  console.log(`  Viewers: ${TOTAL_VIEWERS} | Streamer: ${STREAMER} | Mode: ${MODE}`);
  console.log('='.repeat(70));

  for (const [name, m] of Object.entries(metrics)) {
    if (m.total === 0) continue;
    const avg = m.times.length > 0 ? Math.round(m.times.reduce((a, b) => a + b, 0) / m.times.length) : 0;
    const p50 = percentile(m.times, 50);
    const p95 = percentile(m.times, 95);
    const p99 = percentile(m.times, 99);
    const max = m.times.length > 0 ? Math.max(...m.times) : 0;

    console.log(`\n  ${name.toUpperCase()}`);
    console.log(`    Total: ${m.total} | OK: ${m.ok} | Fail: ${m.fail}${m.empty_url ? ` | Empty URL: ${m.empty_url}` : ''}${m.rate_limited ? ` | Rate Limited (429): ${m.rate_limited}` : ''}`);
    console.log(`    Tempo: avg=${avg}ms | p50=${p50}ms | p95=${p95}ms | p99=${p99}ms | max=${max}ms`);

    if (m.fail > 0) {
      const failRate = ((m.fail / m.total) * 100).toFixed(1);
      console.log(`    ⚠ Taxa de falha: ${failRate}%`);
    }
  }

  console.log('\n' + '='.repeat(70));

  // Veredicto
  const validateP95 = percentile(metrics.validate.times, 95);
  const failRate = metrics.validate.total > 0 ? metrics.validate.fail / metrics.validate.total : 0;

  if (failRate > 0.05) {
    console.log('  ❌ REPROVADO — taxa de falha > 5%');
  } else if (validateP95 > 5000) {
    console.log('  ⚠  ATENÇÃO — validate p95 > 5s (lento mas funcional)');
  } else {
    console.log('  ✅ APROVADO — API aguentou a carga');
  }
  console.log('='.repeat(70) + '\n');
}

async function main() {
  console.log(`\nStress Test: ${TOTAL_VIEWERS} viewers → ${STREAMER} (${MODE})`);
  console.log(`API: ${API_URL}\n`);

  // Verificar se a API está respondendo
  const health = await timedFetch(`${API_URL}/health`);
  if (!health.ok) {
    console.error('❌ API não respondeu no /health. Abortando.');
    process.exit(1);
  }
  console.log(`✓ API online (${health.ms}ms)\n`);

  const viewers = Array.from({ length: TOTAL_VIEWERS }, (_, i) => ({
    id: i + 1,
    uid: uuid(),
  }));

  const startTime = Date.now();

  if (MODE === 'burst') {
    // Todos ao mesmo tempo
    console.log(`Disparando ${TOTAL_VIEWERS} viewers simultaneamente...\n`);
    const promises = viewers.map(v => simulateViewer(v.id, v.uid));
    const results = await Promise.all(promises);

    const completes = results.filter(r => r.step === 'complete').length;
    const validateErrors = results.filter(r => r.step === 'validate' && r.error).length;
    const emptyUrls = results.filter(r => r.result === 'empty_url').length;

    console.log(`\nResultados rápidos: ${completes} completos | ${validateErrors} erros validate | ${emptyUrls} URL vazia`);

  } else if (MODE === 'ramp') {
    // Gradual em 10 segundos
    const rampDurationMs = 10000;
    const delayBetween = rampDurationMs / TOTAL_VIEWERS;

    console.log(`Ramp up: ${TOTAL_VIEWERS} viewers em ${rampDurationMs / 1000}s (${Math.round(1000 / delayBetween)} viewers/s)...\n`);

    const promises = [];
    for (const v of viewers) {
      promises.push(simulateViewer(v.id, v.uid));
      await new Promise(r => setTimeout(r, delayBetween));

      // Progresso a cada 10%
      if (promises.length % Math.ceil(TOTAL_VIEWERS / 10) === 0) {
        const pct = Math.round((promises.length / TOTAL_VIEWERS) * 100);
        process.stdout.write(`  ${pct}% enviados (${promises.length}/${TOTAL_VIEWERS})\r`);
      }
    }
    console.log(`  100% enviados (${TOTAL_VIEWERS}/${TOTAL_VIEWERS})`);

    await Promise.all(promises);
  }

  const totalTime = Date.now() - startTime;
  console.log(`\nTempo total: ${(totalTime / 1000).toFixed(1)}s`);

  printMetrics();
}

main().catch(console.error);
