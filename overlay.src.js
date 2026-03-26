/**
 * Overlay Stream — Live Privada (Bookmarklet)
 *
 * Fluxo:
 *   1. Viewer abre live no Kick.com
 *   2. Roda bookmarklet → painel pede codigo do streamer
 *   3. API valida: streamer existe? URL do Kick bate? sala cheia?
 *   4. API retorna stream URL (CDN com UUID rotativo)
 *   5. Carrega hls.js → injeta player overlay sobre o Kick
 *   6. Kick vai pra 160p (economia de banda)
 *   7. Heartbeat a cada 30s + leave ao sair
 */

(function () {
  'use strict';

  // Evita duplicar
  if (document.getElementById('overlay-stream-ui')) return;

  // ════════════════════════════════════════════════════════════════════════════
  // CONFIG
  // ════════════════════════════════════════════════════════════════════════════

  var API_URL = '__OVERLAY_API_URL__';
  var API_KEY = '__OVERLAY_API_KEY__';
  var HLS_CDN = 'https://cdn.jsdelivr.net/npm/hls.js@1.5.17/dist/hls.min.js';

  // ════════════════════════════════════════════════════════════════════════════
  // DETECÇÃO
  // ════════════════════════════════════════════════════════════════════════════

  var isMobile = (function () {
    var hasTouch = ('ontouchstart' in window) || (navigator.maxTouchPoints > 0);
    var smallScreen = window.innerWidth <= 768;
    var uaCheck = /Android|iPhone|iPad|iPod|webOS|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
    var isIPad = navigator.maxTouchPoints > 1 && /Macintosh/i.test(navigator.userAgent);
    return uaCheck || (hasTouch && smallScreen) || isIPad;
  })();

  function getKickUsername() {
    var pathParts = location.pathname.split('/').filter(Boolean);
    return pathParts[0] || '';
  }

  function getViewerUid() {
    var uid = localStorage.getItem('overlay_viewer_uid');
    if (!uid) {
      uid = crypto.randomUUID();
      localStorage.setItem('overlay_viewer_uid', uid);
    }
    return uid;
  }

  // ════════════════════════════════════════════════════════════════════════════
  // VERIFICAÇÕES
  // ════════════════════════════════════════════════════════════════════════════

  if (!location.hostname.includes('kick.com')) {
    alert('Abra uma live no Kick.com primeiro!');
    return;
  }

  var kickUsername = getKickUsername();
  if (!kickUsername) {
    alert('Nao foi possivel identificar o canal do Kick');
    return;
  }

  var viewerUid = getViewerUid();

  // ════════════════════════════════════════════════════════════════════════════
  // UI — Painel de conexão
  // ════════════════════════════════════════════════════════════════════════════

  var uiStyle = document.createElement('style');
  uiStyle.textContent = [
    '#overlay-stream-ui *{box-sizing:border-box;margin:0;padding:0;}',
    '#os-panel{position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:99999;',
    'background:rgba(8,8,16,0.97);backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);',
    'border:1px solid rgba(255,140,0,0.15);border-radius:20px;padding:32px 28px 28px;width:320px;',
    'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;color:#e0e0e0;',
    'box-shadow:0 24px 80px rgba(0,0,0,0.7),0 0 40px rgba(255,140,0,0.08),0 0 0 1px rgba(255,255,255,0.04);}',
    '#os-panel .os-header{display:flex;flex-direction:column;align-items:center;margin-bottom:20px;position:relative;}',
    '#os-panel .os-logo-img{width:80px;height:80px;margin-bottom:8px;filter:drop-shadow(0 4px 12px rgba(255,140,0,0.3));}',
    '#os-panel .os-brand{font-size:11px;font-weight:600;color:rgba(255,255,255,0.5);text-transform:uppercase;letter-spacing:3px;}',
    '#os-panel .os-close{position:absolute;top:0;right:0;width:28px;height:28px;display:flex;align-items:center;justify-content:center;',
    'border-radius:8px;border:none;background:rgba(255,255,255,0.06);color:#666;font-size:16px;',
    'cursor:pointer;transition:all 0.2s;}',
    '#os-panel .os-close:hover{background:rgba(255,140,0,0.15);color:#ff8c00;}',
    '#os-panel .os-divider{width:60px;height:2px;background:linear-gradient(90deg,#ff8c00,#0088ff);border-radius:1px;margin:12px auto 16px;}',
    '#os-panel .os-label{font-size:11px;font-weight:500;color:#888;text-transform:uppercase;',
    'letter-spacing:0.8px;display:block;margin-bottom:6px;}',
    '#os-panel .os-input{width:100%;padding:12px 14px;background:rgba(255,255,255,0.04);',
    'border:1px solid rgba(255,255,255,0.1);border-radius:12px;color:#fff;font-size:14px;',
    'outline:none;transition:border-color 0.2s,box-shadow 0.2s;}',
    '#os-panel .os-input:focus{border-color:rgba(255,140,0,0.5);box-shadow:0 0 0 3px rgba(255,140,0,0.1);}',
    '#os-panel .os-input::placeholder{color:#555;}',
    '#os-panel .os-btn{width:100%;padding:12px;background:linear-gradient(135deg,#ff8c00,#ff5500,#0088ff);',
    'color:#fff;border:none;border-radius:12px;font-size:14px;font-weight:700;cursor:pointer;',
    'transition:all 0.2s;margin-top:16px;letter-spacing:0.5px;text-shadow:0 1px 2px rgba(0,0,0,0.3);}',
    '#os-panel .os-btn:hover{filter:brightness(1.15);transform:translateY(-1px);',
    'box-shadow:0 6px 20px rgba(255,140,0,0.35);}',
    '#os-panel .os-btn:active{transform:translateY(0);filter:brightness(0.95);}',
    '#os-panel .os-btn:disabled{opacity:0.5;cursor:not-allowed;transform:none;filter:none;box-shadow:none;}',
    '#os-panel .os-status{margin-top:14px;font-size:12px;text-align:center;min-height:18px;',
    'border-radius:8px;padding:0;transition:all 0.3s;}',
    '#os-panel .os-status.has-msg{padding:8px 10px;background:rgba(255,140,0,0.05);border:1px solid rgba(255,140,0,0.1);}',
    // Player controls
    '#hls-overlay::-webkit-media-controls{display:none!important}',
    '#hls-overlay::-webkit-media-controls-enclosure{display:none!important}',
    '#hls-overlay::-webkit-media-controls-panel{display:none!important}',
    '#hls-overlay:-webkit-full-screen{width:100vw!important;height:100vh!important;object-fit:contain;}',
    '#hls-overlay:fullscreen{width:100vw!important;height:100vh!important;object-fit:contain;}',
    // Quality selector
    '#stream-quality-wrap{position:relative;display:inline-block;}',
    '#stream-quality-btn{background:none;border:none;color:#fff;font-size:18px;cursor:pointer;padding:4px 8px;line-height:1;min-width:32px;min-height:32px;}',
    '#stream-quality-menu{display:none;position:absolute;bottom:40px;left:50%;transform:translateX(-50%);',
    'background:rgba(0,0,0,0.9);border:1px solid rgba(255,255,255,0.15);border-radius:8px;',
    'padding:4px 0;min-width:100px;z-index:1001;pointer-events:auto;}',
    '#stream-quality-menu.open{display:block;}',
    '#stream-quality-menu button{display:block;width:100%;padding:12px 20px;background:none;border:none;',
    'color:#ccc;font-size:14px;cursor:pointer;text-align:left;white-space:nowrap;}',
    '#stream-quality-menu button:hover{background:rgba(255,255,255,0.1);color:#fff;}',
    '#stream-quality-menu button.active{color:#00d4ff;font-weight:700;}',
  ].join('\n');
  document.head.appendChild(uiStyle);

  var overlay = document.createElement('div');
  overlay.id = 'overlay-stream-ui';
  overlay.innerHTML = [
    '<div id="os-panel">',
    '  <div class="os-header">',
    '    <img class="os-logo-img" src="__OVERLAY_API_URL__/public/assets/logo-small.png" alt="Udhyog Stream">',
    '    <span class="os-brand">Stream</span>',
    '    <button id="os-close" class="os-close">&times;</button>',
    '  </div>',
    '  <div class="os-divider"></div>',
    '  <label class="os-label">Codigo do Streamer</label>',
    '  <input id="os-code" class="os-input" type="text" placeholder="Digite seu codigo" spellcheck="false" autocomplete="off">',
    '  <button id="os-btn" class="os-btn">Conectar</button>',
    '  <div id="os-status" class="os-status"></div>',
    '</div>',
  ].join('');
  document.body.appendChild(overlay);

  var lastCode = localStorage.getItem('overlay_last_code');
  if (lastCode) document.getElementById('os-code').value = lastCode;

  document.getElementById('os-close').onclick = function () { overlay.remove(); };
  document.getElementById('os-code').addEventListener('keydown', function (e) {
    if (e.key === 'Enter') document.getElementById('os-btn').click();
  });

  function setStatus(msg, color) {
    var s = document.getElementById('os-status');
    s.textContent = msg;
    s.style.color = color || '#888';
    if (msg) s.classList.add('has-msg');
    else s.classList.remove('has-msg');
  }

  // ════════════════════════════════════════════════════════════════════════════
  // DEVICE INFO
  // ════════════════════════════════════════════════════════════════════════════

  function getDeviceInfo() {
    var ua = navigator.userAgent;
    var platform = isMobile ? 'mobile' : 'desktop';

    // OS detection
    var os = 'Unknown';
    var os_version = '';
    if (/Windows NT (\d+\.\d+)/.test(ua)) { os = 'Windows'; os_version = RegExp.$1; }
    else if (/Mac OS X (\d+[._]\d+[._]?\d*)/.test(ua)) { os = 'macOS'; os_version = RegExp.$1.replace(/_/g, '.'); }
    else if (/Android (\d+[\.\d]*)/.test(ua)) { os = 'Android'; os_version = RegExp.$1; }
    else if (/iPhone OS (\d+_\d+)/.test(ua) || /iPad.*OS (\d+_\d+)/.test(ua)) { os = 'iOS'; os_version = RegExp.$1.replace(/_/g, '.'); }
    else if (/Linux/.test(ua)) { os = 'Linux'; }

    // Device model
    var device_model = '';
    if (/iPhone/.test(ua)) {
      // iPhone — tentar identificar pelo tamanho de tela
      var w = screen.width; var h = screen.height;
      var ratio = window.devicePixelRatio || 1;
      var key = Math.min(w,h) + 'x' + Math.max(w,h) + '@' + ratio;
      var iphoneModels = {
        // Legacy @2x
        '320x568@2': 'iPhone SE (1st)',
        '375x667@2': 'iPhone 6/7/8/SE2/SE3',
        '414x896@2': 'iPhone XR/11',
        // 5.8" @3x
        '375x812@3': 'iPhone X/XS/11 Pro/12 mini/13 mini',
        // 6.1" standard @3x
        '390x844@3': 'iPhone 12/12 Pro/13/13 Pro/14',
        '393x852@3': 'iPhone 14 Pro/15/15 Pro/16/16e/16 Plus',
        // 6.5-6.7" large @3x
        '414x736@3': 'iPhone 6+/7+/8+',
        '414x896@3': 'iPhone XS Max/11 Pro Max',
        '428x926@3': 'iPhone 12 Pro Max/13 Pro Max/14 Plus',
        '430x932@3': 'iPhone 14 Pro Max/15 Plus/15 Pro Max',
        // iPhone 16 @3x
        '402x874@3': 'iPhone 16 Pro/17/17 Pro',
        '440x956@3': 'iPhone 16 Pro Max/17 Pro Max',
        // iPhone Air
        '420x912@3': 'iPhone Air',
      };
      device_model = iphoneModels[key] || 'iPhone';
    } else if (/iPad/.test(ua)) {
      device_model = 'iPad';
    } else if (/Android/.test(ua)) {
      // Android — pegar modelo do user agent (só se tiver Build)
      var match = ua.match(/;\s*([^;)]+)\s*Build/);
      if (match && !/rv:/.test(match[1])) device_model = match[1].trim();
      // Fallback: tentar userAgentData (Chrome 90+)
      if (!device_model && navigator.userAgentData && navigator.userAgentData.getHighEntropyValues) {
        try {
          navigator.userAgentData.getHighEntropyValues(['model']).then(function(vals) {
            if (vals.model) {
              device_model = vals.model;
              // Atualizar no localStorage pra próximo envio
              localStorage.setItem('vody_device_model', vals.model);
            }
          });
        } catch(e) {}
      }
      if (!device_model) device_model = localStorage.getItem('vody_device_model') || '';
    }

    // Browser detection (iOS browsers first — all use Safari engine but have unique identifiers)
    var browser = 'Unknown';
    var browser_version = '';
    if (/CriOS\/(\d+[\.\d]*)/.test(ua)) { browser = 'Chrome (iOS)'; browser_version = RegExp.$1; }
    else if (/FxiOS\/(\d+[\.\d]*)/.test(ua)) { browser = 'Firefox (iOS)'; browser_version = RegExp.$1; }
    else if (/EdgiOS\/(\d+[\.\d]*)/.test(ua)) { browser = 'Edge (iOS)'; browser_version = RegExp.$1; }
    else if (/OPR\/(\d+[\.\d]*)/.test(ua)) { browser = 'Opera'; browser_version = RegExp.$1; }
    else if (/Edg\/(\d+[\.\d]*)/.test(ua)) { browser = 'Edge'; browser_version = RegExp.$1; }
    else if (/Chrome\/(\d+[\.\d]*)/.test(ua) && !/Edg/.test(ua)) { browser = 'Chrome'; browser_version = RegExp.$1; }
    else if (/Firefox\/(\d+[\.\d]*)/.test(ua)) { browser = 'Firefox'; browser_version = RegExp.$1; }
    else if (/Safari\/(\d+[\.\d]*)/.test(ua) && !/Chrome/.test(ua) && !/CriOS/.test(ua)) { browser = 'Safari'; browser_version = RegExp.$1; }

    return {
      platform: platform,
      os: os,
      os_version: os_version,
      device_model: device_model,
      browser: browser,
      browser_version: browser_version,
      user_agent: ua,
      is_mobile: isMobile,
      kick_username: getKickLoggedUser(),
    };
  }

  function getKickLoggedUser() {
    try {
      // Pega do botão de perfil — o alt da img tem o username
      var profileBtn = document.querySelector('button[data-testid="navbar-account"] img[alt]')
        || document.querySelector('button[aria-haspopup="menu"] img.rounded-full[alt]');
      if (profileBtn && profileBtn.alt) return profileBtn.alt;
      return '';
    } catch (e) {
      return '';
    }
  }

  // ════════════════════════════════════════════════════════════════════════════
  // HEARTBEAT + METRICS + CLEANUP
  // ════════════════════════════════════════════════════════════════════════════

  var heartbeatInterval = null;
  var segmentsLoaded = 0;
  var pathCheckInterval = null;

  // Player health tracking
  var playerHealth = {
    buffering_count: 0,
    total_buffering_ms: 0,
    quality_switches: 0,
    errors: [],
    paused_by_user: false,
    _bufferingStart: 0,
  };

  function setupPlayerHealthTracking(video) {
    video.addEventListener('waiting', function () {
      playerHealth.buffering_count++;
      playerHealth._bufferingStart = Date.now();
    });
    video.addEventListener('playing', function () {
      if (playerHealth._bufferingStart > 0) {
        playerHealth.total_buffering_ms += Date.now() - playerHealth._bufferingStart;
        playerHealth._bufferingStart = 0;
      }
      playerHealth.paused_by_user = false;
    });
    video.addEventListener('pause', function () {
      // Se não está em fullscreen e o video do overlay pausou, foi ação do user
      if (!document.fullscreenElement && !document.webkitFullscreenElement) {
        playerHealth.paused_by_user = true;
      }
    });
    video.addEventListener('stalled', function () {
      playerHealth.errors.push({ type: 'STALLED', at: new Date().toISOString() });
    });
    video.addEventListener('error', function () {
      var code = video.error ? video.error.code : 0;
      var msg = video.error ? video.error.message : '';
      playerHealth.errors.push({ type: 'VIDEO_ERROR', code: code, msg: msg, at: new Date().toISOString() });
    });

    // HLS.js error tracking
    var hls = window._vodyHls;
    if (hls) {
      hls.on(Hls.Events.ERROR, function (event, data) {
        playerHealth.errors.push({
          type: data.type,
          details: data.details,
          fatal: data.fatal,
          at: new Date().toISOString(),
        });
      });
      hls.on(Hls.Events.LEVEL_SWITCHED, function () {
        playerHealth.quality_switches++;
      });
    }
  }

  function getPlayerHealthSnapshot(video) {
    var snapshot = {
      buffering_count: playerHealth.buffering_count,
      total_buffering_ms: playerHealth.total_buffering_ms,
      quality_switches: playerHealth.quality_switches,
      paused_by_user: playerHealth.paused_by_user,
      is_paused: video ? video.paused : false,
      buffer_health: 0,
      latency_seconds: 0,
      errors: playerHealth.errors.slice(-10), // últimos 10 erros
    };
    // Buffer health: quanto tempo de video está buffered à frente
    if (video && video.buffered.length > 0) {
      snapshot.buffer_health = Math.round((video.buffered.end(video.buffered.length - 1) - video.currentTime) * 10) / 10;
    }
    // Latency: diferença entre o edge do buffer e o currentTime (estimativa)
    var hls = window._vodyHls;
    if (hls && hls.latency !== undefined) {
      snapshot.latency_seconds = Math.round(hls.latency * 10) / 10;
    }
    return snapshot;
  }

  function startHeartbeat(streamerCode) {
    if (heartbeatInterval) clearInterval(heartbeatInterval);
    heartbeatInterval = setInterval(function () {
      // Heartbeat
      fetch(API_URL + '/api/viewer/heartbeat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Api-Key': API_KEY },
        body: JSON.stringify({ id_streamer: streamerCode, viewer_uid: viewerUid }),
      }).catch(function () {});

      // Metrics update com player health
      var video = window._vodyVideo;
      var health = getPlayerHealthSnapshot(video);
      fetch(API_URL + '/api/metrics/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Api-Key': API_KEY },
        body: JSON.stringify({
          id_streamer: streamerCode,
          viewer_uid: viewerUid,
          segments_loaded: segmentsLoaded,
          current_quality: window._vodyCurrentQuality || '720p',
          player_health: health,
        }),
      }).catch(function () {});
    }, 30000);
  }

  var consecutiveEmptyUrl = 0;

  function startPathCheck(streamerCode) {
    if (pathCheckInterval) clearInterval(pathCheckInterval);
    pathCheckInterval = setInterval(function () {
      fetch(API_URL + '/api/streamer/validate/' + encodeURIComponent(streamerCode), {
        headers: { 'X-Api-Key': API_KEY },
      })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        // Stream encerrado pelo servidor (VPS chamou /api/live/end)
        if (data.stream_ended) {
          console.log('[STREAM] Stream encerrado pelo servidor. Recarregando...');
          showStreamEnded();
          return;
        }

        // Stream encerrado — URL vazia
        if (!data.valid || !data.streamer.stream_url) {
          consecutiveEmptyUrl++;
          console.warn('[STREAM] stream_url vazio (' + consecutiveEmptyUrl + '/5)');
          if (consecutiveEmptyUrl >= 5) {
            showStreamEnded();
          }
          return;
        }
        consecutiveEmptyUrl = 0;

        // Atualizar contador de viewers (sem request extra) — DESABILITADO TEMPORARIAMENTE
        // if (data.streamer.current_viewers !== undefined) {
        //   var el = document.getElementById('stream-viewer-count-text');
        //   if (el) el.textContent = data.streamer.current_viewers + ' assistindo';
        // }

        var newBase = data.streamer.stream_url.replace(/\/master\.m3u8$/, '');
        var oldBase = window._vodyStreamBase;
        if (newBase && oldBase && newBase !== oldBase) {
          console.log('[STREAM] UUID rotacionou, reconectando...');
          window._vodyStreamBase = newBase;
          var hls = window._vodyHls;
          if (hls) {
            var quality = window._vodyCurrentQuality || '720p';
            hls.loadSource(newBase + '/' + quality + '/stream.m3u8');
            console.log('[STREAM] Reconectado: ' + newBase);
          }
        }
      })
      .catch(function () {});
    }, 60000);
  }

  // ── Stream encerrado — limpa tudo ──
  function showStreamEnded() {
    console.log('[STREAM] Stream encerrado. Limpando tudo.');

    // Parar HLS
    if (window._vodyHls) {
      window._vodyHls.destroy();
      window._vodyHls = null;
    }

    // Parar intervalos
    if (heartbeatInterval) { clearInterval(heartbeatInterval); heartbeatInterval = null; }
    if (pathCheckInterval) { clearInterval(pathCheckInterval); pathCheckInterval = null; }

    // Leave
    var code = localStorage.getItem('overlay_active_streamer');
    if (code) {
      navigator.sendBeacon(
        API_URL + '/api/viewer/leave',
        new Blob([JSON.stringify({ id_streamer: code, viewer_uid: viewerUid })], { type: 'application/json' })
      );
      localStorage.removeItem('overlay_active_streamer');
    }

    // Remover overlay video
    var hlsVideo = document.getElementById('hls-overlay');
    if (hlsVideo) hlsVideo.remove();
    var volCtrl = document.getElementById('hls-vol-ctrl');
    if (volCtrl) volCtrl.remove();
    var fsBtn = document.getElementById('stream-fs');
    if (fsBtn) fsBtn.remove();
    var qBtn = document.getElementById('stream-quality-btn');
    if (qBtn) qBtn.remove();
    var qMenu = document.getElementById('stream-quality-menu');
    if (qMenu) qMenu.remove();
    var blocker = document.getElementById('stream-kick-blocker');
    if (blocker) blocker.remove();
    var viewerCounter = document.getElementById('stream-viewer-counter');
    if (viewerCounter) viewerCounter.remove();

    // Restaurar Kick
    var player = document.getElementById('injected-channel-player');
    if (player) {
      var kickVideo = player.querySelector('video:not(#hls-overlay)');
      if (kickVideo) {
        kickVideo.style.display = '';
        kickVideo.muted = false;
        kickVideo.volume = 1;
        kickVideo.play().catch(function () {});
      }
    }

    // Mostrar mensagem
    var endMsg = document.createElement('div');
    endMsg.id = 'stream-stream-ended';
    endMsg.style.cssText = 'position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:99999;background:rgba(10,10,14,0.95);backdrop-filter:blur(16px);border:1px solid rgba(255,255,255,0.06);border-radius:16px;padding:32px 40px;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;text-align:center;box-shadow:0 24px 80px rgba(0,0,0,0.6);';
    endMsg.innerHTML = '<div style="font-size:40px;margin-bottom:12px;">📡</div><div style="color:#fff;font-size:16px;font-weight:600;margin-bottom:8px;">Stream Encerrado</div><div style="color:#888;font-size:13px;">O streamer finalizou a transmissao.</div><button id="stream-end-close" style="margin-top:16px;padding:8px 24px;background:linear-gradient(135deg,#ff6b35,#0099cc);color:#fff;border:none;border-radius:8px;font-size:13px;font-weight:600;cursor:pointer;">Fechar</button>';
    document.body.appendChild(endMsg);
    document.getElementById('stream-end-close').onclick = function () { location.reload(); };

    // Auto-reload após 10s
    setTimeout(function () { location.reload(); }, 10000);
  }

  function sendMetricsJoin(streamerCode) {
    fetch(API_URL + '/api/metrics/join', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Api-Key': API_KEY },
      body: JSON.stringify({
        id_streamer: streamerCode,
        viewer_uid: viewerUid,
        device_info: getDeviceInfo(),
      }),
    }).catch(function () {});
  }

  window.addEventListener('beforeunload', function () {
    var code = localStorage.getItem('overlay_active_streamer');
    if (code) {
      navigator.sendBeacon(
        API_URL + '/api/viewer/leave',
        new Blob([JSON.stringify({ id_streamer: code, viewer_uid: viewerUid })], { type: 'application/json' })
      );
      localStorage.removeItem('overlay_active_streamer');
    }
  });

  // ════════════════════════════════════════════════════════════════════════════
  // AUTH + CONNECT (API antiga + CDN stream URL)
  // ════════════════════════════════════════════════════════════════════════════

  document.getElementById('os-btn').onclick = async function () {
    var streamerCode = document.getElementById('os-code').value.trim();
    var btn = this;

    if (!streamerCode) { setStatus('Preencha o codigo do Streamer', '#f44336'); return; }

    btn.disabled = true;
    btn.textContent = 'Validando...';
    setStatus('Verificando streamer...', '#00d4ff');

    try {
      // 1. Validar streamer na API antiga
      var resp = await fetch(API_URL + '/api/streamer/validate/' + encodeURIComponent(streamerCode), {
        headers: { 'X-Api-Key': API_KEY },
      });
      var data = await resp.json();

      if (!resp.ok || !data.valid) {
        setStatus('Streamer nao encontrado no sistema', '#f44336');
        btn.disabled = false;
        btn.textContent = 'Conectar';
        return;
      }

      // 2. Validar URL — viewer está no canal certo do Kick?
      var dbLink = data.streamer.link || '';
      var dbUsername = '';
      try {
        dbUsername = new URL(dbLink).pathname.split('/').filter(Boolean)[0] || '';
      } catch (e) {
        dbUsername = dbLink.replace(/^https?:\/\/(www\.)?kick\.com\/?/i, '').split('/')[0];
      }

      if (dbUsername.toLowerCase() !== kickUsername.toLowerCase()) {
        setStatus('Voce nao esta no canal deste streamer', '#f44336');
        btn.disabled = false;
        btn.textContent = 'Conectar';
        return;
      }

      // 3. Buscar stream URL via CDN (API monta a URL com UUID rotativo)
      var streamUrl = data.streamer.stream_url || '';
      if (!streamUrl) {
        setStatus('Stream offline no momento', '#ff9800');
        btn.disabled = false;
        btn.textContent = 'Conectar';
        return;
      }

      // 4. Entrar na sala (limite de viewers)
      setStatus('Entrando na sala...', '#00d4ff');
      var joinResp = await fetch(API_URL + '/api/viewer/join', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Api-Key': API_KEY },
        body: JSON.stringify({ id_streamer: streamerCode, viewer_uid: viewerUid }),
      });
      var joinData = await joinResp.json();

      if (!joinResp.ok) {
        if (joinResp.status === 403) {
          setStatus('Sala cheia! ' + joinData.current_viewers + '/' + joinData.max_spectators, '#f44336');
        } else {
          setStatus(joinData.message || 'Erro ao entrar na sala', '#f44336');
        }
        btn.disabled = false;
        btn.textContent = 'Conectar';
        return;
      }

      localStorage.setItem('overlay_last_code', streamerCode);
      localStorage.setItem('overlay_active_streamer', streamerCode);

      setStatus('Carregando player...', '#00d4ff');

      // 5. Carregar hls.js e injetar player
      loadHLS(function () {
        var player = document.getElementById('injected-channel-player');
        if (!player) {
          setStatus('Player do Kick nao encontrado', '#f44336');
          btn.disabled = false;
          btn.textContent = 'Conectar';
          return;
        }

        var kickVideo = player.querySelector('video');
        if (kickVideo) {
          kickVideo.muted = false;
          kickVideo.volume = 0;
        }

        if (isMobile) {
          injectMobile(player, kickVideo, streamUrl);
        } else {
          injectDesktop(player, kickVideo, streamUrl);
        }

        // 6. Métricas + heartbeat + path check
        sendMetricsJoin(streamerCode);
        startHeartbeat(streamerCode);
        startPathCheck(streamerCode);
        // startViewerCounter(streamerCode, player); // DESABILITADO TEMPORARIAMENTE

        setStatus('Conectado!', '#4caf50');
        setTimeout(function () { overlay.remove(); }, 1500);
      });

    } catch (e) {
      console.error('[STREAM] Erro:', e);
      setStatus('Erro de conexao com o servidor', '#f44336');
      btn.disabled = false;
      btn.textContent = 'Conectar';
    }
  };

  // ════════════════════════════════════════════════════════════════════════════
  // HLS LOADER (puro, sem P2P)
  // ════════════════════════════════════════════════════════════════════════════

  function loadHLS(callback) {
    if (typeof Hls !== 'undefined') { callback(); return; }

    var script = document.createElement('script');
    script.src = HLS_CDN;
    script.onload = callback;
    script.onerror = function () {
      setStatus('Erro ao carregar player HLS', '#f44336');
    };
    document.head.appendChild(script);
  }

  // Guarda a base URL pra trocar qualidade (ex: https://live.vody.gg/{uuid}/beliene)
  // e a qualidade atual
  window._vodyStreamBase = '';
  window._vodyCurrentQuality = isMobile ? '720p' : '1080p';

  function startHLS(video, streamUrl) {
    var defaultQuality = isMobile ? '720p' : '1080p';
    var base = streamUrl.replace(/\/master\.m3u8$/, '');
    window._vodyStreamBase = base;
    window._vodyCurrentQuality = defaultQuality;
    window._vodyVideo = video;
    window._vodyIsNativeHLS = false;
    var initialUrl = base + '/' + defaultQuality + '/stream.m3u8';

    if (typeof Hls !== 'undefined' && Hls.isSupported()) {
      var hls = new Hls({
        lowLatencyMode: false,
        liveSyncDurationCount: 4,
        liveMaxLatencyDurationCount: 8,
        backBufferLength: 15,
        enableWorker: true,
        maxBufferLength: 30,
        maxMaxBufferLength: 60,
        maxBufferHole: 1.5,
        fragLoadingTimeOut: 45000,
        fragLoadingMaxRetry: 6,
        fragLoadingRetryDelay: 3000,
        levelLoadingTimeOut: 20000,
        levelLoadingMaxRetry: 6,
        levelLoadingRetryDelay: 3000,
      });
      hls.attachMedia(video);
      hls.loadSource(initialUrl);
      hls.on(Hls.Events.MANIFEST_PARSED, function () {
        var playPromise = video.play();
        if (playPromise) {
          playPromise.catch(function () {
            // Autoplay bloqueado — iniciar mutado e tentar de novo
            video.muted = true;
            video.play().catch(function () {});
          });
        }
      });
      hls.on(Hls.Events.FRAG_LOADED, function () {
        segmentsLoaded++;
      });
      var consecutiveFatalErrors = 0;
      var lastFatalTime = 0;
      var mediaRecoverAttempts = 0;
      var isRecreating = false;

      // Função para destruir e recriar hls.js quando está morto
      function recreateHls() {
        if (isRecreating) return;
        isRecreating = true;
        console.warn('[STREAM] Recriando hls.js...');
        try {
          if (window._vodyHls) {
            window._vodyHls.destroy();
            window._vodyHls = null;
          }
        } catch (e) {}
        setTimeout(function () {
          var quality = window._vodyCurrentQuality || defaultQuality;
          var url = window._vodyStreamBase + '/' + quality + '/stream.m3u8';
          var newHls = new Hls({
            lowLatencyMode: false,
            liveSyncDurationCount: 4,
            liveMaxLatencyDurationCount: 8,
            backBufferLength: 15,
            enableWorker: true,
            maxBufferLength: 30,
            maxMaxBufferLength: 60,
            maxBufferHole: 1.5,
            fragLoadingTimeOut: 45000,
            fragLoadingMaxRetry: 6,
            fragLoadingRetryDelay: 3000,
            levelLoadingTimeOut: 20000,
            levelLoadingMaxRetry: 6,
            levelLoadingRetryDelay: 3000,
          });
          newHls.attachMedia(video);
          newHls.loadSource(url);
          newHls.on(Hls.Events.MANIFEST_PARSED, function () {
            video.play().catch(function () {
              video.muted = true;
              video.play().catch(function () {});
            });
          });
          newHls.on(Hls.Events.FRAG_LOADED, function () {
            segmentsLoaded++;
            consecutiveFatalErrors = 0;
            mediaRecoverAttempts = 0;
          });
          newHls.on(Hls.Events.ERROR, function (event, data) {
            handleHlsError(data);
          });
          window._vodyHls = newHls;
          hls = newHls;
          isRecreating = false;
          console.log('[STREAM] hls.js recriado: ' + url);
        }, 2000);
      }

      function handleHlsError(data) {
        // Para erros não-fatais de buffer, tentar nudge suave antes de escalar
        if (!data.fatal) {
          if (data.details === 'bufferStalledError' && window._vodyHls && window._vodyHls.media) {
            var vid = window._vodyHls.media;
            var h = window._vodyHls;
            // Se tem buffer à frente, pular o gap pra destravar o áudio
            if (vid.buffered.length > 0 && h.liveSyncPosition) {
              var bufEnd = vid.buffered.end(vid.buffered.length - 1);
              if (bufEnd - vid.currentTime > 2) {
                vid.currentTime = vid.currentTime + 0.5;
              }
            }
          }
          return;
        }

        var now = Date.now();
        if (now - lastFatalTime < 10000) {
          consecutiveFatalErrors++;
        } else {
          consecutiveFatalErrors = 1;
        }
        lastFatalTime = now;

        // Stream realmente encerrado (muitos erros seguidos por muito tempo)
        if (consecutiveFatalErrors >= 30) {
          console.warn('[STREAM] 30+ erros fatais consecutivos — stream provavelmente encerrado');
          showStreamEnded();
          return;
        }

        if (data.type === Hls.ErrorTypes.NETWORK_ERROR) {
          console.warn('[STREAM] Network error, tentando reconectar...');
          setTimeout(function () {
            if (window._vodyHls) window._vodyHls.startLoad();
          }, 5000);
        } else if (data.type === Hls.ErrorTypes.MEDIA_ERROR) {
          mediaRecoverAttempts++;
          if (mediaRecoverAttempts <= 3) {
            console.warn('[STREAM] Media error (' + mediaRecoverAttempts + '/3), recuperando...');
            if (window._vodyHls) window._vodyHls.recoverMediaError();
          } else {
            console.warn('[STREAM] Media error persistente — recriando player...');
            mediaRecoverAttempts = 0;
            recreateHls();
          }
        }
      }

      hls.on(Hls.Events.ERROR, function (event, data) {
        handleHlsError(data);
      });
      hls.on(Hls.Events.FRAG_LOADED, function () {
        // Reset contadores quando um fragmento carrega com sucesso
        consecutiveFatalErrors = 0;
        mediaRecoverAttempts = 0;
      });
      window._vodyHls = hls;

      // Sincronização com live edge — evita loop/atraso
      setInterval(function () {
        var h = window._vodyHls;
        if (h && h.media && h.liveSyncPosition) {
          var behind = h.liveSyncPosition - h.media.currentTime;
          if (behind > 30) {
            console.warn('[STREAM] ' + Math.round(behind) + 's atras do live — pulando pro live edge');
            h.media.currentTime = h.liveSyncPosition;
          }
        }
      }, 10000);

      // Watchdog — detecta player realmente morto
      // Só recria se: segmentos pararam + buffer vazio + não pausado pelo user
      var lastSegCount = 0;
      var staleCheckCount = 0;
      setInterval(function () {
        var h = window._vodyHls;
        if (!h || !h.media || isRecreating) return;

        var vid = h.media;
        var segsStopped = (segmentsLoaded === lastSegCount);
        var bufferEmpty = vid.buffered.length === 0 || (vid.buffered.length > 0 && (vid.buffered.end(vid.buffered.length - 1) - vid.currentTime) < 1);
        var notPaused = !vid.paused;

        if (segsStopped && bufferEmpty && notPaused) {
          staleCheckCount++;
          if (staleCheckCount >= 6) { // 6 x 15s = 90s sem segmentos + buffer vazio + tocando
            console.warn('[STREAM] Player morto (90s sem segmentos, buffer vazio) — recriando...');
            staleCheckCount = 0;
            recreateHls();
          }
        } else {
          staleCheckCount = 0;
        }
        lastSegCount = segmentsLoaded;
      }, 15000);

      console.log('[STREAM] HLS iniciado: ' + initialUrl);
    } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
      // Safari / iOS — HLS nativo
      window._vodyIsNativeHLS = true;
      video.src = initialUrl;
      var playPromise = video.play();
      if (playPromise) {
        playPromise.catch(function () {
          video.muted = true;
          video.play().catch(function () {});
        });
      }

      // Safari pausa ao sair do fullscreen — re-play automaticamente
      video.addEventListener('pause', function () {
        if (!document.fullscreenElement && !document.webkitFullscreenElement) {
          setTimeout(function () {
            if (video.paused) {
              video.play().catch(function () {});
            }
          }, 300);
        }
      });

      // Contar segmentos via timeupdate (estimativa: 1 seg a cada 4s)
      var lastSegTime = 0;
      video.addEventListener('timeupdate', function () {
        var now = Math.floor(video.currentTime / 4);
        if (now > lastSegTime) {
          segmentsLoaded += (now - lastSegTime);
          lastSegTime = now;
        }
      });

      console.log('[STREAM] HLS nativo (Safari): ' + initialUrl);
    }
  }

  // ════════════════════════════════════════════════════════════════════════════
  // PLAYER DESKTOP — overlay sobre Kick + 160p
  // ════════════════════════════════════════════════════════════════════════════

  function injectDesktop(player, kickVideo, streamUrl) {
    var old = document.getElementById('hls-overlay');
    if (old) old.remove();

    // Tenta mudar Kick pra 160p
    setTimeout(function () { trySet160p(player); }, 3000);

    var video = document.createElement('video');
    video.id = 'hls-overlay';
    video.autoplay = true;
    video.playsInline = true;
    video.setAttribute('playsinline', '');
    video.setAttribute('webkit-playsinline', '');
    video.disableRemotePlayback = true;
    video.style.cssText = 'position:absolute;top:0;left:0;width:100%;height:100%;border:none;z-index:999;background:#000;object-fit:contain;pointer-events:none;';

    player.style.position = 'relative';
    player.appendChild(video);

    createVolumeControls(video, player, false);
    createFullscreenButton(video, player);

    startHLS(video, streamUrl);
    setupPlayerHealthTracking(video);

    // Mantém Kick mutado
    setInterval(function () {
      var v = player.querySelector('video:not(#hls-overlay)');
      if (v) { v.muted = false; v.volume = 0; }
    }, 2000);
  }

  // ════════════════════════════════════════════════════════════════════════════
  // PLAYER MOBILE — overlay sobre Kick
  // ════════════════════════════════════════════════════════════════════════════

  function injectMobile(player, kickVideo, streamUrl) {
    var old = document.getElementById('hls-overlay');
    if (old) old.remove();

    // Espera Kick carregar e pausa
    var checkCount = 0;
    var checkInterval = setInterval(function () {
      var v = player.querySelector('video');
      checkCount++;
      var isPlaying = v && !v.paused && v.currentTime > 0 && v.readyState >= 3;

      if (isPlaying || checkCount > 40) {
        clearInterval(checkInterval);
        if (v) {
          v.pause();
          v.autoplay = false;
          v.style.display = 'none';
        }
        doInjectMobile(player, streamUrl);
      }
    }, 200);
  }

  function doInjectMobile(player, streamUrl) {
    // Bloqueador de interação com Kick
    var blocker = document.createElement('div');
    blocker.id = 'stream-kick-blocker';
    blocker.style.cssText = 'position:absolute;top:0;left:0;width:100%;height:100%;z-index:998;background:transparent;pointer-events:auto;';
    player.appendChild(blocker);

    // Esconde controles do Kick
    var css = document.createElement('style');
    css.textContent = [
      '#injected-channel-player button:not(#stream-fs):not(#stream-mute):not(#stream-quality-btn):not(#stream-quality-menu button){pointer-events:none!important;opacity:0!important;}',
      '#stream-quality-menu button{pointer-events:auto!important;opacity:1!important;}',
      '#injected-channel-player [class*="controls"]{pointer-events:none!important;opacity:0!important;}',
      '#injected-channel-player [class*="overlay-container"]{pointer-events:none!important;}',
    ].join('');
    document.head.appendChild(css);

    var video = document.createElement('video');
    video.id = 'hls-overlay';
    video.autoplay = true;
    video.playsInline = true;
    video.setAttribute('playsinline', '');
    video.setAttribute('webkit-playsinline', '');
    video.style.cssText = 'width:100%;height:100%;position:absolute;top:0;left:0;background:#000;object-fit:contain;z-index:999;';

    player.style.position = 'relative';
    player.appendChild(video);

    createVolumeControls(video, player, true);
    createFullscreenButton(video, player);

    startHLS(video, streamUrl);
    setupPlayerHealthTracking(video);

    // Impede Kick de retomar
    setInterval(function () {
      var kv = player.querySelector('video:not(#hls-overlay)');
      if (kv && !kv.paused) {
        kv.pause();
        kv.autoplay = false;
      }
    }, 2000);
  }

  // ════════════════════════════════════════════════════════════════════════════
  // KICK 160p
  // ════════════════════════════════════════════════════════════════════════════

  function trySet160p(player) {
    try {
      player.dispatchEvent(new MouseEvent('mouseenter', { bubbles: true }));
      player.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));

      setTimeout(function () {
        var settingsBtn = player.querySelector('button[aria-label="Settings"]')
          || player.querySelector('button[aria-label="Configuracoes"]');

        if (!settingsBtn) {
          var btns = player.querySelectorAll('button');
          for (var i = 0; i < btns.length; i++) {
            if (btns[i].querySelector('svg path[d*="M25.7"]')) {
              settingsBtn = btns[i];
              break;
            }
          }
        }
        if (!settingsBtn) return;

        settingsBtn.click();

        setTimeout(function () {
          var options = document.querySelectorAll('[role="menuitemradio"]');
          for (var j = 0; j < options.length; j++) {
            if (options[j].textContent.includes('160')) {
              options[j].click();
              console.log('[STREAM] Kick mudado pra 160p');
              return;
            }
          }
        }, 800);
      }, 800);
    } catch (e) {}
  }

  // ════════════════════════════════════════════════════════════════════════════
  // CONTROLES — Volume, Fullscreen, Revert
  // ════════════════════════════════════════════════════════════════════════════

  function createVolumeControls(video, parent, alwaysVisible) {
    var volWrap = document.createElement('div');
    volWrap.id = 'hls-vol-ctrl';
    volWrap.style.cssText = 'position:absolute;bottom:12px;left:12px;z-index:1000;display:flex;align-items:center;gap:6px;background:rgba(0,0,0,0.7);padding:6px 10px;border-radius:6px;pointer-events:auto;opacity:' + (alwaysVisible ? '1' : '0') + ';transition:opacity 0.3s;';

    var muteBtn = document.createElement('button');
    muteBtn.id = 'stream-mute';
    muteBtn.textContent = '\u{1F50A}';
    muteBtn.style.cssText = 'background:none;border:none;color:#fff;font-size:18px;cursor:pointer;padding:0;';
    muteBtn.onclick = function () {
      video.muted = !video.muted;
      muteBtn.textContent = video.muted ? '\u{1F507}' : '\u{1F50A}';
      volSlider.value = video.muted ? 0 : video.volume * 100;
    };

    var volSlider = document.createElement('input');
    volSlider.type = 'range'; volSlider.min = '0'; volSlider.max = '100'; volSlider.value = '100';
    volSlider.style.cssText = 'width:80px;height:4px;cursor:pointer;accent-color:#00d4ff;';
    volSlider.oninput = function () {
      video.volume = this.value / 100;
      video.muted = this.value == 0;
      muteBtn.textContent = this.value == 0 ? '\u{1F507}' : '\u{1F50A}';
    };

    var qualityWrap = createQualitySelector();

    volWrap.appendChild(muteBtn);
    volWrap.appendChild(volSlider);
    volWrap.appendChild(qualityWrap);
    parent.appendChild(volWrap);

    if (!alwaysVisible) {
      parent.addEventListener('mouseenter', function () { volWrap.style.opacity = '1'; });
      parent.addEventListener('mouseleave', function () { volWrap.style.opacity = '0'; });
    }
  }

  function createQualitySelector() {
    var qualities = ['1080p', '720p'];
    var wrap = document.createElement('div');
    wrap.id = 'stream-quality-wrap';

    var btn = document.createElement('button');
    btn.id = 'stream-quality-btn';
    btn.innerHTML = '\u2699';
    btn.title = 'Qualidade';

    var menu = document.createElement('div');
    menu.id = 'stream-quality-menu';

    var menuOpen = false;

    btn.addEventListener('click', function (e) {
      e.preventDefault();
      e.stopPropagation();

      if (menuOpen) {
        menu.classList.remove('open');
        menuOpen = false;
        return;
      }

      menu.innerHTML = '';
      qualities.forEach(function (q) {
        var item = document.createElement('button');
        item.textContent = q;
        if (window._vodyCurrentQuality === q) item.classList.add('active');
        item.addEventListener('click', function (ev) {
          ev.preventDefault();
          ev.stopPropagation();
          if (window._vodyCurrentQuality === q) { menu.classList.remove('open'); menuOpen = false; return; }
          var base = window._vodyStreamBase;
          if (!base) { menu.classList.remove('open'); menuOpen = false; return; }
          var newUrl = base + '/' + q + '/stream.m3u8';
          var hls = window._vodyHls;
          if (hls) {
            hls.loadSource(newUrl);
          } else if (window._vodyIsNativeHLS && window._vodyVideo) {
            var vid = window._vodyVideo;
            vid.src = newUrl;
            vid.play().catch(function () {});
          }
          window._vodyCurrentQuality = q;
          console.log('[STREAM] Qualidade: ' + q);
          menu.classList.remove('open');
          menuOpen = false;
        });
        menu.appendChild(item);
      });

      menu.classList.add('open');
      menuOpen = true;
    });

    // Fechar menu ao tocar fora (com delay pra não conflitar com o botão)
    document.addEventListener('click', function () {
      if (menuOpen) {
        setTimeout(function () {
          menu.classList.remove('open');
          menuOpen = false;
        }, 50);
      }
    });

    wrap.appendChild(btn);
    wrap.appendChild(menu);
    return wrap;
  }

  function createFullscreenButton(video, player) {
    var fsBtn = document.createElement('button');
    fsBtn.id = 'stream-fs';
    fsBtn.textContent = '\u26F6';
    fsBtn.style.cssText = 'position:absolute;top:10px;right:10px;z-index:1000;background:rgba(0,0,0,0.7);color:#fff;border:none;border-radius:4px;padding:6px 10px;font-size:16px;cursor:pointer;pointer-events:auto;';
    fsBtn._simulated = false;
    fsBtn._originalPlayerStyle = '';
    fsBtn._originalVideoStyle = '';

    fsBtn.onclick = function () {
      if (fsBtn._simulated) {
        player.style.cssText = fsBtn._originalPlayerStyle;
        video.style.cssText = fsBtn._originalVideoStyle;
        document.body.style.overflow = '';
        fsBtn._simulated = false;
        fsBtn.textContent = '\u26F6';
        return;
      }
      if (document.fullscreenElement || document.webkitFullscreenElement) {
        if (document.exitFullscreen) document.exitFullscreen();
        else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
        return;
      }
      if (player.requestFullscreen) {
        player.requestFullscreen({ navigationUI: 'hide' }).catch(function () { tryVideoFs(); });
      } else if (player.webkitRequestFullscreen) {
        player.webkitRequestFullscreen();
      } else { tryVideoFs(); }

      function tryVideoFs() {
        if (video.requestFullscreen) {
          video.requestFullscreen({ navigationUI: 'hide' }).catch(function () {
            if (video.webkitEnterFullscreen) video.webkitEnterFullscreen();
            else simulateFs();
          });
        } else if (video.webkitEnterFullscreen) { video.webkitEnterFullscreen(); }
        else { simulateFs(); }
      }
      function simulateFs() {
        fsBtn._originalPlayerStyle = player.style.cssText;
        fsBtn._originalVideoStyle = video.style.cssText;
        player.style.cssText = 'position:fixed!important;top:0!important;left:0!important;width:100vw!important;height:100vh!important;z-index:999999!important;background:#000!important;';
        video.style.cssText = 'width:100%;height:100%;object-fit:contain;position:absolute;top:0;left:0;';
        document.body.style.overflow = 'hidden';
        fsBtn._simulated = true;
        fsBtn.textContent = '\u2715';
      }
    };

    function updateIcon() {
      if (!fsBtn._simulated) {
        fsBtn.textContent = (document.fullscreenElement || document.webkitFullscreenElement) ? '\u2715' : '\u26F6';
      }
    }
    document.addEventListener('fullscreenchange', updateIcon);
    document.addEventListener('webkitfullscreenchange', updateIcon);

    player.appendChild(fsBtn);
  }


  // ════════════════════════════════════════════════════════════════════════════
  // VIEWER COUNTER — contador ao vivo no player (usa dados do path check, sem request extra)
  // ════════════════════════════════════════════════════════════════════════════

  function startViewerCounter(streamerCode, player) {
    var counter = document.createElement('div');
    counter.id = 'stream-viewer-counter';
    counter.style.cssText = 'position:absolute;bottom:44px;left:12px;z-index:1000;display:flex;align-items:center;gap:6px;background:rgba(0,0,0,0.7);padding:5px 10px;border-radius:6px;pointer-events:none;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;font-size:12px;color:#fff;';

    var dot = document.createElement('span');
    dot.style.cssText = 'width:6px;height:6px;border-radius:50%;background:#4caf50;box-shadow:0 0 6px rgba(76,175,80,0.6);';

    var text = document.createElement('span');
    text.id = 'stream-viewer-count-text';
    text.textContent = '...';

    counter.appendChild(dot);
    counter.appendChild(text);
    player.appendChild(counter);

    // Buscar contagem inicial
    fetch(API_URL + '/api/viewer/count/' + encodeURIComponent(streamerCode), {
      headers: { 'X-Api-Key': API_KEY },
    })
    .then(function (r) { return r.json(); })
    .then(function (data) {
      if (data.current_viewers !== undefined) {
        text.textContent = data.current_viewers + ' assistindo';
      }
    })
    .catch(function () {});
    // Atualizações seguintes vêm do path check (a cada 60s) — zero requests extras
  }

})();
