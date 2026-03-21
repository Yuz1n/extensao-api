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
    'background:rgba(10,10,14,0.95);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);',
    'border:1px solid rgba(255,255,255,0.06);border-radius:16px;padding:28px 24px 24px;width:300px;',
    'font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;color:#e0e0e0;',
    'box-shadow:0 24px 80px rgba(0,0,0,0.6),0 0 0 1px rgba(255,255,255,0.04);}',
    '#os-panel .os-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;}',
    '#os-panel .os-logo{display:flex;align-items:center;gap:8px;}',
    '#os-panel .os-dot{width:8px;height:8px;border-radius:50%;background:#00d4ff;',
    'box-shadow:0 0 8px rgba(0,212,255,0.5);animation:os-pulse 2s ease-in-out infinite;}',
    '@keyframes os-pulse{0%,100%{opacity:1}50%{opacity:0.4}}',
    '#os-panel .os-title{font-size:14px;font-weight:600;color:#fff;letter-spacing:0.3px;}',
    '#os-panel .os-close{width:28px;height:28px;display:flex;align-items:center;justify-content:center;',
    'border-radius:8px;border:none;background:rgba(255,255,255,0.06);color:#666;font-size:16px;',
    'cursor:pointer;transition:all 0.2s;}',
    '#os-panel .os-close:hover{background:rgba(255,255,255,0.12);color:#fff;}',
    '#os-panel .os-label{font-size:11px;font-weight:500;color:#666;text-transform:uppercase;',
    'letter-spacing:0.8px;display:block;margin-bottom:6px;}',
    '#os-panel .os-input{width:100%;padding:10px 12px;background:rgba(255,255,255,0.04);',
    'border:1px solid rgba(255,255,255,0.08);border-radius:10px;color:#fff;font-size:14px;',
    'outline:none;transition:border-color 0.2s,box-shadow 0.2s;}',
    '#os-panel .os-input:focus{border-color:rgba(0,212,255,0.4);box-shadow:0 0 0 3px rgba(0,212,255,0.08);}',
    '#os-panel .os-input::placeholder{color:#444;}',
    '#os-panel .os-btn{width:100%;padding:11px;background:linear-gradient(135deg,#00d4ff,#0099cc);',
    'color:#000;border:none;border-radius:10px;font-size:13px;font-weight:700;cursor:pointer;',
    'transition:all 0.2s;margin-top:14px;letter-spacing:0.3px;}',
    '#os-panel .os-btn:hover{filter:brightness(1.1);transform:translateY(-1px);',
    'box-shadow:0 4px 16px rgba(0,212,255,0.3);}',
    '#os-panel .os-btn:active{transform:translateY(0);filter:brightness(0.95);}',
    '#os-panel .os-btn:disabled{opacity:0.5;cursor:not-allowed;transform:none;filter:none;box-shadow:none;}',
    '#os-panel .os-status{margin-top:14px;font-size:12px;text-align:center;min-height:18px;',
    'border-radius:8px;padding:0;transition:all 0.3s;}',
    '#os-panel .os-status.has-msg{padding:8px 10px;background:rgba(255,255,255,0.03);}',
    // Player controls
    '#hls-overlay::-webkit-media-controls{display:none!important}',
    '#hls-overlay::-webkit-media-controls-enclosure{display:none!important}',
    '#hls-overlay::-webkit-media-controls-panel{display:none!important}',
    '#hls-overlay:-webkit-full-screen{width:100vw!important;height:100vh!important;object-fit:contain;}',
    '#hls-overlay:fullscreen{width:100vw!important;height:100vh!important;object-fit:contain;}',
    // Quality selector
    '#vody-quality-wrap{position:relative;display:inline-block;}',
    '#vody-quality-btn{background:none;border:none;color:#fff;font-size:18px;cursor:pointer;padding:4px 8px;line-height:1;min-width:32px;min-height:32px;}',
    '#vody-quality-menu{display:none;position:absolute;bottom:40px;left:50%;transform:translateX(-50%);',
    'background:rgba(0,0,0,0.9);border:1px solid rgba(255,255,255,0.15);border-radius:8px;',
    'padding:4px 0;min-width:100px;z-index:1001;pointer-events:auto;}',
    '#vody-quality-menu.open{display:block;}',
    '#vody-quality-menu button{display:block;width:100%;padding:12px 20px;background:none;border:none;',
    'color:#ccc;font-size:14px;cursor:pointer;text-align:left;white-space:nowrap;}',
    '#vody-quality-menu button:hover{background:rgba(255,255,255,0.1);color:#fff;}',
    '#vody-quality-menu button.active{color:#00d4ff;font-weight:700;}',
  ].join('\n');
  document.head.appendChild(uiStyle);

  var overlay = document.createElement('div');
  overlay.id = 'overlay-stream-ui';
  overlay.innerHTML = [
    '<div id="os-panel">',
    '  <div class="os-header">',
    '    <div class="os-logo">',
    '      <span class="os-dot"></span>',
    '      <span class="os-title">Vody Stream</span>',
    '    </div>',
    '    <button id="os-close" class="os-close">&times;</button>',
    '  </div>',
    '  <label class="os-label">Codigo do Streamer</label>',
    '  <input id="os-code" class="os-input" type="text" placeholder="ex: beliene" spellcheck="false" autocomplete="off">',
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

  function startPathCheck(streamerCode) {
    if (pathCheckInterval) clearInterval(pathCheckInterval);
    pathCheckInterval = setInterval(function () {
      fetch(API_URL + '/api/streamer/validate/' + encodeURIComponent(streamerCode), {
        headers: { 'X-Api-Key': API_KEY },
      })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        if (!data.valid || !data.streamer.stream_url) return;
        var newBase = data.streamer.stream_url.replace(/\/master\.m3u8$/, '');
        var oldBase = window._vodyStreamBase;
        if (newBase && oldBase && newBase !== oldBase) {
          console.log('[VODY] UUID rotacionou, reconectando...');
          window._vodyStreamBase = newBase;
          var hls = window._vodyHls;
          if (hls) {
            var quality = window._vodyCurrentQuality || '720p';
            hls.loadSource(newBase + '/' + quality + '/stream.m3u8');
            console.log('[VODY] Reconectado: ' + newBase);
          }
        }
      })
      .catch(function () {});
    }, 60000);
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

        var info = joinData.max_spectators > 0
          ? ' (' + joinData.current_viewers + '/' + joinData.max_spectators + ')'
          : ' (' + joinData.current_viewers + ' assistindo)';
        setStatus('Overlay injetado!' + info, '#4caf50');
        setTimeout(function () { overlay.remove(); }, 1500);
      });

    } catch (e) {
      console.error('[VODY] Erro:', e);
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
        lowLatencyMode: true,
        liveSyncDurationCount: 3,
        liveMaxLatencyDurationCount: 5,
        backBufferLength: 0,
        enableWorker: true,
        maxBufferLength: 10,
        maxMaxBufferLength: 20,
        maxBufferHole: 0.8,
        fragLoadingTimeOut: 25000,
        fragLoadingMaxRetry: 8,
        fragLoadingRetryDelay: 1500,
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
      hls.on(Hls.Events.ERROR, function (event, data) {
        if (data.fatal) {
          if (data.type === Hls.ErrorTypes.NETWORK_ERROR) {
            console.warn('[VODY] Network error, tentando reconectar...');
            setTimeout(function () { hls.startLoad(); }, 3000);
          } else if (data.type === Hls.ErrorTypes.MEDIA_ERROR) {
            console.warn('[VODY] Media error, recuperando...');
            hls.recoverMediaError();
          }
        }
      });
      window._vodyHls = hls;
      console.log('[VODY] HLS iniciado: ' + initialUrl);
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

      console.log('[VODY] HLS nativo (Safari): ' + initialUrl);
    }
  }

  // ════════════════════════════════════════════════════════════════════════════
  // PLAYER DESKTOP — overlay sobre Kick + 160p
  // ════════════════════════════════════════════════════════════════════════════

  function injectDesktop(player, kickVideo, streamUrl) {
    var old = document.getElementById('hls-overlay');
    if (old) old.remove();
    var oldBtn = document.getElementById('hls-revert-btn');
    if (oldBtn) oldBtn.remove();

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
    createRevertButton();

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
    var oldBtn = document.getElementById('hls-revert-btn');
    if (oldBtn) oldBtn.remove();

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
    blocker.id = 'vody-kick-blocker';
    blocker.style.cssText = 'position:absolute;top:0;left:0;width:100%;height:100%;z-index:998;background:transparent;pointer-events:auto;';
    player.appendChild(blocker);

    // Esconde controles do Kick
    var css = document.createElement('style');
    css.textContent = [
      '#injected-channel-player button:not(#vody-fs):not(#vody-mute):not(#vody-quality-btn):not(#vody-quality-menu button){pointer-events:none!important;opacity:0!important;}',
      '#vody-quality-menu button{pointer-events:auto!important;opacity:1!important;}',
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
    createRevertButton();

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
              console.log('[VODY] Kick mudado pra 160p');
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
    muteBtn.id = 'vody-mute';
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
    wrap.id = 'vody-quality-wrap';

    var btn = document.createElement('button');
    btn.id = 'vody-quality-btn';
    btn.innerHTML = '\u2699';
    btn.title = 'Qualidade';

    var menu = document.createElement('div');
    menu.id = 'vody-quality-menu';

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
          console.log('[VODY] Qualidade: ' + q);
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
    fsBtn.id = 'vody-fs';
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

  function createRevertButton() {
    var revertBtn = document.createElement('button');
    revertBtn.id = 'hls-revert-btn';
    revertBtn.textContent = 'Reverter';
    revertBtn.style.cssText = 'position:fixed;bottom:20px;right:20px;z-index:10000;padding:10px 15px;background:#1a1a1a;color:#00d4ff;border:2px solid #00d4ff;border-radius:5px;cursor:pointer;font-weight:bold;font-family:sans-serif;';
    revertBtn.onclick = function () { location.reload(); };
    document.body.appendChild(revertBtn);
  }

})();
