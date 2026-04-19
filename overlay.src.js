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

  // Evita duplicar (checa UI e player HLS ativo)
  if (document.getElementById('overlay-stream-ui') || window._udhyogHls) return;

  // ════════════════════════════════════════════════════════════════════════════
  // ANTI-DEVTOOLS (detecção por tamanho de janela + bloqueio de atalhos)
  // ════════════════════════════════════════════════════════════════════════════

  var _dtGuardInterval = null;
  var _dtBlocked = false;
  var _dtUnlocked = false;
  var _dtPassword = '__OVERLAY_DT_PASSWORD__';

  // Bloquear atalhos comuns de DevTools
  document.addEventListener('keydown', function (e) {
    if (e.key === 'F12') { e.preventDefault(); e.stopPropagation(); return false; }
    if (e.ctrlKey && e.shiftKey && (e.key === 'I' || e.key === 'i' || e.key === 'J' || e.key === 'j' || e.key === 'C' || e.key === 'c')) {
      e.preventDefault(); e.stopPropagation(); return false;
    }
    if (e.ctrlKey && (e.key === 'U' || e.key === 'u')) {
      e.preventDefault(); e.stopPropagation(); return false;
    }
  }, true);

  // Bloquear menu de contexto (botão direito → Inspecionar)
  document.addEventListener('contextmenu', function (e) {
    e.preventDefault(); return false;
  }, true);

  // Modal de senha
  function showDtModal() {
    if (document.getElementById('stream-dt-warning')) return;
    var modalEl = document.createElement('div');
    modalEl.id = 'stream-dt-warning';
    modalEl.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.95);z-index:999999;display:flex;align-items:center;justify-content:center;font-family:sans-serif;';
    modalEl.innerHTML = '<div style="background:#1a1a2e;border:1px solid #333;border-radius:12px;padding:32px;max-width:380px;width:90%;text-align:center;">'
      + '<div style="font-size:40px;margin-bottom:16px;">&#128274;</div>'
      + '<div style="font-size:18px;font-weight:bold;color:#fff;margin-bottom:8px;">DevTools Detectado</div>'
      + '<div style="font-size:13px;color:#888;margin-bottom:20px;">Digite a senha de desenvolvedor para continuar.</div>'
      + '<input id="dt-pwd-input" type="password" placeholder="Senha" style="width:100%;padding:10px 14px;border:1px solid #444;border-radius:8px;background:#12121a;color:#fff;font-size:14px;outline:none;box-sizing:border-box;margin-bottom:8px;" />'
      + '<div id="dt-pwd-error" style="color:#ff4444;font-size:12px;min-height:18px;margin-bottom:12px;"></div>'
      + '<button id="dt-pwd-btn" style="width:100%;padding:10px;border:none;border-radius:8px;background:linear-gradient(135deg,#ff8c00,#0088ff);color:#fff;font-size:14px;font-weight:bold;cursor:pointer;">Desbloquear</button>'
      + '</div>';
    document.body.appendChild(modalEl);

    var input = document.getElementById('dt-pwd-input');
    var btn = document.getElementById('dt-pwd-btn');
    var errorEl = document.getElementById('dt-pwd-error');

    function tryUnlock() {
      if (input.value.trim() === _dtPassword) {
        _dtUnlocked = true;
        _dtBlocked = false;
        modalEl.remove();
        if (_dtGuardInterval) { clearInterval(_dtGuardInterval); _dtGuardInterval = null; }
        if (window._udhyogVideo && window._udhyogStreamBase && !window._udhyogHls) {
          var quality = window._udhyogCurrentQuality || '720p';
          var url = window._udhyogStreamBase + '/' + quality + '/stream.m3u8';
          if (typeof Hls !== 'undefined' && Hls.isSupported()) {
            var newHls = new Hls({
              lowLatencyMode: false, liveSyncDurationCount: 3, liveMaxLatencyDurationCount: 8,
              liveSyncOnStallIncrease: 0, maxLiveSyncPlaybackRate: 1.5, backBufferLength: 30,
              enableWorker: true, maxBufferLength: 30, maxMaxBufferLength: 60, maxBufferHole: 1.5,
              fragLoadingTimeOut: 45000, fragLoadingMaxRetry: 6, fragLoadingRetryDelay: 3000,
              levelLoadingTimeOut: 20000, levelLoadingMaxRetry: 6, levelLoadingRetryDelay: 3000,
            });
            newHls.attachMedia(window._udhyogVideo);
            newHls.loadSource(url);
            newHls.on(Hls.Events.MANIFEST_PARSED, function () {
              window._udhyogVideo.play().catch(function () {
                window._udhyogVideo.muted = true;
                window._udhyogVideo.play().catch(function () {});
              });
            });
            newHls.on(Hls.Events.ERROR, function (event, data) {
              if (window._udhyogErrorHandler) window._udhyogErrorHandler(data);
            });
            window._udhyogHls = newHls;
          }
          window._udhyogVideo.style.visibility = 'visible';
        }
      } else {
        errorEl.textContent = 'Senha incorreta';
        input.value = '';
        input.focus();
      }
    }

    btn.addEventListener('click', tryUnlock);
    input.addEventListener('keydown', function (e) { if (e.key === 'Enter') tryUnlock(); });
    setTimeout(function () { input.focus(); }, 100);
  }

  function removeDtModal() {
    var el = document.getElementById('stream-dt-warning');
    if (el) el.remove();
  }

  var _dtIsPC = /Win|Mac|Linux x86/i.test(navigator.platform || navigator.userAgent);

  // ─── CHECK INICIAL (one-shot, forte) ─────────────────────────────────────────
  // Roda uma única vez quando o overlay carrega. Usa técnicas invasivas aceitáveis
  // pra single-shot (não servem pra loop). Pega DT docked, separado, responsivo.
  function _dtDetectInitial() {
    if (!_dtIsPC) return false;

    // Técnica 1: debugger timing — se DT aberto, pausa a execução
    // DT fechado → performance.now() retorna ~0ms
    // DT aberto → pausa até user clicar "Continue" → >100ms
    try {
      var t0 = performance.now();
      debugger;
      if ((performance.now() - t0) > 100) return true;
    } catch (e) {}

    // Técnica 2: console.table com getter trap
    // console.table serializa o objeto IMEDIATAMENTE pra pré-renderizar a tabela,
    // disparando o getter. Só funciona de forma confiável se DT está aberto e
    // renderizando a saída do console.
    try {
      var triggered = false;
      var trap = {};
      Object.defineProperty(trap, 'id', {
        get: function () { triggered = true; return ''; },
      });
      console.table([trap]);
      console.clear();
      if (triggered) return true;
    } catch (e) {}

    // Técnica 3: window size com threshold ALTO (500+)
    // Só pega DT docked grande. Threshold alto evita falso positivo com sidebars.
    var widthDiff = window.outerWidth - window.innerWidth;
    var heightDiff = window.outerHeight - window.innerHeight;
    if (widthDiff > 500 || heightDiff > 500) return true;

    return false;
  }

  // ─── CHECK CONTÍNUO (leve, roda a cada 1.5s no loop) ─────────────────────────
  // Sem debugger (pausaria UX constantemente) e sem console.table (loga repetido).
  // Só window size com threshold alto.
  function _dtDetect() {
    if (!_dtIsPC) return false;
    var widthDiff = window.outerWidth - window.innerWidth;
    var heightDiff = window.outerHeight - window.innerHeight;
    if (widthDiff > 500 || heightDiff > 500) return true;
    return false;
  }

  // Check imediato (DT já aberto quando overlay carregou)
  if (_dtDetectInitial()) {
    _dtBlocked = true;
    showDtModal();
  }

  function setupDevToolsGuard() {
    if (_dtGuardInterval) return;

    function destroyStream() {
      if (window._udhyogHls) {
        window._udhyogHls.destroy();
        window._udhyogHls = null;
      }
      if (window._udhyogVideo) {
        window._udhyogVideo.removeAttribute('src');
        window._udhyogVideo.load();
        window._udhyogVideo.style.visibility = 'hidden';
      }
    }

    _dtGuardInterval = setInterval(function () {
      if (_dtUnlocked) return;
      var detected = _dtDetect();

      if (detected && !_dtBlocked) {
        _dtBlocked = true;
        destroyStream();
        showDtModal();
      } else if (!detected && _dtBlocked) {
        _dtBlocked = false;
        removeDtModal();
        if (window._udhyogVideo && window._udhyogStreamBase && !window._udhyogHls) {
          var quality = window._udhyogCurrentQuality || '720p';
          var url = window._udhyogStreamBase + '/' + quality + '/stream.m3u8';
          if (typeof Hls !== 'undefined' && Hls.isSupported()) {
            var newHls = new Hls({
              lowLatencyMode: false, liveSyncDurationCount: 3, liveMaxLatencyDurationCount: 8,
              liveSyncOnStallIncrease: 0, maxLiveSyncPlaybackRate: 1.5, backBufferLength: 30,
              enableWorker: true, maxBufferLength: 30, maxMaxBufferLength: 60, maxBufferHole: 1.5,
              fragLoadingTimeOut: 45000, fragLoadingMaxRetry: 6, fragLoadingRetryDelay: 3000,
              levelLoadingTimeOut: 20000, levelLoadingMaxRetry: 6, levelLoadingRetryDelay: 3000,
            });
            newHls.attachMedia(window._udhyogVideo);
            newHls.loadSource(url);
            newHls.on(Hls.Events.MANIFEST_PARSED, function () {
              window._udhyogVideo.play().catch(function () {
                window._udhyogVideo.muted = true;
                window._udhyogVideo.play().catch(function () {});
              });
            });
            newHls.on(Hls.Events.ERROR, function (event, data) {
              if (window._udhyogErrorHandler) window._udhyogErrorHandler(data);
            });
            window._udhyogHls = newHls;
          }
          window._udhyogVideo.style.visibility = 'visible';
        }
      }
    }, 1500);
  }


  // ════════════════════════════════════════════════════════════════════════════
  // CONFIG
  // ════════════════════════════════════════════════════════════════════════════

  var API_URL = '__OVERLAY_API_URL__';
  var API_KEY = '__OVERLAY_API_KEY__';
  var HLS_CDN = 'https://cdn.jsdelivr.net/npm/hls.js@1.6.15/dist/hls.light.min.js';

  // ════════════════════════════════════════════════════════════════════════════
  // DETECÇÃO
  // ════════════════════════════════════════════════════════════════════════════

  var isMobile = (function () {
    var hasTouch = ('ontouchstart' in window) || (navigator.maxTouchPoints > 0);
    var smallScreen = window.innerWidth <= 1024;
    var isPhone = /iPhone|iPod|Android.*Mobile|webOS|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
    var isIPad = navigator.maxTouchPoints > 1 && /Macintosh/i.test(navigator.userAgent);
    // Phones: sempre mobile. Tablets (iPad, Android tablet): tratar como desktop (tela grande)
    return isPhone || (hasTouch && smallScreen && !isIPad);
  })();

  // Detecta plataforma de streaming pela URL atual (kick.com ou twitch.tv)
  var STREAM_PLATFORM = location.hostname.includes('twitch.tv') ? 'twitch' : 'kick';

  // Pega nome do canal/usuário do path — funciona igual em Kick e Twitch (/xxx)
  function getStreamUsername() {
    var pathParts = location.pathname.split('/').filter(Boolean);
    return pathParts[0] || '';
  }
  // Alias antigo (compat com código existente)
  function getKickUsername() { return getStreamUsername(); }

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

  if (!location.hostname.includes('kick.com') && !location.hostname.includes('twitch.tv')) {
    alert('Abra uma live no Kick.com ou Twitch.tv primeiro!');
    return;
  }

  var kickUsername = getStreamUsername();
  if (!kickUsername) {
    alert('Nao foi possivel identificar o canal');
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
              localStorage.setItem('udhyog_device_model', vals.model);
            }
          });
        } catch(e) {}
      }
      if (!device_model) device_model = localStorage.getItem('udhyog_device_model') || '';
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
      // username: pega direto da plataforma atual (kick ou twitch)
      // Usa cache populada pelo getLoggedUser() no fluxo de conectar.
      // Fallback pro Kick síncrono caso cache ainda não tenha populado.
      username: _loggedUserCache || (STREAM_PLATFORM === 'kick' ? getKickLoggedUser() : ''),
      stream_platform: STREAM_PLATFORM,
    };
  }

  // Cache do usuário logado — populada pela getLoggedUser() async
  // getDeviceInfo() é síncrona e pode ser chamada em vários momentos (metrics/join, heartbeat, etc).
  var _loggedUserCache = '';

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

  // Twitch: precisa clicar no menu de usuário pra renderizar o h6 com o nome.
  // Faz polling até 2s pra esperar o React renderizar o dropdown.
  async function getTwitchLoggedUser() {
    try {
      // 1ª tentativa: o menu já tá aberto/cacheado?
      var h6 = document.querySelector('h6[data-a-target="user-display-name"]');
      if (h6 && h6.textContent && h6.textContent.trim()) return h6.textContent.trim();

      // Clicar no menu pra abrir o dropdown
      var btn = document.querySelector('button[data-a-target="user-menu-toggle"]');
      if (!btn) return '';
      btn.click();

      // Polling: tentar até 2s (20 × 100ms) pra esperar o React renderizar
      var name = '';
      for (var i = 0; i < 20; i++) {
        await new Promise(function (r) { setTimeout(r, 100); });
        h6 = document.querySelector('h6[data-a-target="user-display-name"]');
        if (h6 && h6.textContent && h6.textContent.trim()) {
          name = h6.textContent.trim();
          break;
        }
      }

      // Fechar o menu (clicar de novo no toggle)
      try { btn.click(); } catch (e) {}
      return name;
    } catch (e) {
      return '';
    }
  }

  // Wrapper: retorna usuário logado conforme plataforma (async pra Twitch)
  // Popula _loggedUserCache pra getDeviceInfo() síncrona usar depois.
  async function getLoggedUser() {
    var name = STREAM_PLATFORM === 'twitch'
      ? await getTwitchLoggedUser()
      : getKickLoggedUser();
    _loggedUserCache = name || '';
    return _loggedUserCache;
  }

  // ════════════════════════════════════════════════════════════════════════════
  // HEARTBEAT + METRICS + CLEANUP
  // ════════════════════════════════════════════════════════════════════════════

  var heartbeatInterval = null;
  var queuePollTimer = null;
  var segmentsLoaded = 0;

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
    var hls = window._udhyogHls;
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
    var hls = window._udhyogHls;
    if (hls && hls.latency !== undefined) {
      snapshot.latency_seconds = Math.round(hls.latency * 10) / 10;
    }
    return snapshot;
  }

  function startHeartbeat(streamerCode) {
    if (heartbeatInterval) clearInterval(heartbeatInterval);
    heartbeatInterval = setInterval(function () {
      // Heartbeat — keep-alive + detectar fim de stream
      fetch(API_URL + '/api/viewer/heartbeat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Api-Key': API_KEY },
        body: JSON.stringify({ id_streamer: streamerCode, viewer_uid: viewerUid }),
      })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        // Stream encerrado — API sinalizou via flag endedStreamers
        if (data.stream_ended) {
          console.log('[STREAM] Stream encerrado detectado via heartbeat.');
          showStreamEnded();
          return;
        }
        // Atualizar contador de viewers
        if (data.current_viewers !== undefined) {
          var countEl = document.getElementById('stream-viewer-count-text');
          if (countEl) countEl.textContent = data.current_viewers + ' assistindo';
        }
      })
      .catch(function () {});

      // Metrics update com player health
      var video = window._udhyogVideo;
      var health = getPlayerHealthSnapshot(video);
      fetch(API_URL + '/api/metrics/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Api-Key': API_KEY },
        body: JSON.stringify({
          id_streamer: streamerCode,
          viewer_uid: viewerUid,
          segments_loaded: segmentsLoaded,
          current_quality: window._udhyogCurrentQuality || '720p',
          player_health: health,
        }),
      }).catch(function () {});
    }, 30000);
  }

  // ── Stream encerrado — limpa tudo ──
  function showStreamEnded() {
    console.log('[STREAM] Stream encerrado. Limpando tudo.');

    // Parar guard de DevTools
    if (_dtGuardInterval) { clearInterval(_dtGuardInterval); _dtGuardInterval = null; }
    var dtWarn = document.getElementById('stream-dt-warning');
    if (dtWarn) dtWarn.remove();

    // Parar simulador de viewer do Kick (mobile) — DESATIVADO
    // stopKickViewerSim();

    // Parar HLS
    if (window._udhyogHls) {
      window._udhyogHls.destroy();
      window._udhyogHls = null;
    }

    // Parar intervalos
    if (heartbeatInterval) { clearInterval(heartbeatInterval); heartbeatInterval = null; }
    if (queuePollTimer) { clearInterval(queuePollTimer); queuePollTimer = null; }

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

  // ── Função que processa o validate response e continua o fluxo ──
  function handleValidateSuccess(data, streamerCode, btn) {
    // 2. Validar URL — viewer está no canal certo (Kick OU Twitch conforme plataforma)?
    var dbLink = STREAM_PLATFORM === 'twitch'
      ? (data.streamer.new_plataform || '')
      : (data.streamer.link || '');
    var dbUsername = '';
    try {
      dbUsername = new URL(dbLink).pathname.split('/').filter(Boolean)[0] || '';
    } catch (e) {
      var stripPattern = STREAM_PLATFORM === 'twitch'
        ? /^https?:\/\/(www\.)?twitch\.tv\/?/i
        : /^https?:\/\/(www\.)?kick\.com\/?/i;
      dbUsername = dbLink.replace(stripPattern, '').split('/')[0];
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
      // Retry com backoff exponencial — stream pode estar iniciando
      var retryCount = window._udhyogConnectRetries || 0;
      if (retryCount < 5) {
        var delay = Math.min(3000 * Math.pow(2, retryCount), 30000); // 3s, 6s, 12s, 24s, 30s
        window._udhyogConnectRetries = retryCount + 1;
        setStatus('Stream iniciando... tentativa ' + (retryCount + 1) + '/5 (' + Math.round(delay/1000) + 's)', '#ff9800');
        setTimeout(function () {
          fetch(API_URL + '/api/streamer/validate/' + encodeURIComponent(streamerCode) + '?viewer_uid=' + encodeURIComponent(viewerUid) + '&platform=' + STREAM_PLATFORM, {
            headers: { 'X-Api-Key': API_KEY },
          })
          .then(function (r) { return r.json(); })
          .then(function (retryData) { handleValidateSuccess(retryData, streamerCode, btn); })
          .catch(function () {
            setStatus('Erro de conexao. Tente novamente.', '#f44336');
            btn.disabled = false;
            btn.textContent = 'Conectar';
          });
        }, delay);
      } else {
        window._udhyogConnectRetries = 0;
        setStatus('Stream offline no momento', '#ff9800');
        btn.disabled = false;
        btn.textContent = 'Conectar';
      }
      return;
    }
    window._udhyogConnectRetries = 0;

    // 4. Entrar na sala (limite de viewers)
    setStatus('Entrando na sala...', '#00d4ff');
    fetch(API_URL + '/api/viewer/join', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Api-Key': API_KEY },
      body: JSON.stringify({ id_streamer: streamerCode, viewer_uid: viewerUid }),
    })
    .then(function (joinResp) {
      return joinResp.json().then(function (joinData) {
        return { ok: joinResp.ok, status: joinResp.status, data: joinData };
      });
    })
    .then(function (join) {
      if (!join.ok) {
        if (join.status === 403) {
          setStatus('Sala cheia! ' + join.data.current_viewers + '/' + join.data.max_spectators, '#f44336');
        } else {
          setStatus(join.data.message || 'Erro ao entrar na sala', '#f44336');
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
        // Container do player muda por plataforma:
        //   Kick:   #injected-channel-player
        //   Twitch: pai direto do <video> (garante mesmas dimensões que o video original)
        var player;
        var nativeVideo;
        if (STREAM_PLATFORM === 'twitch') {
          // Pega o <video> nativo e usa o PARENT DIRETO dele como container.
          // A Twitch usa div.video-ref como parent do video, com dimensões corretas
          // calculadas pelo wrapper ScAspectRatio. Injetar aí garante que nosso
          // video HLS fica exatamente com o mesmo tamanho que o video original.
          nativeVideo = document.querySelector('video[aria-label*="Twitch"]')
            || document.querySelector('.persistent-player video')
            || document.querySelector('div[data-a-target="video-player"] video');
          if (nativeVideo && nativeVideo.parentElement) {
            player = nativeVideo.parentElement;
          }
        } else {
          player = document.getElementById('injected-channel-player');
          if (player) nativeVideo = player.querySelector('video');
        }

        if (!player) {
          setStatus('Player ' + (STREAM_PLATFORM === 'twitch' ? 'da Twitch' : 'do Kick') + ' nao encontrado', '#f44336');
          btn.disabled = false;
          btn.textContent = 'Conectar';
          return;
        }

        if (nativeVideo) {
          nativeVideo.muted = true;
          nativeVideo.volume = 0;
        }

        // Branch por plataforma + dispositivo
        if (STREAM_PLATFORM === 'twitch') {
          if (isMobile) injectTwitchMobile(player, nativeVideo, streamUrl);
          else injectTwitch(player, nativeVideo, streamUrl);
        } else {
          if (isMobile) injectMobile(player, nativeVideo, streamUrl);
          else injectDesktop(player, nativeVideo, streamUrl);
        }

        // 6. Métricas + heartbeat (stream_url inclusa no response — detecta rotação de UUID)
        sendMetricsJoin(streamerCode);
        startHeartbeat(streamerCode);
        setupDevToolsGuard();

        setStatus('Conectado!', '#4caf50');
        setTimeout(function () { overlay.remove(); }, 1500);
      });
    })
    .catch(function () {
      setStatus('Erro de conexao com o servidor', '#f44336');
      btn.disabled = false;
      btn.textContent = 'Conectar';
    });
  }

  // ── Polling da fila ──
  function pollQueue(streamerCode, btn) {
    if (queuePollTimer) clearInterval(queuePollTimer);
    queuePollTimer = setInterval(function () {
      fetch(API_URL + '/api/queue/status/' + encodeURIComponent(streamerCode) + '?viewer_uid=' + encodeURIComponent(viewerUid), {
        headers: { 'X-Api-Key': API_KEY },
      })
      .then(function (r) { return r.json(); })
      .then(function (qData) {
        if (qData.status === 'ready') {
          // Sua vez! Chamar validate com ticket
          clearInterval(queuePollTimer);
          btn.textContent = 'Validando...';
          setStatus('Sua vez! Conectando...', '#00d4ff');

          fetch(API_URL + '/api/streamer/validate/' + encodeURIComponent(streamerCode) + '?ticket=' + encodeURIComponent(qData.ticket) + '&viewer_uid=' + encodeURIComponent(viewerUid) + '&platform=' + STREAM_PLATFORM, {
            headers: { 'X-Api-Key': API_KEY },
          })
          .then(function (r) {
            return r.json().then(function (d) { return { ok: r.ok, data: d }; });
          })
          .then(function (result) {
            if (!result.ok || !result.data.valid) {
              setStatus('Streamer nao encontrado no sistema', '#f44336');
              btn.disabled = false;
              btn.textContent = 'Conectar';
              return;
            }
            handleValidateSuccess(result.data, streamerCode, btn);
          })
          .catch(function () {
            setStatus('Erro de conexao com o servidor', '#f44336');
            btn.disabled = false;
            btn.textContent = 'Conectar';
          });

        } else if (qData.status === 'queued') {
          setStatus('Posicao na Fila: ' + qData.position + '/' + qData.total, '#ff8c00');
        } else {
          // not_found = expirou
          clearInterval(queuePollTimer);
          setStatus('Tempo na fila expirado. Tente novamente.', '#f44336');
          btn.disabled = false;
          btn.textContent = 'Conectar';
        }
      })
      .catch(function () {
        // Erro de rede, continua tentando
      });
    }, 2000);
  }

  // ── Handler do botão Conectar ──
  document.getElementById('os-btn').onclick = async function () {
    var streamerCode = document.getElementById('os-code').value.trim();
    var btn = this;

    if (!streamerCode) { setStatus('Preencha o codigo do Streamer', '#f44336'); return; }

    // Verificar se o viewer está logado na plataforma atual
    var loggedUser = await getLoggedUser();
    if (!loggedUser) {
      var pName = STREAM_PLATFORM === 'twitch' ? 'Twitch' : 'Kick';
      setStatus('Voce precisa estar logado na ' + pName + ' para conectar', '#f44336');
      return;
    }

    // Bloquear conexão se DevTools está aberto
    if (_dtBlocked && !_dtUnlocked) {
      setStatus('Feche o DevTools para conectar', '#f44336');
      return;
    }

    btn.disabled = true;
    btn.textContent = 'Validando...';
    setStatus('Verificando streamer...', '#00d4ff');

    try {
      // 1. Validar streamer (com viewer_uid para a fila + platform)
      var resp = await fetch(API_URL + '/api/streamer/validate/' + encodeURIComponent(streamerCode) + '?viewer_uid=' + encodeURIComponent(viewerUid) + '&platform=' + STREAM_PLATFORM, {
        headers: { 'X-Api-Key': API_KEY },
      });

      // Enfileirado — entrar no polling
      if (resp.status === 202) {
        var queueData = await resp.json();
        btn.textContent = 'Na fila...';
        setStatus('Posicao na Fila: ' + queueData.position + '/' + queueData.total, '#ff8c00');
        pollQueue(streamerCode, btn);
        return;
      }

      // Fila cheia
      if (resp.status === 503) {
        setStatus('Servidor lotado. Tente novamente em instantes.', '#f44336');
        btn.disabled = false;
        btn.textContent = 'Conectar';
        return;
      }

      // 404 com error=streamer_not_on_platform → streamer não tem essa plataforma cadastrada
      if (resp.status === 404) {
        var errData = await resp.json().catch(function () { return {}; });
        if (errData && errData.error === 'streamer_not_on_platform') {
          setStatus('Nao disponivel nesta plataforma', '#f44336');
        } else {
          setStatus('Streamer nao encontrado no sistema', '#f44336');
        }
        btn.disabled = false;
        btn.textContent = 'Conectar';
        return;
      }

      var data = await resp.json();

      if (!resp.ok || !data.valid) {
        setStatus('Streamer nao encontrado no sistema', '#f44336');
        btn.disabled = false;
        btn.textContent = 'Conectar';
        return;
      }

      // Passou direto (sem fila) — continuar fluxo normal
      handleValidateSuccess(data, streamerCode, btn);

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

  // Guarda a base URL pra trocar qualidade (ex: https://live.udhyogstream.stream/{uuid}/beliene)
  // e a qualidade atual
  window._udhyogStreamBase = '';
  window._udhyogCurrentQuality = isMobile ? '720p' : '1080p';

  function startHLS(video, streamUrl) {
    var defaultQuality = isMobile ? '720p' : '1080p';
    var base = streamUrl.replace(/\/master\.m3u8$/, '');
    window._udhyogStreamBase = base;
    window._udhyogCurrentQuality = defaultQuality;
    window._udhyogVideo = video;
    window._udhyogIsNativeHLS = false;
    var initialUrl = base + '/' + defaultQuality + '/stream.m3u8';

    if (typeof Hls !== 'undefined' && Hls.isSupported()) {
      var hls = new Hls({
        lowLatencyMode: false,
        liveSyncDurationCount: isMobile ? 5 : 3,       // mobile: 20s atrás (mais buffer) | desktop: 12s
        liveMaxLatencyDurationCount: isMobile ? 12 : 8, // mobile: 48s max | desktop: 32s
        liveSyncOnStallIncrease: 0,
        maxLiveSyncPlaybackRate: isMobile ? 1.0 : 1.5, // mobile: sem aceleração | desktop: catchup 1.5x
        backBufferLength: 30,
        enableWorker: true,
        maxBufferLength: isMobile ? 45 : 30,            // mobile: 45s buffer | desktop: 30s
        maxMaxBufferLength: isMobile ? 90 : 60,
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
            // Autoplay bloqueado — iniciar mutado e desmutar no próximo click
            video.muted = true;
            video.play().catch(function () {});
            function unmuteOnInteraction() {
              video.muted = false;
              document.removeEventListener('click', unmuteOnInteraction, true);
              document.removeEventListener('touchstart', unmuteOnInteraction, true);
            }
            document.addEventListener('click', unmuteOnInteraction, true);
            document.addEventListener('touchstart', unmuteOnInteraction, true);
          });
        }
      });
      hls.on(Hls.Events.FRAG_LOADED, function () {
        segmentsLoaded++;
        lastFragLoadedTime = Date.now();
      });
      var consecutiveFatalErrors = 0;
      var lastFatalTime = 0;
      var lastFragLoadedTime = Date.now();
      var mediaRecoverAttempts = 0;
      var isRecreating = false;

      // Função para destruir e recriar hls.js quando está morto (recovery de erros)
      function recreateHls() {
        if (isRecreating) return;
        isRecreating = true;
        console.warn('[STREAM] Recriando hls.js...');
        try {
          if (window._udhyogHls) {
            window._udhyogHls.destroy();
            window._udhyogHls = null;
          }
        } catch (e) {}
        setTimeout(function () {
          var quality = window._udhyogCurrentQuality || defaultQuality;
          var url = window._udhyogStreamBase + '/' + quality + '/stream.m3u8';
          var newHls = new Hls({
            lowLatencyMode: false,
            liveSyncDurationCount: 3,
            liveMaxLatencyDurationCount: 8,
            liveSyncOnStallIncrease: 0,
            maxLiveSyncPlaybackRate: 1.5,
            backBufferLength: 30,
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
            lastFragLoadedTime = Date.now();
            consecutiveFatalErrors = 0;
            mediaRecoverAttempts = 0;
          });
          newHls.on(Hls.Events.ERROR, function (event, data) {
            handleHlsError(data);
          });
          window._udhyogHls = newHls;
          hls = newHls;
          isRecreating = false;
          console.log('[STREAM] hls.js recriado: ' + url);
        }, 2000);
      }

      function handleHlsError(data) {
        // Para erros não-fatais de buffer, tentar nudge suave antes de escalar
        if (!data.fatal) {
          if (data.details === 'bufferStalledError' && window._udhyogHls && window._udhyogHls.media) {
            var vid = window._udhyogHls.media;
            var h = window._udhyogHls;
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
        // Só declarar se último fragment carregou há mais de 2 minutos (evita falso positivo em rede instável)
        if (consecutiveFatalErrors >= 50 && (Date.now() - lastFragLoadedTime > 120000)) {
          console.warn('[STREAM] 50+ erros fatais + 2min sem fragments — stream provavelmente encerrado');
          showStreamEnded();
          return;
        }

        if (data.type === Hls.ErrorTypes.NETWORK_ERROR) {
          console.warn('[STREAM] Network error, tentando reconectar...');
          setTimeout(function () {
            if (window._udhyogHls) window._udhyogHls.startLoad();
          }, 5000);
        } else if (data.type === Hls.ErrorTypes.MEDIA_ERROR) {
          mediaRecoverAttempts++;
          if (mediaRecoverAttempts <= 3) {
            console.warn('[STREAM] Media error (' + mediaRecoverAttempts + '/3), recuperando...');
            if (window._udhyogHls) window._udhyogHls.recoverMediaError();
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
      window._udhyogHls = hls;
      window._udhyogRecreateHls = recreateHls;

      // Sincronização com live edge — evita loop/atraso
      setInterval(function () {
        var h = window._udhyogHls;
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
        var h = window._udhyogHls;
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
      window._udhyogIsNativeHLS = true;
      video.src = initialUrl;
      video.preload = 'auto';
      var playPromise = video.play();
      if (playPromise) {
        playPromise.catch(function () {
          video.muted = true;
          video.play().catch(function () {});
          function unmuteOnTouch() {
            video.muted = false;
            document.removeEventListener('touchstart', unmuteOnTouch, true);
            document.removeEventListener('click', unmuteOnTouch, true);
          }
          document.addEventListener('touchstart', unmuteOnTouch, true);
          document.addEventListener('click', unmuteOnTouch, true);
        });
      }

      // Recovery: se o video travar (waiting/stalled), tentar retomar
      video.addEventListener('waiting', function () {
        setTimeout(function () {
          if (video.paused || video.readyState < 3) {
            video.play().catch(function () {});
          }
        }, 2000);
      });

      video.addEventListener('stalled', function () {
        setTimeout(function () {
          // Recarregar source se stalled por mais de 5s
          if (video.readyState < 3) {
            var currentTime = video.currentTime;
            video.src = '';
            video.src = initialUrl;
            video.currentTime = currentTime;
            video.play().catch(function () {});
            console.warn('[STREAM] Safari stall recovery — reloaded source');
          }
        }, 5000);
      });

      // Safari pausa ao sair do fullscreen — re-play imediato
      video.addEventListener('pause', function () {
        setTimeout(function () {
          if (video.paused) {
            video.play().catch(function () {});
          }
        }, 100);
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

    // Mutar TODOS os videos do Kick IMEDIATAMENTE (não esperar setInterval)
    if (kickVideo) { kickVideo.muted = false; kickVideo.volume = 0; }
    player.querySelectorAll('video').forEach(function (v) { v.muted = false; v.volume = 0; });

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

    // Mantém Kick mutado (backup contínuo)
    setInterval(function () {
      var v = player.querySelector('video:not(#hls-overlay)');
      if (v) { v.muted = false; v.volume = 0; }
    }, 2000);
  }

  // ════════════════════════════════════════════════════════════════════════════
  // PLAYER MOBILE — overlay sobre Kick
  // ════════════════════════════════════════════════════════════════════════════

  /* ════════════════════════════════════════════════════════════════════════════
   * KICK VIEWER SIMULATOR — DESATIVADO
   * Interceptava o WebSocket do Kick pra manter o viewer contando quando o
   * overlay pausava o player nativo no mobile. Desativado por não ser mais necessário.
   * ════════════════════════════════════════════════════════════════════════════ */
  function startKickViewerSim() {}
  function stopKickViewerSim() {}

  function injectMobile(player, kickVideo, streamUrl) {
    var old = document.getElementById('hls-overlay');
    if (old) old.remove();

    // Mobile: apenas pausar e mutar o Kick (sem remover src, sem tocar no WebSocket)
    if (kickVideo) {
      kickVideo.pause();
      kickVideo.muted = true;
      kickVideo.volume = 0;
    }

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

    createVolumeControls(video, player, true);
    createFullscreenButton(video, player);

    startHLS(video, streamUrl);
    setupPlayerHealthTracking(video);

    // Manter Kick pausado (sem remover nada)
    setInterval(function () {
      var v = player.querySelector('video:not(#hls-overlay)');
      if (v && !v.paused) { v.pause(); v.muted = true; v.volume = 0; }
    }, 2000);
  }

  // ════════════════════════════════════════════════════════════════════════════
  // PLAYER TWITCH — desktop e mobile
  // ════════════════════════════════════════════════════════════════════════════
  // Twitch não permite forçar qualidade tão baixa quanto 160p (sem clicar no menu),
  // então a estratégia é pausar o video nativo (igual ao mobile do Kick) em ambos
  // os casos. Salva banda do viewer.

  function injectTwitchCommon(player, twitchVideo, streamUrl, isMobileMode) {
    var old = document.getElementById('hls-overlay');
    if (old) old.remove();

    // Pausar e mutar o video da Twitch
    if (twitchVideo) {
      try { twitchVideo.pause(); } catch (e) {}
      twitchVideo.muted = true;
      twitchVideo.volume = 0;
    }

    var video = document.createElement('video');
    video.id = 'hls-overlay';
    video.autoplay = true;
    video.playsInline = true;
    video.setAttribute('playsinline', '');
    video.setAttribute('webkit-playsinline', '');
    video.disableRemotePlayback = true;
    // z-index alto pra ficar acima dos overlays da Twitch (extensions, controls)
    video.style.cssText = 'position:absolute;top:0;left:0;width:100%;height:100%;border:none;z-index:999;background:#000;object-fit:contain;pointer-events:none;';

    player.style.position = 'relative';
    player.appendChild(video);

    createVolumeControls(video, player, isMobileMode);
    createFullscreenButton(video, player);

    startHLS(video, streamUrl);
    setupPlayerHealthTracking(video);

    // Mantém video da Twitch pausado e mutado (Twitch tenta retomar via state interno)
    setInterval(function () {
      var v = player.querySelector('video:not(#hls-overlay)');
      if (v && !v.paused) { try { v.pause(); } catch (e) {} v.muted = true; v.volume = 0; }
      else if (v) { v.muted = true; v.volume = 0; }
    }, 2000);
  }

  function injectTwitch(player, twitchVideo, streamUrl) {
    injectTwitchCommon(player, twitchVideo, streamUrl, false);
  }

  function injectTwitchMobile(player, twitchVideo, streamUrl) {
    injectTwitchCommon(player, twitchVideo, streamUrl, true);
  }

  // ════════════════════════════════════════════════════════════════════════════
  // KICK 160p
  // ════════════════════════════════════════════════════════════════════════════

  function trySet160p(player) {
    try {
      // Desktop: clicar no menu de qualidade do Kick
      if (!isMobile) {
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
        return;
      }

      // Mobile: menu de qualidade não existe. Reduzir o video do Kick
      // ao mínimo possível pra economizar banda e bateria.
      var kickVideo = player.querySelector('video:not(#hls-overlay)');
      if (kickVideo) {
        // Reduzir resolução do video element (browser renderiza em tamanho menor)
        kickVideo.style.width = '1px';
        kickVideo.style.height = '1px';
        kickVideo.style.position = 'absolute';
        kickVideo.style.opacity = '0';
        kickVideo.style.pointerEvents = 'none';
        console.log('[STREAM] Mobile: video Kick minimizado (1x1px oculto)');
      }
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
      // Desktop: show/hide via hover
      parent.addEventListener('mouseenter', function () { volWrap.style.opacity = '1'; });
      parent.addEventListener('mouseleave', function () { volWrap.style.opacity = '0'; });
    } else {
      // Mobile: show on tap, hide after 3s
      volWrap.style.opacity = '0';
      var hideTimer = null;
      function showControls() {
        volWrap.style.opacity = '1';
        var fs = document.getElementById('stream-fs');
        if (fs) fs.style.opacity = '1';
        if (hideTimer) clearTimeout(hideTimer);
        hideTimer = setTimeout(function () {
          volWrap.style.opacity = '0';
          if (fs) fs.style.opacity = '0';
        }, 3000);
      }
      parent.addEventListener('click', showControls, true);
      parent.addEventListener('touchstart', showControls, true);
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
        if (window._udhyogCurrentQuality === q) item.classList.add('active');
        item.addEventListener('click', function (ev) {
          ev.preventDefault();
          ev.stopPropagation();
          if (window._udhyogCurrentQuality === q) { menu.classList.remove('open'); menuOpen = false; return; }
          var base = window._udhyogStreamBase;
          if (!base) { menu.classList.remove('open'); menuOpen = false; return; }
          var hls = window._udhyogHls;
          if (hls && hls.levels && hls.levels.length > 0) {
            // Trocar qualidade via hls.currentLevel — pegar nível mais próximo
            var targetHeight = (q === '1080p') ? 1080 : 720;
            var bestIdx = -1;
            var bestDiff = Infinity;
            for (var li = 0; li < hls.levels.length; li++) {
              var diff = Math.abs(hls.levels[li].height - targetHeight);
              if (diff < bestDiff) {
                bestDiff = diff;
                bestIdx = li;
              }
            }
            if (bestIdx >= 0) hls.currentLevel = bestIdx;
          } else if (window._udhyogIsNativeHLS && window._udhyogVideo) {
            var base = window._udhyogStreamBase;
            var vid = window._udhyogVideo;
            vid.src = base + '/' + q + '/stream.m3u8';
            vid.play().catch(function () {});
          }
          window._udhyogCurrentQuality = q;
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
    fsBtn.style.cssText = 'position:absolute;top:10px;right:10px;z-index:1000;background:rgba(0,0,0,0.7);color:#fff;border:none;border-radius:4px;padding:6px 10px;font-size:16px;cursor:pointer;pointer-events:auto;opacity:' + (isMobile ? '0' : '1') + ';transition:opacity 0.3s;';
    fsBtn._simulated = false;
    fsBtn._originalPlayerStyle = '';
    fsBtn._originalVideoStyle = '';

    // Styles fixos pra não perder ao restaurar
    var hlsOverlayStyle = 'position:absolute;top:0;left:0;width:100%;height:100%;border:none;z-index:999;background:#000;object-fit:contain;pointer-events:none;';

    fsBtn.onclick = function () {
      if (fsBtn._simulated) {
        player.style.cssText = fsBtn._originalPlayerStyle;
        video.style.cssText = hlsOverlayStyle;
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
        player.style.cssText = 'position:fixed!important;top:0!important;left:0!important;width:100vw!important;height:100vh!important;z-index:999999!important;background:#000!important;';
        video.style.cssText = 'width:100%;height:100%;object-fit:contain;position:absolute;top:0;left:0;z-index:999;';
        document.body.style.overflow = 'hidden';
        fsBtn._simulated = true;
        fsBtn.textContent = '\u2715';
      }
    };

    function updateIcon() {
      if (!fsBtn._simulated) {
        var isFs = document.fullscreenElement || document.webkitFullscreenElement;
        fsBtn.textContent = isFs ? '\u2715' : '\u26F6';
        // Ao sair do fullscreen nativo, garantir que o video mantém o style correto
        if (!isFs) {
          video.style.cssText = hlsOverlayStyle;
          // Forçar retomada se travou durante transição
          if (video.paused && window._udhyogHls) {
            video.play().catch(function () {});
          }
        }
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
