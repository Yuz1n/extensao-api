/**
 * Modulo PixGG — busca donates da conta PixGG (udhyog) pra validar pagamentos
 * de cobranca dos streamers (desbloqueio automatico).
 *
 * AUTH: usa um access_token (JWT) guardado na env PIXGG_ACCESS_TOKEN.
 *   - Dura ~30 dias (720h), mas EXPIRA. Renovacao MANUAL:
 *     pegar token novo no DevTools do PixGG (logado) -> atualizar env -> redeploy.
 *   - getTokenDaysLeft() avisa quando esta perto de expirar (evita desbloqueio
 *     parar em silencio e streamers ficarem presos).
 *
 * WAF: o PixGG roda atras de AWS API Gateway e bloqueia requests sem cara de
 *   browser (barrou o Postman por User-Agent). Por isso mandamos User-Agent +
 *   Origin + Referer iguais aos do navegador.
 *
 * SEGURANCA: o token da acesso de LEITURA aos donates (nomes, valores, mensagens)
 *   da conta. E secret + dado pessoal (LGPD). Server-side APENAS — nunca no
 *   front/overlay/exe.
 */

const PIXGG_BASE = 'https://app.pixgg.com';

// Headers que fazem a request parecer vir do navegador (necessario pro WAF)
function _browserHeaders(token) {
  return {
    'Authorization': `Bearer ${token}`,
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8',
    'Origin': 'https://pixgg.com',
    'Referer': 'https://pixgg.com/',
  };
}

// Decodifica o exp do JWT (sem validar assinatura — so pra saber quando expira)
function getTokenExpiry(token) {
  try {
    const payload = JSON.parse(Buffer.from(token.split('.')[1], 'base64').toString('utf8'));
    if (!payload.exp) return null;
    return new Date(payload.exp * 1000);
  } catch (e) {
    return null;
  }
}

// Quantos dias faltam pro token expirar (null se nao decodificar)
function getTokenDaysLeft(token) {
  const exp = getTokenExpiry(token);
  if (!exp) return null;
  return Math.round((exp.getTime() - Date.now()) / (1000 * 60 * 60 * 24));
}

/**
 * Busca donates da conta. Filtra por donatorNickName se passado.
 * Retorna { ok, status, donations, error }.
 *   - ok=false + status=401 -> token expirou (renovar env)
 *   - ok=false + status=403 -> WAF bloqueou o servidor
 */
async function getDonations({ donatorNickName = '', page = 1, pageSize = 10 } = {}) {
  const token = process.env.PIXGG_ACCESS_TOKEN || '';
  if (!token) {
    return { ok: false, status: 0, donations: [], error: 'PIXGG_ACCESS_TOKEN nao configurado' };
  }

  const url = `${PIXGG_BASE}/Reports/Donations?page=${page}&pageSize=${pageSize}&donatorNickName=${encodeURIComponent(donatorNickName)}`;

  let resp;
  try {
    resp = await fetch(url, { method: 'GET', headers: _browserHeaders(token) });
  } catch (e) {
    return { ok: false, status: 0, donations: [], error: `Falha de rede: ${e.message}` };
  }

  if (resp.status === 401) {
    return { ok: false, status: 401, donations: [], error: 'Token expirado/invalido — renovar PIXGG_ACCESS_TOKEN' };
  }
  if (resp.status === 403) {
    return { ok: false, status: 403, donations: [], error: 'Bloqueado pelo WAF — servidor barrado pelo PixGG' };
  }
  if (!resp.ok) {
    return { ok: false, status: resp.status, donations: [], error: `HTTP ${resp.status}` };
  }

  let data;
  try {
    data = await resp.json();
  } catch (e) {
    return { ok: false, status: resp.status, donations: [], error: 'Resposta nao-JSON (pode ser pagina de bloqueio)' };
  }

  return { ok: true, status: resp.status, donations: Array.isArray(data) ? data : [], error: null };
}

module.exports = { getDonations, getTokenExpiry, getTokenDaysLeft };
