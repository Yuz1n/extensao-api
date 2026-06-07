/**
 * Cotação USD→BRL pra exibir os valores (que são em dólar) em real no dashboard.
 *
 * Fonte primária: AwesomeAPI (brasileira, gratuita, sem chave).
 * Fallback: open.er-api.com.
 * Cache de 1h em memória — não faz sentido cotar a cada request, e evita
 * depender da disponibilidade da API de câmbio em todo carregamento.
 */

let _cached = { rate: null, at: 0 };
const CACHE_MS = 60 * 60 * 1000; // 1 hora

// Arredonda a cotação pra 2 casas olhando SÓ a 3ª casa decimal:
//   3ª casa > 5  → sobe   (5.1671 → 5.17)
//   3ª casa <= 5 → trunca (5.1651 → 5.16)
// Usa string pra ser imune a erro de ponto flutuante.
function roundRate(rate) {
  if (!rate || rate <= 0) return rate;
  const s = Number(rate).toFixed(4);          // "5.1671"
  const dot = s.indexOf('.');
  const third = parseInt(s[dot + 3], 10);     // 3ª casa decimal
  const twoDec = parseFloat(s.slice(0, dot + 3)); // 5.16
  return third > 5 ? Math.round((twoDec + 0.01) * 100) / 100 : twoDec;
}

async function getUsdBrlRate() {
  const now = Date.now();
  if (_cached.rate && (now - _cached.at) < CACHE_MS) return _cached.rate;

  // 1ª opção: AwesomeAPI (USD-BRL, campo bid)
  try {
    const r = await fetch('https://economia.awesomeapi.com.br/last/USD-BRL');
    if (r.ok) {
      const d = await r.json();
      const bid = parseFloat(d && d.USDBRL && d.USDBRL.bid);
      if (bid > 0) { const v = roundRate(bid); _cached = { rate: v, at: now }; return v; }
    }
  } catch (e) { /* tenta fallback */ }

  // Fallback: open.er-api.com
  try {
    const r = await fetch('https://open.er-api.com/v6/latest/USD');
    if (r.ok) {
      const d = await r.json();
      const brl = d && d.rates && d.rates.BRL;
      if (brl > 0) { const v = roundRate(brl); _cached = { rate: v, at: now }; return v; }
    }
  } catch (e) { /* sem cotação */ }

  // Tudo falhou — devolve o último cache (mesmo expirado) ou null
  return _cached.rate;
}

module.exports = { getUsdBrlRate, roundRate };
