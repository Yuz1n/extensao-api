/**
 * Build script — Injeta variáveis do .env.overlay e ofusca o overlay
 * Uso: node build-overlay.js
 */

const fs = require('fs');
const path = require('path');
const JavaScriptObfuscator = require('javascript-obfuscator');

// 1. Ler .env.overlay
const envVars = {};
fs.readFileSync(path.join(__dirname, '.env.overlay'), 'utf8').split('\n').forEach(line => {
  line = line.trim();
  if (!line || line.startsWith('#')) return;
  const [key, ...rest] = line.split('=');
  envVars[key.trim()] = rest.join('=').trim();
});
console.log('[BUILD] Variáveis:', Object.keys(envVars).join(', '));

// 2. Ler overlay.src.js e substituir placeholders
let code = fs.readFileSync(path.join(__dirname, 'overlay.src.js'), 'utf8');
for (const [key, value] of Object.entries(envVars)) {
  code = code.replace(new RegExp(`__${key}__`, 'g'), value);
}

// 3. Ofuscar
const result = JavaScriptObfuscator.obfuscate(code, {
  compact: true,
  controlFlowFlattening: true,
  controlFlowFlatteningThreshold: 0.5,
  deadCodeInjection: false,
  stringArray: true,
  stringArrayEncoding: ['base64'],
  stringArrayThreshold: 0.8,
  splitStrings: true,
  splitStringsChunkLength: 8,
  renameGlobals: false,
  selfDefending: false,
  identifierNamesGenerator: 'hexadecimal',
  transformObjectKeys: true,
  unicodeEscapeSequence: false,
});

// 4. Salvar
const outPath = path.join(__dirname, 'public', 'overlay.js');
fs.writeFileSync(outPath, result.getObfuscatedCode());

const srcSize = fs.statSync(path.join(__dirname, 'overlay.src.js')).size;
const outSize = fs.statSync(outPath).size;
console.log(`[BUILD] ${(srcSize/1024).toFixed(1)}KB → ${(outSize/1024).toFixed(1)}KB`);
console.log('[BUILD] Concluído!');
