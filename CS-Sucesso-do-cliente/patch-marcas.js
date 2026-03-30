/**
 * Patch dados.js com dados completos de Marcas e Planos do Excel.
 * Mescla linhas duplicadas (plano + Oráculo) somando valores.
 * Match por: CNPJ exato -> raiz CNPJ -> nome exato -> nome parcial.
 */
const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');
const DIR = __dirname;

function normalize(s) { return s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim(); }

function extractSheetDate(name) {
    let m = name.match(/(\d{2})-?(\d{4})/); if (m) return m[2] + '-' + m[1];
    m = name.match(/(\d{2})-?(\d{2})$/); if (m) return '20' + m[2] + '-' + m[1]; return '0000-00';
}

// 1. Read Excel
const wb = XLSX.readFile(path.join(DIR, 'Marcas e Planos.xlsx'));
const vestiSheets = wb.SheetNames.filter(s => s.toLowerCase().includes('vesti') && !s.toLowerCase().includes('starter'));
const starterSheets = wb.SheetNames.filter(s => s.toLowerCase().includes('starter'));
vestiSheets.sort((a, b) => extractSheetDate(b).localeCompare(extractSheetDate(a)));
starterSheets.sort((a, b) => extractSheetDate(b).localeCompare(extractSheetDate(a)));

// Starter first (lower priority), then Vesti (overwrites)
const sheetsToRead = [];
if (starterSheets.length > 0) sheetsToRead.push(starterSheets[0]);
if (vestiSheets.length > 0) sheetsToRead.push(vestiSheets[0]);

// Collect ALL lines per CNPJ
const allLinesByCnpj = {};
for (const sheetName of sheetsToRead) {
    const ws = wb.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });
    for (const row of rows) {
        const cnpj = String(row['CPFCNPJ'] || row['CPF e CNPJ'] || '').replace(/[.\-\/\s]/g, '');
        if (!cnpj || cnpj.length < 11) continue;
        if (!allLinesByCnpj[cnpj]) allLinesByCnpj[cnpj] = [];
        allLinesByCnpj[cnpj].push({
            marca: row['MARCA'] || '',
            plano: (row['PLANO'] || '').trim(),
            setup: parseFloat(row['SETUP']) || 0,
            mensalidade: parseFloat(row['MENSALIDADE']) || 0,
            integracao: parseFloat(row['INTEGRAÇÃO'] || row['INTEGRACAO']) || 0,
            assistente: parseFloat(row['ASSISTENTE']) || 0,
            filial: parseFloat(row['FILIAL']) || 0,
            descontos: parseFloat(row['DESCONTOS']) || 0,
            totalCobrado: parseFloat(row['TOTAL COBRADO'] || row['TOTAL_COBRADO']) || 0,
            observacoes: row['OBSERVAÇÕES'] || row['OBSERVACOES'] || '',
            subconta: row['Subconta'] || '',
        });
    }
    console.log('Sheet "' + sheetName + '"');
}

// 2. Merge: combine all lines per CNPJ
// Main plan (non-Oráculo/Integração/Pacote) provides plano name
// All numeric fields are summed across lines
const marcasByCnpj = {};
for (const [cnpj, lines] of Object.entries(allLinesByCnpj)) {
    const isExtra = (p) => /oraculo|oráculo|integração|integracao|pacote/i.test(p);
    const main = lines.find(l => !isExtra(l.plano)) || lines[0];
    const merged = {
        marca: main.marca,
        plano: main.plano,
        setup: 0, mensalidade: 0, integracao: 0, assistente: 0,
        filial: 0, descontos: 0, totalCobrado: 0,
        observacoes: main.observacoes,
        subconta: main.subconta,
    };
    for (const line of lines) {
        merged.setup += line.setup;
        merged.mensalidade += line.mensalidade;
        merged.integracao += line.integracao;
        merged.assistente += line.assistente;
        merged.filial += line.filial;
        merged.descontos += line.descontos;
        merged.totalCobrado += line.totalCobrado;
    }
    for (const k of ['setup', 'mensalidade', 'integracao', 'assistente', 'filial', 'descontos', 'totalCobrado']) {
        merged[k] = Math.round(merged[k] * 100) / 100;
    }
    marcasByCnpj[cnpj] = merged;
}
console.log('Merged CNPJs:', Object.keys(marcasByCnpj).length);

// Build by-name map (keep highest totalCobrado per name)
const marcasByName = {};
for (const m of Object.values(marcasByCnpj)) {
    const name = normalize(m.marca);
    if (name && name.length >= 3) {
        if (!marcasByName[name] || m.totalCobrado > marcasByName[name].totalCobrado) {
            marcasByName[name] = m;
        }
    }
}

// Verify
const aero = marcasByCnpj['31144403000138'];
console.log('Aero Summer merged:', aero ? 'plano=' + aero.plano + ' mensal=' + aero.mensalidade + ' assist=' + aero.assistente + ' filial=' + aero.filial + ' total=' + aero.totalCobrado : 'NOT FOUND');
const pury = marcasByCnpj['12576594000162'];
console.log('Pury merged:', pury ? 'plano=' + pury.plano + ' mensal=' + pury.mensalidade + ' assist=' + pury.assistente + ' total=' + pury.totalCobrado : 'NOT FOUND');

// 3. Load dados.js
console.log('\nCarregando dados.js...');
const content = fs.readFileSync(path.join(DIR, 'dados.js'), 'utf8');
const fn = new Function(content + '; return DADOS;');
const DADOS = fn();

// 4. Match and patch
let byCnpj = 0, byRoot = 0, byName = 0, byPartial = 0;
for (const e of DADOS.empresas) {
    const cnpj = (e.cnpj || '').replace(/[.\-\/]/g, '');
    const nomeNorm = normalize(e.nome || '');

    // 4a. Exact CNPJ
    let marca = marcasByCnpj[cnpj];
    if (marca) { byCnpj++; }

    // 4b. CNPJ root (first 8 digits) - pick best match
    if (!marca && cnpj.length >= 8) {
        const root = cnpj.substring(0, 8);
        let best = null;
        for (const [mcnpj, mdata] of Object.entries(marcasByCnpj)) {
            if (mcnpj.substring(0, 8) === root) {
                if (!best || mdata.totalCobrado > best.totalCobrado) best = mdata;
            }
        }
        if (best) { marca = best; byRoot++; }
    }

    // 4c. Name exact
    if (!marca) { marca = marcasByName[nomeNorm]; if (marca) byName++; }

    // 4d. Name partial (min 5 chars)
    if (!marca && nomeNorm.length >= 5) {
        for (const [mName, mData] of Object.entries(marcasByName)) {
            if (mName.length >= 5 && (nomeNorm.includes(mName) || mName.includes(nomeNorm))) {
                marca = mData; byPartial++; break;
            }
        }
    }

    if (marca) {
        e.plano = marca.plano || e.plano || '';
        e.planoMensalidade = marca.mensalidade;
        e.planoIntegracao = marca.integracao;
        e.planoAssistente = marca.assistente;
        e.planoFilial = marca.filial;
        e.planoDescontos = marca.descontos;
        e.planoTotalCobrado = marca.totalCobrado;
        e.planoSetup = marca.setup;
        e.planoObservacoes = marca.observacoes;
        e.planoSubconta = marca.subconta;
    }
}

const total = byCnpj + byRoot + byName + byPartial;
console.log('Matched:', total, '(CNPJ:', byCnpj, '| Root:', byRoot, '| Name:', byName, '| Partial:', byPartial, ')');

// Verify examples
console.log('\nExemplos:');
['aero summer 1', 'aero summer 2', 'pury', 'tricomix', 'bauarte', 'diamantes', 'kelly rodrigues store', 'hemix'].forEach(n => {
    const m = DADOS.empresas.filter(e => e.nome.toLowerCase().includes(n));
    m.slice(0, 1).forEach(e => console.log('  ' + e.nome + ' | plano:' + e.plano + ' mensal:' + e.planoMensalidade + ' assist:' + e.planoAssistente + ' filial:' + e.planoFilial + ' desc:' + e.planoDescontos + ' total:' + e.planoTotalCobrado));
});

console.log('\nStats:');
console.log('Com mensalidade>0:', DADOS.empresas.filter(e => e.planoMensalidade > 0).length);
console.log('Com assistente>0:', DADOS.empresas.filter(e => e.planoAssistente > 0).length);
console.log('Com totalCobrado>0:', DADOS.empresas.filter(e => e.planoTotalCobrado > 0).length);

// 5. Save
const output = 'const DADOS = ' + JSON.stringify(DADOS);
fs.writeFileSync(path.join(DIR, 'dados.js'), output, 'utf-8');
console.log('\ndados.js salvo (' + (output.length / 1024 / 1024).toFixed(1) + ' MB)');
