/**
 * build-data.js
 * Processa os CSVs do CS-Painel e gera dados.js para o dashboard.
 * Usa streaming para lidar com arquivos grandes (Consulta1 ~215MB, DOMAINS_Pedidos ~73MB).
 *
 * Uso: node build-data.js
 */

const fs = require('fs');
const path = require('path');
const readline = require('readline');
const DIR = __dirname;

// ---------------------------------------------------------------------------
// CSV parser (streaming, lida com campos entre aspas)
// ---------------------------------------------------------------------------

function parseCSVLine(line) {
    const fields = [];
    let current = '';
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (inQuotes) {
            if (ch === '"' && line[i + 1] === '"') { current += '"'; i++; }
            else if (ch === '"') { inQuotes = false; }
            else { current += ch; }
        } else {
            if (ch === '"') { inQuotes = true; }
            else if (ch === ',') { fields.push(current.trim()); current = ''; }
            else { current += ch; }
        }
    }
    fields.push(current.trim());
    return fields;
}

async function readCSV(filename, onRow, limit) {
    const filePath = path.join(DIR, filename);
    if (!fs.existsSync(filePath)) { console.log('  SKIP: ' + filename + ' not found'); return; }
    const stat = fs.statSync(filePath);
    if (stat.size === 0) { console.log('  SKIP: ' + filename + ' is empty'); return; }
    const stream = fs.createReadStream(filePath, { encoding: 'utf-8' });
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
    let headers = null;
    let count = 0;
    for await (const line of rl) {
        if (!line.trim()) continue;
        const fields = parseCSVLine(line);
        if (!headers) { headers = fields; continue; }
        const row = {};
        headers.forEach((h, i) => { row[h] = fields[i] || ''; });
        onRow(row);
        count++;
        if (limit && count >= limit) break;
    }
    console.log('  ' + filename + ': ' + count + ' rows');
    return count;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sim(val) {
    return (val || '').trim().toLowerCase() === 'sim';
}

function num(val) {
    if (!val || val === '') return 0;
    const n = parseFloat(String(val).replace(/\s/g, ''));
    return isNaN(n) ? 0 : n;
}

/** Extrai "YYYY-MM" de datas no formato "YYYY-MM-DD HH:MM:SS,mmm" ou "YYYY-MM-DD" */
function extractYM(dateStr) {
    if (!dateStr) return null;
    const m = dateStr.match(/^(\d{4}-\d{2})/);
    return m ? m[1] : null;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
    const t0 = Date.now();
    console.log('=== build-data.js ===');
    console.log('Diretorio: ' + DIR);
    console.log('');

    // -----------------------------------------------------------------------
    // 1. Domains.csv  (master list)
    // -----------------------------------------------------------------------
    console.log('[1/6] Domains.csv');
    const domainsMap = {}; // id -> domain object
    await readCSV('Domains.csv', (row) => {
        const id = row.id;
        if (!id) return;
        domainsMap[id] = {
            id: id,
            nome: row.name || '',
            criadoEm: row.created_at || '',
            canal: row.Canal || '',
            anjo: row.Anjo || '',
            modulo: row.modulos || '',
            moduloVendas: row['Módulo de Vendas Ativo'] || '',
            integracaoAtiva: false,
            vestipagoAtivo: false,
            freteAtivo: false,
            assistenteAtivo: false,
            totalPedidos: 0,
            gmvTotal: 0,
            pedidosVestipago: 0,
            pedidosFrete: 0,
            mediaGMV: 0,
            planoCanal: '',
            m: {}  // monthly data keyed by "YYYY-MM"
        };
    });
    const domainCount = Object.keys(domainsMap).length;
    console.log('  Domains carregados: ' + domainCount);
    console.log('');

    // -----------------------------------------------------------------------
    // 2. DOMAINS_Pedidos_Por_Mes.csv  (monthly metrics per domain)
    // -----------------------------------------------------------------------
    console.log('[2/6] DOMAINS_Pedidos_Por_Mes.csv');
    const mensalAgg = {}; // "YYYY-MM" -> aggregated monthly totals
    await readCSV('DOMAINS_Pedidos_Por_Mes.csv', (row) => {
        const ym = extractYM(row.Date);
        if (!ym) return;
        const id = row.id;

        const pedidos   = num(row['Qntd Pedidos Mensal']);
        const gmv       = num(row['GMV Mensal']);
        const mais25    = sim(row['+25 No Mês']);
        const mais80    = sim(row['+80 No Mês']);
        const integAtiva = sim(row['Integração Ativa']);
        const vestiAtivo = sim(row['Vestipago Ativo']);
        const freteAtivo = sim(row['Frete Ativo']);
        const assistAtivo = sim(row['Assistente Ativo']);
        const temPedidos = sim(row['Tem Pedidos']);
        const perda25    = sim(row['Perda +25']);
        const perda80    = sim(row['Perda +80']);
        const perdaInteg = sim(row['Perda Integração']);
        const perdaVesti = sim(row['Perda Vestipago']);
        const perdaFrete = sim(row['Perda Frete']);

        // Per-domain monthly record
        if (domainsMap[id]) {
            domainsMap[id].m[ym] = [
                pedidos, gmv,
                mais25 ? 1 : 0, mais80 ? 1 : 0,
                integAtiva ? 1 : 0, vestiAtivo ? 1 : 0,
                freteAtivo ? 1 : 0, assistAtivo ? 1 : 0,
                perda25 ? 1 : 0, perda80 ? 1 : 0,
                perdaInteg ? 1 : 0, perdaVesti ? 1 : 0,
                perdaFrete ? 1 : 0
            ];
            // Update latest feature flags on the domain
            if (integAtiva) domainsMap[id].integracaoAtiva = true;
            if (vestiAtivo) domainsMap[id].vestipagoAtivo = true;
            if (freteAtivo) domainsMap[id].freteAtivo = true;
            if (assistAtivo) domainsMap[id].assistenteAtivo = true;
        }

        // Global monthly aggregation
        if (!mensalAgg[ym]) {
            mensalAgg[ym] = {
                totalDomains: 0, comPedidos: 0,
                mais25: 0, mais80: 0,
                vestipago: 0, integracao: 0, frete: 0, assistente: 0,
                gmv: 0,
                perda25: 0, perda80: 0, perdaInteg: 0, perdaVesti: 0, perdaFrete: 0
            };
        }
        const ma = mensalAgg[ym];
        ma.totalDomains++;
        if (temPedidos) ma.comPedidos++;
        if (mais25) ma.mais25++;
        if (mais80) ma.mais80++;
        if (vestiAtivo) ma.vestipago++;
        if (integAtiva) ma.integracao++;
        if (freteAtivo) ma.frete++;
        if (assistAtivo) ma.assistente++;
        ma.gmv += gmv;
        if (perda25) ma.perda25++;
        if (perda80) ma.perda80++;
        if (perdaInteg) ma.perdaInteg++;
        if (perdaVesti) ma.perdaVesti++;
        if (perdaFrete) ma.perdaFrete++;
    });
    console.log('  Meses encontrados: ' + Object.keys(mensalAgg).length);
    console.log('');

    // -----------------------------------------------------------------------
    // 3. Consulta1.csv  (order-level data, ~215MB)
    // -----------------------------------------------------------------------
    console.log('[3/6] Consulta1.csv');
    await readCSV('Consulta1.csv', (row) => {
        const domId = row.domainId;
        if (!domId) return;
        const total = num(row.summary_total);
        const vesti = sim(row['Vestipago Ativo']);
        const frete = sim(row['Frete BRHUB']);

        if (domainsMap[domId]) {
            domainsMap[domId].totalPedidos++;
            domainsMap[domId].gmvTotal += total;
            if (vesti) domainsMap[domId].pedidosVestipago++;
            if (frete) domainsMap[domId].pedidosFrete++;
        }
    });
    console.log('');

    // -----------------------------------------------------------------------
    // 4. Customers.csv
    // -----------------------------------------------------------------------
    console.log('[4/6] Customers.csv');
    const customersMap = {}; // id -> { name, custom_variables_value, token_subconta }
    await readCSV('Customers.csv', (row) => {
        if (row.id) {
            customersMap[row.id] = {
                name: row.name || '',
                customVar: row.custom_variables_value || '',
                token: row.token_subconta || ''
            };
        }
    });
    console.log('');

    // -----------------------------------------------------------------------
    // 5. Mudança de Plano.csv
    // -----------------------------------------------------------------------
    console.log('[5/6] Mudança de Plano.csv');
    await readCSV('Mudança de Plano.csv', (row) => {
        const id = row.id;
        if (!id || !domainsMap[id]) return;
        domainsMap[id].mediaGMV = num(row['Média GMV Mensal']);
        domainsMap[id].planoCanal = row.Canal || '';
    });
    console.log('');

    // -----------------------------------------------------------------------
    // 6. Calendario.csv
    // -----------------------------------------------------------------------
    console.log('[6/6] Calendario.csv');
    const mesesSet = new Set();
    await readCSV('Calendario.csv', (row) => {
        if (row.AnoMes) mesesSet.add(row.AnoMes);
    });
    console.log('');

    // -----------------------------------------------------------------------
    // Build output structures
    // -----------------------------------------------------------------------
    console.log('Montando dados de saida...');

    // domains array
    const domains = Object.values(domainsMap).map(d => ({
        id: d.id,
        nome: d.nome,
        criadoEm: d.criadoEm,
        canal: d.canal,
        anjo: d.anjo,
        modulo: d.modulo,
        moduloVendas: d.moduloVendas,
        integracaoAtiva: d.integracaoAtiva,
        vestipagoAtivo: d.vestipagoAtivo,
        freteAtivo: d.freteAtivo,
        assistenteAtivo: d.assistenteAtivo,
        totalPedidos: d.totalPedidos,
        gmvTotal: Math.round(d.gmvTotal * 100) / 100,
        pedidosVestipago: d.pedidosVestipago,
        pedidosFrete: d.pedidosFrete,
        mediaGMV: Math.round(d.mediaGMV * 100) / 100,
        planoCanal: d.planoCanal,
        m: d.m
    }));

    // mensal array (sorted by month)
    const mesesKeys = Object.keys(mensalAgg).sort();
    const mensal = mesesKeys.map(mes => {
        const a = mensalAgg[mes];
        return {
            mes,
            totalDomains: a.totalDomains,
            comPedidos: a.comPedidos,
            mais25: a.mais25,
            mais80: a.mais80,
            vestipago: a.vestipago,
            integracao: a.integracao,
            frete: a.frete,
            assistente: a.assistente,
            gmv: Math.round(a.gmv * 100) / 100,
            perda25: a.perda25,
            perda80: a.perda80,
            perdaInteg: a.perdaInteg,
            perdaVesti: a.perdaVesti,
            perdaFrete: a.perdaFrete
        };
    });

    // meses list from Calendario or from mensalAgg
    const meses = mesesSet.size > 0
        ? Array.from(mesesSet).sort()
        : mesesKeys;

    const dados = {
        domains,
        mensal,
        meses,
        totalDomains: domainCount,
        geradoEm: new Date().toISOString()
    };

    // -----------------------------------------------------------------------
    // Write dados.js
    // -----------------------------------------------------------------------
    const outPath = path.join(DIR, 'dados.js');
    const content = 'const DADOS = ' + JSON.stringify(dados) + ';\n';
    fs.writeFileSync(outPath, content, 'utf-8');

    const sizeMB = (Buffer.byteLength(content, 'utf-8') / (1024 * 1024)).toFixed(2);
    console.log('');
    console.log('=== Resumo ===');
    console.log('  Domains:       ' + domains.length);
    console.log('  Meses:         ' + meses.length);
    console.log('  Mensal records: ' + mensal.length);
    console.log('  Customers:     ' + Object.keys(customersMap).length);
    console.log('  Arquivo:       ' + outPath);
    console.log('  Tamanho:       ' + sizeMB + ' MB');
    console.log('  Tempo:         ' + ((Date.now() - t0) / 1000).toFixed(1) + 's');
    console.log('  Gerado em:     ' + dados.geradoEm);
    console.log('');
    console.log('Pronto!');
}

main().catch(err => {
    console.error('ERRO:', err);
    process.exit(1);
});
