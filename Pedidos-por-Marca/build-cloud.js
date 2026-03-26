/**
 * Cloud build for Pedidos por Marca - fetches from Fabric DAX API.
 * Required env vars: FABRIC_REFRESH_TOKEN, FABRIC_TENANT_ID
 */
const fs = require('fs');
const path = require('path');
const https = require('https');
const querystring = require('querystring');

const DIR = __dirname;
const WORKSPACE_ID = 'aced753a-0f0e-4bcf-9264-72f6496cf2cf';
const DATASET_ID = 'e6c74524-e355-4447-9eb4-baae76b84dc4';
const FABRIC_REFRESH_TOKEN = process.env.FABRIC_REFRESH_TOKEN || '';
const FABRIC_TENANT_ID = process.env.FABRIC_TENANT_ID || '';

function httpsRequest(options, body) {
    return new Promise((resolve, reject) => {
        const req = https.request(options, (res) => {
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => resolve({ statusCode: res.statusCode, body: Buffer.concat(chunks).toString() }));
        });
        req.on('error', reject);
        if (body) req.write(body);
        req.end();
    });
}

async function getAccessToken() {
    const postBody = querystring.stringify({
        client_id: '1950a258-227b-4e31-a9cf-717495945fc2',
        grant_type: 'refresh_token',
        refresh_token: FABRIC_REFRESH_TOKEN,
        scope: 'https://analysis.windows.net/powerbi/api/.default offline_access',
    });
    const res = await httpsRequest({
        hostname: 'login.microsoftonline.com',
        path: '/' + FABRIC_TENANT_ID + '/oauth2/v2.0/token',
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(postBody) },
    }, postBody);
    const data = JSON.parse(res.body);
    if (!data.access_token) throw new Error('Token failed: ' + (data.error_description || ''));
    if (data.refresh_token) {
        fs.writeFileSync(path.join(DIR, '..', 'CS-Sucesso-do-cliente', '.new_refresh_token'), data.refresh_token, 'utf-8');
    }
    return data.access_token;
}

async function daxQuery(token, query, label) {
    console.log('  ' + label + '...');
    const bodyStr = JSON.stringify({ queries: [{ query }], serializerSettings: { includeNulls: true } });
    const res = await httpsRequest({
        hostname: 'api.powerbi.com',
        path: '/v1.0/myorg/groups/' + WORKSPACE_ID + '/datasets/' + DATASET_ID + '/executeQueries',
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(bodyStr) },
    }, bodyStr);
    if (res.statusCode !== 200) { console.error('  ERROR ' + label + ': HTTP ' + res.statusCode); return []; }
    const data = JSON.parse(res.body);
    if (data.error) { console.error('  ERROR: ' + JSON.stringify(data.error).substring(0, 300)); return []; }
    const rows = (data.results && data.results[0] && data.results[0].tables && data.results[0].tables[0] && data.results[0].tables[0].rows) || [];
    const cleaned = rows.map(row => {
        const obj = {};
        for (const [key, val] of Object.entries(row)) {
            const m = key.match(/\[(.+)\]$/);
            obj[m ? m[1] : key] = val;
        }
        return obj;
    });
    console.log('  ' + label + ': ' + cleaned.length + ' rows');
    return cleaned;
}

async function main() {
    console.log('=== Pedidos por Marca - Cloud Build ===\n');

    if (!FABRIC_REFRESH_TOKEN || !FABRIC_TENANT_ID) throw new Error('FABRIC_REFRESH_TOKEN and FABRIC_TENANT_ID required');

    const token = await getAccessToken();
    console.log('Authenticated.\n');

    // Queries em paralelo
    const [cadastros, marcas, pedidos] = await Promise.all([
        daxQuery(token, "EVALUATE 'Cadastros Empresas'", 'Cadastros'),
        daxQuery(token, "EVALUATE 'Marcas e Planos'", 'Marcas e Planos'),
        daxQuery(token, `EVALUATE SELECTCOLUMNS('Merged Pedidos', "EmpId", 'Merged Pedidos'[ID Empresa], "Dt", 'Merged Pedidos'[Data Criacao], "V", 'Merged Pedidos'[Total], "Pg", 'Merged Pedidos'[Pago], "Cn", 'Merged Pedidos'[Cancelado], "Pn", 'Merged Pedidos'[Pendente], "Mt", 'Merged Pedidos'[docs.payment.method])`, 'Pedidos individuais'),
    ]);

    // Empresas
    const empresas = {};
    for (const r of cadastros) {
        const id = r['Id Empresa']; if (!id) continue;
        empresas[id] = { id, nome: r['Nome Fantasia'] || r['Nome do Dominio'] || '', cnpj: r['CNPJ'] || '', anjo: r['Anjo'] || '', canal: r['Canal de Vendas'] || '' };
    }

    // Marcas by CNPJ
    const marcasByCnpj = {};
    for (const r of marcas) {
        const cnpj = r['CPFCNPJ'] || '';
        if (cnpj) marcasByCnpj[cnpj] = { marca: r['MARCA'] || '', plano: r['PLANO'] || '' };
    }
    for (const e of Object.values(empresas)) {
        const cnpjNum = (e.cnpj || '').replace(/[.\-\/]/g, '');
        const m = marcasByCnpj[cnpjNum];
        e.marca = m ? m.marca : '';
        e.plano = m ? m.plano : '';
    }

    // Pedidos agrupados por empresa
    const pedidosPorEmp = {};
    for (const r of pedidos) {
        const empId = r['EmpId']; if (!empId || !empresas[empId]) continue;
        if (!pedidosPorEmp[empId]) pedidosPorEmp[empId] = [];
        const status = r['Pg'] === true || r['Pg'] === 'True' ? 'P' : r['Cn'] === true || r['Cn'] === 'True' ? 'C' : r['Pn'] === true || r['Pn'] === 'True' ? 'E' : 'O';
        const dt = (r['Dt'] || '').toString().substring(0, 10);
        const met = (r['Mt'] || '').toString().substring(0, 15);
        pedidosPorEmp[empId].push([dt, Math.round((parseFloat(r['V']) || 0) * 100) / 100, status, met]);
    }

    // Sort by date desc
    for (const id of Object.keys(pedidosPorEmp)) {
        pedidosPorEmp[id].sort((a, b) => b[0].localeCompare(a[0]));
    }

    // Empresa list
    const empList = Object.values(empresas)
        .filter(e => pedidosPorEmp[e.id] && pedidosPorEmp[e.id].length > 0)
        .map(e => ({ id: e.id, nome: e.nome, cnpj: e.cnpj, marca: e.marca, plano: e.plano, anjo: e.anjo, canal: e.canal, qtd: pedidosPorEmp[e.id].length }))
        .sort((a, b) => b.qtd - a.qtd);

    // Save dados.js (empresa list only)
    fs.writeFileSync(path.join(DIR, 'dados.js'), 'const DADOS=' + JSON.stringify({ empresas: empList, gerado: new Date().toISOString() }) + ';', 'utf-8');

    // Save chunks
    const dataDir = path.join(DIR, 'data');
    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir);
    // Clean old chunks
    for (const f of fs.readdirSync(dataDir)) { if (f.startsWith('chunk_')) fs.unlinkSync(path.join(dataDir, f)); }

    const BATCH = 50;
    const empIds = empList.map(e => e.id);
    let totalPed = 0;
    for (let i = 0; i < empIds.length; i += BATCH) {
        const batch = {};
        for (let j = i; j < Math.min(i + BATCH, empIds.length); j++) {
            const id = empIds[j];
            batch[id] = pedidosPorEmp[id] || [];
            totalPed += batch[id].length;
        }
        fs.writeFileSync(path.join(dataDir, 'chunk_' + Math.floor(i / BATCH) + '.js'), 'loadChunk(' + JSON.stringify(batch) + ');', 'utf-8');
    }

    const chunkMap = {};
    empIds.forEach((id, i) => { chunkMap[id] = Math.floor(i / BATCH); });
    fs.writeFileSync(path.join(DIR, 'chunks.js'), 'const CHUNKS=' + JSON.stringify(chunkMap) + ';', 'utf-8');

    console.log('\nEmpresas: ' + empList.length);
    console.log('Total pedidos: ' + totalPed);
    console.log('Chunks: ' + Math.ceil(empIds.length / BATCH));
    console.log('Done.');
}
main().catch(e => { console.error('FATAL:', e); process.exit(1); });
