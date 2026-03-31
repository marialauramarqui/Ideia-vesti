/**
 * Busca coluna "Integração" de Cadastros Empresas e atualiza dados.js.
 */
const https = require('https');
const fs = require('fs');
const path = require('path');
const qs = require('querystring');
const DIR = __dirname;

function loadEnv() {
    const f = fs.readFileSync(path.join(DIR, '.env'), 'utf-8');
    const E = {};
    f.split('\n').forEach(l => { const m = l.match(/^([^#=]+)=(.*)$/); if (m) E[m[1].trim()] = m[2].trim(); });
    return E;
}

function hr(o, b) {
    return new Promise((r, j) => {
        const q = https.request(o, res => {
            const c = [];
            res.on('data', d => c.push(d));
            res.on('end', () => r({ s: res.statusCode, b: Buffer.concat(c).toString() }));
        });
        q.on('error', j);
        if (b) q.write(b);
        q.end();
    });
}

async function main() {
    console.log('=== Patch Integração ===\n');

    const ENV = loadEnv();
    const body = qs.stringify({
        client_id: '14d82eec-204b-4c2f-b7e8-296a70dab67e',
        grant_type: 'refresh_token',
        refresh_token: ENV.FABRIC_REFRESH_TOKEN,
        scope: 'https://analysis.windows.net/powerbi/api/.default',
    });
    const tr = await hr({
        hostname: 'login.microsoftonline.com',
        path: '/' + ENV.FABRIC_TENANT_ID + '/oauth2/v2.0/token',
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(body) },
    }, body);
    const td = JSON.parse(tr.b);
    if (td.refresh_token) {
        let env = fs.readFileSync(path.join(DIR, '.env'), 'utf-8');
        env = env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m, 'FABRIC_REFRESH_TOKEN=' + td.refresh_token);
        fs.writeFileSync(path.join(DIR, '.env'), env, 'utf-8');
    }
    const token = td.access_token;
    if (!token) { console.error('No token'); process.exit(1); }

    const WS = 'aced753a-0f0e-4bcf-9264-72f6496cf2cf';
    const DS = 'e6c74524-e355-4447-9eb4-baae76b84dc4';

    console.log('Buscando Integração de Cadastros Empresas...');
    const dax = "EVALUATE FILTER(SELECTCOLUMNS('Cadastros Empresas', \"id\", 'Cadastros Empresas'[Id Empresa], \"integ\", 'Cadastros Empresas'[Integração]), NOT ISBLANK([integ]))";
    const qBody = JSON.stringify({ queries: [{ query: dax }], serializerSettings: { includeNulls: true } });
    const qRes = await hr({
        hostname: 'api.powerbi.com',
        path: '/v1.0/myorg/groups/' + WS + '/datasets/' + DS + '/executeQueries',
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(qBody) },
    }, qBody);
    const qData = JSON.parse(qRes.b);
    if (!qData.results) { console.error('Query failed:', JSON.stringify(qData).substring(0, 300)); process.exit(1); }
    const rows = qData.results[0].tables[0].rows;
    console.log('Empresas com Integração:', rows.length);

    // Build map by company ID
    const integMap = {};
    rows.forEach(r => {
        const id = r['[id]'];
        const integ = r['[integ]'];
        if (id && integ) integMap[id] = integ;
    });

    // Distinct values
    const vals = {};
    rows.forEach(r => { vals[r['[integ]']] = (vals[r['[integ]']] || 0) + 1; });
    console.log('Valores:', JSON.stringify(vals));

    // Load dados.js
    console.log('\nCarregando dados.js...');
    const content = fs.readFileSync(path.join(DIR, 'dados.js'), 'utf8');
    const fn = new Function(content + '; return DADOS;');
    const DADOS = fn();

    let matched = 0;
    for (const e of DADOS.empresas) {
        if (integMap[e.id]) {
            e.integracao = integMap[e.id];
            matched++;
        }
    }
    console.log('Matched:', matched + '/' + DADOS.empresas.length);

    const output = 'const DADOS = ' + JSON.stringify(DADOS);
    fs.writeFileSync(path.join(DIR, 'dados.js'), output, 'utf-8');
    console.log('dados.js salvo');
}
main().catch(e => { console.error('ERRO:', e.message); process.exit(1); });
