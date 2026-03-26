// Diagnose: why some companies have no historical orders
const https = require('https');
const qs = require('querystring');
const { Connection, Request } = require('tedious');

const RT = process.env.FABRIC_REFRESH_TOKEN || '';
const TID = process.env.FABRIC_TENANT_ID || '';
const WORKSPACE_ID = 'aced753a-0f0e-4bcf-9264-72f6496cf2cf';
const DATASET_ID = 'e6c74524-e355-4447-9eb4-baae76b84dc4';
const SQL_SERVER = '7sowj2vsfd6efgf3phzgjfmvaq-nrdsskmspnteherwztit766zc4.datawarehouse.fabric.microsoft.com';

function req(opts, body) {
    return new Promise((res, rej) => {
        const r = https.request(opts, resp => { const c = []; resp.on('data', d => c.push(d)); resp.on('end', () => res({ status: resp.statusCode, body: Buffer.concat(c).toString() })); });
        r.on('error', rej); if (body) r.write(body); r.end();
    });
}

function runSQL(token, query, label) {
    return new Promise((resolve, reject) => {
        console.log('  ' + label + '...');
        const conn = new Connection({
            server: SQL_SERVER,
            authentication: { type: 'azure-active-directory-access-token', options: { token } },
            options: { database: 'VestiHouse', encrypt: true, port: 1433, requestTimeout: 120000 },
        });
        const rows = [];
        conn.on('connect', err => {
            if (err) { reject(err); return; }
            const request = new Request(query, err => { if (err) reject(err); conn.close(); });
            request.on('row', columns => { const row = {}; columns.forEach(col => { row[col.metadata.colName] = col.value; }); rows.push(row); });
            request.on('requestCompleted', () => { console.log('  ' + label + ': ' + rows.length + ' rows'); resolve(rows); });
            conn.execSql(request);
        });
        conn.connect();
    });
}

async function main() {
    // Tokens
    const pb1 = qs.stringify({ client_id: '1950a258-227b-4e31-a9cf-717495945fc2', grant_type: 'refresh_token', refresh_token: RT, scope: 'https://analysis.windows.net/powerbi/api/.default offline_access' });
    const tr1 = await req({ hostname: 'login.microsoftonline.com', path: '/' + TID + '/oauth2/v2.0/token', method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(pb1) } }, pb1);
    const pbiToken = JSON.parse(tr1.body).access_token;

    const pb2 = qs.stringify({ client_id: '1950a258-227b-4e31-a9cf-717495945fc2', grant_type: 'refresh_token', refresh_token: RT, scope: 'https://database.windows.net/.default offline_access' });
    const tr2 = await req({ hostname: 'login.microsoftonline.com', path: '/' + TID + '/oauth2/v2.0/token', method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(pb2) } }, pb2);
    const sqlToken = JSON.parse(tr2.body).access_token;
    console.log('Authenticated.\n');

    // 1. Get distinct company_ids from ODBC_Quotes
    const quotesCompanies = await runSQL(sqlToken,
        "SELECT company_id, COUNT(*) as cnt, MIN(created_at) as min_dt, MAX(created_at) as max_dt FROM dbo.ODBC_Quotes GROUP BY company_id",
        'ODBC_Quotes company_ids');

    // 2. Get distinct company_ids from Anterior2023
    const anteriorCompanies = await runSQL(sqlToken,
        "SELECT company_id, COUNT(*) as cnt, MIN(created_at) as min_dt, MAX(created_at) as max_dt FROM dbo.OBDC_Quotes_Anterior2023 GROUP BY company_id",
        'Anterior2023 company_ids');

    // 3. Get empresa IDs from Cadastros
    const bodyStr = JSON.stringify({ queries: [{ query: "EVALUATE SELECTCOLUMNS('Cadastros Empresas', \"id\", 'Cadastros Empresas'[Id Empresa], \"nome\", 'Cadastros Empresas'[Nome Fantasia], \"dom\", 'Cadastros Empresas'[Id Dominio])" }], serializerSettings: { includeNulls: true } });
    const r = await req({
        hostname: 'api.powerbi.com', path: '/v1.0/myorg/groups/' + WORKSPACE_ID + '/datasets/' + DATASET_ID + '/executeQueries', method: 'POST',
        headers: { 'Authorization': 'Bearer ' + pbiToken, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(bodyStr) },
    }, bodyStr);
    const data = JSON.parse(r.body);
    const cadastros = (data.results[0].tables[0].rows || []).map(row => {
        const obj = {}; for (const [k, v] of Object.entries(row)) { const m = k.match(/\[(.+)\]$/); obj[m ? m[1] : k] = v; } return obj;
    });
    console.log('Cadastros: ' + cadastros.length);

    const cadastroIds = new Set(cadastros.map(c => c.id));
    const cadastroByDom = {};
    cadastros.forEach(c => { if (c.dom) cadastroByDom[String(c.dom)] = c.id; });

    // 4. Analysis
    const quotesIds = new Set(quotesCompanies.map(c => c.company_id));
    const anteriorIds = new Set(anteriorCompanies.map(c => c.company_id));

    const quotesMatched = quotesCompanies.filter(c => cadastroIds.has(c.company_id));
    const quotesUnmatched = quotesCompanies.filter(c => !cadastroIds.has(c.company_id));
    const anteriorMatched = anteriorCompanies.filter(c => cadastroIds.has(c.company_id));
    const anteriorUnmatched = anteriorCompanies.filter(c => !cadastroIds.has(c.company_id));

    console.log('\n=== ANALYSIS ===');
    console.log('ODBC_Quotes: ' + quotesCompanies.length + ' distinct companies');
    console.log('  Matched to Cadastros: ' + quotesMatched.length + ' (' + quotesMatched.reduce((s, c) => s + c.cnt, 0) + ' pedidos)');
    console.log('  NOT matched: ' + quotesUnmatched.length + ' (' + quotesUnmatched.reduce((s, c) => s + c.cnt, 0) + ' pedidos)');

    console.log('Anterior2023: ' + anteriorCompanies.length + ' distinct companies');
    console.log('  Matched to Cadastros: ' + anteriorMatched.length + ' (' + anteriorMatched.reduce((s, c) => s + c.cnt, 0) + ' pedidos)');
    console.log('  NOT matched: ' + anteriorUnmatched.length + ' (' + anteriorUnmatched.reduce((s, c) => s + c.cnt, 0) + ' pedidos)');

    // 5. Check if unmatched use domain_id instead
    console.log('\n=== Check domain_id mapping ===');
    const unmatchedSample = quotesUnmatched.slice(0, 5);
    for (const u of unmatchedSample) {
        // Check if company_id looks like a domain_id (numeric)
        const isDomLike = /^\d+$/.test(u.company_id);
        const domMatch = cadastroByDom[u.company_id];
        console.log('  ' + u.company_id + ' (cnt=' + u.cnt + ') isDomainLike=' + isDomLike + ' domMatch=' + (domMatch || 'NO'));
    }

    // 6. Check ODBC_Quotes for domain_id field
    const hasDomain = await runSQL(sqlToken,
        "SELECT TOP 5 company_id, domain_id FROM dbo.ODBC_Quotes WHERE domain_id IS NOT NULL",
        'Check domain_id');
    if (hasDomain.length > 0) {
        console.log('\n=== ODBC_Quotes has domain_id! ===');
        hasDomain.forEach(r => console.log('  company_id=' + r.company_id + ' domain_id=' + r.domain_id));

        // Check how many can be matched via domain_id
        const domMatchCount = await runSQL(sqlToken,
            "SELECT COUNT(DISTINCT q.company_id) as cnt FROM dbo.ODBC_Quotes q WHERE q.company_id NOT IN (SELECT [Id Empresa] FROM dbo.SucessodoCliente_Cadastro_Empresa WHERE [Id Empresa] IS NOT NULL) AND CAST(q.domain_id AS VARCHAR) IN (SELECT CAST([Id Dominio] AS VARCHAR) FROM dbo.SucessodoCliente_Cadastro_Empresa WHERE [Id Dominio] IS NOT NULL)",
            'Domain fallback count');
        console.log('Companies matchable via domain_id: ' + (domMatchCount[0] ? domMatchCount[0].cnt : '?'));
    }
}
main().catch(e => console.error('FATAL:', e));
