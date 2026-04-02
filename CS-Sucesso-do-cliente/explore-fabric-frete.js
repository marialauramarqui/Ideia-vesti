/**
 * Follow-up: get meaningful freight sample data with company + monthly breakdown.
 */
const https = require('https');
const fs = require('fs');
const path = require('path');
const querystring = require('querystring');

const WORKSPACE_ID = '0f5bd202-471f-482d-bf3d-38295044d7db';
const DATASET_ID = '92a0cf18-2bfd-4b02-873f-615df3ce2d7f';

function loadEnv() {
    const envPath = path.join(__dirname, '.env');
    const env = {};
    fs.readFileSync(envPath, 'utf-8').split('\n').forEach(line => {
        const m = line.match(/^([^=]+)=(.*)$/);
        if (m) env[m[1].trim()] = m[2].trim();
    });
    return env;
}
const ENV = loadEnv();

function httpsRequest(options, body) {
    return new Promise((resolve, reject) => {
        const req = https.request(options, res => {
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => {
                const raw = Buffer.concat(chunks).toString();
                try { resolve({ status: res.statusCode, data: JSON.parse(raw) }); }
                catch { resolve({ status: res.statusCode, data: raw }); }
            });
        });
        req.on('error', reject);
        if (body) req.write(body);
        req.end();
    });
}

async function getToken() {
    const body = querystring.stringify({
        client_id: ENV.FABRIC_CLIENT_ID,
        grant_type: 'refresh_token',
        refresh_token: ENV.FABRIC_REFRESH_TOKEN,
        scope: 'https://analysis.windows.net/powerbi/api/.default offline_access',
    });
    const res = await httpsRequest({
        hostname: 'login.microsoftonline.com',
        path: `/${ENV.FABRIC_TENANT_ID}/oauth2/v2.0/token`,
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(body) },
    }, body);
    if (res.data.refresh_token && res.data.refresh_token !== ENV.FABRIC_REFRESH_TOKEN) {
        const envPath = path.join(__dirname, '.env');
        let env = fs.readFileSync(envPath, 'utf-8');
        env = env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m, 'FABRIC_REFRESH_TOKEN=' + res.data.refresh_token);
        fs.writeFileSync(envPath, env, 'utf-8');
    }
    return res.data.access_token;
}

async function dax(token, query) {
    const body = JSON.stringify({ queries: [{ query }], serializerSettings: { includeNulls: true } });
    const res = await httpsRequest({
        hostname: 'api.powerbi.com',
        path: `/v1.0/myorg/groups/${WORKSPACE_ID}/datasets/${DATASET_ID}/executeQueries`,
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
    }, body);
    if (res.status === 200 && res.data.results && res.data.results[0]) {
        return res.data.results[0].tables[0].rows || [];
    }
    console.log('Query failed:', JSON.stringify((res.data || {}).error || res.data).substring(0, 500));
    return [];
}

async function main() {
    const token = await getToken();
    console.log('Token OK\n');

    // 1. Sample of orders WITH freight > 0 and company name
    console.log('=== Sample: Orders with Valor Frete > 0 ===');
    let rows = await dax(token, `
        EVALUATE
        TOPN(30,
            FILTER('Merged', 'Merged'[Valor Frete] > 0),
            'Merged'[Valor Frete], DESC
        )
    `);
    rows.forEach((r, i) => {
        console.log(`  [${i}] Company=${r['Merged[Companies.company_name]']} | Cliente=${r['Merged[Cliente]']} | Codigo=${r['Merged[Codigo]']} | Valor=${r['Merged[Valor]']} | Frete=${r['Merged[Valor Frete]']} | Status=${r['Merged[Status Pedido]']} | Pagamento=${r['Merged[Data Pagamento]']}`);
    });

    // 2. Monthly freight by company (using Companies.company_name and Recebido as date)
    console.log('\n=== Monthly freight totals by company (top 50 rows) ===');
    rows = await dax(token, `
        EVALUATE
        TOPN(50,
            ADDCOLUMNS(
                SUMMARIZE(
                    FILTER('Merged', 'Merged'[Valor Frete] > 0),
                    'Merged'[Companies.company_name],
                    "Mes", FORMAT('Merged'[Recebido], "YYYY-MM")
                ),
                "TotalFrete", CALCULATE(SUM('Merged'[Valor Frete])),
                "QtdPedidos", CALCULATE(COUNTROWS('Merged'))
            ),
            [TotalFrete], DESC
        )
    `);
    rows.forEach((r, i) => console.log(`  [${i}] ${JSON.stringify(r)}`));

    // 3. Total freight per company (all time)
    console.log('\n=== Total freight per company (all time, top 30) ===');
    rows = await dax(token, `
        EVALUATE
        TOPN(30,
            ADDCOLUMNS(
                SUMMARIZE('Merged', 'Merged'[Companies.company_name]),
                "TotalFrete", CALCULATE(SUM('Merged'[Valor Frete])),
                "TotalPedidos", CALCULATE(COUNTROWS('Merged')),
                "TotalValor", CALCULATE(SUM('Merged'[Valor]))
            ),
            [TotalFrete], DESC
        )
    `);
    rows.forEach((r, i) => console.log(`  [${i}] ${JSON.stringify(r)}`));

    // 4. Count of distinct companies
    console.log('\n=== Stats ===');
    rows = await dax(token, `
        EVALUATE
        ROW(
            "TotalRows", COUNTROWS('Merged'),
            "RowsWithFrete", CALCULATE(COUNTROWS('Merged'), 'Merged'[Valor Frete] > 0),
            "TotalFrete", SUM('Merged'[Valor Frete]),
            "AvgFrete", AVERAGE('Merged'[Valor Frete]),
            "DistinctCompanies", DISTINCTCOUNT('Merged'[Companies.company_name])
        )
    `);
    rows.forEach(r => console.log(`  ${JSON.stringify(r)}`));

    console.log('\n=== Done ===');
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
