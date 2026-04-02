/**
 * Explore Fabric Power BI datasets in a workspace.
 * Lists datasets, probes table names, prints columns, and looks for freight data.
 */
const https = require('https');
const fs = require('fs');
const path = require('path');
const querystring = require('querystring');

// ── Config ──
const WORKSPACE_ID = '0f5bd202-471f-482d-bf3d-38295044d7db';
const REPORT_ID = 'fca15a53-6315-412b-a1e1-eb55106da0e8';

// ── Load .env ──
function loadEnv() {
    const envPath = path.join(__dirname, '.env');
    if (!fs.existsSync(envPath)) return {};
    const env = {};
    fs.readFileSync(envPath, 'utf-8').split('\n').forEach(line => {
        const m = line.match(/^([^=]+)=(.*)$/);
        if (m) env[m[1].trim()] = m[2].trim();
    });
    return env;
}
const ENV = loadEnv();
const TENANT_ID = ENV.FABRIC_TENANT_ID;
const REFRESH_TOKEN = ENV.FABRIC_REFRESH_TOKEN;
const CLIENT_ID = ENV.FABRIC_CLIENT_ID || '14d82eec-204b-4c2f-b7e8-296a70dab67e';

// ── HTTP helpers ──
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

async function getAccessToken() {
    console.log('=== Getting access token via refresh_token ===');
    const body = querystring.stringify({
        client_id: CLIENT_ID,
        grant_type: 'refresh_token',
        refresh_token: REFRESH_TOKEN,
        scope: 'https://analysis.windows.net/powerbi/api/.default offline_access',
    });
    const res = await httpsRequest({
        hostname: 'login.microsoftonline.com',
        path: `/${TENANT_ID}/oauth2/v2.0/token`,
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(body) },
    }, body);

    if (res.data.access_token) {
        console.log('Token obtained OK (expires_in=' + res.data.expires_in + 's)');
        // Save new refresh token if returned
        if (res.data.refresh_token && res.data.refresh_token !== REFRESH_TOKEN) {
            const envPath = path.join(__dirname, '.env');
            let env = fs.readFileSync(envPath, 'utf-8');
            env = env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m, 'FABRIC_REFRESH_TOKEN=' + res.data.refresh_token);
            fs.writeFileSync(envPath, env, 'utf-8');
            console.log('(Updated refresh token in .env)');
        }
        return res.data.access_token;
    }
    console.error('Token error:', JSON.stringify(res.data, null, 2));
    process.exit(1);
}

async function apiGet(token, urlPath) {
    const res = await httpsRequest({
        hostname: 'api.powerbi.com',
        path: urlPath,
        method: 'GET',
        headers: { 'Authorization': 'Bearer ' + token },
    });
    return res;
}

async function executeDAX(token, workspaceId, datasetId, daxQuery) {
    const body = JSON.stringify({
        queries: [{ query: daxQuery }],
        serializerSettings: { includeNulls: true },
    });
    const res = await httpsRequest({
        hostname: 'api.powerbi.com',
        path: `/v1.0/myorg/groups/${workspaceId}/datasets/${datasetId}/executeQueries`,
        method: 'POST',
        headers: {
            'Authorization': 'Bearer ' + token,
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(body),
        },
    }, body);
    return res;
}

// ── Main ──
async function main() {
    const token = await getAccessToken();

    // 1. List ALL datasets in workspace
    console.log('\n=== Listing datasets in workspace ' + WORKSPACE_ID + ' ===');
    const dsRes = await apiGet(token, `/v1.0/myorg/groups/${WORKSPACE_ID}/datasets`);
    if (dsRes.status !== 200) {
        console.error('Failed to list datasets:', dsRes.status, JSON.stringify(dsRes.data));
        process.exit(1);
    }
    const datasets = dsRes.data.value || [];
    console.log('Found ' + datasets.length + ' dataset(s):');
    datasets.forEach(ds => {
        console.log(`  - ${ds.name}  (id: ${ds.id})  [configured=${ds.isRefreshable}, effectiveIdentity=${ds.isEffectiveIdentityRequired}]`);
    });

    // 2. Also list reports
    console.log('\n=== Listing reports in workspace ===');
    const rpRes = await apiGet(token, `/v1.0/myorg/groups/${WORKSPACE_ID}/reports`);
    if (rpRes.status === 200 && rpRes.data.value) {
        rpRes.data.value.forEach(r => {
            console.log(`  - ${r.name}  (id: ${r.id})  datasetId: ${r.datasetId}`);
        });
    }

    // 3. For each dataset, try common table names
    const TABLE_NAMES = [
        'Pedidos', 'Orders', 'Merged Pedidos', 'Frete', 'Shipping',
        'f_Pedidos', 'Entregas', 'Vendas', 'Sales', 'Clientes', 'Customers',
        'Empresas', 'Lojas', 'Stores', 'Produtos', 'Products',
        'Calendario', 'Calendar', 'Date', 'Datas', 'Medidas',
        'Dim_Empresa', 'Fato_Pedidos', 'Fact_Orders', 'Dim_Cliente',
        'Base', 'Dados', 'Tabela1', 'Sheet1', 'Planilha1',
        'Merged', 'Query1', 'Resultado', 'Resumo', 'Financeiro',
        'Faturas', 'Invoices', 'Pagamentos', 'Payments',
        'Itens', 'Items', 'Detalhes', 'Details',
        'Transportadora', 'Carrier', 'Logistica', 'Logistics'
    ];

    const foundTables = {}; // datasetId -> [{ name, columns, rows }]

    for (const ds of datasets) {
        console.log(`\n========================================`);
        console.log(`=== Probing dataset: ${ds.name} (${ds.id}) ===`);
        console.log(`========================================`);
        foundTables[ds.id] = [];

        // First try to get tables via DMV
        console.log('\n--- Trying DMV to list all tables ---');
        const dmvRes = await executeDAX(token, WORKSPACE_ID, ds.id,
            `SELECT [Name] FROM $SYSTEM.TMSCHEMA_TABLES WHERE NOT [IsHidden]`
        );
        let dmvTableNames = [];
        if (dmvRes.status === 200 && dmvRes.data.results && dmvRes.data.results[0]) {
            const rows = dmvRes.data.results[0].tables[0].rows;
            if (rows && rows.length) {
                dmvTableNames = rows.map(r => r['[Name]'] || Object.values(r)[0]);
                console.log('DMV found tables: ' + dmvTableNames.join(', '));
            }
        } else {
            console.log('DMV query failed or unsupported, trying manual probing...');
        }

        // Combine DMV tables + manual list (deduplicated)
        const allNames = [...new Set([...dmvTableNames, ...TABLE_NAMES])];

        for (const tbl of allNames) {
            const dax = `EVALUATE TOPN(1, '${tbl}')`;
            const res = await executeDAX(token, WORKSPACE_ID, ds.id, dax);
            if (res.status === 200 && res.data.results && res.data.results[0]) {
                const tableData = res.data.results[0].tables[0];
                const rows = tableData.rows || [];
                if (rows.length > 0) {
                    const cols = Object.keys(rows[0]);
                    console.log(`\n  TABLE FOUND: '${tbl}'`);
                    console.log(`  Columns (${cols.length}):`);
                    cols.forEach(c => console.log(`    - ${c}`));
                    foundTables[ds.id].push({ name: tbl, columns: cols });
                }
            }
        }
    }

    // 4. Print summary & find freight-related columns
    console.log('\n\n================================================');
    console.log('=== SUMMARY: All tables and columns found ===');
    console.log('================================================');

    const freteColumns = []; // { dsId, dsName, table, column }

    for (const ds of datasets) {
        const tables = foundTables[ds.id];
        if (!tables || !tables.length) {
            console.log(`\nDataset "${ds.name}": No tables found`);
            continue;
        }
        console.log(`\nDataset "${ds.name}" (${ds.id}):`);
        for (const tbl of tables) {
            console.log(`  Table: '${tbl.name}' (${tbl.columns.length} cols)`);
            for (const col of tbl.columns) {
                const colLower = col.toLowerCase();
                const isFrete = colLower.includes('frete') || colLower.includes('shipping') ||
                    colLower.includes('freight') || colLower.includes('envio') ||
                    colLower.includes('entrega') || colLower.includes('transport');
                const marker = isFrete ? ' *** FREIGHT-RELATED ***' : '';
                console.log(`    - ${col}${marker}`);
                if (isFrete) {
                    freteColumns.push({ dsId: ds.id, dsName: ds.name, table: tbl.name, column: col });
                }
            }
        }
    }

    // 5. For freight columns, get sample data
    if (freteColumns.length > 0) {
        console.log('\n\n================================================');
        console.log('=== FREIGHT-RELATED COLUMNS: Sample data ===');
        console.log('================================================');

        for (const fc of freteColumns) {
            console.log(`\n--- ${fc.dsName} / '${fc.table}' / ${fc.column} ---`);

            // Try to find a company identifier column in the same table
            const tblInfo = foundTables[fc.dsId].find(t => t.name === fc.table);
            const companyCol = tblInfo.columns.find(c => {
                const cl = c.toLowerCase();
                return cl.includes('empresa') || cl.includes('company') || cl.includes('loja') ||
                    cl.includes('store') || cl.includes('client') || cl.includes('marca') ||
                    cl.includes('brand') || cl.includes('nome') || cl.includes('name') ||
                    cl.includes('cnpj') || cl.includes('razao');
            });
            const dateCol = tblInfo.columns.find(c => {
                const cl = c.toLowerCase();
                return cl.includes('data') || cl.includes('date') || cl.includes('mes') ||
                    cl.includes('month') || cl.includes('periodo') || cl.includes('created');
            });

            // Sample raw data
            const sampleDax = `EVALUATE TOPN(20, '${fc.table}')`;
            const sampleRes = await executeDAX(token, WORKSPACE_ID, fc.dsId, sampleDax);
            if (sampleRes.status === 200 && sampleRes.data.results && sampleRes.data.results[0]) {
                const rows = sampleRes.data.results[0].tables[0].rows || [];
                console.log(`Sample rows (up to 20):`);
                rows.forEach((r, i) => {
                    const parts = [];
                    if (companyCol) parts.push(`${companyCol}=${r[companyCol]}`);
                    if (dateCol) parts.push(`${dateCol}=${r[dateCol]}`);
                    parts.push(`${fc.column}=${r[fc.column]}`);
                    console.log(`  [${i}] ${parts.join(' | ')}`);
                });
            }

            // Monthly aggregation if both company and date cols exist
            if (companyCol && dateCol) {
                console.log(`\nMonthly aggregation (company + month -> SUM freight):`);
                const aggDax = `EVALUATE
                    SUMMARIZECOLUMNS(
                        ${companyCol}, ${dateCol},
                        "TotalFrete", SUM(${fc.column})
                    )`;
                const aggRes = await executeDAX(token, WORKSPACE_ID, fc.dsId, aggDax);
                if (aggRes.status === 200 && aggRes.data.results && aggRes.data.results[0]) {
                    const rows = aggRes.data.results[0].tables[0].rows || [];
                    console.log(`Aggregated rows: ${rows.length}`);
                    rows.slice(0, 30).forEach((r, i) => console.log(`  [${i}] ${JSON.stringify(r)}`));
                    if (rows.length > 30) console.log(`  ... (${rows.length - 30} more rows)`);
                } else {
                    console.log('Aggregation query failed:', JSON.stringify((aggRes.data || {}).error || aggRes.data).substring(0, 500));

                    // Fallback: simpler aggregation
                    console.log('\nTrying simpler aggregation...');
                    const simpleDax = `EVALUATE TOPN(30, SUMMARIZE('${fc.table}', ${companyCol}, ${fc.column}))`;
                    const simpleRes = await executeDAX(token, WORKSPACE_ID, fc.dsId, simpleDax);
                    if (simpleRes.status === 200 && simpleRes.data.results && simpleRes.data.results[0]) {
                        const rows = simpleRes.data.results[0].tables[0].rows || [];
                        rows.forEach((r, i) => console.log(`  [${i}] ${JSON.stringify(r)}`));
                    } else {
                        console.log('Simple aggregation also failed:', JSON.stringify((simpleRes.data || {}).error || simpleRes.data).substring(0, 500));
                    }
                }
            }
        }
    } else {
        console.log('\n\nNo freight-related columns found in any dataset.');
    }

    // 6. Also try to get the report's associated dataset and list all tables with DMV COLUMNS
    console.log('\n\n================================================');
    console.log('=== Detailed column info via DMV for each dataset ===');
    console.log('================================================');
    for (const ds of datasets) {
        console.log(`\n--- Dataset: ${ds.name} ---`);
        const colRes = await executeDAX(token, WORKSPACE_ID, ds.id,
            `SELECT [TableID].[Name] AS [Table], [Name] AS [Column], [DataType], [IsHidden] FROM $SYSTEM.TMSCHEMA_COLUMNS WHERE NOT [IsHidden]`
        );
        if (colRes.status === 200 && colRes.data.results && colRes.data.results[0]) {
            const rows = colRes.data.results[0].tables[0].rows || [];
            console.log(`Total visible columns: ${rows.length}`);
            rows.forEach(r => {
                const tbl = r['[Table]'] || '';
                const col = r['[Column]'] || '';
                const dt = r['[DataType]'] || '';
                const colLower = col.toLowerCase();
                const marker = (colLower.includes('frete') || colLower.includes('shipping') ||
                    colLower.includes('freight') || colLower.includes('envio') ||
                    colLower.includes('entrega') || colLower.includes('transport')) ? ' *** FREIGHT ***' : '';
                console.log(`  ${tbl}.${col} (${dt})${marker}`);
            });
        } else {
            console.log('DMV columns query failed:', JSON.stringify((colRes.data || {}).error || colRes.data).substring(0, 300));
        }
    }

    console.log('\n=== Done ===');
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
