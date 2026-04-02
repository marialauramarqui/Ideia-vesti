/**
 * Explore Oráculo datasets in workspace 63a65f3e-d96b-446e-a01d-f219132e1144
 * to discover date/month columns and test monthly aggregation.
 * V2: Skip DMV (not supported), use direct TOPN probing.
 */
const https = require('https');
const fs = require('fs');
const path = require('path');
const querystring = require('querystring');

const ORACULO_PAINEIS_WS_ID = '63a65f3e-d96b-446e-a01d-f219132e1144';

// Load .env
function loadEnv() {
    const envPath = path.join(__dirname, '.env');
    if (!fs.existsSync(envPath)) return {};
    const env = {};
    fs.readFileSync(envPath, 'utf-8').split('\n').forEach(line => {
        const m = line.match(/^([^#=]+)=(.*)$/);
        if (m) env[m[1].trim()] = m[2].trim();
    });
    return env;
}
const ENV = loadEnv();
const TENANT_ID = ENV.FABRIC_TENANT_ID;
const REFRESH_TOKEN = ENV.FABRIC_REFRESH_TOKEN;
const CLIENT_ID = ENV.FABRIC_CLIENT_ID || '14d82eec-204b-4c2f-b7e8-296a70dab67e';

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
    console.log('=== Getting access token ===');
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
        console.log('Token OK (expires_in=' + res.data.expires_in + 's)');
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

async function executeDAX(token, datasetId, daxQuery) {
    const body = JSON.stringify({
        queries: [{ query: daxQuery }],
        serializerSettings: { includeNulls: true },
    });
    return httpsRequest({
        hostname: 'api.powerbi.com',
        path: `/v1.0/myorg/groups/${ORACULO_PAINEIS_WS_ID}/datasets/${datasetId}/executeQueries`,
        method: 'POST',
        headers: {
            'Authorization': 'Bearer ' + token,
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(body),
        },
    }, body);
}

function getRows(res) {
    if (res.status === 200 && res.data.results && res.data.results[0] && res.data.results[0].tables && res.data.results[0].tables[0]) {
        return res.data.results[0].tables[0].rows || [];
    }
    return null;
}

function getError(res) {
    if (res.data && res.data.error) return res.data.error.message || JSON.stringify(res.data.error).substring(0, 400);
    if (typeof res.data === 'string') return res.data.substring(0, 400);
    return JSON.stringify(res.data).substring(0, 400);
}

async function main() {
    const token = await getAccessToken();

    // ====== STEP 1: List all datasets ======
    console.log('\n======================================================');
    console.log('STEP 1: List all datasets');
    console.log('======================================================');
    const dsRes = await httpsRequest({
        hostname: 'api.powerbi.com',
        path: `/v1.0/myorg/groups/${ORACULO_PAINEIS_WS_ID}/datasets`,
        method: 'GET',
        headers: { 'Authorization': 'Bearer ' + token },
    });
    const allDatasets = dsRes.data.value || [];
    console.log('Found ' + allDatasets.length + ' datasets');
    const datasets = allDatasets.filter(ds => ds.name !== 'Report Usage Metrics Model');

    // Find Egoiste datasets
    const egoisteDatasets = datasets.filter(ds => /egoiste/i.test(ds.name));
    console.log('\nEgoiste datasets:');
    egoisteDatasets.forEach(ds => console.log(`  "${ds.name}" => ${ds.id}`));

    // ====== STEP 2: Pick ONE working dataset and explore its schema ======
    // Use a well-known brand that should have data. Try CajuBrasil first, then others.
    const testCandidates = [
        ...egoisteDatasets,
        datasets.find(ds => /cajubrasil/i.test(ds.name)),
        datasets.find(ds => /gilkai/i.test(ds.name)),
        datasets.find(ds => /dolps/i.test(ds.name)),
        datasets[0],
    ].filter(Boolean);

    console.log('\n======================================================');
    console.log('STEP 2: Probe table columns via TOPN(1) on test datasets');
    console.log('======================================================');

    // Tables we know exist from build-cloud.js DAX
    const probeTables = [
        'f_Pedidos Oraculo',
        'f_Interacoes Oraculo Semanal',
        'Calendario',
        'Calendar',
        'Medidas',
        'd_Empresa',
        'd_Status',
        'f_Atendimentos',
        'f_Vendas',
    ];

    // For each test dataset, probe tables
    const dsColumns = {}; // dsId -> { tableName: [colNames] }
    for (const ds of testCandidates.slice(0, 3)) {
        console.log(`\n--- Dataset: "${ds.name}" (${ds.id}) ---`);
        dsColumns[ds.id] = {};

        // First, verify the totals DAX works
        const totalDax = "EVALUATE ROW(\"pedidos\", COUNTROWS('f_Pedidos Oraculo'), \"interacoes\", COUNTROWS('f_Interacoes Oraculo Semanal'), \"atendimentos\", [KPI Atendimentos Oraculo], \"pctIA\", [KPI % Atendimento Oraculo], \"vendas\", [KPI Vendas Totais])";
        const totalRes = await executeDAX(token, ds.id, totalDax);
        const totalRows = getRows(totalRes);
        if (totalRows && totalRows.length > 0) {
            console.log('  TOTALS: ' + JSON.stringify(totalRows[0]));
        } else {
            console.log('  TOTALS FAILED: ' + getError(totalRes));
        }

        // Probe each table
        for (const tbl of probeTables) {
            const res = await executeDAX(token, ds.id, `EVALUATE TOPN(1, '${tbl}')`);
            const rows = getRows(res);
            if (rows && rows.length > 0) {
                const cols = Object.keys(rows[0]);
                dsColumns[ds.id][tbl] = cols;
                console.log(`\n  TABLE "${tbl}" - ${cols.length} columns:`);
                for (const c of cols) {
                    const val = rows[0][c];
                    const valStr = val === null ? 'NULL' : typeof val === 'string' ? `"${val.substring(0, 60)}"` : val;
                    console.log(`    - ${c} = ${valStr}`);
                }
            } else if (rows && rows.length === 0) {
                console.log(`  TABLE "${tbl}" - exists but EMPTY`);
                dsColumns[ds.id][tbl] = [];
            } else {
                // Check if it's "table not found" vs other error
                const err = getError(res);
                if (err.includes('find') || err.includes('not found') || err.includes('Cannot find')) {
                    console.log(`  TABLE "${tbl}" - NOT FOUND`);
                } else {
                    console.log(`  TABLE "${tbl}" - ERROR: ${err.substring(0, 150)}`);
                }
            }
        }

        // Also try TOPN(3) for the main fact tables to see more sample data
        for (const tbl of ['f_Pedidos Oraculo', 'f_Interacoes Oraculo Semanal']) {
            if (!dsColumns[ds.id][tbl] || dsColumns[ds.id][tbl].length === 0) continue;
            console.log(`\n  SAMPLE DATA: TOPN(3, '${tbl}'):`);
            const res = await executeDAX(token, ds.id, `EVALUATE TOPN(3, '${tbl}')`);
            const rows = getRows(res);
            if (rows) {
                rows.forEach((r, i) => console.log(`    [${i}] ${JSON.stringify(r)}`));
            }
        }
    }

    // ====== STEP 3: Monthly aggregation attempts ======
    console.log('\n======================================================');
    console.log('STEP 3: Monthly aggregation attempts');
    console.log('======================================================');

    // Use the first dataset that had data
    const workingDs = testCandidates.find(ds => {
        const tables = dsColumns[ds.id];
        return tables && tables['f_Pedidos Oraculo'] && tables['f_Pedidos Oraculo'].length > 0;
    });

    if (!workingDs) {
        console.log('No dataset with f_Pedidos Oraculo data found among test candidates.');
        console.log('Trying all datasets to find one that works...');

        for (const ds of datasets) {
            const res = await executeDAX(token, ds.id, "EVALUATE ROW(\"p\", COUNTROWS('f_Pedidos Oraculo'))");
            const rows = getRows(res);
            if (rows && rows.length > 0 && rows[0]['[p]'] > 0) {
                console.log(`  Found working dataset: "${ds.name}" with ${rows[0]['[p]']} pedidos`);
                // Now probe columns
                const probeRes = await executeDAX(token, ds.id, `EVALUATE TOPN(1, 'f_Pedidos Oraculo')`);
                const probeRows = getRows(probeRes);
                if (probeRows && probeRows.length > 0) {
                    dsColumns[ds.id] = dsColumns[ds.id] || {};
                    dsColumns[ds.id]['f_Pedidos Oraculo'] = Object.keys(probeRows[0]);
                    console.log('  Columns: ' + dsColumns[ds.id]['f_Pedidos Oraculo'].join(', '));
                    console.log('  Row: ' + JSON.stringify(probeRows[0]));
                    await runMonthlyTests(token, ds, dsColumns);
                }
                break;
            }
        }
    } else {
        await runMonthlyTests(token, workingDs, dsColumns);
    }

    // ====== STEP 4: Egoiste deep-dive ======
    console.log('\n======================================================');
    console.log('STEP 4: Egoiste deep-dive');
    console.log('======================================================');

    for (const ds of egoisteDatasets) {
        console.log(`\n--- "${ds.name}" (${ds.id}) ---`);

        // Individual KPIs
        const kpis = [
            ["COUNTROWS('f_Pedidos Oraculo')", "pedidos_count"],
            ["COUNTROWS('f_Interacoes Oraculo Semanal')", "interacoes_count"],
            ["[KPI Atendimentos Oraculo]", "atendimentos"],
            ["[KPI % Atendimento Oraculo]", "pctIA"],
            ["[KPI Vendas Totais]", "vendas"],
        ];
        for (const [expr, label] of kpis) {
            const res = await executeDAX(token, ds.id, `EVALUATE ROW("v", ${expr})`);
            const rows = getRows(res);
            if (rows && rows.length > 0) {
                console.log(`  ${label}: ${rows[0]['[v]']}`);
            } else {
                console.log(`  ${label}: FAILED - ${getError(res).substring(0, 150)}`);
            }
        }

        // Probe f_Pedidos Oraculo
        const pedRes = await executeDAX(token, ds.id, `EVALUATE TOPN(5, 'f_Pedidos Oraculo')`);
        const pedRows = getRows(pedRes);
        if (pedRows && pedRows.length > 0) {
            console.log(`\n  f_Pedidos Oraculo sample (${pedRows.length} rows):`);
            console.log(`  Columns: ${Object.keys(pedRows[0]).join(', ')}`);
            pedRows.forEach((r, i) => console.log(`    [${i}] ${JSON.stringify(r)}`));
        } else if (pedRows && pedRows.length === 0) {
            console.log('  f_Pedidos Oraculo: EXISTS but EMPTY (0 rows)');
        } else {
            console.log('  f_Pedidos Oraculo: ' + getError(pedRes).substring(0, 200));
        }

        // Probe f_Interacoes Oraculo Semanal
        const intRes = await executeDAX(token, ds.id, `EVALUATE TOPN(5, 'f_Interacoes Oraculo Semanal')`);
        const intRows = getRows(intRes);
        if (intRows && intRows.length > 0) {
            console.log(`\n  f_Interacoes Oraculo Semanal sample (${intRows.length} rows):`);
            console.log(`  Columns: ${Object.keys(intRows[0]).join(', ')}`);
            intRows.forEach((r, i) => console.log(`    [${i}] ${JSON.stringify(r)}`));
        } else if (intRows && intRows.length === 0) {
            console.log('  f_Interacoes Oraculo Semanal: EXISTS but EMPTY (0 rows)');
        } else {
            console.log('  f_Interacoes Oraculo Semanal: ' + getError(intRes).substring(0, 200));
        }
    }

    console.log('\n\n=== EXPLORATION COMPLETE ===');
}

async function runMonthlyTests(token, ds, dsColumns) {
    console.log(`\nMonthly tests on "${ds.name}" (${ds.id}):`);
    const pedCols = dsColumns[ds.id]['f_Pedidos Oraculo'] || [];
    console.log('  f_Pedidos columns: ' + pedCols.join(', '));

    // Identify date columns from the column names
    const dateLikeCols = pedCols.filter(c =>
        /date|data|created|updated|dt_|periodo|semana|week|month|year|calendar/i.test(c)
    );
    console.log('  Date-like columns: ' + (dateLikeCols.length ? dateLikeCols.join(', ') : 'NONE'));

    // Try monthly aggregation with each date-like column
    for (const rawCol of dateLikeCols) {
        // Extract just the column name from '[table[col]]' format
        const colName = rawCol.replace(/.*\[/, '').replace(/\]$/, '');
        console.log(`\n  --- Monthly via '${colName}' ---`);

        // Try Year/MonthNo hierarchy
        const dax1 = `EVALUATE SUMMARIZECOLUMNS('f_Pedidos Oraculo'[${colName}].[Year], 'f_Pedidos Oraculo'[${colName}].[MonthNo], "pedidos", COUNTROWS('f_Pedidos Oraculo'))`;
        console.log(`  Query: ...${colName}.[Year], ...${colName}.[MonthNo]...`);
        const res1 = await executeDAX(token, ds.id, dax1);
        const rows1 = getRows(res1);
        if (rows1 && rows1.length > 0) {
            console.log(`  SUCCESS! ${rows1.length} monthly rows:`);
            rows1.forEach((r, i) => console.log(`    [${i}] ${JSON.stringify(r)}`));
        } else {
            console.log(`  Failed: ${getError(res1).substring(0, 200)}`);

            // Try FORMAT approach
            const dax2 = `EVALUATE ADDCOLUMNS(VALUES('f_Pedidos Oraculo'[${colName}]), "pedidos", CALCULATE(COUNTROWS('f_Pedidos Oraculo')))`;
            console.log(`  Fallback: VALUES + CALCULATE...`);
            const res2 = await executeDAX(token, ds.id, dax2);
            const rows2 = getRows(res2);
            if (rows2 && rows2.length > 0) {
                console.log(`  ${rows2.length} rows (first 15):`);
                rows2.slice(0, 15).forEach((r, i) => console.log(`    [${i}] ${JSON.stringify(r)}`));
            } else {
                console.log(`  Also failed: ${getError(res2).substring(0, 200)}`);
            }
        }
    }

    // If no date columns found in f_Pedidos, try common column names directly
    if (dateLikeCols.length === 0) {
        console.log('\n  No date columns detected. Trying common date column names...');
        const guesses = ['created_at', 'dt_pedido', 'data_pedido', 'data', 'date', 'data_criacao', 'periodo', 'semana', 'week_start'];
        for (const col of guesses) {
            const dax = `EVALUATE TOPN(5, ADDCOLUMNS(VALUES('f_Pedidos Oraculo'[${col}]), "cnt", CALCULATE(COUNTROWS('f_Pedidos Oraculo'))))`;
            const res = await executeDAX(token, ds.id, dax);
            const rows = getRows(res);
            if (rows && rows.length > 0) {
                console.log(`  Column '${col}' EXISTS! ${rows.length} values:`);
                rows.forEach((r, i) => console.log(`    [${i}] ${JSON.stringify(r)}`));
                break;
            }
        }
    }

    // Try monthly on interacoes too
    const intCols = dsColumns[ds.id]['f_Interacoes Oraculo Semanal'] || [];
    if (intCols.length > 0) {
        const intDateCols = intCols.filter(c => /date|data|created|updated|dt_|periodo|semana|week/i.test(c));
        console.log('\n  f_Interacoes date-like columns: ' + (intDateCols.length ? intDateCols.join(', ') : 'NONE'));
        for (const rawCol of intDateCols.slice(0, 2)) {
            const colName = rawCol.replace(/.*\[/, '').replace(/\]$/, '');
            const dax = `EVALUATE SUMMARIZECOLUMNS('f_Interacoes Oraculo Semanal'[${colName}].[Year], 'f_Interacoes Oraculo Semanal'[${colName}].[MonthNo], "interacoes", COUNTROWS('f_Interacoes Oraculo Semanal'))`;
            const res = await executeDAX(token, ds.id, dax);
            const rows = getRows(res);
            if (rows && rows.length > 0) {
                console.log(`  Interacoes monthly via '${colName}': SUCCESS! ${rows.length} rows:`);
                rows.forEach((r, i) => console.log(`    [${i}] ${JSON.stringify(r)}`));
            } else {
                console.log(`  Interacoes monthly via '${colName}': Failed`);
            }
        }
    }

    // Try the full monthly KPI query combining all metrics
    console.log('\n  --- Comprehensive monthly KPI query attempts ---');
    // If we found date columns, try combining
    if (dateLikeCols.length > 0) {
        const colName = dateLikeCols[0].replace(/.*\[/, '').replace(/\]$/, '');
        const fullDax = `EVALUATE SUMMARIZECOLUMNS('f_Pedidos Oraculo'[${colName}].[Year], 'f_Pedidos Oraculo'[${colName}].[MonthNo], "pedidos", COUNTROWS('f_Pedidos Oraculo'), "vendas", [KPI Vendas Totais])`;
        console.log(`  Full monthly DAX with '${colName}'...`);
        const res = await executeDAX(token, ds.id, fullDax);
        const rows = getRows(res);
        if (rows && rows.length > 0) {
            console.log(`  ${rows.length} rows:`);
            rows.forEach((r, i) => console.log(`    [${i}] ${JSON.stringify(r)}`));
        } else {
            console.log(`  Failed: ${getError(res).substring(0, 200)}`);
        }
    }
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
