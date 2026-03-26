// Explore VestiLake ODBC_Quotes - get columns and sample data via SQL endpoint
const https = require('https');
const qs = require('querystring');

const RT = process.env.FABRIC_REFRESH_TOKEN || '';
const TID = process.env.FABRIC_TENANT_ID || '';
const LAKE_WS = '2929476c-7b92-4366-9236-ccd13ffbd917';
const LAKE_ID = '21b85aa7-d4d3-4221-9365-ea024dc2461a';

function req(opts, body) {
    return new Promise((res, rej) => {
        const r = https.request(opts, resp => {
            const c = []; resp.on('data', d => c.push(d));
            resp.on('end', () => res({ status: resp.statusCode, body: Buffer.concat(c).toString() }));
        });
        r.on('error', rej); if (body) r.write(body); r.end();
    });
}

async function main() {
    // Get token
    const pb = qs.stringify({ client_id: '1950a258-227b-4e31-a9cf-717495945fc2', grant_type: 'refresh_token', refresh_token: RT, scope: 'https://analysis.windows.net/powerbi/api/.default offline_access' });
    const tr = await req({ hostname: 'login.microsoftonline.com', path: '/' + TID + '/oauth2/v2.0/token', method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(pb) } }, pb);
    const td = JSON.parse(tr.body);
    if (!td.access_token) { console.log('Token failed:', td.error_description || ''); return; }
    const token = td.access_token;
    console.log('Authenticated.\n');

    // Try to get Lakehouse properties (SQL endpoint info)
    console.log('=== Lakehouse properties ===');
    const lp = await req({ hostname: 'api.fabric.microsoft.com', path: '/v1/workspaces/' + LAKE_WS + '/lakehouses/' + LAKE_ID, method: 'GET', headers: { 'Authorization': 'Bearer ' + token } });
    console.log('Status:', lp.status);
    const lpData = JSON.parse(lp.body);
    console.log(JSON.stringify(lpData, null, 2).substring(0, 1000));

    // List SQL endpoints in workspace
    console.log('\n=== SQL Endpoints / Warehouses ===');
    const items = await req({ hostname: 'api.fabric.microsoft.com', path: '/v1/workspaces/' + LAKE_WS + '/items', method: 'GET', headers: { 'Authorization': 'Bearer ' + token } });
    const itemsData = JSON.parse(items.body);
    (itemsData.value || []).forEach(i => {
        if (i.type === 'SQLEndpoint' || i.type === 'Warehouse' || i.type === 'Lakehouse' || i.type === 'SemanticModel') {
            console.log(' - ' + i.type + ': ' + i.displayName + ' | ID: ' + i.id);
        }
    });

    // Try DAX on Painel CS dataset with ODBC_Quotes
    console.log('\n=== Try DAX query on Painel CS with different table names ===');
    const dsId = '583e34d7-6dd1-467b-86aa-3b74cfe1ca56';
    for (const tbl of ['ODBC_Quotes', 'dbo.ODBC_Quotes', 'OBDC_Quotes_Anterior2023']) {
        const bodyStr = JSON.stringify({ queries: [{ query: "EVALUATE TOPN(2, '" + tbl + "')" }], serializerSettings: { includeNulls: true } });
        const r = await req({
            hostname: 'api.powerbi.com', path: '/v1.0/myorg/groups/' + LAKE_WS + '/datasets/' + dsId + '/executeQueries', method: 'POST',
            headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(bodyStr) },
        }, bodyStr);
        const data = JSON.parse(r.body);
        if (r.status === 200 && data.results && data.results[0] && data.results[0].tables && data.results[0].tables[0] && data.results[0].tables[0].rows.length > 0) {
            console.log('\nSUCCESS with table: ' + tbl);
            const rows = data.results[0].tables[0].rows;
            console.log('Columns:');
            Object.keys(rows[0]).forEach(k => console.log('  ' + k + ' = ' + JSON.stringify(rows[0][k]).substring(0, 100)));
            console.log('Row count in sample: ' + rows.length);
        } else {
            const err = data.error ? JSON.stringify(data.error).substring(0, 150) : 'HTTP ' + r.status;
            console.log(tbl + ': ' + err);
        }
    }

    // Try querying the Lakehouse's default dataset (same ID as lakehouse)
    console.log('\n=== Try DAX on Lakehouse default dataset ===');
    for (const dsIdTry of [LAKE_ID]) {
        const bodyStr = JSON.stringify({ queries: [{ query: "EVALUATE TOPN(2, ODBC_Quotes)" }], serializerSettings: { includeNulls: true } });
        const r = await req({
            hostname: 'api.powerbi.com', path: '/v1.0/myorg/groups/' + LAKE_WS + '/datasets/' + dsIdTry + '/executeQueries', method: 'POST',
            headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(bodyStr) },
        }, bodyStr);
        const data = JSON.parse(r.body);
        if (r.status === 200 && data.results && data.results[0] && data.results[0].tables && data.results[0].tables[0] && data.results[0].tables[0].rows.length > 0) {
            console.log('SUCCESS with Lakehouse dataset!');
            const rows = data.results[0].tables[0].rows;
            console.log('Columns:');
            Object.keys(rows[0]).forEach(k => console.log('  ' + k + ' = ' + JSON.stringify(rows[0][k]).substring(0, 100)));
        } else {
            const err = data.error ? JSON.stringify(data.error).substring(0, 200) : 'HTTP ' + r.status;
            console.log('Lakehouse dataset: ' + err);
        }
    }

    // List ALL datasets to find SQL endpoint dataset
    console.log('\n=== All datasets in workspace ===');
    const allDs = await req({ hostname: 'api.powerbi.com', path: '/v1.0/myorg/groups/' + LAKE_WS + '/datasets', method: 'GET', headers: { 'Authorization': 'Bearer ' + token } });
    const allDsData = JSON.parse(allDs.body);
    for (const d of (allDsData.value || [])) {
        console.log(' - ' + d.name + ' | ' + d.id + ' | isRefreshable: ' + d.isRefreshable);
        // Try ODBC_Quotes on each
        const bodyStr = JSON.stringify({ queries: [{ query: "EVALUATE TOPN(1, ODBC_Quotes)" }], serializerSettings: { includeNulls: true } });
        const r = await req({
            hostname: 'api.powerbi.com', path: '/v1.0/myorg/groups/' + LAKE_WS + '/datasets/' + d.id + '/executeQueries', method: 'POST',
            headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(bodyStr) },
        }, bodyStr);
        if (r.status === 200) {
            const rData = JSON.parse(r.body);
            if (rData.results && rData.results[0] && rData.results[0].tables && rData.results[0].tables[0] && rData.results[0].tables[0].rows.length > 0) {
                console.log('   ^^^ HAS ODBC_Quotes! Columns:');
                Object.keys(rData.results[0].tables[0].rows[0]).forEach(k => console.log('     ' + k + ' = ' + JSON.stringify(rData.results[0].tables[0].rows[0][k]).substring(0, 100)));
            }
        }
    }
}
main().catch(e => console.error(e));
