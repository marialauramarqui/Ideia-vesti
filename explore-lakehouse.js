// Explore VestiLake ODBC_Quotes via SQL Endpoint dataset
const https = require('https');
const qs = require('querystring');

const RT = process.env.FABRIC_REFRESH_TOKEN || '';
const TID = process.env.FABRIC_TENANT_ID || '';
const LAKE_WS = '2929476c-7b92-4366-9236-ccd13ffbd917';
const SQL_ENDPOINT_ID = '96a89d4a-b486-478e-b6a8-5ecff8c0eabc';

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
    const pb = qs.stringify({ client_id: '1950a258-227b-4e31-a9cf-717495945fc2', grant_type: 'refresh_token', refresh_token: RT, scope: 'https://analysis.windows.net/powerbi/api/.default offline_access' });
    const tr = await req({ hostname: 'login.microsoftonline.com', path: '/' + TID + '/oauth2/v2.0/token', method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(pb) } }, pb);
    const td = JSON.parse(tr.body);
    if (!td.access_token) { console.log('Token failed'); return; }
    const token = td.access_token;
    console.log('Authenticated.\n');

    // Try DAX on SQL endpoint dataset
    console.log('=== DAX on SQL Endpoint dataset ===');
    const tables = ['ODBC_Quotes', 'dbo_ODBC_Quotes', 'OBDC_Quotes_Anterior2023', 'dbo_OBDC_Quotes_Anterior2023'];
    for (const tbl of tables) {
        const bodyStr = JSON.stringify({ queries: [{ query: "EVALUATE TOPN(3, " + tbl + ")" }], serializerSettings: { includeNulls: true } });
        const r = await req({
            hostname: 'api.powerbi.com', path: '/v1.0/myorg/groups/' + LAKE_WS + '/datasets/' + SQL_ENDPOINT_ID + '/executeQueries', method: 'POST',
            headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(bodyStr) },
        }, bodyStr);
        if (r.status === 200) {
            const data = JSON.parse(r.body);
            if (data.results && data.results[0] && data.results[0].tables && data.results[0].tables[0] && data.results[0].tables[0].rows.length > 0) {
                console.log('\nSUCCESS: ' + tbl);
                const rows = data.results[0].tables[0].rows;
                console.log('Columns (' + Object.keys(rows[0]).length + '):');
                Object.keys(rows[0]).forEach(k => console.log('  ' + k + ' = ' + JSON.stringify(rows[0][k]).substring(0, 120)));
                console.log('\nRow 2:');
                if (rows[1]) Object.keys(rows[1]).forEach(k => console.log('  ' + k + ' = ' + JSON.stringify(rows[1][k]).substring(0, 120)));

                // Count total rows
                const countBody = JSON.stringify({ queries: [{ query: "EVALUATE ROW(\"cnt\", COUNTROWS(" + tbl + "))" }], serializerSettings: { includeNulls: true } });
                const cr = await req({
                    hostname: 'api.powerbi.com', path: '/v1.0/myorg/groups/' + LAKE_WS + '/datasets/' + SQL_ENDPOINT_ID + '/executeQueries', method: 'POST',
                    headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(countBody) },
                }, countBody);
                if (cr.status === 200) {
                    const crd = JSON.parse(cr.body);
                    if (crd.results && crd.results[0] && crd.results[0].tables[0]) {
                        console.log('\nTotal rows: ' + JSON.stringify(crd.results[0].tables[0].rows[0]));
                    }
                }
            } else {
                const err = r.status === 200 && JSON.parse(r.body).error ? JSON.stringify(JSON.parse(r.body).error).substring(0, 150) : 'HTTP ' + r.status;
                console.log(tbl + ': FAILED - ' + err);
            }
        } else {
            console.log(tbl + ': HTTP ' + r.status + ' ' + r.body.substring(0, 150));
        }
    }

    // Also try OBDC_Quotes_Anterior2023
    console.log('\n=== Also try Anterior2023 ===');
    for (const tbl of ['OBDC_Quotes_Anterior2023']) {
        const countBody = JSON.stringify({ queries: [{ query: "EVALUATE ROW(\"cnt\", COUNTROWS(" + tbl + "))" }], serializerSettings: { includeNulls: true } });
        const cr = await req({
            hostname: 'api.powerbi.com', path: '/v1.0/myorg/groups/' + LAKE_WS + '/datasets/' + SQL_ENDPOINT_ID + '/executeQueries', method: 'POST',
            headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(countBody) },
        }, countBody);
        console.log(tbl + ':', cr.status, cr.body.substring(0, 300));
    }
}
main().catch(e => console.error(e));
