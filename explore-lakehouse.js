// Explore VestiLake ODBC_Quotes via SQL endpoint (tedious TDS)
const https = require('https');
const qs = require('querystring');
const { Connection, Request } = require('tedious');

const RT = process.env.FABRIC_REFRESH_TOKEN || '';
const TID = process.env.FABRIC_TENANT_ID || '';
const SQL_SERVER = '7sowj2vsfd6efgf3phzgjfmvaq-nrdsskmspnteherwztit766zc4.datawarehouse.fabric.microsoft.com';
const DB_NAME = 'VestiHouse';

function req(opts, body) {
    return new Promise((res, rej) => {
        const r = https.request(opts, resp => {
            const c = []; resp.on('data', d => c.push(d));
            resp.on('end', () => res({ status: resp.statusCode, body: Buffer.concat(c).toString() }));
        });
        r.on('error', rej); if (body) r.write(body); r.end();
    });
}

async function getToken() {
    const pb = qs.stringify({ client_id: '1950a258-227b-4e31-a9cf-717495945fc2', grant_type: 'refresh_token', refresh_token: RT, scope: 'https://database.windows.net/.default offline_access' });
    const tr = await req({ hostname: 'login.microsoftonline.com', path: '/' + TID + '/oauth2/v2.0/token', method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(pb) } }, pb);
    const td = JSON.parse(tr.body);
    if (!td.access_token) throw new Error('Token failed: ' + (td.error_description || ''));
    return td.access_token;
}

function runSQL(token, query) {
    return new Promise((resolve, reject) => {
        const config = {
            server: SQL_SERVER,
            authentication: { type: 'azure-active-directory-access-token', options: { token } },
            options: { database: DB_NAME, encrypt: true, port: 1433, requestTimeout: 60000 },
        };
        const conn = new Connection(config);
        const rows = [];
        conn.on('connect', err => {
            if (err) { reject(err); return; }
            const request = new Request(query, (err) => {
                if (err) reject(err);
                conn.close();
            });
            request.on('row', columns => {
                const row = {};
                columns.forEach(col => { row[col.metadata.colName] = col.value; });
                rows.push(row);
            });
            request.on('requestCompleted', () => resolve(rows));
            conn.execSql(request);
        });
        conn.connect();
    });
}

async function main() {
    console.log('=== Explore ODBC_Quotes via SQL ===\n');
    const token = await getToken();
    console.log('Authenticated.\n');

    // Get columns info
    console.log('--- ODBC_Quotes columns ---');
    const cols = await runSQL(token, "SELECT TOP 1 * FROM dbo.ODBC_Quotes");
    if (cols.length > 0) {
        Object.entries(cols[0]).forEach(([k, v]) => console.log('  ' + k + ' (' + typeof v + ') = ' + JSON.stringify(v).substring(0, 100)));
    }

    // Count rows
    console.log('\n--- Row counts ---');
    const cnt = await runSQL(token, "SELECT COUNT(*) as cnt FROM dbo.ODBC_Quotes");
    console.log('ODBC_Quotes rows: ' + (cnt[0] ? cnt[0].cnt : '?'));

    // Also check Anterior2023
    try {
        const cnt2 = await runSQL(token, "SELECT COUNT(*) as cnt FROM dbo.OBDC_Quotes_Anterior2023");
        console.log('OBDC_Quotes_Anterior2023 rows: ' + (cnt2[0] ? cnt2[0].cnt : '?'));

        const cols2 = await runSQL(token, "SELECT TOP 1 * FROM dbo.OBDC_Quotes_Anterior2023");
        if (cols2.length > 0) {
            console.log('\n--- OBDC_Quotes_Anterior2023 columns ---');
            Object.entries(cols2[0]).forEach(([k, v]) => console.log('  ' + k + ' (' + typeof v + ') = ' + JSON.stringify(v).substring(0, 100)));
        }
    } catch (e) { console.log('Anterior2023 error: ' + e.message); }

    // Sample 3 rows
    console.log('\n--- Sample ODBC_Quotes (3 rows) ---');
    const sample = await runSQL(token, "SELECT TOP 3 * FROM dbo.ODBC_Quotes ORDER BY created_at DESC");
    sample.forEach((r, i) => { console.log('Row ' + i + ':'); Object.entries(r).forEach(([k, v]) => console.log('  ' + k + ' = ' + JSON.stringify(v).substring(0, 100))); });

    // Date range
    console.log('\n--- Date range ---');
    const dr = await runSQL(token, "SELECT MIN(created_at) as min_date, MAX(created_at) as max_date FROM dbo.ODBC_Quotes");
    if (dr[0]) console.log('From: ' + dr[0].min_date + ' To: ' + dr[0].max_date);
}
main().catch(e => console.error('FATAL:', e));
