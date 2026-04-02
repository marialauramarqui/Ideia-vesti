/**
 * Explore Fabric datasets to find the best source of active companies.
 * Uses correct workspace/dataset GUIDs from build-cloud.js.
 */
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const https = require('https');
const querystring = require('querystring');

const DIR = __dirname;
const TENANT_ID = process.env.FABRIC_TENANT_ID;
const CLIENT_ID = process.env.FABRIC_CLIENT_ID || '14d82eec-204b-4c2f-b7e8-296a70dab67e';
let REFRESH_TOKEN = process.env.FABRIC_REFRESH_TOKEN;

// Correct full GUIDs (from build-cloud.js)
const CADASTROS = { ws: 'aced753a-0f0e-4bcf-9264-72f6496cf2cf', ds: 'e6c74524-e355-4447-9eb4-baae76b84dc4' };
const PAINEL_CS = { ws: '2929476c-7b92-4366-9236-ccd13ffbd917', ds: '583e34d7-6dd1-467b-86aa-3b74cfe1ca56' };
const METRICAS  = { ws: '786bfd95-0733-4fcb-aa84-ef2c97518959', ds: '6d232602-d209-4dab-8be5-d9c34db57c0b' };

function httpsRequest(options, postData) {
  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, headers: res.headers, body: JSON.parse(data) }); }
        catch { resolve({ status: res.statusCode, headers: res.headers, body: data }); }
      });
    });
    req.on('error', reject);
    if (postData) req.write(postData);
    req.end();
  });
}

async function getToken() {
  const postData = querystring.stringify({
    client_id: CLIENT_ID,
    grant_type: 'refresh_token',
    refresh_token: REFRESH_TOKEN,
    scope: 'https://analysis.windows.net/powerbi/api/.default offline_access',
  });
  const res = await httpsRequest({
    hostname: 'login.microsoftonline.com',
    path: `/${TENANT_ID}/oauth2/v2.0/token`,
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(postData) }
  }, postData);
  if (res.status !== 200) throw new Error('Token failed: ' + JSON.stringify(res.body));
  // Save rotated refresh token
  if (res.body.refresh_token) {
    const envPath = path.join(DIR, '.env');
    let env = fs.readFileSync(envPath, 'utf-8');
    env = env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m, 'FABRIC_REFRESH_TOKEN=' + res.body.refresh_token);
    fs.writeFileSync(envPath, env, 'utf-8');
  }
  return res.body.access_token;
}

async function daxQuery(token, ws, ds, query) {
  const body = JSON.stringify({ queries: [{ query }], serializerSettings: { includeNulls: true } });
  const res = await httpsRequest({
    hostname: 'api.powerbi.com',
    path: `/v1.0/myorg/groups/${ws}/datasets/${ds}/executeQueries`,
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body)
    }
  }, body);
  if (res.status !== 200) {
    const info = res.headers?.['x-powerbi-error-info'] || '';
    return { error: true, status: res.status, info, body: typeof res.body === 'string' ? res.body.substring(0, 300) : JSON.stringify(res.body).substring(0, 500) };
  }
  return res.body;
}

function getRows(result) {
  if (result.error) return null;
  return result.results?.[0]?.tables?.[0]?.rows || [];
}

function printResult(label, result) {
  console.log(`\n--- ${label} ---`);
  if (result.error) {
    console.log(`  ERROR ${result.status} (${result.info}): ${result.body}`);
    return;
  }
  const rows = getRows(result);
  if (!rows || rows.length === 0) { console.log('  (no rows)'); return; }
  console.log(`  ${rows.length} rows. Columns: ${Object.keys(rows[0]).join(', ')}`);
  for (const row of rows.slice(0, 30)) {
    console.log('   ', JSON.stringify(row));
  }
}

async function main() {
  console.log('Getting token...');
  const token = await getToken();
  console.log('Token obtained.\n');

  // ============================================================
  // SECTION 1: Confeccao Metricas 2025
  // ============================================================
  console.log('========================================');
  console.log('  CONFECCAO METRICAS 2025');
  console.log('========================================');

  const m1 = await daxQuery(token, METRICAS.ws, METRICAS.ds,
    'EVALUATE ROW("Total", COUNTROWS(Query1))'
  );
  printResult('Query1 - Total Rows', m1);

  const m2 = await daxQuery(token, METRICAS.ws, METRICAS.ds,
    `EVALUATE SUMMARIZE(Query1, Query1[Status Empresa 2], "Count", COUNTROWS(Query1))`
  );
  printResult('Query1 - Count by [Status Empresa 2]', m2);

  const m2b = await daxQuery(token, METRICAS.ws, METRICAS.ds,
    `EVALUATE SUMMARIZE(Query1, Query1[Status Empresa], "Count", COUNTROWS(Query1))`
  );
  printResult('Query1 - Count by [Status Empresa]', m2b);

  // List tables
  const mTables = await daxQuery(token, METRICAS.ws, METRICAS.ds,
    `EVALUATE SELECTCOLUMNS(INFO.TABLES(), "Name", [Name], "Rows", [RowCount], "Hidden", [IsHidden])`
  );
  printResult('Metricas - All Tables', mTables);

  // ============================================================
  // SECTION 2: Painel CS
  // ============================================================
  console.log('\n========================================');
  console.log('  PAINEL CS');
  console.log('========================================');

  // List tables first
  const pTables = await daxQuery(token, PAINEL_CS.ws, PAINEL_CS.ds,
    `EVALUATE SELECTCOLUMNS(INFO.TABLES(), "Name", [Name], "Rows", [RowCount], "Hidden", [IsHidden])`
  );
  printResult('Painel CS - All Tables', pTables);

  // DOMAINS_Pedidos_Por_Mes structure
  const p1 = await daxQuery(token, PAINEL_CS.ws, PAINEL_CS.ds,
    'EVALUATE TOPN(5, DOMAINS_Pedidos_Por_Mes)'
  );
  printResult('DOMAINS_Pedidos_Por_Mes - TOPN(5)', p1);

  // Count rows and distinct domains
  const p2 = await daxQuery(token, PAINEL_CS.ws, PAINEL_CS.ds,
    `EVALUATE ROW("TotalRows", COUNTROWS(DOMAINS_Pedidos_Por_Mes), "DistinctDomains", DISTINCTCOUNT(DOMAINS_Pedidos_Por_Mes[Domain]))`
  );
  printResult('DOMAINS_Pedidos_Por_Mes - Counts', p2);

  // Domains table structure
  const p3 = await daxQuery(token, PAINEL_CS.ws, PAINEL_CS.ds,
    'EVALUATE TOPN(3, Domains)'
  );
  printResult('Domains - TOPN(3)', p3);

  // Domains total
  const p4 = await daxQuery(token, PAINEL_CS.ws, PAINEL_CS.ds,
    'EVALUATE ROW("Total", COUNTROWS(Domains))'
  );
  printResult('Domains - Total Rows', p4);

  // Domains - Canal distribution
  const p5 = await daxQuery(token, PAINEL_CS.ws, PAINEL_CS.ds,
    `EVALUATE SUMMARIZE(Domains, Domains[Canal], "Count", COUNTROWS(Domains))`
  );
  printResult('Domains - Count by Canal', p5);

  // Domains - check for status/active columns by looking at column names
  if (!p3.error) {
    const rows = getRows(p3);
    if (rows && rows.length > 0) {
      const cols = Object.keys(rows[0]);
      console.log('\n  All Domains columns:');
      cols.forEach(c => console.log('    -', c));

      const statusCols = cols.filter(c => /status|ativ|active|cancel|desativ|plano|plan|trial|churn/i.test(c));
      if (statusCols.length > 0) {
        console.log('\n  Status-like columns found:', statusCols);
        for (const col of statusCols) {
          const cleanCol = col.replace(/^Domains\[/, '').replace(/\]$/, '');
          const rStat = await daxQuery(token, PAINEL_CS.ws, PAINEL_CS.ds,
            `EVALUATE SUMMARIZE(Domains, Domains[${cleanCol}], "Count", COUNTROWS(Domains))`
          );
          printResult(`Domains - Values of [${cleanCol}]`, rStat);
        }
      } else {
        console.log('\n  No status-like columns found in Domains table.');
      }
    }
  }

  // ============================================================
  // SECTION 3: Cadastros Empresas (for reference)
  // ============================================================
  console.log('\n========================================');
  console.log('  CADASTROS EMPRESAS (reference)');
  console.log('========================================');

  const c1 = await daxQuery(token, CADASTROS.ws, CADASTROS.ds,
    `EVALUATE SELECTCOLUMNS(INFO.TABLES(), "Name", [Name], "Rows", [RowCount], "Hidden", [IsHidden])`
  );
  printResult('Cadastros - All Tables', c1);

  // Check the main table structure (first few rows)
  if (!c1.error) {
    const tables = getRows(c1);
    if (tables) {
      const mainTable = tables.find(t => t['[Name]']?.includes('Empresa') || t['[Name]']?.includes('Cadastro'));
      if (mainTable) {
        const tName = mainTable['[Name]'];
        console.log(`\n  Checking main table: ${tName}`);
        const c2 = await daxQuery(token, CADASTROS.ws, CADASTROS.ds,
          `EVALUATE TOPN(2, '${tName}')`
        );
        printResult(`${tName} - TOPN(2)`, c2);

        const c3 = await daxQuery(token, CADASTROS.ws, CADASTROS.ds,
          `EVALUATE ROW("Total", COUNTROWS('${tName}'))`
        );
        printResult(`${tName} - Total Rows`, c3);
      }
    }
  }

  // ============================================================
  // SUMMARY
  // ============================================================
  console.log('\n\n========================================');
  console.log('  SUMMARY');
  console.log('========================================');
  console.log('Check the output above to compare:');
  console.log('  - Confeccao Metricas: Status Empresa counts');
  console.log('  - Painel CS Domains: total + any status columns');
  console.log('  - Cadastros Empresas: total companies');
}

main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
