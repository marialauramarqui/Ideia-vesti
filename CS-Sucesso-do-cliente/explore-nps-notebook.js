/**
 * Explore NPS Notebook in Fabric & query NPS data from datasets
 */
const fs = require('fs');
const https = require('https');
const querystring = require('querystring');

// Load .env manually
const envPath = __dirname + '/.env';
const envContent = fs.readFileSync(envPath, 'utf8');
const envVars = {};
for (const line of envContent.split('\n')) {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith('#')) continue;
  const eqIdx = trimmed.indexOf('=');
  if (eqIdx > 0) envVars[trimmed.substring(0, eqIdx)] = trimmed.substring(eqIdx + 1);
}

const TENANT_ID = envVars.FABRIC_TENANT_ID;
const REFRESH_TOKEN = envVars.FABRIC_REFRESH_TOKEN;
const CLIENT_ID = envVars.FABRIC_CLIENT_ID || '14d82eec-204b-4c2f-b7e8-296a70dab67e';

const WS = '2929476c-7b92-4366-9236-ccd13ffbd917';
const NOTEBOOK_ID = '22d8312b-77c5-41ac-8f48-b3e91d2e4ed2';
const PAINEL_CS_DATASET = '583e34d7-6dd1-467b-86aa-3b74cfe1ca56';
const ORACULO_DATASET = 'c6a480e9-2db4-45f7-ba67-b489407f59e6';

function httpsRequest(options, postData) {
  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: data }));
    });
    req.on('error', reject);
    if (postData) req.write(postData);
    req.end();
  });
}

async function getToken() {
  console.log('=== Getting Access Token ===');
  const body = querystring.stringify({
    grant_type: 'refresh_token',
    client_id: CLIENT_ID,
    refresh_token: REFRESH_TOKEN,
    scope: 'https://analysis.windows.net/powerbi/api/.default offline_access',
  });

  const res = await httpsRequest({
    hostname: 'login.microsoftonline.com',
    path: `/${TENANT_ID}/oauth2/v2.0/token`,
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(body) },
  }, body);

  const parsed = JSON.parse(res.body);
  if (parsed.access_token) {
    console.log('Token obtained successfully (first 50 chars):', parsed.access_token.substring(0, 50));
    return parsed.access_token;
  }
  console.error('Token error:', res.body.substring(0, 500));
  throw new Error('Failed to get token');
}

async function fabricGet(token, url, label) {
  console.log(`\n=== ${label} ===`);
  console.log(`GET ${url}`);
  const u = new URL(url);
  const res = await httpsRequest({
    hostname: u.hostname,
    path: u.pathname + u.search,
    method: 'GET',
    headers: { 'Authorization': `Bearer ${token}` },
  });
  console.log(`Status: ${res.status}`);
  try {
    const j = JSON.parse(res.body);
    console.log(JSON.stringify(j, null, 2).substring(0, 3000));
    return j;
  } catch {
    console.log('Body:', res.body.substring(0, 1000));
    return null;
  }
}

async function daxQuery(token, datasetId, datasetLabel, daxExpression) {
  console.log(`\n--- DAX on ${datasetLabel}: ${daxExpression} ---`);
  const body = JSON.stringify({ queries: [{ query: daxExpression }], serializerSettings: { includeNulls: true } });
  const res = await httpsRequest({
    hostname: 'api.powerbi.com',
    path: `/v1.0/myorg/datasets/${datasetId}/executeQueries`,
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(body),
    },
  }, body);
  console.log(`Status: ${res.status}`);
  try {
    const j = JSON.parse(res.body);
    if (j.results && j.results[0] && j.results[0].tables) {
      const tbl = j.results[0].tables[0];
      console.log(`Columns: ${JSON.stringify(tbl.columns)}`);
      console.log(`Rows (first 5): ${JSON.stringify((tbl.rows || []).slice(0, 5), null, 2)}`);
      return tbl;
    }
    console.log(JSON.stringify(j, null, 2).substring(0, 1500));
    return j;
  } catch {
    console.log('Body:', res.body.substring(0, 1500));
    return null;
  }
}

async function main() {
  const token = await getToken();

  // 1. Notebook endpoints
  await fabricGet(token,
    `https://api.powerbi.com/v1.0/myorg/groups/${WS}/notebooks/${NOTEBOOK_ID}`,
    'PBI Notebook endpoint (myorg/groups)');

  await fabricGet(token,
    `https://api.fabric.microsoft.com/v1/workspaces/${WS}/notebooks/${NOTEBOOK_ID}`,
    'Fabric Notebook endpoint');

  await fabricGet(token,
    `https://api.fabric.microsoft.com/v1/workspaces/${WS}/items/${NOTEBOOK_ID}`,
    'Fabric Items endpoint');

  // Try to get notebook definition/content
  await fabricGet(token,
    `https://api.fabric.microsoft.com/v1/workspaces/${WS}/notebooks/${NOTEBOOK_ID}/getDefinition`,
    'Fabric Notebook getDefinition');

  await fabricGet(token,
    `https://api.fabric.microsoft.com/v1/workspaces/${WS}/items/${NOTEBOOK_ID}/getDefinition`,
    'Fabric Items getDefinition');

  // 2. List all items in workspace to find NPS-related items
  await fabricGet(token,
    `https://api.fabric.microsoft.com/v1/workspaces/${WS}/items`,
    'All items in workspace');

  // 3. List tables in datasets
  await fabricGet(token,
    `https://api.powerbi.com/v1.0/myorg/datasets/${PAINEL_CS_DATASET}/tables`,
    'Painel CS dataset tables');

  await fabricGet(token,
    `https://api.powerbi.com/v1.0/myorg/datasets/${ORACULO_DATASET}/tables`,
    'Oraculo dataset tables');

  // 4. DAX queries on Painel CS dataset for NPS
  console.log('\n=== DAX Queries on Painel CS ===');

  // First discover all tables via INFO functions
  await daxQuery(token, PAINEL_CS_DATASET, 'PainelCS',
    "EVALUATE SELECTCOLUMNS(INFO.TABLES(), \"Name\", [Name])");

  const npsQueries = [
    "EVALUATE TOPN(1, 'NPS')",
    "EVALUATE TOPN(1, nps)",
    "EVALUATE TOPN(1, 'f_NPS')",
    "EVALUATE TOPN(1, 'Pesquisa')",
    "EVALUATE TOPN(1, 'Survey')",
    "EVALUATE TOPN(1, 'fNPS')",
    "EVALUATE TOPN(1, 'dim_NPS')",
    "EVALUATE TOPN(1, 'Satisfaction')",
  ];

  for (const q of npsQueries) {
    await daxQuery(token, PAINEL_CS_DATASET, 'PainelCS', q);
  }

  // 5. DAX queries on Oraculo dataset
  console.log('\n=== DAX Queries on Oraculo ===');

  await daxQuery(token, ORACULO_DATASET, 'Oraculo',
    "EVALUATE SELECTCOLUMNS(INFO.TABLES(), \"Name\", [Name])");

  for (const q of npsQueries) {
    await daxQuery(token, ORACULO_DATASET, 'Oraculo', q);
  }

  // 6. If we find NPS table, get more rows and structure
  console.log('\n=== Trying broader NPS searches ===');

  // Search for any table with NPS in the name via DAX filter
  await daxQuery(token, PAINEL_CS_DATASET, 'PainelCS',
    "EVALUATE FILTER(SELECTCOLUMNS(INFO.TABLES(), \"Name\", [Name]), CONTAINSSTRING([Name], \"NPS\"))");
  await daxQuery(token, PAINEL_CS_DATASET, 'PainelCS',
    "EVALUATE FILTER(SELECTCOLUMNS(INFO.TABLES(), \"Name\", [Name]), CONTAINSSTRING([Name], \"nps\"))");
  await daxQuery(token, PAINEL_CS_DATASET, 'PainelCS',
    "EVALUATE FILTER(SELECTCOLUMNS(INFO.TABLES(), \"Name\", [Name]), CONTAINSSTRING(LOWER([Name]), \"nps\"))");
  await daxQuery(token, PAINEL_CS_DATASET, 'PainelCS',
    "EVALUATE FILTER(SELECTCOLUMNS(INFO.TABLES(), \"Name\", [Name]), CONTAINSSTRING(LOWER([Name]), \"pesquisa\"))");

  // Same for Oraculo
  await daxQuery(token, ORACULO_DATASET, 'Oraculo',
    "EVALUATE FILTER(SELECTCOLUMNS(INFO.TABLES(), \"Name\", [Name]), CONTAINSSTRING(LOWER([Name]), \"nps\"))");
  await daxQuery(token, ORACULO_DATASET, 'Oraculo',
    "EVALUATE FILTER(SELECTCOLUMNS(INFO.TABLES(), \"Name\", [Name]), CONTAINSSTRING(LOWER([Name]), \"pesquisa\"))");
}

main().catch(e => console.error('FATAL:', e.message));
