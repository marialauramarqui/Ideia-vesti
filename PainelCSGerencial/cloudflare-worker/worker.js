// Cloudflare Worker: recebe upload da planilha do Diogo, commita no repo e
// dispara o GitHub Action 'onlog-ingest.yml'.
//
// Variaveis de ambiente (Workers > Settings > Variables):
//   GITHUB_TOKEN    (Secret) - PAT fine-grained com Contents:Write + Actions:Write em vesti-mobi/Ideia-vesti
//   GITHUB_OWNER    = "vesti-mobi"
//   GITHUB_REPO     = "Ideia-vesti"
//   UPLOAD_PASSWORD (Secret) - senha compartilhada que a UI envia
//   WORKFLOW_FILE   = "onlog-ingest.yml"
//
// Endpoints:
//   POST /upload  body: { password, filename, xlsxBase64 }
//   GET  /status?run_id=...   (opcional - ver status do workflow)
//   OPTIONS *  (CORS preflight)

const CORS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '86400',
};

function json(body, status = 200) {
    return new Response(JSON.stringify(body), {
        status,
        headers: { 'Content-Type': 'application/json', ...CORS },
    });
}

async function gh(env, path, init = {}) {
    const headers = {
        'Authorization': `Bearer ${env.GITHUB_TOKEN}`,
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
        'User-Agent': 'cf-worker-onlog',
        ...(init.headers || {}),
    };
    const r = await fetch(`https://api.github.com${path}`, { ...init, headers });
    const text = await r.text();
    let parsed = null;
    try { parsed = text ? JSON.parse(text) : null; } catch (_) {}
    if (!r.ok) {
        const msg = (parsed && parsed.message) || text || r.statusText;
        throw new Error(`GitHub API ${r.status}: ${msg}`);
    }
    return parsed;
}

function sanitizeFilename(name) {
    return String(name || 'upload.xlsx')
        .replace(/[^a-zA-Z0-9._-]/g, '_')
        .slice(0, 120);
}

async function handleUpload(req, env) {
    const body = await req.json().catch(() => null);
    if (!body) return json({ error: 'JSON invalido' }, 400);

    const { password, filename, xlsxBase64 } = body;
    if (!password || password !== env.UPLOAD_PASSWORD) {
        return json({ error: 'Senha incorreta' }, 401);
    }
    if (!xlsxBase64 || typeof xlsxBase64 !== 'string') {
        return json({ error: 'xlsxBase64 ausente' }, 400);
    }
    // Limite ~10 MB depois de base64 (~13.3 MB string). Cloudflare Free aceita ate ~100MB.
    if (xlsxBase64.length > 14 * 1024 * 1024) {
        return json({ error: 'arquivo muito grande (>10MB)' }, 413);
    }

    const ts = new Date().toISOString().replace(/[:T]/g, '-').slice(0, 16);
    const safeName = sanitizeFilename(filename || 'planilha.xlsx');
    const path = `PainelCSGerencial/_uploads/${ts}_${safeName}`;

    // 1) Commitar o arquivo
    await gh(env, `/repos/${env.GITHUB_OWNER}/${env.GITHUB_REPO}/contents/${encodeURI(path)}`, {
        method: 'PUT',
        body: JSON.stringify({
            message: `Onlog upload: ${safeName}`,
            content: xlsxBase64,
            committer: { name: 'onlog-uploader', email: 'noreply@vesti.mobi' },
        }),
    });

    // 2) Disparar o workflow
    await gh(env, `/repos/${env.GITHUB_OWNER}/${env.GITHUB_REPO}/actions/workflows/${env.WORKFLOW_FILE}/dispatches`, {
        method: 'POST',
        body: JSON.stringify({
            ref: 'main',
            inputs: { xlsx_path: path },
        }),
    });

    return json({ ok: true, path, message: 'Planilha recebida. Build em andamento - dashboard atualiza em ~2-3 min.' });
}

async function handleStatus(env) {
    // Lista os 5 ultimos runs do workflow
    const runs = await gh(env, `/repos/${env.GITHUB_OWNER}/${env.GITHUB_REPO}/actions/workflows/${env.WORKFLOW_FILE}/runs?per_page=5`);
    return json({
        runs: (runs.workflow_runs || []).map(r => ({
            id: r.id,
            status: r.status,
            conclusion: r.conclusion,
            createdAt: r.created_at,
            url: r.html_url,
        })),
    });
}

export default {
    async fetch(req, env) {
        if (req.method === 'OPTIONS') return new Response(null, { headers: CORS });
        const url = new URL(req.url);
        try {
            if (req.method === 'POST' && url.pathname === '/upload') return await handleUpload(req, env);
            if (req.method === 'GET' && url.pathname === '/status') return await handleStatus(env);
            return json({ error: 'rota nao encontrada' }, 404);
        } catch (e) {
            return json({ error: e.message || String(e) }, 500);
        }
    },
};
