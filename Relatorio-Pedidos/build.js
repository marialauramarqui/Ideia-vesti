const fs = require('fs');
const path = require('path');
const readline = require('readline');
const DIR = path.join(__dirname, '..', 'CS-Sucesso-do-cliente');

function parseCSVLine(line) {
    const fields = []; let current = ''; let inQ = false;
    for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (inQ) { if (ch === '"' && line[i+1] === '"') { current += '"'; i++; } else if (ch === '"') inQ = false; else current += ch; }
        else { if (ch === '"') inQ = true; else if (ch === ',') { fields.push(current.trim()); current = ''; } else current += ch; }
    }
    fields.push(current.trim());
    return fields;
}

async function readCSV(filename, onRow) {
    const fp = path.join(DIR, filename);
    if (!fs.existsSync(fp)) { console.log('SKIP: ' + filename); return; }
    const rl = readline.createInterface({ input: fs.createReadStream(fp, { encoding: 'utf-8' }), crlfDelay: Infinity });
    let headers = null; let count = 0;
    for await (const line of rl) {
        if (!line.trim()) continue;
        const fields = parseCSVLine(line);
        if (!headers) { headers = fields; continue; }
        const row = {}; headers.forEach((h, i) => { row[h] = fields[i] || ''; });
        onRow(row); count++;
    }
    console.log(filename + ': ' + count + ' rows');
}

async function main() {
    console.log('Building...\n');

    // Empresas
    const empresas = {};
    await readCSV('Cadastros Empresas.csv', r => {
        const id = r['Id Empresa']; if (!id) return;
        empresas[id] = {
            id, nome: r['Nome Fantasia'] || r['Nome do Dominio'] || '', cnpj: r['CNPJ'] || '',
            anjo: r['Anjo'] || '', canal: r['Canal de Vendas'] || '',
            temInteg: r['Tem Integração?'] || '', tipoInteg: r['Domains.integration_type'] || '',
            dominio: r['Nome do Dominio'] || '', idDominio: r['Id Dominio'] || '',
        };
    });

    // Config
    await readCSV('Config Empresas.csv', r => {
        const id = r['docs.companyId'];
        if (id && empresas[id]) {
            empresas[id].cartao = r['docs.creditCard.isEnabled'] === 'True';
            empresas[id].pix = r['docs.pix.isEnabled'] === 'True';
        }
    });

    // Marcas e Planos (by CNPJ)
    const marcas = {};
    await readCSV('Marcas e Planos.csv', r => {
        const cnpj = r['CPFCNPJ'] || '';
        if (cnpj) marcas[cnpj] = { marca: r['MARCA'], plano: r['PLANO'], cobrado: parseFloat(r['TOTAL_COBRADO']) || 0 };
    });

    // Pedidos - agregar por empresa
    const pedEmp = {}; // {empId: {total,pagos,canc,pend,val,valPag,meses:{mes:{tot,pag,canc,pend,val,valPag}}}}
    await readCSV('Merged Pedidos.csv', r => {
        const empId = r['ID Empresa']; if (!empId || !empresas[empId]) return;
        if (!pedEmp[empId]) pedEmp[empId] = { total: 0, pagos: 0, canc: 0, pend: 0, val: 0, valPag: 0, meses: {} };
        const p = pedEmp[empId];
        const tot = parseFloat(r['Total']) || 0;
        const isPago = r['Pago'] === 'True';
        const isCanc = r['Cancelado'] === 'True';
        const isPend = r['Pendente'] === 'True';
        p.total++; p.val += tot;
        if (isPago) { p.pagos++; p.valPag += tot; }
        if (isCanc) p.canc++;
        if (isPend) p.pend++;
        const dt = (r['Data Criacao'] || '').match(/(\d{4})-(\d{2})/);
        if (dt) {
            const mk = dt[1] + '-' + dt[2];
            if (!p.meses[mk]) p.meses[mk] = { tot: 0, pag: 0, canc: 0, pend: 0, val: 0, valPag: 0 };
            const m = p.meses[mk]; m.tot++; m.val += tot;
            if (isPago) { m.pag++; m.valPag += tot; }
            if (isCanc) m.canc++;
            if (isPend) m.pend++;
        }
    });

    // Montar lista
    const MAX_TICKET = 500000;
    const lista = Object.values(empresas).map(e => {
        const p = pedEmp[e.id] || { total: 0, pagos: 0, canc: 0, pend: 0, val: 0, valPag: 0, meses: {} };
        const cnpjNum = (e.cnpj || '').replace(/[.\-\/]/g, '');
        const marca = marcas[cnpjNum];
        // Filtrar outliers
        if (p.total > 0 && p.val / p.total > MAX_TICKET) { p.val = 0; p.valPag = 0; }
        return {
            nome: e.nome, cnpj: e.cnpj, anjo: e.anjo, canal: e.canal,
            cartao: e.cartao ? 'Sim' : 'Não', pix: e.pix ? 'Sim' : 'Não',
            integ: e.temInteg, tipoInteg: e.tipoInteg,
            plano: marca ? marca.plano : '', marcaNome: marca ? marca.marca : '',
            cobrado: marca ? marca.cobrado : 0,
            pedidos: p.total, pagos: p.pagos, cancelados: p.canc, pendentes: p.pend,
            valTotal: Math.round(p.val), valPagos: Math.round(p.valPag),
            meses: Object.entries(p.meses).sort(([a],[b]) => a.localeCompare(b)).map(([m, d]) => [m, d.tot, d.pag, d.canc, d.pend, Math.round(d.val), Math.round(d.valPag)]),
        };
    }).filter(e => e.pedidos > 0).sort((a, b) => b.pedidos - a.pedidos);

    const output = { empresas: lista, gerado: new Date().toISOString() };
    fs.writeFileSync(path.join(__dirname, 'dados.js'), 'const DADOS=' + JSON.stringify(output) + ';', 'utf-8');
    console.log('\nEmpresas com pedidos: ' + lista.length);
    console.log('Output: dados.js (' + (fs.statSync(path.join(__dirname, 'dados.js')).size / 1024).toFixed(0) + ' KB)');
}
main().catch(e => { console.error(e); process.exit(1); });
