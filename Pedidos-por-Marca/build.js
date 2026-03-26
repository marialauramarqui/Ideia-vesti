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
    console.log('Building pedidos por marca...\n');

    // Empresas
    const empresas = {};
    const empByDom = {};
    await readCSV('Cadastros Empresas.csv', r => {
        const id = r['Id Empresa']; if (!id) return;
        empresas[id] = { id, nome: r['Nome Fantasia'] || r['Nome do Dominio'] || '', cnpj: r['CNPJ'] || '', anjo: r['Anjo'] || '', canal: r['Canal de Vendas'] || '', idDom: r['Id Dominio'] || '' };
        if (r['Id Dominio']) empByDom[r['Id Dominio']] = empresas[id];
    });

    // Marcas
    const marcasByCnpj = {};
    await readCSV('Marcas e Planos.csv', r => {
        const cnpj = r['CPFCNPJ'] || '';
        if (cnpj) marcasByCnpj[cnpj] = { marca: r['MARCA'] || '', plano: r['PLANO'] || '' };
    });

    // Associar marca a empresa
    for (const e of Object.values(empresas)) {
        const cnpjNum = (e.cnpj || '').replace(/[.\-\/]/g, '');
        const m = marcasByCnpj[cnpjNum];
        e.marca = m ? m.marca : '';
        e.plano = m ? m.plano : '';
    }

    // Pedidos - cada pedido individual, agrupado por empresa
    // Formato compacto: [data, valor, status, metodo_pagamento, idPedido, nomeDominio]
    // status: P=pago, C=cancelado, E=pendente, O=outro
    const pedidosPorEmp = {};
    await readCSV('Merged Pedidos.csv', r => {
        const empId = r['ID Empresa'];
        const emp = empresas[empId] || (r['ID Dominio'] ? empByDom[r['ID Dominio']] : null);
        if (!emp) return;
        const id = emp.id;
        if (!pedidosPorEmp[id]) pedidosPorEmp[id] = [];
        const status = r['Pago'] === 'True' ? 'P' : r['Cancelado'] === 'True' ? 'C' : r['Pendente'] === 'True' ? 'E' : 'O';
        // [data(YYYY-MM-DD), valor, status, metodo]
        const dt = (r['Data Criacao'] || '').substring(0, 10);
        const met = (r['docs.payment.method'] || '').substring(0, 15);
        pedidosPorEmp[id].push([dt, Math.round((parseFloat(r['Total']) || 0) * 100) / 100, status, met]);
    });

    // Build empresa list (only with orders)
    const empList = Object.values(empresas)
        .filter(e => pedidosPorEmp[e.id] && pedidosPorEmp[e.id].length > 0)
        .map(e => ({
            id: e.id, nome: e.nome, cnpj: e.cnpj, marca: e.marca, plano: e.plano,
            anjo: e.anjo, canal: e.canal, qtd: pedidosPorEmp[e.id].length,
        }))
        .sort((a, b) => b.qtd - a.qtd);

    // Sort each company's orders by date desc
    for (const id of Object.keys(pedidosPorEmp)) {
        pedidosPorEmp[id].sort((a, b) => b[0].localeCompare(a[0]));
    }

    // Save empresa list (small file)
    const indexJs = 'const DADOS=' + JSON.stringify({ empresas: empList, gerado: new Date().toISOString() }) + ';';
    fs.writeFileSync(path.join(__dirname, 'dados.js'), indexJs, 'utf-8');
    console.log('\nEmpresas com pedidos: ' + empList.length);
    console.log('dados.js: ' + (indexJs.length / 1024).toFixed(0) + ' KB');

    // Save pedidos in chunks (by company, in batch files)
    const dataDir = path.join(__dirname, 'data');
    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir);
    const BATCH = 50; // companies per file
    const empIds = empList.map(e => e.id);
    let totalPed = 0;
    for (let i = 0; i < empIds.length; i += BATCH) {
        const batch = {};
        for (let j = i; j < Math.min(i + BATCH, empIds.length); j++) {
            const id = empIds[j];
            batch[id] = pedidosPorEmp[id] || [];
            totalPed += batch[id].length;
        }
        const fn = 'chunk_' + Math.floor(i / BATCH) + '.js';
        fs.writeFileSync(path.join(dataDir, fn), 'loadChunk(' + JSON.stringify(batch) + ');', 'utf-8');
    }
    // Save mapping: empresa id -> chunk number
    const chunkMap = {};
    empIds.forEach((id, i) => { chunkMap[id] = Math.floor(i / BATCH); });
    fs.writeFileSync(path.join(__dirname, 'chunks.js'), 'const CHUNKS=' + JSON.stringify(chunkMap) + ';', 'utf-8');
    console.log('Total pedidos: ' + totalPed);
    console.log('Chunks: ' + Math.ceil(empIds.length / BATCH) + ' files in data/');
}
main().catch(e => { console.error(e); process.exit(1); });
