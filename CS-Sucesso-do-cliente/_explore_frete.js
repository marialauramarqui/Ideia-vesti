const https=require('https'),fs=require('fs'),path=require('path'),qs=require('querystring');
const DIR=__dirname;

function loadEnv(){
    const envFile=fs.readFileSync(path.join(DIR,'.env'),'utf-8');
    const ENV={};
    envFile.split('\n').forEach(l=>{const m=l.match(/^([^#=]+)=(.*)$/);if(m)ENV[m[1].trim()]=m[2].trim();});
    return ENV;
}

function hr(o,b){
    return new Promise((r,j)=>{
        const q=https.request(o,res=>{
            const c=[];
            res.on('data',d=>c.push(d));
            res.on('end',()=>{
                const raw=Buffer.concat(c).toString();
                r({s:res.statusCode,b:raw});
            });
        });
        q.on('error',j);
        if(b)q.write(b);
        q.end();
    });
}

async function getToken(){
    const ENV=loadEnv();
    const body=qs.stringify({
        client_id:'14d82eec-204b-4c2f-b7e8-296a70dab67e',
        grant_type:'refresh_token',
        refresh_token:ENV.FABRIC_REFRESH_TOKEN,
        scope:'https://analysis.windows.net/powerbi/api/.default'
    });
    const tr=await hr({
        hostname:'login.microsoftonline.com',
        path:'/'+ENV.FABRIC_TENANT_ID+'/oauth2/v2.0/token',
        method:'POST',
        headers:{'Content-Type':'application/x-www-form-urlencoded','Content-Length':Buffer.byteLength(body)}
    },body);
    const td=JSON.parse(tr.b);
    if(td.refresh_token){
        let env=fs.readFileSync(path.join(DIR,'.env'),'utf-8');
        env=env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m,'FABRIC_REFRESH_TOKEN='+td.refresh_token);
        fs.writeFileSync(path.join(DIR,'.env'),env,'utf-8');
    }
    return td.access_token;
}

async function daxQ(token, ws, ds, query){
    const qB=JSON.stringify({queries:[{query:query}],serializerSettings:{includeNulls:true}});
    const r=await hr({
        hostname:'api.powerbi.com',
        path:'/v1.0/myorg/groups/'+ws+'/datasets/'+ds+'/executeQueries',
        method:'POST',
        headers:{'Authorization':'Bearer '+token,'Content-Type':'application/json','Content-Length':Buffer.byteLength(qB)}
    },qB);
    try{
        const d=JSON.parse(r.b);
        if(d.results && d.results[0] && d.results[0].tables[0] && d.results[0].tables[0].rows)
            return d.results[0].tables[0].rows;
    }catch(e){}
    return null;
}

async function main(){
    const token=await getToken();
    if(!token){console.error('No token');return;}
    console.log('Token OK');

    const WS1='2929476c-7b92-4366-9236-ccd13ffbd917';
    const DS1='583e34d7-6dd1-467b-86aa-3b74cfe1ca56';

    // 1. Check report dataset
    const rRes=await hr({hostname:'api.powerbi.com',path:'/v1.0/myorg/groups/'+WS1+'/reports/5a92eabf-63f2-4626-bd6c-9af02fbb8f00',method:'GET',headers:{'Authorization':'Bearer '+token}});
    try{
        const report=JSON.parse(rRes.b);
        console.log('Report:', report.name, '| DS:', report.datasetId);
    }catch(e){console.log('Report info error:', rRes.b.substring(0,200));}

    // 2. Check Invoices Produto values (Frete might be a Produto type)
    console.log('\n--- Checking Produto values in Invoices ---');
    const prods=await daxQ(token,WS1,DS1,"EVALUATE SUMMARIZECOLUMNS(Invoices[Produto], \"cnt\", COUNTROWS(Invoices))");
    if(prods) console.log('Produto values:', JSON.stringify(prods));
    else console.log('Produto query failed');

    // 3. Check if Frete is a separate invoice product
    console.log('\n--- Checking Frete invoices ---');
    const freteInv=await daxQ(token,WS1,DS1,"EVALUATE TOPN(5, FILTER(Invoices, CONTAINSSTRING(Invoices[Produto], \"Frete\")))");
    if(freteInv && freteInv.length>0){
        console.log('Frete invoices found:', freteInv.length);
        console.log('Sample:', JSON.stringify(freteInv[0]));
    } else {
        console.log('No Frete in Produto');
        // Try filter on Plano
        const fretePlano=await daxQ(token,WS1,DS1,"EVALUATE TOPN(5, FILTER(Invoices, CONTAINSSTRING(Invoices[Plano], \"Frete\")))");
        if(fretePlano && fretePlano.length>0){
            console.log('Frete in Plano found:', fretePlano.length);
            console.log('Sample:', JSON.stringify(fretePlano[0]));
        } else {
            console.log('No Frete in Plano either');
        }
    }

    // 4. Get Subscriptions/Items from Iugu (maybe Frete Ativo is there)
    console.log('\n--- Searching all tables for Frete ---');
    const tableNames=['Subscription_items','Items','Subscriptions','Planos','Measures','Subscription'];
    for(const t of tableNames){
        const rows=await daxQ(token,WS1,DS1,"EVALUATE TOPN(1, '"+t+"')");
        if(rows && rows.length>0){
            const cols=Object.keys(rows[0]);
            console.log('TABLE '+t+': '+cols.join(', '));
        }
    }

    // 5. Monthly freight from second workspace
    console.log('\n--- Freight from Relatorio Confeccoes ---');
    const WS2='0f5bd202-471f-482d-bf3d-38295044d7db';
    const DS2='92a0cf18-2bfd-4b02-873f-615df3ce2d7f';
    const freteSample=await daxQ(token,WS2,DS2,"EVALUATE TOPN(5, SUMMARIZECOLUMNS(Merged[Companies.company_name], \"TotalFrete\", SUM(Merged[Valor Frete])), [TotalFrete], DESC)");
    if(freteSample) console.log('Top frete:', JSON.stringify(freteSample));

    // Companies with frete > 0 count
    const freteCount=await daxQ(token,WS2,DS2,"EVALUATE ROW(\"empresas\", CALCULATE(DISTINCTCOUNT(Merged[Companies.company_name]), Merged[Valor Frete]>0), \"total_frete\", CALCULATE(SUM(Merged[Valor Frete]), Merged[Valor Frete]>0))");
    if(freteCount) console.log('Frete stats:', JSON.stringify(freteCount));
}
main().catch(e=>console.error('FATAL:',e.message));
