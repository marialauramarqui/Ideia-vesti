const https=require('https'),fs=require('fs'),path=require('path'),qs=require('querystring');
const DIR=__dirname;
function loadEnv(){const f=fs.readFileSync(path.join(DIR,'.env'),'utf-8');const E={};f.split('\n').forEach(l=>{const m=l.match(/^([^#=]+)=(.*)$/);if(m)E[m[1].trim()]=m[2].trim();});return E;}
function hr(o,b){return new Promise((r,j)=>{const q=https.request(o,res=>{const c=[];res.on('data',d=>c.push(d));res.on('end',()=>r({s:res.statusCode,b:Buffer.concat(c).toString()}));});q.on('error',j);if(b)q.write(b);q.end();});}

async function main(){
    const ENV=loadEnv();
    const body=qs.stringify({client_id:'14d82eec-204b-4c2f-b7e8-296a70dab67e',grant_type:'refresh_token',refresh_token:ENV.FABRIC_REFRESH_TOKEN,scope:'https://analysis.windows.net/powerbi/api/.default'});
    const tr=await hr({hostname:'login.microsoftonline.com',path:'/'+ENV.FABRIC_TENANT_ID+'/oauth2/v2.0/token',method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded','Content-Length':Buffer.byteLength(body)}},body);
    const td=JSON.parse(tr.b);
    if(td.refresh_token){let env=fs.readFileSync(path.join(DIR,'.env'),'utf-8');env=env.replace(/^FABRIC_REFRESH_TOKEN=.*$/m,'FABRIC_REFRESH_TOKEN='+td.refresh_token);fs.writeFileSync(path.join(DIR,'.env'),env,'utf-8');}
    const token=td.access_token;
    if(!token){console.error('No token');return;}

    const WS='aced753a-0f0e-4bcf-9264-72f6496cf2cf';

    // 1. Get report dataset
    const rRes=await hr({hostname:'api.powerbi.com',path:'/v1.0/myorg/groups/'+WS+'/reports/4a2d7b2c-875d-4f57-97ac-8c90229c1d2d',method:'GET',headers:{'Authorization':'Bearer '+token}});
    const report=JSON.parse(rRes.b);
    console.log('Report:', report.name, '| datasetId:', report.datasetId);
    const DS=report.datasetId;

    // 2. List datasets
    const dsRes=await hr({hostname:'api.powerbi.com',path:'/v1.0/myorg/groups/'+WS+'/datasets',method:'GET',headers:{'Authorization':'Bearer '+token}});
    const datasets=JSON.parse(dsRes.b).value||[];
    console.log('\nDatasets:');
    datasets.forEach(d=>console.log('  '+d.id+' | '+d.name));

    // 3. Try to find Integração table
    const tables=['Integração','Integracao','Integration','Config Empresas','Cadastros Empresas','Empresas','Config'];
    for(const t of tables){
        const dax="EVALUATE TOPN(3, '"+t+"')";
        const qB=JSON.stringify({queries:[{query:dax}],serializerSettings:{includeNulls:true}});
        const qR=await hr({hostname:'api.powerbi.com',path:'/v1.0/myorg/groups/'+WS+'/datasets/'+DS+'/executeQueries',method:'POST',headers:{'Authorization':'Bearer '+token,'Content-Type':'application/json','Content-Length':Buffer.byteLength(qB)}},qB);
        try{
            const d=JSON.parse(qR.b);
            if(d.results&&d.results[0].tables[0].rows&&d.results[0].tables[0].rows.length>0){
                const rows=d.results[0].tables[0].rows;
                const cols=Object.keys(rows[0]);
                console.log('\nFOUND TABLE: '+t+' ('+cols.length+' cols)');
                cols.forEach(c=>console.log('  '+c));
                console.log('Sample:', JSON.stringify(rows[0]).substring(0,500));
            }
        }catch(e){}
    }

    // 4. Also check if Integração is a column in Config Empresas
    const dax2="EVALUATE TOPN(5, SELECTCOLUMNS('Config Empresas', \"id\", 'Config Empresas'[companies.id], \"nome\", 'Config Empresas'[companies.company_name], \"integ\", 'Config Empresas'[settings.erpIntegration]))";
    const q2=JSON.stringify({queries:[{query:dax2}],serializerSettings:{includeNulls:true}});
    const r2=await hr({hostname:'api.powerbi.com',path:'/v1.0/myorg/groups/'+WS+'/datasets/'+DS+'/executeQueries',method:'POST',headers:{'Authorization':'Bearer '+token,'Content-Type':'application/json','Content-Length':Buffer.byteLength(q2)}},q2);
    try{
        const d2=JSON.parse(r2.b);
        if(d2.results){
            console.log('\nConfig Empresas erpIntegration:', JSON.stringify(d2.results[0].tables[0].rows));
        }else{
            console.log('\nerpIntegration failed, trying other columns...');
        }
    }catch(e){console.log('Parse error');}

    // 5. Search for integration-related columns in Config Empresas
    const dax3="EVALUATE TOPN(1, 'Config Empresas')";
    const q3=JSON.stringify({queries:[{query:dax3}],serializerSettings:{includeNulls:true}});
    const r3=await hr({hostname:'api.powerbi.com',path:'/v1.0/myorg/groups/'+WS+'/datasets/'+DS+'/executeQueries',method:'POST',headers:{'Authorization':'Bearer '+token,'Content-Type':'application/json','Content-Length':Buffer.byteLength(q3)}},q3);
    try{
        const d3=JSON.parse(r3.b);
        if(d3.results&&d3.results[0].tables[0].rows.length>0){
            const cols=Object.keys(d3.results[0].tables[0].rows[0]);
            const integCols=cols.filter(c=>c.toLowerCase().includes('integ')||c.toLowerCase().includes('erp'));
            console.log('\nIntegration-related cols in Config Empresas:');
            integCols.forEach(c=>console.log('  '+c+' = '+d3.results[0].tables[0].rows[0][c]));
        }
    }catch(e){}
}
main().catch(e=>console.error(e.message));
