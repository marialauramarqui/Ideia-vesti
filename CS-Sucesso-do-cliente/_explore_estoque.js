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

    const WS='786bfd95-0733-4fcb-aa84-ef2c97518959';

    // 1. Get report info
    const rRes=await hr({hostname:'api.powerbi.com',path:'/v1.0/myorg/groups/'+WS+'/reports/7ab830dd-0881-469e-b1a1-13c973c5c071',method:'GET',headers:{'Authorization':'Bearer '+token}});
    const report=JSON.parse(rRes.b);
    console.log('Report:', report.name, '| DS:', report.datasetId);
    const DS=report.datasetId;

    // 2. List datasets
    const dsRes=await hr({hostname:'api.powerbi.com',path:'/v1.0/myorg/groups/'+WS+'/datasets',method:'GET',headers:{'Authorization':'Bearer '+token}});
    const datasets=JSON.parse(dsRes.b).value||[];
    console.log('\nDatasets:');
    datasets.forEach(d=>console.log('  '+d.id+' | '+d.name));

    // 3. Find tables with estoque/status
    const tables=['Companies','Empresas','Cadastros','Config','Domains','Subscriptions','Customers','Company','Domain'];
    for(const t of tables){
        const dax="EVALUATE TOPN(1, '"+t+"')";
        const qBody=JSON.stringify({queries:[{query:dax}],serializerSettings:{includeNulls:true}});
        const qRes=await hr({hostname:'api.powerbi.com',path:'/v1.0/myorg/groups/'+WS+'/datasets/'+DS+'/executeQueries',method:'POST',headers:{'Authorization':'Bearer '+token,'Content-Type':'application/json','Content-Length':Buffer.byteLength(qBody)}},qBody);
        try{
            const d=JSON.parse(qRes.b);
            if(d.results&&d.results[0].tables[0].rows&&d.results[0].tables[0].rows.length>0){
                const cols=Object.keys(d.results[0].tables[0].rows[0]);
                const hasTarget=cols.some(c=>c.toLowerCase().includes('estoque')||c.toLowerCase().includes('status'));
                console.log('\nTABLE: '+t+' ('+cols.length+' cols)'+(hasTarget?' *** MATCH ***':''));
                if(hasTarget) cols.forEach(c=>console.log('  '+c+' = '+JSON.stringify(d.results[0].tables[0].rows[0][c]).substring(0,60)));
                else cols.forEach(c=>console.log('  '+c));
            }
        }catch(e){}
    }
}
main().catch(e=>console.error(e.message));
