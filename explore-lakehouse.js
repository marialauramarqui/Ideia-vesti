const https=require('https'),qs=require('querystring'),{Connection,Request}=require('tedious');
const RT=process.env.FABRIC_REFRESH_TOKEN||'',TID=process.env.FABRIC_TENANT_ID||'';
function req(o,b){return new Promise((res,rej)=>{const r=https.request(o,resp=>{const c=[];resp.on('data',d=>c.push(d));resp.on('end',()=>res({status:resp.statusCode,body:Buffer.concat(c).toString()}));});r.on('error',rej);if(b)r.write(b);r.end();});}
function runSQL(token,query,label){return new Promise((resolve,reject)=>{console.log('  '+label+'...');const conn=new Connection({server:'7sowj2vsfd6efgf3phzgjfmvaq-nrdsskmspnteherwztit766zc4.datawarehouse.fabric.microsoft.com',authentication:{type:'azure-active-directory-access-token',options:{token}},options:{database:'VestiHouse',encrypt:true,port:1433,requestTimeout:120000}});const rows=[];conn.on('connect',err=>{if(err){reject(err);return;}const request=new Request(query,err=>{if(err)reject(err);conn.close();});request.on('row',columns=>{const row={};columns.forEach(col=>{row[col.metadata.colName]=col.value;});rows.push(row);});request.on('requestCompleted',()=>{console.log('  '+label+': '+rows.length+' rows');resolve(rows);});conn.execSql(request);});conn.connect();});}
async function main(){
    const pb=qs.stringify({client_id:'1950a258-227b-4e31-a9cf-717495945fc2',grant_type:'refresh_token',refresh_token:RT,scope:'https://database.windows.net/.default offline_access'});
    const tr=await req({hostname:'login.microsoftonline.com',path:'/'+TID+'/oauth2/v2.0/token',method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded','Content-Length':Buffer.byteLength(pb)}},pb);
    const token=JSON.parse(tr.body).access_token;

    // Check MongoDB_Pedidos_Geral
    console.log('=== MongoDB_Pedidos_Geral ===');
    const cols=await runSQL(token,"SELECT TOP 1 * FROM dbo.MongoDB_Pedidos_Geral",'cols');
    if(cols[0]) Object.entries(cols[0]).forEach(([k,v])=>console.log('  '+k+' ('+typeof v+') = '+JSON.stringify(v).substring(0,80)));

    const cnt=await runSQL(token,"SELECT COUNT(*) as cnt FROM dbo.MongoDB_Pedidos_Geral",'count');
    console.log('Total rows:',cnt[0]?cnt[0].cnt:'?');

    const dr=await runSQL(token,"SELECT MIN(created_at) as mn, MAX(created_at) as mx FROM dbo.MongoDB_Pedidos_Geral",'date range');
    if(dr[0]) console.log('From:',dr[0].mn,'To:',dr[0].mx);
}
main().catch(e=>console.error('FATAL:',e));
