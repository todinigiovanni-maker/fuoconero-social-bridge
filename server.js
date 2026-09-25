import http from "node:http";
import crypto from "node:crypto";

const PORT=process.env.PORT||10000;
const BASE=(process.env.FNS_BASE_URL||"https://fuoconero.com").replace(/\/$/,"");
const API="/wp-json/fuoconero-social/v2";

function json(res,status,data){res.writeHead(status,{"content-type":"application/json; charset=utf-8"});res.end(JSON.stringify(data));}
function body(req){return new Promise((resolve,reject)=>{let s="";req.on("data",c=>{s+=c;if(s.length>2_000_000)req.destroy();});req.on("end",()=>resolve(s));req.on("error",reject);});}
function auth(method,route,raw){
 const key=process.env.FNS_KEY, secret=process.env.FNS_SECRET;
 if(!key||!secret) throw new Error("FNS credentials not configured");
 const ts=Math.floor(Date.now()/1000).toString();
 const nonce=crypto.randomBytes(16).toString("hex");
 const hash=crypto.createHash("sha256").update(raw).digest("hex");
 const canonical=[BASE,method,route,ts,nonce,hash].join("\n");
 const sig=crypto.createHmac("sha256",Buffer.from(secret,"hex")).update(canonical).digest("hex");
 return {"Content-Type":"application/json","X-FNS-Key":key,"X-FNS-Timestamp":ts,"X-FNS-Nonce":nonce,"X-FNS-Signature":sig};
}
async function wp(method,route,payload){
 const raw=payload===undefined?"":JSON.stringify(payload);
 const r=await fetch(BASE+route,{method,headers:auth(method,route,raw),body:method==="GET"?undefined:raw});
 const t=await r.text(); let data; try{data=JSON.parse(t)}catch{data={raw:t}};
 return {status:r.status,data};
}
const server=http.createServer(async(req,res)=>{
 try{
  const u=new URL(req.url,"http://localhost");
  if(req.method==="GET"&&u.pathname==="/health") return json(res,200,{ok:true,service:"fuoconero-social-bridge",mode:"prepare-status"});
  if(req.method==="GET"&&u.pathname==="/capabilities"){const x=await wp("GET",API+"/capabilities");return json(res,x.status,x.data);}
  if(req.method==="POST"&&u.pathname==="/storage/drive"){const raw=await body(req);const p=JSON.parse(raw||"{}");const x=await wp("POST",API+"/storage/drive",p);return json(res,x.status,x.data);}
  if(req.method==="POST"&&u.pathname==="/prepare"){const raw=await body(req);const p=JSON.parse(raw||"{}");const x=await wp("POST",API+"/prepare",p);return json(res,x.status,x.data);}
  const m=u.pathname.match(/^\/jobs\/([^/]+)$/);
  if(req.method==="GET"&&m){const x=await wp("GET",API+"/jobs/"+encodeURIComponent(m[1]));return json(res,x.status,x.data);}
  return json(res,404,{error:"not_found"});
 }catch(e){return json(res,500,{error:"bridge_error",message:e.message});}
});
server.listen(PORT,()=>console.log("Fuoconero Social Bridge listening on",PORT));