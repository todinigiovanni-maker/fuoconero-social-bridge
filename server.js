import http from "node:http";
import crypto from "node:crypto";

const PORT=process.env.PORT||10000;
const BASE=(process.env.FNS_BASE_URL||"https://fuoconero.com").replace(/\/$/,"");
const REST="/wp-json/fuoconero-social/v2";
const SIGN="/fuoconero-social/v2";
const TEST_DRIVE_ID="1R_EHFzEAE9JfyaZP9B6nIoEte2oKDXVg";

function json(res,status,data){res.writeHead(status,{"content-type":"application/json; charset=utf-8"});res.end(JSON.stringify(data));}
function body(req){return new Promise((resolve,reject)=>{let s="";req.on("data",c=>{s+=c;if(s.length>2_000_000)req.destroy();});req.on("end",()=>resolve(s));req.on("error",reject);});}
function auth(method,signRoute,raw){
 const key=process.env.FNS_KEY, secret=process.env.FNS_SECRET;
 if(!key||!secret) throw new Error("FNS credentials not configured");
 if(!/^[0-9a-fA-F]{64}$/.test(secret)) throw new Error("FNS_SECRET must be 64 hex chars");
 const ts=Math.floor(Date.now()/1000).toString();
 const nonce=crypto.randomBytes(16).toString("hex");
 const hash=crypto.createHash("sha256").update(raw).digest("hex");
 const canonical=[BASE,method,signRoute,ts,nonce,hash].join("\n");
 const sig=crypto.createHmac("sha256",Buffer.from(secret,"hex")).update(canonical).digest("hex");
 return {"Content-Type":"application/json","X-FNS-Key":key,"X-FNS-Timestamp":ts,"X-FNS-Nonce":nonce,"X-FNS-Signature":sig};
}
async function wp(method,path,payload){
 const raw=payload===undefined?"":JSON.stringify(payload);
 const restRoute=REST+path, signRoute=SIGN+path;
 const r=await fetch(BASE+restRoute,{method,headers:auth(method,signRoute,raw),body:method==="GET"?undefined:raw});
 const t=await r.text(); let data; try{data=JSON.parse(t)}catch{data={raw:t}};
 return {status:r.status,data};
}
async function selfTest(){
 const stamp=Date.now();
 console.log("SELFTEST start");
 const cap=await wp("GET","/capabilities");
 console.log("SELFTEST capabilities",cap.status,JSON.stringify(cap.data));
 if(cap.status<200||cap.status>=300) return;
 const st=await wp("POST","/storage/drive",{request_id:`render-drive-${stamp}`,drive_file_id:TEST_DRIVE_ID});
 console.log("SELFTEST storage",st.status,JSON.stringify(st.data));
 if(st.status<200||st.status>=300||!st.data?.storage_id) return;
 const prep=await wp("POST","/prepare",{
   request_id:`render-prepare-${stamp}`,
   storage_id:st.data.storage_id,
   title:"Sogni confusi",
   caption:"Test tecnico Render — nessuna pubblicazione.",
   facebook_caption:"Test tecnico Render — nessuna pubblicazione.",
   targets:["ig_reel"]
 });
 console.log("SELFTEST prepare",prep.status,JSON.stringify(prep.data));
 const jobId=prep.data?.job_id;
 if(jobId){
   const job=await wp("GET","/jobs/"+encodeURIComponent(jobId));
   console.log("SELFTEST status",job.status,JSON.stringify(job.data));
 }
 console.log("SELFTEST end — publish endpoint not implemented");
}
const server=http.createServer(async(req,res)=>{
 try{
  const u=new URL(req.url,"http://localhost");
  if(req.method==="GET"&&u.pathname==="/health") return json(res,200,{ok:true,service:"fuoconero-social-bridge",mode:"prepare-status"});
  if(req.method==="GET"&&u.pathname==="/capabilities"){const x=await wp("GET","/capabilities");return json(res,x.status,x.data);}
  if(req.method==="POST"&&u.pathname==="/storage/drive"){const raw=await body(req);const p=JSON.parse(raw||"{}");const x=await wp("POST","/storage/drive",p);return json(res,x.status,x.data);}
  if(req.method==="POST"&&u.pathname==="/prepare"){const raw=await body(req);const p=JSON.parse(raw||"{}");const x=await wp("POST","/prepare",p);return json(res,x.status,x.data);}
  const m=u.pathname.match(/^\/jobs\/([^/]+)$/);
  if(req.method==="GET"&&m){const x=await wp("GET","/jobs/"+encodeURIComponent(m[1]));return json(res,x.status,x.data);}
  return json(res,404,{error:"not_found"});
 }catch(e){return json(res,500,{error:"bridge_error",message:e.message});}
});
server.listen(PORT,()=>{
 console.log("Fuoconero Social Bridge listening on",PORT);
 if(process.env.FNS_SELFTEST==="1") selfTest().catch(e=>console.error("SELFTEST error",e.message));
});