import http from "node:http";
import {worker} from "./worker.js";
import crypto from "node:crypto";
import {readFile} from "node:fs/promises";

const PORT=process.env.PORT||10000;
const BASE=(process.env.FNS_BASE_URL||"https://fuoconero.com").replace(/\/$/,"");
const REST="/wp-json/fuoconero-social/v2";
const SIGN="/fuoconero-social/v2";

function json(res,status,data){res.writeHead(status,{"content-type":"application/json; charset=utf-8"});res.end(JSON.stringify(data));}
function body(req){return new Promise((resolve,reject)=>{let s="";req.on("data",c=>{s+=c;if(s.length>2_000_000)req.destroy();});req.on("end",()=>resolve(s));req.on("error",reject);});}
function auth(method,signRoute,raw){
 const key=process.env.FNS_KEY, secret=process.env.FNS_SECRET;
 if(!key||!secret) throw new Error("FNS credentials not configured");
 if(!/^[0-9a-fA-F]{64}$/.test(secret)) throw new Error("FNS_SECRET must be 64 hex chars");
 const ts=Math.floor(Date.now()/1000).toString(), nonce=crypto.randomBytes(16).toString("hex");
 const hash=crypto.createHash("sha256").update(raw).digest("hex");
 const canonical=[BASE,method,signRoute,ts,nonce,hash].join("\n");
 const sig=crypto.createHmac("sha256",Buffer.from(secret,"hex")).update(canonical).digest("hex");
 return {"Content-Type":"application/json","X-FNS-Key":key,"X-FNS-Timestamp":ts,"X-FNS-Nonce":nonce,"X-FNS-Signature":sig};
}
async function forward(req,path,raw){
 const headers={"Content-Type":"application/json"};
 for(const name of ["X-FNS-Key","X-FNS-Timestamp","X-FNS-Nonce","X-FNS-Signature"]){
  const value=req.headers[name.toLowerCase()];
  if(typeof value!=="string"||!value||value.length>160) return {status:401,data:{error:"authentication_required"}};
  headers[name]=value;
 }
 // Preserve caller identity, raw body and signature. WordPress validates HMAC,
 // host/route binding, timestamp, durable nonce, scopes and rate limit.
 const r=await fetch(BASE+REST+path,{method:req.method,headers,body:req.method==="GET"?undefined:raw,signal:AbortSignal.timeout(90000),redirect:"error"});
 let data;try{data=await r.json();}catch{data={error:"invalid_upstream_response"};}
 return {status:r.status,data};
}
async function wp(method,path,payload){
 const raw=payload===undefined?"":JSON.stringify(payload), restRoute=REST+path, signRoute=SIGN+path;
 const r=await fetch(BASE+restRoute,{method,headers:auth(method,signRoute,raw),body:method==="GET"?undefined:raw,signal:AbortSignal.timeout(180000),redirect:"error"});
 const t=await r.text(); let data; try{data=JSON.parse(t)}catch{data={raw:t}};
 return {status:r.status,data};
}
function yesNo(value, fallback="no"){
 if(value===true||value==="yes") return "yes";
 if(value===false||value==="no") return "no";
 return fallback;
}
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function allDestinationsSucceeded(job){
 const d=Array.isArray(job?.destinations)?job.destinations:[];
 return d.length>0 && d.every(x=>x?.status==='success');
}
async function cleanupPublishedJob(jobId,storageId){
 if(!jobId||!storageId)return;
 for(let attempt=0;attempt<80;attempt++){
  await sleep(attempt===0?5000:15000);
  const job=await wp("GET","/jobs/"+encodeURIComponent(jobId));
  if(job.status!==200){console.warn("CLEANUP status unavailable",jobId,job.status);continue;}
  const destinations=Array.isArray(job.data?.destinations)?job.data.destinations:[];
  if(destinations.some(x=>x?.status==='error')){console.warn("CLEANUP retained after publication error",jobId);return;}
  if(!allDestinationsSucceeded(job.data))continue;
  const del=await wp("POST","/storage/delete",{request_id:"cleanup-"+jobId,storage_id:storageId});
  console.log("CLEANUP result",jobId,del.status,JSON.stringify(del.data));
  return;
 }
 console.warn("CLEANUP retained after timeout",jobId);
}
async function runCommand(){
 let c; try{c=JSON.parse(await readFile(new URL("./command.json",import.meta.url),"utf8"));}catch(e){console.error("COMMAND read error",e.message);return;}
 if(!c||c.action==="noop"){console.log("COMMAND idle",c?.id||"none");return;}
 if(!c.id){console.error("COMMAND invalid: missing id");return;}
 if(c.action==="drivecheck"){
  if(!c.drive_file_id){console.error("COMMAND invalid drivecheck");return;}
  // Read-only/import diagnostic against an already existing Drive MP4.
  // It never prepares, confirms or publishes.
  const r=await wp("POST","/storage/drive",{request_id:"drivecheck-"+c.id,drive_file_id:c.drive_file_id});
  console.log("COMMAND drivecheck result",c.id,r.status,JSON.stringify(r.data));
  return;
 }
 if(c.action==="render"){
  const p=c.payload;
  if(!p||typeof p!=="object"||!p.request_id||!Number.isInteger(p.post_id)||!p.category||(!p.music_title&&!p.music_id)||!p.outputs||typeof p.outputs!=="object"){
   console.error("COMMAND invalid render");return;
  }
  // Repository-triggered render is deliberately render-only. It uses the existing
  // server-side max-render identity and cannot prepare, confirm or publish.
  console.log("COMMAND render requested",c.id,"post",p.post_id);
  const created=await wp("POST","/reel-maker/render-jobs",p);
  console.log("COMMAND render result",c.id,created.status,JSON.stringify(created.data));
  if(created.status>=200&&created.status<300) void pump();
  return;
 }
 if(c.action==="confirm"){
  if(!c.job_id||!c.digest||c.confirmed!==true){console.error("COMMAND invalid confirm");return;}
  console.log("COMMAND confirm requested",c.id,"job",c.job_id);
  const job=await wp("GET","/jobs/"+encodeURIComponent(c.job_id));
  console.log("COMMAND confirm precheck",c.id,job.status,JSON.stringify(job.data));
  if(job.status!==200||job.data?.status!=="prepared"||job.data?.digest!==c.digest){console.error("COMMAND confirm blocked: job not prepared or digest mismatch");return;}
  if(process.env.FNS_ALLOW_CONFIRM!=="1"){console.log("COMMAND confirm dry-run OK",c.id,"— server-side confirm disabled");return;}
  const conf=await wp("POST","/jobs/"+encodeURIComponent(c.job_id)+"/confirm",{confirmed:true,digest:c.digest});
  console.log("COMMAND confirm result",c.id,conf.status,JSON.stringify(conf.data));
  if(conf.status>=200&&conf.status<300){
   const storageId=conf.data?.payload?.storage_id||job.data?.payload?.storage_id;
   if(storageId) void cleanupPublishedJob(c.job_id,storageId);
  }
  return;
 }
 if(c.action!=="prepare"){console.error("COMMAND rejected action",c.action);return;}
 if(!c.drive_file_id||!c.title||!Array.isArray(c.targets)||!c.targets.length){console.error("COMMAND invalid prepare");return;}
 console.log("COMMAND start",c.id);
 const st=await wp("POST","/storage/drive",{request_id:"drive-"+c.id,drive_file_id:c.drive_file_id});
 console.log("COMMAND storage",c.id,st.status,JSON.stringify(st.data));
 if(st.status<200||st.status>=300||!st.data?.storage_id) return;
 const prepPayload={
  request_id:"prepare-"+c.id,
  storage_id:st.data.storage_id,
  title:c.title,
  caption:c.caption||"",
  facebook_caption:c.facebook_caption||c.caption||"",
  targets:c.targets,
  youtube_privacy:c.youtube_privacy||"private",
  made_for_kids:yesNo(c.made_for_kids),
  synthetic_media:yesNo(c.synthetic_media)
 };
 const prep=await wp("POST","/prepare",prepPayload);
 console.log("COMMAND prepare",c.id,prep.status,JSON.stringify(prep.data));
 if(prep.data?.job_id){const job=await wp("GET","/jobs/"+encodeURIComponent(prep.data.job_id));console.log("COMMAND status",c.id,job.status,JSON.stringify(job.data));}
 console.log("COMMAND end",c.id,"— no publication");
}
const pump=worker(wp);
const server=http.createServer(async(req,res)=>{
 try{
  const u=new URL(req.url,"http://localhost");
  if(req.method==="GET"&&u.pathname==="/health"){void pump();return json(res,200,{ok:true,service:"fuoconero-social-bridge",version:"0.4.4",mode:"authenticated-remote-render"});}
  if(u.search) return json(res,400,{error:"query_not_allowed"});
  const renderPath=/^\/reel-maker\/(?:render-jobs(?:\/[a-f0-9-]{36}(?:\/output)?)?|presets|article\/[0-9]+)$/.test(u.pathname);
  if(renderPath && ((req.method==="POST"&&u.pathname==="/reel-maker/render-jobs")||(req.method==="GET"&&u.pathname!=="/reel-maker/render-jobs"))){
   const raw=req.method==="POST"?await body(req):"";const x=await forward(req,u.pathname,raw);json(res,x.status,x.data);if(x.status>=200&&x.status<300)void pump();return;
  }
  if(req.method==="GET"&&u.pathname==="/capabilities"){const x=await forward(req,"/capabilities","");return json(res,x.status,x.data);}
  if(req.method==="POST"&&u.pathname==="/storage/drive"){const raw=await body(req);const x=await forward(req,"/storage/drive",raw);return json(res,x.status,x.data);}
  if(req.method==="POST"&&u.pathname==="/prepare"){const raw=await body(req);const x=await forward(req,"/prepare",raw);return json(res,x.status,x.data);}
  const m=u.pathname.match(/^\/jobs\/([^/]+)$/);
  if(req.method==="GET"&&m){const x=await forward(req,"/jobs/"+encodeURIComponent(m[1]),"");return json(res,x.status,x.data);}
  return json(res,404,{error:"not_found"});
 }catch(e){return json(res,500,{error:"bridge_error",message:e.message});}
});
for(const signal of ['SIGTERM','SIGINT']){
 process.on(signal,()=>{console.warn('PROCESS signal',signal,'— active render will be recovered from durable job state');});
}
server.listen(PORT,()=>{
 console.log("Fuoconero Social Bridge listening on",PORT);
 void pump();
 runCommand().catch(e=>console.error("COMMAND error",e.message));
});

