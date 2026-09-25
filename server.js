import http from "node:http";
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
async function wp(method,path,payload){
 const raw=payload===undefined?"":JSON.stringify(payload), restRoute=REST+path, signRoute=SIGN+path;
 const r=await fetch(BASE+restRoute,{method,headers:auth(method,signRoute,raw),body:method==="GET"?undefined:raw});
 const t=await r.text(); let data; try{data=JSON.parse(t)}catch{data={raw:t}};
 return {status:r.status,data};
}
async function runCommand(){
 let c; try{c=JSON.parse(await readFile(new URL("./command.json",import.meta.url),"utf8"));}catch(e){console.error("COMMAND read error",e.message);return;}
 if(!c||c.action==="noop"){console.log("COMMAND idle",c?.id||"none");return;}
 if(!c.id){console.error("COMMAND invalid: missing id");return;}
 if(c.action==="confirm"){
  if(!c.job_id||!c.digest||c.confirmed!==true){console.error("COMMAND invalid confirm");return;}
  console.log("COMMAND confirm requested",c.id,"job",c.job_id);
  const job=await wp("GET","/jobs/"+encodeURIComponent(c.job_id));
  console.log("COMMAND confirm precheck",c.id,job.status,JSON.stringify(job.data));
  if(job.status!==200||job.data?.status!=="prepared"||job.data?.digest!==c.digest){console.error("COMMAND confirm blocked: job not prepared or digest mismatch");return;}
  if(process.env.FNS_ALLOW_CONFIRM!=="1"){console.log("COMMAND confirm dry-run OK",c.id,"— server-side confirm disabled");return;}
  const conf=await wp("POST","/jobs/"+encodeURIComponent(c.job_id)+"/confirm",{confirmed:true,digest:c.digest});
  console.log("COMMAND confirm result",c.id,conf.status,JSON.stringify(conf.data)); return;
 }
 if(c.action!=="prepare"){console.error("COMMAND rejected action",c.action);return;}
 if(!c.drive_file_id||!c.title||!Array.isArray(c.targets)||!c.targets.length){console.error("COMMAND invalid prepare");return;}
 console.log("COMMAND start",c.id);
 const st=await wp("POST","/storage/drive",{request_id:"drive-"+c.id,drive_file_id:c.drive_file_id});
 console.log("COMMAND storage",c.id,st.status,JSON.stringify(st.data));
 if(st.status<200||st.status>=300||!st.data?.storage_id) return;
 const prep=await wp("POST","/prepare",{request_id:"prepare-"+c.id,storage_id:st.data.storage_id,title:c.title,caption:c.caption||"",facebook_caption:c.facebook_caption||c.caption||"",targets:c.targets,youtube_privacy:c.youtube_privacy||"private",made_for_kids:c.made_for_kids==="yes",synthetic_media:c.synthetic_media==="yes"});
 console.log("COMMAND prepare",c.id,prep.status,JSON.stringify(prep.data));
 if(prep.data?.job_id){const job=await wp("GET","/jobs/"+encodeURIComponent(prep.data.job_id));console.log("COMMAND status",c.id,job.status,JSON.stringify(job.data));}
 console.log("COMMAND end",c.id,"— no publication");
}
const server=http.createServer(async(req,res)=>{
 try{
  const u=new URL(req.url,"http://localhost");
  if(req.method==="GET"&&u.pathname==="/health") return json(res,200,{ok:true,service:"fuoconero-social-bridge",mode:"prepare-status-command"});
  if(req.method==="GET"&&u.pathname==="/capabilities"){const x=await wp("GET","/capabilities");return json(res,x.status,x.data);}
  if(req.method==="POST"&&u.pathname==="/storage/drive"){const raw=await body(req);const x=await wp("POST","/storage/drive",JSON.parse(raw||"{}"));return json(res,x.status,x.data);}
  if(req.method==="POST"&&u.pathname==="/prepare"){const raw=await body(req);const x=await wp("POST","/prepare",JSON.parse(raw||"{}"));return json(res,x.status,x.data);}
  const m=u.pathname.match(/^\/jobs\/([^/]+)$/);
  if(req.method==="GET"&&m){const x=await wp("GET","/jobs/"+encodeURIComponent(m[1]));return json(res,x.status,x.data);}
  return json(res,404,{error:"not_found"});
 }catch(e){return json(res,500,{error:"bridge_error",message:e.message});}
});
server.listen(PORT,()=>{
 console.log("Fuoconero Social Bridge listening on",PORT);
 runCommand().catch(e=>console.error("COMMAND error",e.message));
});