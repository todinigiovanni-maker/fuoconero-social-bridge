import http from "node:http";
import {worker} from "./worker.js";
import crypto from "node:crypto";
import {readFile,writeFile} from "node:fs/promises";

const PORT=process.env.PORT||10000;
const BASE=(process.env.FNS_BASE_URL||"https://fuoconero.com").replace(/\/$/,"");
const REST="/wp-json/fuoconero-social/v2";
const SIGN="/fuoconero-social/v2";
const CATEGORY_LIBRARY={
  "ani-male":"animale",
  "fisica-mente":"fisicamente",
  "natural-mente":"naturalmente",
  "il-mondo-visto-dal-nero":"mondo",
  "poesie":"poesie",
  "canzoni":"canzoni",
  "dossier-fuoconero":"dossier"
};


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

const scheduledTimers=new Map();
function findDriveFileId(output,kind){
 const roots=[output?.[kind],output?.outputs?.[kind],output?.data?.[kind],output?.data?.outputs?.[kind]];
 for(const x of roots){
  const id=x?.drive_file_id||x?.drive?.file_id||x?.file_id;
  if(typeof id==="string"&&id)return id;
 }
 return null;
}
async function prepareAndConfirmScheduled(spec,driveFileId,suffix){
 const id=spec.id+"-"+suffix;
 const st=await wp("POST","/storage/drive",{request_id:"drive-"+id,drive_file_id:driveFileId});
 console.log("SCHEDULE storage",id,st.status,JSON.stringify(st.data));
 if(st.status<200||st.status>=300||!st.data?.storage_id)throw new Error("storage import failed "+id);
 const prep=await wp("POST","/prepare",{
  request_id:"prepare-"+id,storage_id:st.data.storage_id,title:spec.title,
  caption:spec.caption||"",facebook_caption:spec.facebook_caption||spec.caption||"",
  targets:spec.targets,youtube_privacy:spec.youtube_privacy||"public",
  made_for_kids:yesNo(spec.made_for_kids),synthetic_media:yesNo(spec.synthetic_media)
 });
 console.log("SCHEDULE prepare",id,prep.status,JSON.stringify(prep.data));
 if(prep.status<200||prep.status>=300||!prep.data?.job_id)throw new Error("prepare failed "+id);
 const job=await wp("GET","/jobs/"+encodeURIComponent(prep.data.job_id));
 if(job.status!==200||job.data?.status!=="prepared"||!job.data?.digest)throw new Error("prepared job unavailable "+id);
 if(process.env.FNS_ALLOW_CONFIRM!=="1")throw new Error("FNS_ALLOW_CONFIRM is disabled");
 const conf=await wp("POST","/jobs/"+encodeURIComponent(prep.data.job_id)+"/confirm",{confirmed:true,digest:job.data.digest});
 console.log("SCHEDULE confirm",id,conf.status,JSON.stringify(conf.data));
 if(conf.status<200||conf.status>=300)throw new Error("confirm failed "+id);
 const storageId=conf.data?.payload?.storage_id||job.data?.payload?.storage_id||st.data.storage_id;
 if(storageId)void cleanupPublishedJob(prep.data.job_id,storageId);
 return prep.data.job_id;
}
async function executeScheduledPublication(item){
 console.log("SCHEDULE execute",item.id,item.render_job_id);
 const out=await wp("GET","/reel-maker/render-jobs/"+encodeURIComponent(item.render_job_id)+"/output");
 if(out.status!==200)throw new Error("render output unavailable "+item.id);
 const reelId=findDriveFileId(out.data,"reel"),storyId=findDriveFileId(out.data,"story");
 if(!reelId||!storyId)throw new Error("approved Reel/Story Drive IDs unavailable "+item.id);
 await prepareAndConfirmScheduled({...item,...item.reel,targets:item.reel?.targets||["instagram_reel","facebook_reel","youtube_short"]},reelId,"reel");
 await prepareAndConfirmScheduled({...item,...item.story,targets:item.story?.targets||["ig_story","fb_story"]},storyId,"story");
 console.log("SCHEDULE complete",item.id);
}
function schedulePublicationItem(item){
 if(!item?.id||!item?.render_job_id||!item?.publish_at||!item?.title)return false;
 const when=Date.parse(item.publish_at);if(!Number.isFinite(when))return false;
 const run=()=>executeScheduledPublication(item).catch(e=>console.error("SCHEDULE failed",item.id,e.message));
 const delay=Math.max(0,when-Date.now());
 const t=setTimeout(run,delay);scheduledTimers.set(item.id,t);
 console.log("SCHEDULE queued",item.id,item.publish_at,"in_ms",delay);
 return true;
}
function keepScheduledServiceAwake(){
 if(!scheduledTimers.size)return;
 const url=process.env.RENDER_EXTERNAL_URL||process.env.FNS_SELF_URL;
 if(url)fetch(url.replace(/\/$/,"")+"/health",{signal:AbortSignal.timeout(15000)}).catch(()=>{});
}
setInterval(keepScheduledServiceAwake,240000).unref();


async function telegramChatId(){
 const token=process.env.FNS_TELEGRAM_BOT_TOKEN;if(!token)return null;
 try{
  const r=await fetch("https://api.telegram.org/bot"+token+"/getUpdates",{signal:AbortSignal.timeout(15000)});
  const j=await r.json();const updates=Array.isArray(j?.result)?j.result:[];
  for(let i=updates.length-1;i>=0;i--){const id=updates[i]?.message?.chat?.id;if(id)return id;}
 }catch(e){console.warn("TELEGRAM chat lookup failed",e.message);}
 return null;
}
async function telegramNotify(message){
 const token=process.env.FNS_TELEGRAM_BOT_TOKEN;if(!token)return false;
 const chatId=process.env.FNS_TELEGRAM_CHAT_ID||await telegramChatId();
 if(!chatId){console.warn("TELEGRAM no chat id — send /start to the bot");return false;}
 try{
  const r=await fetch("https://api.telegram.org/bot"+token+"/sendMessage",{
   method:"POST",headers:{"content-type":"application/json"},
   body:JSON.stringify({chat_id:chatId,text:message,disable_web_page_preview:true}),
   signal:AbortSignal.timeout(15000)
  });
  if(!r.ok){console.warn("TELEGRAM send failed",r.status);return false;}
  console.log("TELEGRAM notification sent");return true;
 }catch(e){console.warn("TELEGRAM send failed",e.message);return false;}
}
async function recentPublishedPosts(){
 const u=new URL(BASE+"/wp-json/wp/v2/posts");
 u.searchParams.set("status","publish");u.searchParams.set("per_page","10");u.searchParams.set("orderby","date");u.searchParams.set("order","desc");
 u.searchParams.set("_fields","id,date,date_gmt,link,title,excerpt,categories");
 const r=await fetch(u,{headers:{"user-agent":"FuoconeroSocialBridge/0.4.8"},signal:AbortSignal.timeout(30000)});
 if(!r.ok)throw new Error("WordPress posts feed HTTP "+r.status);
 const a=await r.json();
 return (Array.isArray(a)?a:[]).map(p=>({
  ...p,title:decodeHtml(p?.title?.rendered||p?.title||""),
  excerpt:decodeHtml(p?.excerpt?.rendered||p?.excerpt||"")
 }));
}

const AUTO_REEL_CATEGORY_IDS={
 "789517870":"animale","577762893":"fisicamente","790278878":"naturalmente",
 "790278776":"mondo","14831":"poesie","11817":"canzoni"
};
const AUTO_STATE_URL=new URL("./auto-reel-state.json",import.meta.url);
let autoReelBusy=false;
function decodeHtml(s=""){return String(s).replace(/<[^>]*>/g," ").replace(/&#8230;|&hellip;/g,"…").replace(/&#8211;|&ndash;/g,"–").replace(/&#8212;|&mdash;/g,"—").replace(/&#8217;|&rsquo;/g,"’").replace(/&amp;/g,"&").replace(/&quot;/g,'"').replace(/&#\d+;/g," ").replace(/\s+/g," ").trim();}
function shortText(s,max=118){s=decodeHtml(s);if(s.length<=max)return s;const x=s.slice(0,max-1);return x.slice(0,Math.max(40,x.lastIndexOf(" ")))+"…";}
function autoScenes(post){
 const title=shortText(post.title,105), excerpt=decodeHtml(post.excerpt||"");
 const bits=excerpt.split(/(?<=[.!?])\s+/).filter(Boolean);
 const middle=shortText(bits[0]||excerpt||title,125), second=shortText(bits[1]||excerpt||"Scopri cosa racconta l’articolo.",125);
 return {
  reel:[title,middle,second,"Leggi la storia completa su fuoconero.com"],
  story:[title,middle,"La storia completa è su fuoconero.com"]
 };
}
async function autoState(){try{return JSON.parse(await readFile(AUTO_STATE_URL,"utf8"));}catch{return {seen:[]};}}
async function saveAutoState(s){try{await writeFile(AUTO_STATE_URL,JSON.stringify(s));}catch(e){console.warn("AUTO_REEL state write failed",e.message);}}
async function autoReelTick(){
 if(autoReelBusy)return;autoReelBusy=true;
 try{
  const postTime=p=>new Date((p.date_gmt||p.date)+"Z").getTime();
  const posts=(await recentPublishedPosts()).sort((a,b)=>postTime(a)-postTime(b));
  console.log("AUTO_REEL scan",posts.length,posts.map(p=>({id:p.id,date:p.date,date_gmt:p.date_gmt,categories:p.categories,title:p.title})));
  const state=await autoState(),seen=new Set(state.seen||[]);
  const now=Date.now(),firstRun=!state.initialized;
  for(const post of posts){
   if(seen.has(String(post.id))){console.log("AUTO_REEL skip seen",post.id,post.title);continue;}
   const age=now-postTime(post);
   // On first startup only consider genuinely fresh posts, preventing archive backfill.
   if(firstRun&&(age<0||age>120*60*1000)){console.log("AUTO_REEL skip first-run age",post.id,Math.round(age/60000),post.title);seen.add(String(post.id));continue;}
   const cats=(post.categories||[]).map(String),category=cats.map(x=>AUTO_REEL_CATEGORY_IDS[x]).find(Boolean);
   if(!category){console.log("AUTO_REEL skip category",post.id,cats,post.title);seen.add(String(post.id));continue;}
   console.log("AUTO_REEL eligible",post.id,category,Math.round(age/60000),post.title);
   await telegramNotify("🔥 Fuoconero Social\nNuovo articolo rilevato:\n"+post.title+"\n\n🎬 Creo Reel + Story.");
   const scenes=autoScenes(post);
   const payload={
    request_id:"fuoconero-auto-v2-post-"+post.id+"-reel-story",post_id:Number(post.id),category,
    music_id:process.env.FNS_AUTO_MUSIC_ID||"1Tf5mgp47tL7Gx1DB_yh0j39p06xIl62B",
    outputs:{reel:{preset:"articolo",scene_texts:scenes.reel},story:{preset:"story",scene_texts:scenes.story}},
    publication_authorized:false
   };
   const created=await wp("POST","/reel-maker/render-jobs",payload);
   console.log("AUTO_REEL enqueue",post.id,created.status,JSON.stringify(created.data));
   if(created.status>=200&&created.status<300){
    seen.add(String(post.id));
    await telegramNotify("⚙️ Fuoconero Social\nReel + Story accodati per:\n"+post.title+"\n\nNessuna pubblicazione social senza approvazione.");
    void pump();
   }else{
    await telegramNotify("⚠️ Fuoconero Social\nNon sono riuscito ad accodare Reel + Story per:\n"+post.title);
   }
  }
  state.initialized=true;state.seen=[...seen].slice(-1000);await saveAutoState(state);
 }catch(e){console.warn("AUTO_REEL tick failed",e.message);await telegramNotify("⚠️ Fuoconero Social\nControllo nuovi articoli fallito: "+e.message);}
 finally{autoReelBusy=false;}
}
setInterval(()=>void autoReelTick(),300000).unref();

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
 if(c.action==="inspect"){
  const ids=Array.isArray(c.job_ids)?c.job_ids:[c.job_id].filter(Boolean);
  for(const id of ids){const j=await wp("GET","/jobs/"+encodeURIComponent(id));console.log("COMMAND inspect",c.id,id,j.status,JSON.stringify(j.data));}
  return;
 }
 if(c.action==="scheduled_publish_batch"){
  const items=Array.isArray(c.items)?c.items:[];
  if(!items.length||items.length>25){console.error("COMMAND invalid scheduled_publish_batch");return;}
  let accepted=0;for(const item of items)if(schedulePublicationItem(item))accepted++;
  console.log("COMMAND scheduled_publish_batch",c.id,"accepted",accepted,"of",items.length);
  return;
 }
 if(c.action==="render_batch"){
  const items=Array.isArray(c.items)?c.items:[];
  if(!items.length||items.length>25){console.error("COMMAND invalid render_batch");return;}
  console.log("COMMAND render_batch requested",c.id,"items",items.length);
  for(const item of items){
   const p={...(item?.payload||item)};
   if(p.category&&CATEGORY_LIBRARY[p.category]) p.category=CATEGORY_LIBRARY[p.category];
   if(!p||typeof p!=="object"||!p.request_id||!Number.isInteger(p.post_id)||!p.category||(!p.music_title&&!p.music_id)||!p.outputs||typeof p.outputs!=="object"){
    console.error("COMMAND render_batch invalid item",p?.request_id||"unknown");continue;
   }
   const created=await wp("POST","/reel-maker/render-jobs",p);
   console.log("COMMAND render_batch result",c.id,p.request_id,created.status,JSON.stringify(created.data));
  }
  void pump();
  return;
 }
 if(c.action==="render"){
  const p={...c.payload};
  if(p.category&&CATEGORY_LIBRARY[p.category]) p.category=CATEGORY_LIBRARY[p.category];
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
  if(req.method==="GET"&&u.pathname==="/health"){void pump();return json(res,200,{ok:true,service:"fuoconero-social-bridge",version:"0.4.18",mode:"authenticated-remote-render"});}
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
 setTimeout(()=>void autoReelTick(),15000);
 runCommand().catch(e=>console.error("COMMAND error",e.message));
});

