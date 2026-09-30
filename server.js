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
function verifiedAccountsForTargets(prepData,targets=[]){
 const snapshot=prepData?.payload?.accounts;
 const accounts=snapshot?.accounts;
 if(!snapshot||!Number(snapshot.checked_at)||!accounts||Array.isArray(accounts)) return {ok:false,missing:["verification"]};
 const required=new Set();
 for(const t of targets||[]){
  if(/^ig_/.test(t))required.add("instagram");
  else if(/^fb_/.test(t))required.add("facebook");
  else if(t==="youtube_short")required.add("youtube");
 }
 const missing=[...required].filter(name=>accounts?.[name]?.ok!==true);
 return {ok:missing.length===0,missing,checked_at:Number(snapshot.checked_at)||0};
}
async function cleanupPublishedJob(jobId,storageId,meta={}){
 if(!jobId||!storageId)return false;
 for(let attempt=0;attempt<80;attempt++){
  await sleep(attempt===0?5000:15000);
  const job=await wp("GET","/jobs/"+encodeURIComponent(jobId));
  if(job.status!==200){console.warn("CLEANUP status unavailable",jobId,job.status);continue;}
  const destinations=Array.isArray(job.data?.destinations)?job.data.destinations:[];
  if(destinations.some(x=>x?.status==='error')){
   console.warn("CLEANUP retained after publication error",jobId);
   if(meta.notify!==false)await telegramNotify("⚠️ Fuoconero Social\\nPubblicazione incompleta: "+(meta.title||jobId)+" ("+(meta.kind||"media")+").\\nIl file resta su Drive.");
   return false;
  }
  if(!allDestinationsSucceeded(job.data))continue;
  if(meta.notify!==false)await telegramNotify("✅ Fuoconero Social\\n"+(meta.kind==="story"?"Story":"Reel")+" pubblicat"+(meta.kind==="story"?"a":"o")+" correttamente: "+(meta.title||"Fuoconero")+".");
  if(meta.kind==="reel"){
   const arc=await wp("POST","/storage/archive-tiktok",{request_id:"archive-tiktok-"+jobId,storage_id:storageId});
   console.log("CLEANUP TikTok archive",jobId,arc.status,JSON.stringify(arc.data));
   if(arc.status<200||arc.status>=300||arc.data?.archived!==true){
    console.warn("CLEANUP TikTok archive failed; retained in temporary Drive folder",jobId,storageId);
    if(meta.notify!==false)await telegramNotify("⚠️ Fuoconero Social\\nReel pubblicato, ma non sono riuscito a spostarlo nella cartella TikTok. Il file resta nei temporanei — "+(meta.title||jobId)+".");
    return false;
   }
   if(meta.notify!==false)await telegramNotify("📦 Fuoconero Social\\nReel spostato in “TikTok - Da pubblicare” — "+(meta.title||jobId)+".");
   return true;
  }
  const del=await wp("POST","/storage/delete",{request_id:"cleanup-"+jobId,storage_id:storageId});
  console.log("CLEANUP result",jobId,del.status,JSON.stringify(del.data));
  if(del.status<200||del.status>=300){
   await telegramNotify("⚠️ Fuoconero Social\\nPubblicazione riuscita, ma non sono riuscito a eliminare il file temporaneo da Drive: "+(meta.title||jobId)+".");
   return false;
  }
  if(meta.notify!==false)await telegramNotify("🧹 Fuoconero Social\\nCleanup completato: Story eliminata da Drive — "+(meta.title||jobId)+".");
  return true;
 }
 console.warn("CLEANUP retained after timeout",jobId);
 if(meta.notify!==false)await telegramNotify("⚠️ Fuoconero Social\\nNon ho ancora la conferma finale di tutti i social per "+(meta.title||jobId)+". Il file resta su Drive.");
 return false;
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
  request_id:"prepare-"+id,storage_id:st.data.storage_id,title:String(spec.title||"Fuoconero").slice(0,100),
  caption:spec.caption||"",facebook_caption:spec.facebook_caption||spec.caption||"",
  targets:spec.targets,youtube_privacy:spec.youtube_privacy||"public",
  made_for_kids:yesNo(spec.made_for_kids),synthetic_media:yesNo(spec.synthetic_media)
 });
 console.log("SCHEDULE prepare",id,prep.status,JSON.stringify(prep.data));
 if(prep.status<200||prep.status>=300||!prep.data?.job_id)throw new Error("prepare failed "+id);
 if(prep.data?.status!=="prepared"||!prep.data?.digest)throw new Error("prepared job unavailable "+id);
 // WordPress /prepare + /confirm remain the authoritative publication gate.
 // Do not require the optional accounts snapshot here: current plugin versions may
 // return accounts:[]/checked_at:0 even when the configured publishers are usable.
 if(process.env.FNS_ALLOW_CONFIRM!=="1")throw new Error("FNS_ALLOW_CONFIRM is disabled");
 const conf=await wp("POST","/jobs/"+encodeURIComponent(prep.data.job_id)+"/confirm",{confirmed:true,digest:prep.data.digest});
 console.log("SCHEDULE confirm",id,conf.status,JSON.stringify(conf.data));
 if(conf.status<200||conf.status>=300)throw new Error("confirm failed "+id);
 const tick=await wp("POST","/publish-worker/tick",{});
 console.log("SCHEDULE publish tick",id,tick.status,JSON.stringify(tick.data));
 if(tick.status<200||tick.status>=300)throw new Error("publish tick failed "+id+" HTTP "+tick.status);
 const storageId=conf.data?.payload?.storage_id||prep.data?.payload?.storage_id||st.data.storage_id;
 if(storageId)void cleanupPublishedJob(prep.data.job_id,storageId,{title:spec.title,kind:suffix,notify:true});
 return prep.data.job_id;
}
async function executeScheduledPublication(item){
 console.log("SCHEDULE execute",item.id,item.render_job_id);
 const attemptId=item.attempt_id||item.id;
 const out=await wp("GET","/reel-maker/render-jobs/"+encodeURIComponent(item.render_job_id)+"/output");
 if(out.status!==200)throw new Error("render output unavailable "+item.id);
 const reelId=findDriveFileId(out.data,"reel"),storyId=findDriveFileId(out.data,"story");
 const explicitKinds=Object.prototype.hasOwnProperty.call(item,"reel")||Object.prototype.hasOwnProperty.call(item,"story");
 const wantsReel=explicitKinds?!!item.reel:!!reelId;
 const wantsStory=explicitKinds?!!item.story:!!storyId;
 if(wantsReel&&!reelId)throw new Error("approved Reel Drive ID unavailable "+item.id);
 if(wantsStory&&!storyId)throw new Error("approved Story Drive ID unavailable "+item.id);
 if(!wantsReel&&!wantsStory)throw new Error("approved Drive media unavailable "+item.id);
 if(wantsReel)await prepareAndConfirmScheduled({...item,...item.reel,id:attemptId,targets:item.reel?.targets||["ig_reel","fb_reel","youtube_short"]},reelId,"reel");
 if(wantsStory)await prepareAndConfirmScheduled({...item,...item.story,id:attemptId,targets:item.story?.targets||["ig_story","fb_story"]},storyId,"story");
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
 // AutoReel also depends on this process staying alive: keep the free Render
 // instance warm even when there are no scheduled social publications.
 const url=process.env.RENDER_EXTERNAL_URL||process.env.FNS_SELF_URL;
 if(url)fetch(url.replace(/\/$/,"")+"/health",{signal:AbortSignal.timeout(15000)}).catch(()=>{});
}
setInterval(keepScheduledServiceAwake,240000).unref();


async function telegramChatId(){
 if(telegramKnownChatId)return telegramKnownChatId;
 try{const st=await autoState();if(st?.telegram_chat_id){telegramKnownChatId=String(st.telegram_chat_id);console.log("TELEGRAM chat restored");return telegramKnownChatId;}}catch(e){console.warn("TELEGRAM chat restore failed",e.message);}
 const token=process.env.FNS_TELEGRAM_BOT_TOKEN;if(!token)return null;
 try{
  const r=await fetch("https://api.telegram.org/bot"+token+"/getUpdates",{signal:AbortSignal.timeout(15000)});
  const j=await r.json();const updates=Array.isArray(j?.result)?j.result:[];
  for(let i=updates.length-1;i>=0;i--){const id=updates[i]?.message?.chat?.id||updates[i]?.callback_query?.message?.chat?.id;if(id){telegramKnownChatId=String(id);try{const st=await autoState();st.telegram_chat_id=telegramKnownChatId;await saveAutoState(st);console.log("TELEGRAM chat persisted");}catch(e){console.warn("TELEGRAM chat persist failed",e.message);}return telegramKnownChatId;}}
 }catch(e){console.warn("TELEGRAM chat lookup failed",e.message);}
 return null;
}
async function telegramNotify(message){
 const token=process.env.FNS_TELEGRAM_BOT_TOKEN;if(!token)return false;
 // Older callers stored literal "\\n" sequences. Normalize them so Telegram
 // renders real line breaks instead of showing backslash-n in the message.
 message=String(message??"").replace(/\\\\n/g,"\n");
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
async function publishedPostById(postId){
 const id=Number(postId);if(!Number.isInteger(id)||id<1)return null;
 const u=new URL(BASE+"/wp-json/wp/v2/posts/"+id);
 u.searchParams.set("context","view");u.searchParams.set("_fields","id,date,date_gmt,link,title,excerpt,categories,status");
 const r=await fetch(u,{headers:{"user-agent":"FuoconeroSocialBridge/0.4.23"},signal:AbortSignal.timeout(30000)});
 if(r.status===404)return null;if(!r.ok)throw new Error("WordPress post lookup HTTP "+r.status);
 const p=await r.json();if(String(p?.status||"publish")!=="publish")return null;
 return {...p,title:decodeHtml(p?.title?.rendered||p?.title||""),excerpt:decodeHtml(p?.excerpt?.rendered||p?.excerpt||"")};
}
async function recentPublishedPosts(){
 const u=new URL(BASE+"/wp-json/wp/v2/posts");
 u.searchParams.set("status","publish");u.searchParams.set("per_page","50");u.searchParams.set("orderby","date");u.searchParams.set("order","desc");
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
 "790278776":"mondo","14831":"poesie","11817":"canzoni","790178670":"dossier"
};
function publicationCategory(post){
 const cats=(post?.categories||[]).map(String);
 const known=cats.map(x=>AUTO_REEL_CATEGORY_IDS[x]).find(Boolean);
 if(known)return known;
 const title=String(post?.title||"").toLowerCase();
 if(title.includes("dossier"))return "dossier";
 return "fuoconero";
}
const AUTO_STATE_URL=new URL("./auto-reel-state.json",import.meta.url);
const AUTO_RETRY_DELAY_MS=2*60*1000;
const CATEGORY_HASHTAGS={animale:"#AniMALE",fisicamente:"#FisicaMENTE",naturalmente:"#NaturalMENTE",mondo:"#IlMondoVistoDalNero",poesie:"#Poesie",canzoni:"#Canzoni",dossier:"#DossierFuoconero",fuoconero:"#Fuoconero"};
function autoPublicationMeta(post,category){
 const title=cleanAutoTitle(post.title);
 const tag=CATEGORY_HASHTAGS[category]||"#Fuoconero";
 const hashtags=tag==="#Fuoconero"?["#Fuoconero"]:["#Fuoconero",tag];
 const caption=(title+"\n\n"+hashtags.join(" ")).trim();
 const link=post.link||BASE+"/?p="+post.id;
 return {title,article_url:link,caption,facebook_caption:(title+"\n\n"+link+"\n\n"+hashtags.join(" ")).trim(),hashtags};
}
const autoRetryTimers=new Map();
let autoReelBusy=false;
function decodeHtml(s=""){return String(s).replace(/<[^>]*>/g," ").replace(/&#8230;|&hellip;/g,"…").replace(/&#8211;|&ndash;/g,"–").replace(/&#8212;|&mdash;/g,"—").replace(/&#8217;|&rsquo;/g,"’").replace(/&amp;/g,"&").replace(/&quot;/g,'"').replace(/&#\d+;/g," ").replace(/\s+/g," ").trim();}
function shortText(s,max=118){s=decodeHtml(s);if(s.length<=max)return s;const x=s.slice(0,max-1);return x.slice(0,Math.max(40,x.lastIndexOf(" ")))+"…";}
function sceneSentence(text,maxWords=13){
 const clean=decodeHtml(text||"").replace(/\s+/g," ").trim().replace(/[.…]+$/,"");
 if(!clean)return "";
 const words=clean.split(" ");
 if(words.length<=maxWords)return clean;
 // Never leave an automatic sentence visibly truncated: keep a complete,
 // compact clause. Ellipses are reserved for text that already uses them
 // intentionally, not as a character-limit marker.
 const clauses=clean.split(/[,;:—–-]\s+/).map(s=>s.trim()).filter(Boolean);
 const complete=clauses.find(s=>s.split(/\s+/).length>=5&&s.split(/\s+/).length<=maxWords);
 if(complete)return complete.replace(/[.…]+$/,"");
 return words.slice(0,maxWords).join(" ").replace(/[,;:]$/,"")+".";
}
function cleanAutoTitle(title){
 return decodeHtml(title||"").replace(/^\s*(?:ANI[.…]*MALE|NATURAL[.…]*MENTE|FISICA[.…]*MENTE|IL MONDO VISTO DAL NERO|DOSSIER FUOCONERO|CANZONI)\\s*[-—–:]+\\s*/i,"").trim();
}
function autoScenes(post){
 const rawTitle=cleanAutoTitle(post.title), excerpt=decodeHtml(post.excerpt||"");
 const bits=excerpt.split(/(?<=[.!?])\s+/).filter(Boolean);
 const title=sceneSentence(rawTitle,11);
 const middle=sceneSentence(bits[0]||excerpt||rawTitle,13);
 const second=sceneSentence(bits[1]||"Scopri la storia completa.",13);
 return {
  reel:[title,middle,second,"Scopri di più su fuoconero.com"],
  story:[title,middle,"Continua su fuoconero.com"]
 };
}
async function autoState(){try{return JSON.parse(await readFile(AUTO_STATE_URL,"utf8"));}catch{return {seen:[]};}}
async function saveAutoState(s){try{await writeFile(AUTO_STATE_URL,JSON.stringify(s));}catch(e){console.warn("AUTO_REEL state write failed",e.message);}}
async function enqueueApprovedPublication(renderJobId,postId){
 const state=await autoState(),queue=Array.isArray(state.approval_queue)?state.approval_queue:[];
 const existing=queue.find(x=>x.render_job_id===renderJobId&&x.status!=="done");
 if(existing)return {position:queue.filter(x=>x.status==="queued"&&Number(x.due_at)<=Number(existing.due_at)).length,due_at:existing.due_at,existing:true};
 const now=Date.now(),last=queue.filter(x=>x.status==="queued").reduce((m,x)=>Math.max(m,Number(x.due_at)||0),0);
 const nextHour=Math.ceil(now/3600000)*3600000;
 const dueAt=Math.max(nextHour,last?last+3600000:0);
 queue.push({render_job_id:renderJobId,post_id:Number(postId),due_at:dueAt,status:"queued",created_at:now});
 state.approval_queue=queue.slice(-100);await saveAutoState(state);
 const ordered=queue.filter(x=>x.status==="queued").sort((a,b)=>a.due_at-b.due_at);
 return {position:ordered.findIndex(x=>x.render_job_id===renderJobId)+1,due_at:dueAt,existing:false};
}
let approvalQueueBusy=false;
async function approvalQueueTick(){
 if(approvalQueueBusy)return;approvalQueueBusy=true;
 try{
  const state=await autoState(),queue=Array.isArray(state.approval_queue)?state.approval_queue:[];
  const item=queue.filter(x=>x.status==="queued"&&Number(x.due_at)<=Date.now()).sort((a,b)=>a.due_at-b.due_at)[0];
  if(!item)return;
  item.status="publishing";await saveAutoState(state);
  try{
   const post=await publishedPostById(item.post_id);if(!post)throw new Error("articolo pubblicato non disponibile su WordPress");
   const category=publicationCategory(post);
   const publication=autoPublicationMeta(post,category);
   const out=await wp("GET","/reel-maker/render-jobs/"+encodeURIComponent(item.render_job_id)+"/output");
   if(out.status!==200)throw new Error("render output unavailable");
   const hasReel=!!findDriveFileId(out.data,"reel"),hasStory=!!findDriveFileId(out.data,"story");
   if(!hasReel&&!hasStory)throw new Error("approved Drive media unavailable");
   const mediaLabel=hasReel&&hasStory?"Reel + Story":hasReel?"Reel":"Story";
   await telegramNotify("🚀 Fuoconero Social\nÈ arrivato il suo turno in coda. Pubblico "+mediaLabel+": "+publication.title);
   await executeScheduledPublication({id:"queue-"+item.render_job_id,attempt_id:"queue-"+item.render_job_id+"-"+Date.now(),render_job_id:item.render_job_id,publish_at:new Date().toISOString(),title:publication.title,caption:publication.caption,facebook_caption:publication.facebook_caption,youtube_privacy:"public",made_for_kids:"no",synthetic_media:"no"});
   state.approval_queue=(state.approval_queue||[]).filter(x=>x.render_job_id!==item.render_job_id);await saveAutoState(state);
  }catch(e){
   item.status="queued";item.due_at=Date.now()+10*60*1000;item.last_error=e.message;await saveAutoState(state);
   await telegramNotify("⚠️ Fuoconero Social\nPubblicazione dalla coda non riuscita: "+e.message+"\nRiprovo automaticamente tra 10 minuti.");
  }
 }catch(e){console.warn("APPROVAL_QUEUE tick failed",e.message);}
 finally{approvalQueueBusy=false;}
}
setInterval(()=>void approvalQueueTick(),30000).unref();
function isArticleNotReady(created){
 return created?.status===400 && /articolo pubblicato e non protetto/i.test(String(created?.data?.message||""));
}
function isExistingRender(created){
 const msg=String(created?.data?.message||created?.data?.error||"");
 return created?.status===409 || /(?:already|gi[aà]\s+(?:esiste|render)|duplicate|duplicat|request[_ -]?id.*(?:used|esiste))/i.test(msg);
}
function scheduleAutoRetry(post){
 const key=String(post.id);
 if(autoRetryTimers.has(key))return;
 const t=setTimeout(()=>{autoRetryTimers.delete(key);void autoReelTick();},AUTO_RETRY_DELAY_MS);
 autoRetryTimers.set(key,t);
 console.log("AUTO_REEL retry scheduled",post.id,"in_ms",AUTO_RETRY_DELAY_MS);
}
async function autoReelTick(){
 if(autoReelBusy)return;autoReelBusy=true;
 try{
  const postTime=p=>new Date((p.date_gmt||p.date)+"Z").getTime();
  const posts=(await recentPublishedPosts()).sort((a,b)=>postTime(a)-postTime(b));
  console.log("AUTO_REEL scan",posts.length,posts.map(p=>({id:p.id,date:p.date,date_gmt:p.date_gmt,categories:p.categories,title:p.title})));
  const state=await autoState(),seen=new Set(state.seen||[]);
  // One-shot recovery for the poisoned legacy 7945 render job. A distinct
  // request id creates a fresh durable job; anti-duplicate protection then
  // resumes normally instead of re-enqueuing the zombie every scan.
  const recoverPostId=Number(process.env.FNS_RECOVER_POST_ID||0);
  const recoveryPending=recoverPostId&&!state.recovered_7945;
  const now=Date.now(),firstRun=!state.initialized;
  for(const post of posts){
   if(seen.has(String(post.id))&&!(recoveryPending&&Number(post.id)===recoverPostId)){console.log("AUTO_REEL skip seen",post.id,post.title);continue;}
   const age=now-postTime(post);
   // On first startup only consider genuinely fresh posts, preventing archive backfill.
   if(firstRun&&(age<0||age>120*60*1000)){console.log("AUTO_REEL skip first-run age",post.id,Math.round(age/60000),post.title);seen.add(String(post.id));continue;}
   const cats=(post.categories||[]).map(String),category=cats.map(x=>AUTO_REEL_CATEGORY_IDS[x]).find(Boolean);
   if(!category){console.log("AUTO_REEL skip category",post.id,cats,post.title);seen.add(String(post.id));continue;}
   console.log("AUTO_REEL eligible",post.id,category,Math.round(age/60000),post.title);
   const scenes=autoScenes(post),publication=autoPublicationMeta(post,category);
   const songTitle=category==="canzoni"?cleanAutoTitle(post.title):null;
   const payload={
    request_id:(recoveryPending&&Number(post.id)===recoverPostId?"fuoconero-auto-v5-recovery-post-"+post.id+"-reel-story":"fuoconero-auto-v5-post-"+post.id+"-reel-story"),post_id:Number(post.id),category,
    ...(category==="canzoni"?{music_title:songTitle}:{music_id:process.env.FNS_AUTO_MUSIC_ID||"1Tf5mgp47tL7Gx1DB_yh0j39p06xIl62B"}),
    outputs:{reel:{preset:"articolo",scene_texts:scenes.reel},story:{preset:"story",scene_texts:scenes.story}},
    publication,
    publication_authorized:false
   };
   const created=await wp("POST","/reel-maker/render-jobs",payload);
   console.log("AUTO_REEL enqueue",post.id,created.status,JSON.stringify(created.data));
   if(recoveryPending&&Number(post.id)===recoverPostId&&created.status>=200&&created.status<300){
    state.recovered_7945=true;
    console.log("AUTO_REEL recovery job created",post.id,created?.data?.render_job_id||"");
   }
   if(created.status>=200&&created.status<300){
    const createdStatus=String(created?.data?.status||"").toLowerCase();
    if(createdStatus==="failed"){
     console.log("AUTO_REEL existing job failed — retry eligible",post.id,created?.data?.render_job_id||"");
     scheduleAutoRetry(post);
     void pump();
     continue;
    }
    seen.add(String(post.id));
    await telegramNotify("🔥 Fuoconero Social\nNuovo articolo rilevato:\n"+post.title+"\n\n⚙️ Reel + Story accodati. Nessuna pubblicazione social senza approvazione.");
    void pump();
   }else if(isExistingRender(created)||/request_id gi[aà] usato per contenuto diverso/i.test(String(created?.data?.message||""))){
    // A deterministic request_id collision also proves this post has already
    // entered the render pipeline. Mark it seen and never notify/retry it.
    console.log("AUTO_REEL skip existing/colliding render",post.id,post.title);
    seen.add(String(post.id));
   }else if(isArticleNotReady(created)){
    console.log("AUTO_REEL article not ready yet",post.id,"— retry without marking seen");
    scheduleAutoRetry(post);
   }else{
    if(category==="canzoni"){
     await telegramNotify("🎵 Fuoconero Social\nNuovo singolo rilevato, ma non trovo il suo audio nella cartella Canzoni:\n"+post.title+"\n\nCarica il file con un nome corrispondente al titolo del singolo. Non userò Fuoconero (rock) come ripiego.");
    }else{
     await telegramNotify("⚠️ Fuoconero Social\nNon sono riuscito ad accodare Reel + Story per:\n"+post.title);
    }
   }
  }
  state.initialized=true;state.seen=[...seen].slice(-1000);await saveAutoState(state);
 }catch(e){console.warn("AUTO_REEL tick failed",e.message);await telegramNotify("⚠️ Fuoconero Social\nControllo nuovi articoli fallito: "+e.message);}
 finally{autoReelBusy=false;}
}
setInterval(()=>void autoReelTick(),300000).unref();

let telegramOffset=null,telegramApprovalBusy=false,telegramKnownChatId=process.env.FNS_TELEGRAM_CHAT_ID||null;
const telegramHandled=new Set();
async function telegramAnswerCallback(token,id,textValue){
 try{await fetch("https://api.telegram.org/bot"+token+"/answerCallbackQuery",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({callback_query_id:id,text:textValue,show_alert:false}),signal:AbortSignal.timeout(15000)});}catch(e){console.warn("TELEGRAM callback answer failed",e.message);}
}
async function telegramApprovalTick(){
 if(telegramApprovalBusy)return;telegramApprovalBusy=true;
 try{
  const token=process.env.FNS_TELEGRAM_BOT_TOKEN;if(!token)return;
  const qs=new URLSearchParams({timeout:"0",limit:"20",allowed_updates:JSON.stringify(["message","callback_query"])});
  if(telegramOffset!==null)qs.set("offset",String(telegramOffset));
  const r=await fetch("https://api.telegram.org/bot"+token+"/getUpdates?"+qs,{signal:AbortSignal.timeout(15000)});
  const j=await r.json(),updates=Array.isArray(j?.result)?j.result:[];
  if(!telegramKnownChatId){for(let i=updates.length-1;i>=0;i--){const id=updates[i]?.message?.chat?.id||updates[i]?.callback_query?.message?.chat?.id;if(id){telegramKnownChatId=String(id);console.log("TELEGRAM chat learned");try{const st=await autoState();st.telegram_chat_id=telegramKnownChatId;await saveAutoState(st);console.log("TELEGRAM chat persisted");}catch(e){console.warn("TELEGRAM chat persist failed",e.message);}break;}}}
  const allowed=String(process.env.FNS_TELEGRAM_CHAT_ID||telegramKnownChatId||"");
  if(telegramOffset===null){
   telegramOffset=updates.length?Math.max(...updates.map(x=>Number(x.update_id)||0))+1:0;
   return;
  }
  for(const update of updates){
   telegramOffset=Math.max(telegramOffset,(Number(update.update_id)||0)+1);
   const q=update?.callback_query,data=String(q?.data||""),chat=String(q?.message?.chat?.id||"");
   if(!q||chat!==allowed||telegramHandled.has(data))continue;
   const m=data.match(/^(approve|queue|reject):([a-f0-9-]{36}):(\d+)$/);if(!m)continue;
   const [,action,renderJobId,postIdRaw]=m,postId=Number(postIdRaw);
   telegramHandled.add(data);
   if(action==="reject"){
    await telegramAnswerCallback(token,q.id,"Rifiutato: nessuna pubblicazione.");
    await telegramNotify("❌ Fuoconero Social\\nRender rifiutato. Nessuna pubblicazione eseguita; i file restano su Drive.");
    continue;
   }
   if(action==="queue"){
    try{
     const queued=await enqueueApprovedPublication(renderJobId,postId);
     const when=new Date(queued.due_at).toLocaleString("it-IT",{timeZone:"Europe/Rome",hour:"2-digit",minute:"2-digit",day:"2-digit",month:"2-digit"});
     await telegramAnswerCallback(token,q.id,queued.existing?"Era già in coda.":"Approvato e accodato.");
     await telegramNotify("🕒 Fuoconero Social\\n"+(queued.existing?"Era già":"Approvato e inserito")+" in coda.\\nPosizione: "+queued.position+"\\nPubblicazione prevista: "+when+".");
    }catch(e){telegramHandled.delete(data);await telegramNotify("⚠️ Fuoconero Social\\nNon sono riuscito ad accodarlo: "+e.message);}
    continue;
   }
   await telegramAnswerCallback(token,q.id,"Approvato. Pubblico ora.");
   try{
    const out=await wp("GET","/reel-maker/render-jobs/"+encodeURIComponent(renderJobId)+"/output");
    if(out.status!==200)throw new Error("render output unavailable");
    const post=await publishedPostById(postId);
    if(!post)throw new Error("articolo pubblicato non disponibile su WordPress");
    const category=publicationCategory(post);
    const publication=autoPublicationMeta(post,category);
    const hasReel=!!findDriveFileId(out.data,"reel"),hasStory=!!findDriveFileId(out.data,"story");
    if(!hasReel&&!hasStory)throw new Error("approved Drive media unavailable");
    const mediaLabel=hasReel&&hasStory?"Reel + Story":hasReel?"Reel":"Story";
    await telegramNotify("🚀 Fuoconero Social\\nApprovazione ricevuta da Telegram. Pubblico "+mediaLabel+": "+publication.title);
    await executeScheduledPublication({
     id:"telegram-"+renderJobId,attempt_id:"telegram-"+renderJobId+"-"+Date.now(),render_job_id:renderJobId,publish_at:new Date().toISOString(),
     title:publication.title,caption:publication.caption,facebook_caption:publication.facebook_caption,
     youtube_privacy:"public",made_for_kids:"no",synthetic_media:"no"
    });
   }catch(e){
    telegramHandled.delete(data);
    console.error("TELEGRAM approval failed",renderJobId,e.message);
    await telegramNotify("⚠️ Fuoconero Social\\nApprovazione ricevuta, ma la pubblicazione non è partita: "+e.message);
   }
  }
 }catch(e){console.warn("TELEGRAM approval poll failed",e.message);}
 finally{telegramApprovalBusy=false;}
}
setInterval(()=>void telegramApprovalTick(),5000).unref();
setTimeout(()=>void telegramApprovalTick(),3000);

async function runCommand(){
 let c; try{c=JSON.parse(await readFile(new URL("./command.json",import.meta.url),"utf8"));}catch(e){console.error("COMMAND read error",e.message);return;}
 if(!c||c.action==="noop"){console.log("COMMAND idle",c?.id||"none");return;}
 if(!c.id){console.error("COMMAND invalid: missing id");return;}
 if(c.action==="inspect_render_output"){
  if(!c.render_job_id){console.error("COMMAND invalid inspect_render_output");return;}
  const out=await wp("GET","/reel-maker/render-jobs/"+encodeURIComponent(c.render_job_id)+"/output");
  console.log("COMMAND inspect_render_output",c.id,c.render_job_id,out.status,JSON.stringify(out.data));
  return;
 }
 if(c.action==="resend_render_ready_batch"){
  const items=Array.isArray(c.items)?c.items:[];
  if(!items.length||items.length>25){console.error("COMMAND invalid resend_render_ready_batch");return;}
  for(const item of items){
   if(!item?.render_job_id||!Number.isInteger(item.post_id)){console.error("COMMAND resend_render_ready_batch invalid item");continue;}
   const out=await wp("GET","/reel-maker/render-jobs/"+encodeURIComponent(item.render_job_id)+"/output");
   if(out.status!==200){console.error("COMMAND resend_render_ready_batch output unavailable",item.render_job_id,out.status);continue;}
   const reelId=findDriveFileId(out.data,"reel"),storyId=findDriveFileId(out.data,"story");
   if(!reelId&&!storyId){console.error("COMMAND resend_render_ready_batch missing Drive IDs",item.render_job_id);continue;}
   const token=process.env.FNS_TELEGRAM_BOT_TOKEN,chatId=process.env.FNS_TELEGRAM_CHAT_ID||await telegramChatId();
   if(!token||!chatId){console.error("COMMAND resend_render_ready_batch Telegram unavailable");continue;}
   const posts=await recentPublishedPosts(),post=posts.find(x=>Number(x.id)===item.post_id);
   const title=post?.title||item.title||("Articolo "+item.post_id);
   const buttons=[];
   if(reelId)buttons.push({text:"APRI REEL",url:"https://drive.google.com/file/d/"+encodeURIComponent(reelId)+"/preview"});
   if(storyId)buttons.push({text:"APRI STORY",url:"https://drive.google.com/file/d/"+encodeURIComponent(storyId)+"/preview"});
   const label=reelId&&storyId?"Reel + Story pronti":reelId?"Reel pronto":"Story pronta";
   const keyboard=[buttons,[{text:"APPROVA E ACCODA",callback_data:"queue:"+item.render_job_id+":"+item.post_id}],[{text:"PUBBLICA ORA",callback_data:"approve:"+item.render_job_id+":"+item.post_id},{text:"RIFIUTA",callback_data:"reject:"+item.render_job_id+":"+item.post_id}]];
   const tr=await fetch("https://api.telegram.org/bot"+token+"/sendMessage",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({chat_id:chatId,text:"Fuoconero Social\n"+label+" su Drive.\n\n"+title+"\n\nApri l'anteprima, poi scegli se accodare, pubblicare subito o rifiutare.",disable_web_page_preview:true,reply_markup:{inline_keyboard:keyboard}}),signal:AbortSignal.timeout(15000)});
   console.log(tr.ok?"COMMAND resend_render_ready_batch sent":"COMMAND resend_render_ready_batch failed "+tr.status,item.render_job_id);
  }
  return;
 }
 if(c.action==="resend_render_ready"){
  if(!c.render_job_id||!Number.isInteger(c.post_id)){console.error("COMMAND invalid resend_render_ready");return;}
  const out=await wp("GET","/reel-maker/render-jobs/"+encodeURIComponent(c.render_job_id)+"/output");
  if(out.status!==200){console.error("COMMAND resend_render_ready output unavailable",c.render_job_id,out.status);return;}
  const reelId=findDriveFileId(out.data,"reel"),storyId=findDriveFileId(out.data,"story");
  if(!reelId||!storyId){console.error("COMMAND resend_render_ready missing Drive IDs",c.render_job_id);return;}
  const reelPreview=out.data?.outputs?.reel?.preview_url||out.data?.reel?.preview_url;
  const storyPreview=out.data?.outputs?.story?.preview_url||out.data?.story?.preview_url;
  const token=process.env.FNS_TELEGRAM_BOT_TOKEN,chatId=process.env.FNS_TELEGRAM_CHAT_ID||await telegramChatId();
  if(!token||!chatId){console.error("COMMAND resend_render_ready Telegram unavailable");return;}
  const posts=await recentPublishedPosts(),post=posts.find(x=>Number(x.id)===c.post_id);
  const title=post?.title||c.title||("Articolo "+c.post_id);
  const reelLink="https://drive.google.com/file/d/"+encodeURIComponent(reelId)+"/preview";
  const storyLink="https://drive.google.com/file/d/"+encodeURIComponent(storyId)+"/preview";
  const message="✅ Fuoconero Social\\nReel + Story pronti su Drive.\\n\\n"+title+"\\n\\nApri le anteprime dai pulsanti qui sotto, poi approva o rifiuta.";
  const r=await fetch("https://api.telegram.org/bot"+token+"/sendMessage",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({chat_id:chatId,text:message,disable_web_page_preview:true,reply_markup:{inline_keyboard:[[{text:"🎬 APRI REEL",url:reelLink},{text:"📱 APRI STORY",url:storyLink}],[{text:"🕒 APPROVA E ACCODA",callback_data:"queue:"+c.render_job_id+":"+c.post_id}],[{text:"🚀 PUBBLICA ORA",callback_data:"approve:"+c.render_job_id+":"+c.post_id},{text:"❌ RIFIUTA",callback_data:"reject:"+c.render_job_id+":"+c.post_id}]]}}),signal:AbortSignal.timeout(15000)});
  console.log(r.ok?"COMMAND resend_render_ready sent":"COMMAND resend_render_ready failed "+r.status,c.render_job_id);
  return;
 }
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
  // Optional archive backfill: enqueue render-only items in the same startup command,
  // so deploying the hourly publication schedule does not need a second restart.
  const renderItems=Array.isArray(c.render_items)?c.render_items:[];
  if(renderItems.length){
   if(renderItems.length>25){console.error("COMMAND invalid render_items");return;}
   console.log("COMMAND scheduled_publish_batch render_items",c.id,renderItems.length);
   for(const item of renderItems){
    const p={...(item?.payload||item)};
    if(p.category&&CATEGORY_LIBRARY[p.category]) p.category=CATEGORY_LIBRARY[p.category];
    if(!p||typeof p!=="object"||!p.request_id||!Number.isInteger(p.post_id)||!p.category||(!p.music_title&&!p.music_id)||!p.outputs||typeof p.outputs!=="object"){
     console.error("COMMAND render_items invalid item",p?.request_id||"unknown");continue;
    }
    const created=await wp("POST","/reel-maker/render-jobs",p);
    console.log("COMMAND render_items result",c.id,p.request_id,created.status,JSON.stringify(created.data));
   }
   void pump();
  }
  return;
 }
 if(c.action==="render_batch_by_slug"){
  const items=Array.isArray(c.items)?c.items:[];
  if(!items.length||items.length>25){console.error("COMMAND invalid render_batch_by_slug");return;}
  console.log("COMMAND render_batch_by_slug requested",c.id,"items",items.length);
  for(const item of items){
   const p={...(item?.payload||item)};
   const slug=typeof p.slug==="string"?p.slug.trim():"";
   if(!slug){console.error("COMMAND render_batch_by_slug missing slug",p?.request_id||"unknown");continue;}
   const lookup=await fetch(BASE+"/wp-json/wp/v2/posts?slug="+encodeURIComponent(slug)+"&_fields=id,slug,status",{signal:AbortSignal.timeout(90000)});
   let matches=[]; try{matches=await lookup.json();}catch{}
   const post=Array.isArray(matches)?matches.find(x=>x?.slug===slug&&x?.status==="publish"):null;
   if(!post?.id){console.error("COMMAND render_batch_by_slug unresolved",slug,lookup.status);continue;}
   p.post_id=Number(post.id); delete p.slug;
   if(p.category&&CATEGORY_LIBRARY[p.category]) p.category=CATEGORY_LIBRARY[p.category];
   if(!p.request_id||!Number.isInteger(p.post_id)||!p.category||(!p.music_title&&!p.music_id)||!p.outputs||typeof p.outputs!=="object"){
    console.error("COMMAND render_batch_by_slug invalid item",p?.request_id||"unknown");continue;
   }
   console.log("COMMAND render_batch_by_slug resolved",slug,"post",p.post_id);
   const created=await wp("POST","/reel-maker/render-jobs",p);
   console.log("COMMAND render_batch_by_slug result",c.id,p.request_id,created.status,JSON.stringify(created.data));
  }
  void pump();
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
  title:String(c.title||"Fuoconero").slice(0,100),
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

const TIKTOK_REDIRECT_URI=process.env.TIKTOK_REDIRECT_URI||"https://social.fuoconero.com/oauth/callback";
function cookieValue(req,name){
 const raw=String(req.headers.cookie||"");
 for(const part of raw.split(";")){
  const [k,...rest]=part.trim().split("=");
  if(k===name)return decodeURIComponent(rest.join("="));
 }
 return "";
}
function html(res,status,title,message){
 res.writeHead(status,{"content-type":"text/html; charset=utf-8","cache-control":"no-store"});
 res.end("<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>"+title+"</title></head><body style=\"font-family:system-ui;max-width:720px;margin:60px auto;padding:0 20px\"><h1>"+title+"</h1><p>"+message+"</p></body></html>");
}
async function saveTikTokOauth(tokens){
 const st=await autoState();
 st.tiktok_oauth={
  access_token:tokens.access_token,
  refresh_token:tokens.refresh_token,
  open_id:tokens.open_id||"",
  scope:tokens.scope||"",
  token_type:tokens.token_type||"Bearer",
  access_expires_at:Date.now()+Math.max(60,Number(tokens.expires_in)||86400)*1000,
  refresh_expires_at:Date.now()+Math.max(60,Number(tokens.refresh_expires_in)||31536000)*1000,
  updated_at:Date.now()
 };
 await saveAutoState(st);
 return st.tiktok_oauth;
}
async function tiktokOauthToken(){
 const key=process.env.TIKTOK_CLIENT_KEY,secret=process.env.TIKTOK_CLIENT_SECRET;
 if(!key||!secret)throw new Error("TikTok client credentials not configured");
 const st=await autoState();let tok=st?.tiktok_oauth;
 if(!tok?.refresh_token)throw new Error("TikTok account not connected");
 if(tok.access_token&&Number(tok.access_expires_at)>Date.now()+5*60*1000)return tok.access_token;
 const form=new URLSearchParams({client_key:key,client_secret:secret,grant_type:"refresh_token",refresh_token:tok.refresh_token});
 const r=await fetch("https://open.tiktokapis.com/v2/oauth/token/",{method:"POST",headers:{"content-type":"application/x-www-form-urlencoded","cache-control":"no-cache"},body:form,signal:AbortSignal.timeout(30000)});
 const data=await r.json();
 if(!r.ok||!data?.access_token)throw new Error("TikTok token refresh failed: "+(data?.error_description||data?.error||r.status));
 tok=await saveTikTokOauth(data);return tok.access_token;
}
async function tiktokCreatorInfo(){
 const token=await tiktokOauthToken();
 const r=await fetch("https://open.tiktokapis.com/v2/post/publish/creator_info/query/",{method:"POST",headers:{authorization:"Bearer "+token,"content-type":"application/json; charset=UTF-8"},body:"{}",signal:AbortSignal.timeout(30000)});
 const data=await r.json();
 if(!r.ok||(data?.error?.code&&data.error.code!=="ok"))throw new Error("TikTok creator info failed: "+(data?.error?.message||data?.error?.code||r.status));
 return data?.data||data;
}

function tiktokAdminCookie(openId){
 const secret=process.env.TIKTOK_CLIENT_SECRET||"";
 return crypto.createHmac("sha256",secret).update(String(openId||"")).digest("hex");
}
function isTikTokAdmin(req,st){
 const got=cookieValue(req,"tiktok_admin_session");
 const openId=st?.tiktok_oauth?.open_id||"";
 if(!got||!openId)return false;
 try{return crypto.timingSafeEqual(Buffer.from(got),Buffer.from(tiktokAdminCookie(openId)));}catch{return false;}
}
function driveDownloadUrl(input){
 const u=new URL(input);
 if(!["drive.google.com","drive.usercontent.google.com"].includes(u.hostname))throw new Error("Only Google Drive video URLs are allowed");
 let id=u.searchParams.get("id");
 if(!id){
  const m=u.pathname.match(/\/file\/d\/([^/]+)/);
  if(m)id=m[1];
 }
 if(!id)throw new Error("Google Drive file ID not found");
 return "https://drive.usercontent.google.com/download?id="+encodeURIComponent(id)+"&export=download&confirm=t";
}
async function downloadTikTokVideo(inputUrl){
 const url=driveDownloadUrl(inputUrl);
 const r=await fetch(url,{redirect:"follow",signal:AbortSignal.timeout(120000)});
 if(!r.ok)throw new Error("Drive download failed HTTP "+r.status);
 const ct=(r.headers.get("content-type")||"").toLowerCase();
 const buf=Buffer.from(await r.arrayBuffer());
 if(buf.length<1000)throw new Error("Downloaded file is unexpectedly small");
 if(buf.length>128*1024*1024)throw new Error("Video exceeds 128 MB test limit");
 if(ct&&ct.includes("text/html"))throw new Error("Drive returned an HTML page instead of the MP4");
 return {buf,contentType:ct.includes("quicktime")?"video/quicktime":ct.includes("webm")?"video/webm":"video/mp4"};
}
async function tiktokPublishStatus(publishId){
 const token=await tiktokOauthToken();
 const r=await fetch("https://open.tiktokapis.com/v2/post/publish/status/fetch/",{
  method:"POST",headers:{authorization:"Bearer "+token,"content-type":"application/json; charset=UTF-8"},
  body:JSON.stringify({publish_id:publishId}),signal:AbortSignal.timeout(30000)
 });
 const data=await r.json();
 if(!r.ok||(data?.error?.code&&data.error.code!=="ok"))throw new Error("TikTok status failed: "+(data?.error?.message||data?.error?.code||r.status));
 return data?.data||data;
}
async function tiktokDirectPostFromUrl({videoUrl,title="",privacyLevel,allowComment=false,allowDuet=false,allowStitch=false,brandOrganic=false,brandContent=false,musicConsent=false}){
 const creator=await tiktokCreatorInfo();
 const options=Array.isArray(creator?.privacy_level_options)?creator.privacy_level_options:[];
 if(!privacyLevel||!options.includes(privacyLevel))throw new Error("Seleziona manualmente una privacy valida tra quelle offerte da TikTok.");
 if(privacyLevel!=="SELF_ONLY")throw new Error("Finché il client non è auditato, il test deve essere pubblicato come SELF_ONLY.");
 if(!musicConsent)throw new Error("Devi accettare la Music Usage Confirmation prima di pubblicare.");
 if(brandContent&&privacyLevel==="SELF_ONLY"){
  // Kept explicit for clarity: TikTok may further restrict branded content according to account settings.
 }
 const {buf,contentType}=await downloadTikTokVideo(videoUrl);
 const token=await tiktokOauthToken();
 const size=buf.length;
 const postInfo={
  title:String(title||"").slice(0,2200),
  privacy_level:privacyLevel,
  disable_comment:creator?.comment_disabled?true:!allowComment,
  disable_duet:creator?.duet_disabled?true:!allowDuet,
  disable_stitch:creator?.stitch_disabled?true:!allowStitch,
  brand_organic_toggle:!!brandOrganic,
  brand_content_toggle:!!brandContent
 };
 const init=await fetch("https://open.tiktokapis.com/v2/post/publish/video/init/",{
  method:"POST",
  headers:{authorization:"Bearer "+token,"content-type":"application/json; charset=UTF-8"},
  body:JSON.stringify({
   post_info:postInfo,
   source_info:{source:"FILE_UPLOAD",video_size:size,chunk_size:size,total_chunk_count:1}
  }),
  signal:AbortSignal.timeout(30000)
 });
 const data=await init.json();
 if(!init.ok||(data?.error?.code&&data.error.code!=="ok")||!data?.data?.upload_url||!data?.data?.publish_id){
  throw new Error("TikTok init failed: "+(data?.error?.message||data?.error?.code||init.status));
 }
 const uploadUrl=data.data.upload_url;
 const uploadMeta=(()=>{try{const x=new URL(uploadUrl);return x.origin+x.pathname;}catch{return "invalid_upload_url";}})();
 console.log("TIKTOK init ok",data.data.publish_id,uploadMeta,data?.error?.log_id||data?.error?.logid||"");
 const up=await fetch(uploadUrl,{
  method:"PUT",
  redirect:"manual",
  headers:{"Content-Type":contentType,"Content-Length":String(size),"Content-Range":"bytes 0-"+(size-1)+"/"+size},
  body:buf,
  signal:AbortSignal.timeout(120000)
 });
 const upText=await up.text().catch(()=> "");
 console.log("TIKTOK upload response",up.status,up.headers.get("location")||"",up.headers.get("content-range")||"",upText.slice(0,500));
 if([301,302,303,307,308].includes(up.status)&&up.headers.get("location")){
  throw new Error("TikTok upload redirected HTTP "+up.status+" to "+up.headers.get("location"));
 }
 if(![200,201,206].includes(up.status)){
  let statusAfter=null;try{statusAfter=await tiktokPublishStatus(data.data.publish_id);}catch(e){statusAfter={error:e.message};}
  console.log("TIKTOK status after upload failure",JSON.stringify(statusAfter));
  throw new Error("TikTok upload failed HTTP "+up.status+(upText?" — "+upText.slice(0,220):""));
 }
 await sleep(2500);
 let status=null;try{status=await tiktokPublishStatus(data.data.publish_id);}catch(e){status={status:"unknown",error:e.message};}
 return {publish_id:data.data.publish_id,privacy_level:privacyLevel,video_size:size,status,creator};
}

const pump=worker(wp,{getTelegramChatId:async()=>process.env.FNS_TELEGRAM_CHAT_ID||telegramKnownChatId||await telegramChatId()||null});
const server=http.createServer(async(req,res)=>{
 try{
  const u=new URL(req.url,"http://localhost");
  if(req.method==="GET"&&u.pathname==="/oauth/tiktok"){
   const key=process.env.TIKTOK_CLIENT_KEY,secret=process.env.TIKTOK_CLIENT_SECRET;
   if(!key||!secret)return html(res,503,"TikTok non configurato","Mancano le credenziali TikTok sul server.");
   const state=crypto.randomBytes(24).toString("hex");
   const authUrl=new URL("https://www.tiktok.com/v2/auth/authorize/");
   authUrl.searchParams.set("client_key",key);
   authUrl.searchParams.set("response_type","code");
   authUrl.searchParams.set("scope","user.info.basic,video.publish,video.upload");
   authUrl.searchParams.set("redirect_uri",TIKTOK_REDIRECT_URI);
   authUrl.searchParams.set("state",state);
   res.writeHead(302,{location:authUrl.toString(),"set-cookie":"tiktok_oauth_state="+encodeURIComponent(state)+"; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=600","cache-control":"no-store"});
   return res.end();
  }
  if(req.method==="GET"&&u.pathname==="/oauth/callback"){
   try{
    const state=u.searchParams.get("state")||"",expected=cookieValue(req,"tiktok_oauth_state");
    if(!state||!expected||state!==expected)return html(res,400,"Collegamento TikTok non riuscito","Controllo di sicurezza OAuth non valido. Riapri il collegamento TikTok e riprova.");
    if(u.searchParams.get("error"))return html(res,400,"Autorizzazione TikTok annullata",String(u.searchParams.get("error_description")||u.searchParams.get("error")));
    const code=u.searchParams.get("code");if(!code)return html(res,400,"Collegamento TikTok non riuscito","TikTok non ha restituito il codice di autorizzazione.");
    const form=new URLSearchParams({client_key:process.env.TIKTOK_CLIENT_KEY||"",client_secret:process.env.TIKTOK_CLIENT_SECRET||"",code,grant_type:"authorization_code",redirect_uri:TIKTOK_REDIRECT_URI});
    const tr=await fetch("https://open.tiktokapis.com/v2/oauth/token/",{method:"POST",headers:{"content-type":"application/x-www-form-urlencoded","cache-control":"no-cache"},body:form,signal:AbortSignal.timeout(30000)});
    const td=await tr.json();
    if(!tr.ok||!td?.access_token)throw new Error(td?.error_description||td?.error||("HTTP "+tr.status));
    await saveTikTokOauth(td);
    const creator=await tiktokCreatorInfo();
    console.log("TIKTOK OAuth connected",creator?.creator_username||creator?.creator_nickname||"creator",td.scope||"");
    res.setHeader("set-cookie",["tiktok_oauth_state=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0","tiktok_admin_session="+encodeURIComponent(tiktokAdminCookie(td.open_id||""))+"; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=86400"]);
    return html(res,200,"TikTok collegato ✅","Fuoconero Social è ora autorizzato sul tuo account TikTok. <a href=\"/tiktok/test\">Apri il test di pubblicazione</a>.");
   }catch(e){console.error("TIKTOK OAuth callback failed",e.message);return html(res,500,"Collegamento TikTok non riuscito","Errore durante l'autorizzazione. Controlla i log del bridge e riprova.");}
  }
  if(req.method==="GET"&&u.pathname==="/oauth/tiktok/status"){
   try{
    const st=await autoState(),tok=st?.tiktok_oauth||null;
    if(!tok?.refresh_token)return json(res,200,{configured:!!(process.env.TIKTOK_CLIENT_KEY&&process.env.TIKTOK_CLIENT_SECRET),connected:false});
    const creator=await tiktokCreatorInfo();
    return json(res,200,{configured:true,connected:true,scope:tok.scope||"",creator:{username:creator?.creator_username||null,nickname:creator?.creator_nickname||null,privacy_level_options:creator?.privacy_level_options||[]}});
   }catch(e){return json(res,200,{configured:!!(process.env.TIKTOK_CLIENT_KEY&&process.env.TIKTOK_CLIENT_SECRET),connected:false,error:e.message});}
  }
  if(req.method==="GET"&&u.pathname==="/tiktok/test"){
   const st=await autoState();
   if(!isTikTokAdmin(req,st))return html(res,403,"Accesso negato","Ricollega TikTok da /oauth/tiktok per aprire questa pagina.");
   try{
    const creator=await tiktokCreatorInfo();
    const nickname=String(creator?.creator_nickname||creator?.creator_username||"Account TikTok").replace(/[<>&"]/g,"");
    const username=String(creator?.creator_username||"").replace(/[<>&"]/g,"");
    const options=Array.isArray(creator?.privacy_level_options)?creator.privacy_level_options:[];
    const optHtml=['<option value="">— Seleziona manualmente —</option>',...options.map(v=>'<option value="'+v+'">'+v+'</option>')].join("");
    const accountLooksPrivate=!options.includes("PUBLIC_TO_EVERYONE");
    const blocked=creator?.creator_username?false:true;
    res.writeHead(200,{"content-type":"text/html; charset=utf-8","cache-control":"no-store"});
    return res.end(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Test TikTok</title>
<style>body{font-family:system-ui;max-width:860px;margin:42px auto;padding:0 20px;line-height:1.45}label{display:block;margin:14px 0 6px}input[type=text],textarea,select{width:100%;box-sizing:border-box;padding:9px}textarea{height:110px}.box{border:1px solid #ddd;border-radius:10px;padding:16px;margin:16px 0}.muted{color:#666}.warn{background:#fff7dc;border:1px solid #e6c55a;padding:12px;border-radius:8px}.ok{background:#edf9ef;border:1px solid #8fd19e;padding:12px;border-radius:8px}iframe{width:100%;height:380px;border:1px solid #ddd;border-radius:8px}.row{display:flex;gap:18px;flex-wrap:wrap}.row label{display:inline-block;margin:6px 0}button{padding:10px 16px;font-weight:700}</style></head><body>
<h1>Test pubblicazione TikTok</h1>
<div class="box"><b>Account:</b> ${nickname}${username?" (@"+username+")":""}<br><b>Durata massima video:</b> ${creator?.max_video_post_duration_sec??"?"} s</div>
${accountLooksPrivate?'<div class="ok">✅ L’account risulta privato secondo le opzioni restituite da TikTok.</div>':'<div class="warn">⚠️ Il tuo account risulta pubblico. Per un client non auditato TikTok richiede un account privato e il post deve essere SELF_ONLY.</div>'}
<p class="muted">Questa pagina ricarica sempre le informazioni più recenti del creator prima del test.</p>
<form method="post" action="/tiktok/test" onsubmit="return confirm('Confermi di voler inviare questo video a TikTok come post privato di test?')">
<label>Link Google Drive MP4</label><input id="video_url" name="video_url" type="text" required oninput="previewDrive(this.value)">
<div class="box"><b>Anteprima</b><br><iframe id="preview" title="Anteprima video"></iframe></div>
<label>Caption / hashtag</label><textarea name="title" maxlength="2200">#Fuoconero test API TikTok</textarea>
<label>Privacy</label><select name="privacy_level" required>${optHtml}</select>
<div class="box"><b>Interazioni</b><p class="muted">Nessuna è attiva per impostazione predefinita.</p>
<div class="row">
<label><input type="checkbox" name="allow_comment" value="1" ${creator?.comment_disabled?"disabled":""}> Consenti commenti ${creator?.comment_disabled?"(disabilitati dal tuo account)":""}</label>
<label><input type="checkbox" name="allow_duet" value="1" ${creator?.duet_disabled?"disabled":""}> Consenti Duet ${creator?.duet_disabled?"(non disponibile)":""}</label>
<label><input type="checkbox" name="allow_stitch" value="1" ${creator?.stitch_disabled?"disabled":""}> Consenti Stitch ${creator?.stitch_disabled?"(non disponibile)":""}</label>
</div></div>
<div class="box"><b>Contenuto commerciale</b><p class="muted">Lascia tutto spento se il video non promuove un’attività, un prodotto o un marchio.</p>
<label><input type="checkbox" id="commercial" onchange="document.getElementById('commercial_opts').style.display=this.checked?'block':'none'"> Questo contenuto promuove me, un brand, un prodotto o un servizio</label>
<div id="commercial_opts" style="display:none">
<label><input type="checkbox" name="brand_organic" value="1"> Il mio brand / la mia attività <span class="muted">(etichetta “Promotional content”)</span></label>
<label><input type="checkbox" name="brand_content" value="1"> Brand o terza parte <span class="muted">(etichetta “Paid partnership”)</span></label>
</div></div>
<div class="box"><label><input type="checkbox" name="music_consent" value="1" required> By posting, you agree to TikTok's Music Usage Confirmation</label></div>
<button type="submit" ${blocked?"disabled":""}>Pubblica test privato</button>
</form>
<script>
function previewDrive(v){const m=v.match(/\\/file\\/d\\/([^/]+)/)||v.match(/[?&]id=([^&]+)/);document.getElementById('preview').src=m?'https://drive.google.com/file/d/'+m[1]+'/preview':'';}
</script></body></html>`);
   }catch(e){console.error("TIKTOK test page failed",e.message);return html(res,500,"Pagina test TikTok non disponibile",String(e.message).replace(/</g,"&lt;"));}
  }
  if(req.method==="POST"&&u.pathname==="/tiktok/test"){
   const st=await autoState();
   if(!isTikTokAdmin(req,st))return html(res,403,"Accesso negato","Ricollega TikTok e riprova.");
   try{
    const raw=await body(req),form=new URLSearchParams(raw);
    const result=await tiktokDirectPostFromUrl({
     videoUrl:form.get("video_url")||"",
     title:form.get("title")||"",
     privacyLevel:form.get("privacy_level")||"",
     allowComment:form.get("allow_comment")==="1",
     allowDuet:form.get("allow_duet")==="1",
     allowStitch:form.get("allow_stitch")==="1",
     brandOrganic:form.get("brand_organic")==="1",
     brandContent:form.get("brand_content")==="1",
     musicConsent:form.get("music_consent")==="1"
    });
    return html(res,200,"Test TikTok inviato ✅","Publish ID: <code>"+result.publish_id+"</code><br>Privacy: <b>"+result.privacy_level+"</b><br>Stato iniziale: <pre>"+JSON.stringify(result.status,null,2).replace(/</g,"&lt;")+"</pre>");
   }catch(e){console.error("TIKTOK test publish failed",e.message);return html(res,500,"Test TikTok non riuscito",String(e.message).replace(/</g,"&lt;"));}
  }
  if(req.method==="GET"&&u.pathname==="/tiktok/status"){
   const st=await autoState();
   if(!isTikTokAdmin(req,st))return html(res,403,"Accesso negato","Ricollega TikTok da /oauth/tiktok per aprire questa pagina.");
   const publishId=u.searchParams.get("publish_id")||"";
   if(!publishId)return html(res,400,"Publish ID mancante","Aggiungi ?publish_id=... all'URL.");
   try{
    const status=await tiktokPublishStatus(publishId);
    const safe=JSON.stringify(status,null,2).replace(/</g,"&lt;");
    return html(res,200,"Stato pubblicazione TikTok","<p>Publish ID: <code>"+publishId.replace(/</g,"&lt;")+"</code></p><pre>"+safe+"</pre>");
   }catch(e){
    console.error("TIKTOK status page failed",publishId,e.message);
    return html(res,500,"Controllo stato TikTok non riuscito",String(e.message).replace(/</g,"&lt;"));
   }
  }
  if(req.method==="GET"&&u.pathname==="/health"){void pump();return json(res,200,{ok:true,service:"fuoconero-social-bridge",version:"0.4.25",mode:"authenticated-remote-render"});}
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

