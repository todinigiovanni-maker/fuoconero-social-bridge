import http from "node:http";
import {worker} from "./worker.js";
import crypto from "node:crypto";
import {readFile,writeFile,mkdtemp,rm,stat} from "node:fs/promises";
import {createReadStream,createWriteStream} from "node:fs";
import {pipeline} from "node:stream/promises";
import {Readable} from "node:stream";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {spawn} from "node:child_process";
import ffmpegPath from "ffmpeg-static";
import Redis from "ioredis";

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
async function sunoCredits(){
 const apiKey=process.env.SUNO_API_KEY;
 if(!apiKey) throw new Error("SUNO_API_KEY not configured");
 const r=await fetch("https://api.aimusicapi.ai/api/v1/get-credits",{
  headers:{Authorization:"Bearer "+apiKey},
  signal:AbortSignal.timeout(30000)
 });
 let data;try{data=await r.json();}catch{data={};}
 if(!r.ok) throw new Error("AI Music API credits check failed HTTP "+r.status);
 return data;
}
async function sunoCreateMusic({title,prompt,tags,mv="chirp-v6"}){
 const apiKey=process.env.SUNO_API_KEY;
 if(!apiKey) throw new Error("SUNO_API_KEY not configured");
 const safeTitle=String(title||"Fuoconero").replace(/\s+/g," ").trim().slice(0,79);
 const payload={task_type:"create_music",custom_mode:true,mv,title:safeTitle,prompt,tags};
 const r=await fetch("https://api.aimusicapi.ai/api/v1/sonic/create",{
  method:"POST",
  headers:{Authorization:"Bearer "+apiKey,"Content-Type":"application/json"},
  body:JSON.stringify(payload),
  signal:AbortSignal.timeout(30000)
 });
 let data;try{data=await r.json();}catch{data={};}
 if(!r.ok||!data?.task_id) throw new Error("AI Music API create failed HTTP "+r.status+": "+JSON.stringify(data).slice(0,500));
 return data;
}
async function sunoTask(taskId){
 const apiKey=process.env.SUNO_API_KEY;
 if(!apiKey) throw new Error("SUNO_API_KEY not configured");
 const r=await fetch("https://api.aimusicapi.ai/api/v1/sonic/task/"+encodeURIComponent(taskId),{
  headers:{Authorization:"Bearer "+apiKey},
  signal:AbortSignal.timeout(30000)
 });
 let data;try{data=await r.json();}catch{data={};}
 if(!r.ok&&r.status!==202) throw new Error("AI Music API task failed HTTP "+r.status);
 return {status:r.status,data};
}
async function verifySunoApi(){
 if(!process.env.SUNO_API_KEY){console.log("SUNO API not configured");return;}
 try{
  const data=await sunoCredits();
  console.log("SUNO API ready credits="+String(data?.credits??"?")+" extra="+String(data?.extra_credits??0));
 }catch(e){console.error("SUNO API check failed",e.message);}
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
 let tick=null;
 for(let attempt=0;attempt<4;attempt++){
  tick=await wp("POST","/publish-worker/tick",{});
  console.log("SCHEDULE publish tick",id,"attempt",attempt+1,tick.status,JSON.stringify(tick.data));
  if(tick.status<200||tick.status>=300)throw new Error("publish tick failed "+id+" HTTP "+tick.status);
  const status=await wp("GET","/jobs/"+encodeURIComponent(prep.data.job_id));
  if(status.status===200&&allDestinationsSucceeded(status.data))break;
  if(status.status===200&&Array.isArray(status.data?.destinations)&&status.data.destinations.some(x=>x?.status==="error")){
   throw new Error("publication failed "+id+": "+JSON.stringify(status.data.destinations));
  }
  if(attempt<3)await sleep(2500);
 }
 const postTick=await wp("GET","/jobs/"+encodeURIComponent(prep.data.job_id));
 if(postTick.status!==200||!allDestinationsSucceeded(postTick.data)){
  console.warn("SCHEDULE publication still queued",id,postTick.status,JSON.stringify(postTick.data?.destinations||[]));
 }
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
async function telegramNotify(message,inlineKeyboard=null){
 const token=process.env.FNS_TELEGRAM_BOT_TOKEN;if(!token)return false;
 // Older callers stored literal "\\n" sequences. Normalize them so Telegram
 // renders real line breaks instead of showing backslash-n in the message.
 message=String(message??"").replace(/\\\\n/g,"\n");
 const chatId=process.env.FNS_TELEGRAM_CHAT_ID||await telegramChatId();
 if(!chatId){console.warn("TELEGRAM no chat id — send /start to the bot");return false;}
 try{
  const body={chat_id:chatId,text:message,disable_web_page_preview:true};
  if(Array.isArray(inlineKeyboard)&&inlineKeyboard.length)body.reply_markup={inline_keyboard:inlineKeyboard};
  const r=await fetch("https://api.telegram.org/bot"+token+"/sendMessage",{
   method:"POST",headers:{"content-type":"application/json"},
   body:JSON.stringify(body),
   signal:AbortSignal.timeout(15000)
  });
  if(!r.ok){console.warn("TELEGRAM send failed",r.status);return false;}
  console.log("TELEGRAM notification sent");return true;
 }catch(e){console.warn("TELEGRAM send failed",e.message);return false;}
}
async function telegramSend(chatId,message,inlineKeyboard=null){
 const token=process.env.FNS_TELEGRAM_BOT_TOKEN;if(!token||!chatId)return false;
 try{
  const body={chat_id:chatId,text:String(message??""),disable_web_page_preview:true};
  if(Array.isArray(inlineKeyboard)&&inlineKeyboard.length)body.reply_markup={inline_keyboard:inlineKeyboard};
  const r=await fetch("https://api.telegram.org/bot"+token+"/sendMessage",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(body),signal:AbortSignal.timeout(15000)});
  if(!r.ok)console.warn("TELEGRAM direct send failed",r.status);
  return r.ok;
 }catch(e){console.warn("TELEGRAM direct send failed",e.message);return false;}
}
async function telegramConfigureCommands(){
 const token=process.env.FNS_TELEGRAM_BOT_TOKEN;if(!token)return;
 try{
  const r=await fetch("https://api.telegram.org/bot"+token+"/setMyCommands",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({commands:[
   {command:"reel",description:"Crea un Reel da un vecchio contenuto"},
   {command:"archivio",description:"Cerca contenuti vecchi da trasformare in Reel"}
  ]}),signal:AbortSignal.timeout(15000)});
  console.log(r.ok?"TELEGRAM commands configured":"TELEGRAM commands config failed "+r.status);
 }catch(e){console.warn("TELEGRAM commands config failed",e.message);}
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
async function searchPublishedPosts(query){
 const q=String(query||"").trim();
 if(!q)return [];
 if(/^\d+$/.test(q)){
  const p=await publishedPostById(Number(q));
  return p?[p]:[];
 }
 const u=new URL(BASE+"/wp-json/wp/v2/posts");
 u.searchParams.set("status","publish");u.searchParams.set("search",q);u.searchParams.set("per_page","8");u.searchParams.set("orderby","relevance");u.searchParams.set("order","desc");
 u.searchParams.set("_fields","id,date,date_gmt,link,title,excerpt,categories");
 const r=await fetch(u,{headers:{"user-agent":"FuoconeroSocialBridge/0.5.0"},signal:AbortSignal.timeout(30000)});
 if(!r.ok)throw new Error("WordPress archive search HTTP "+r.status);
 const a=await r.json();
 return (Array.isArray(a)?a:[]).map(p=>({
  ...p,title:decodeHtml(p?.title?.rendered||p?.title||""),
  excerpt:decodeHtml(p?.excerpt?.rendered||p?.excerpt||"")
 }));
}
async function archivePublishedPage(page=1){
 const u=new URL(BASE+"/wp-json/wp/v2/posts");
 u.searchParams.set("status","publish");u.searchParams.set("per_page","50");u.searchParams.set("page",String(Math.max(1,Number(page)||1)));u.searchParams.set("orderby","date");u.searchParams.set("order","desc");
 u.searchParams.set("_fields","id,date,date_gmt,link,title,excerpt,categories");
 const r=await fetch(u,{headers:{"user-agent":"FuoconeroSocialBridge/0.5.2"},signal:AbortSignal.timeout(30000)});
 if(r.status===400)return [];
 if(!r.ok)throw new Error("WordPress archive feed HTTP "+r.status);
 const a=await r.json();
 return (Array.isArray(a)?a:[]).map(p=>({...p,title:decodeHtml(p?.title?.rendered||p?.title||""),excerpt:decodeHtml(p?.excerpt?.rendered||p?.excerpt||"")}));
}

async function recentPublishedPoems(){
 const u=new URL(BASE+"/wp-json/wp/v2/posts");
 u.searchParams.set("status","publish");u.searchParams.set("categories","14831");
 u.searchParams.set("per_page","50");u.searchParams.set("orderby","date");u.searchParams.set("order","desc");
 u.searchParams.set("_fields","id,date,date_gmt,link,title,excerpt,categories");
 const r=await fetch(u,{headers:{"user-agent":"FuoconeroSocialBridge/0.4.24"},signal:AbortSignal.timeout(30000)});
 if(!r.ok)throw new Error("WordPress poetry feed HTTP "+r.status);
 const a=await r.json();
 return (Array.isArray(a)?a:[]).map(p=>({
  ...p,title:decodeHtml(p?.title?.rendered||p?.title||""),
  excerpt:decodeHtml(p?.excerpt?.rendered||p?.excerpt||"")
 }));
}
function poetryHtmlFromPost(html=""){
 const raw=String(html||"");
 const marked=raw.match(/<div\b[^>]*class=(["'])[^"']*\bfuoconero-poesia\b[^"']*\1[^>]*>([\s\S]*?)<\/div>/i);
 if(marked?.[2]){
  console.log("AUTO_POETRY marked poem block detected");
  return marked[2].replace(/<h[1-6]\b[^>]*>[\s\S]*?<\/h[1-6]>/i,"");
 }
 return raw;
}
function poemTextFromHtml(html=""){
 return String(html||"")
  .replace(/<!--[^]*?-->/g,"")
  .replace(/<(?:script|style)[^>]*>[^]*?<\/(?:script|style)>/gi,"")
  .replace(/<br\s*\/?>/gi,"\n")
  .replace(/<\/(?:p|div|blockquote|li|h[1-6])>/gi,"\n")
  .replace(/<[^>]+>/g," ")
  .replace(/&nbsp;/gi," ")
  .replace(/&#8230;|&hellip;/gi,"…")
  .replace(/&#8211;|&ndash;/gi,"–")
  .replace(/&#8212;|&mdash;/gi,"—")
  .replace(/&#8217;|&rsquo;/gi,"’")
  .replace(/&amp;/gi,"&")
  .replace(/&quot;/gi,'"')
  .replace(/&#39;|&apos;/gi,"'")
  .replace(/&#(\d+);/g,(_,n)=>{try{return String.fromCodePoint(Number(n));}catch{return " ";}})
  .replace(/[ \t]+\n/g,"\n")
  .replace(/\n[ \t]+/g,"\n")
  .replace(/[ \t]{2,}/g," ")
  .replace(/\n{3,}/g,"\n\n")
  .trim();
}
async function publishedPoemText(postId){
 const u=new URL(BASE+"/wp-json/wp/v2/posts/"+encodeURIComponent(postId));
 u.searchParams.set("context","view");u.searchParams.set("_fields","id,status,content");
 const r=await fetch(u,{headers:{"user-agent":"FuoconeroSocialBridge/0.4.25"},signal:AbortSignal.timeout(30000)});
 if(!r.ok)throw new Error("WordPress poem content HTTP "+r.status);
 const p=await r.json();
 const html=p?.content?.rendered||p?.content||"";
 const poemHtml=poetryHtmlFromPost(html);
 const text=poemTextFromHtml(poemHtml);
 console.log("AUTO_POETRY extracted chars",postId,text.length,poemHtml!==html?"marked-block":"full-post");
 return text;
}
async function poetryAudioStatus(postId){
 try{
  const r=await wp("GET","/reel-maker/article/"+encodeURIComponent(postId)+"/audio");
  const a=r?.data?.poetry_audio||{};
  return {status:String(a.status||"missing").toLowerCase(),audio_url:String(a.audio_url||""),drive_file_id:String(a.drive_file_id||""),revision:String(a.revision||"")};
 }catch(e){
  console.warn("AUTO_POETRY audio lookup failed",postId,e.message);
  return {status:"missing",audio_url:"",drive_file_id:"",revision:""};
 }
}
function poetryTitleKey(title=""){
 return decodeHtml(title||"").toLocaleLowerCase("it-IT").normalize("NFKD").replace(/[\u0300-\u036f]/g,"").replace(/[^\p{L}\p{N}]+/gu," ").replace(/\s+/g," ").trim();
}
function rememberPoetryHistory(state,postId,title,status,extra={}){
 const id=String(postId),key=poetryTitleKey(title);
 const history=(state.poetry_history&&typeof state.poetry_history==="object")?state.poetry_history:{};
 history[id]={...(history[id]||{}),post_id:Number(postId),title:String(title||history[id]?.title||""),title_key:key,status,updated_at:Date.now(),...extra};
 state.poetry_history=history;
 if(key){
  const keys=new Set(Array.isArray(state.poetry_title_history)?state.poetry_title_history:[]);
  keys.add(key);
  state.poetry_title_history=[...keys].slice(-2000);
 }
}
function poetryHistoryEvidence(state,post){
 const id=String(post?.id||""),key=poetryTitleKey(post?.title||"");
 const history=(state.poetry_history&&typeof state.poetry_history==="object")?state.poetry_history:{};
 const h=history[id];
 if(h&&["running","generated","approved","rendered","published","manual_done","existing_audio"].includes(String(h.status||"")))return {source:"history",status:h.status};
 const titleHistory=new Set(Array.isArray(state.poetry_title_history)?state.poetry_title_history:[]);
 if(key&&titleHistory.has(key))return {source:"title_history",status:"done"};
 const spoken=(state.spoken_commands&&typeof state.spoken_commands==="object")?state.spoken_commands:{};
 for(const rec of Object.values(spoken)){
  const st=String(rec?.status||"").toLowerCase();
  if(!["running","succeeded"].includes(st))continue;
  if(key&&poetryTitleKey(rec?.title||"")===key)return {source:"spoken_commands",status:st};
 }
 return null;
}
const AUTO_POETRY_MANUAL_DONE=new Set([2293,4796,4820,5261,6800,6816,6990,7053,7248,7359,7443]);

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
const STATE_REDIS_KEY="fuoconero:social:state:v1";
let stateRedis=null;
function redisState(){
 if(!process.env.REDIS_URL)return null;
 if(!stateRedis){
  stateRedis=new Redis(process.env.REDIS_URL,{lazyConnect:true,maxRetriesPerRequest:2,enableReadyCheck:true});
  stateRedis.on("error",e=>console.warn("STATE Redis error",e.message));
 }
 return stateRedis;
}
async function autoState(){
 const r=redisState();
 if(r)try{
  if(r.status==="wait")await r.connect();
  const raw=await r.get(STATE_REDIS_KEY);
  if(raw)return JSON.parse(raw);
 }catch(e){console.warn("STATE Redis read failed; using local fallback",e.message);}
 try{return JSON.parse(await readFile(AUTO_STATE_URL,"utf8"));}catch{return {seen:[]};}
}
async function saveAutoState(state){
 const raw=JSON.stringify(state);
 const r=redisState();
 if(r)try{
  if(r.status==="wait")await r.connect();
  await r.set(STATE_REDIS_KEY,raw);
 }catch(e){console.warn("STATE Redis write failed; using local fallback",e.message);}
 try{await writeFile(AUTO_STATE_URL,raw);}catch(e){console.warn("AUTO_REEL state write failed",e.message);}
}
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
function autoMusicTitleOverride(postId){
 const raw=String(process.env.FNS_AUTO_MUSIC_TITLE_BY_POST||"");
 for(const item of raw.split(";")){
  const i=item.indexOf(":");if(i<1)continue;
  if(Number(item.slice(0,i).trim())===Number(postId)){
   const title=item.slice(i+1).trim();
   return title||null;
  }
 }
 return null;
}
function autoMusicIdOverride(postId){
 const raw=String(process.env.FNS_AUTO_MUSIC_ID_BY_POST||"");
 for(const item of raw.split(";")){
  const i=item.indexOf(":");if(i<1)continue;
  if(Number(item.slice(0,i).trim())===Number(postId)){
   const id=item.slice(i+1).trim();
   return id||null;
  }
 }
 return null;
}
let autoPoetryPollBusy=false;
async function autoPoetryPollTick(){
 if(autoPoetryPollBusy)return;autoPoetryPollBusy=true;
 try{
  const state=await autoState(),pipeline=(state.poetry_pipeline&&typeof state.poetry_pipeline==="object")?state.poetry_pipeline:{};
  const entry=Object.entries(pipeline).find(([,x])=>x&&x.status==="running"&&x.task_id);
  if(!entry)return;
  const [postId,rec]=entry,t=await sunoTask(rec.task_id),payload=t.data;
  const rawSongs=Array.isArray(payload?.data)?payload.data:[];
  const states=rawSongs.map(s=>String(s?.state||"").toLowerCase()).filter(Boolean);
  const status=states.length?(states.every(s=>s==="succeeded")?"succeeded":states.some(s=>s==="failed")?"failed":"running"):String(payload?.state||payload?.status||"").toLowerCase();
  console.log("AUTO_POETRY poll",postId,"state="+status,"clips="+rawSongs.length);
  if(status==="running"||!status)return;
  if(status!=="succeeded"){
   rec.status="failed";rec.last_error="spoken task ended with state "+status;rec.updated_at=Date.now();
   state.poetry_pipeline=pipeline;await saveAutoState(state);
   await telegramNotify("❌ Fuoconero Social — poesia\nGenerazione audio fallita: "+(rec.title||("post "+postId))+".");
   return;
  }
  rec.songs=rawSongs.map(s=>({id:s.clip_id||s.id||null,audio_url:s.audio_url||s.stream_audio_url||null,duration:s.duration||null})).filter(s=>s.audio_url);
  rec.status="generated";rec.updated_at=Date.now();
  state.poetry_pipeline=pipeline;await saveAutoState(state);
  const lines=rec.songs.map((s,i)=>"Versione "+(i+1)+" ("+(s.duration?Number(s.duration).toFixed(1)+" s":"durata n/d")+"): "+s.audio_url);
  const post=await publishedPostById(Number(postId)).catch(()=>null);
  const poemLink=post?.link||"";
  const pickButtons=[
   rec.songs.slice(0,2).map((s,i)=>({text:"✅ VERSIONE "+(i+1),callback_data:"poetrypick:"+postId+":"+(i+1)})),
   poemLink?[{text:"🔗 APRI POESIA",url:poemLink}]:[],
   [{text:"❌ SCARTA POESIA",callback_data:"poetryskip:"+postId}]
  ].filter(row=>row.length);
  await telegramNotify("🎙️ Fuoconero Social — poesia pronta\n"+(rec.title||("Post "+postId))+(poemLink?"\n"+poemLink:"")+"\n\n"+lines.join("\n")+"\n\nScegli direttamente qui sotto quale audio approvare.",pickButtons);
 }catch(e){console.warn("AUTO_POETRY poll failed",e.message);}
 finally{autoPoetryPollBusy=false;}
}
setInterval(()=>void autoPoetryPollTick(),30000).unref();

async function resendPendingPoetryControls(){
 try{
  const state=await autoState();
  if(Number(state.poetry_skip_button_version||0)>=1)return;
  const pipeline=(state.poetry_pipeline&&typeof state.poetry_pipeline==="object")?state.poetry_pipeline:{};
  const pending=Object.entries(pipeline).find(([,x])=>x&&x.status==="generated"&&Array.isArray(x.songs)&&x.songs.length);
  state.poetry_skip_button_version=1;
  await saveAutoState(state);
  if(!pending)return;
  const [postId,rec]=pending;
  const lines=rec.songs.map((s,i)=>"Versione "+(i+1)+" ("+(s.duration?Number(s.duration).toFixed(1)+" s":"durata n/d")+"): "+s.audio_url);
  const post=await publishedPostById(Number(postId)).catch(()=>null);
  const poemLink=post?.link||"";
  const buttons=[
   rec.songs.slice(0,2).map((s,i)=>({text:"✅ VERSIONE "+(i+1),callback_data:"poetrypick:"+postId+":"+(i+1)})),
   poemLink?[{text:"🔗 APRI POESIA",url:poemLink}]:[],
   [{text:"❌ SCARTA POESIA",callback_data:"poetryskip:"+postId}]
  ].filter(row=>row.length);
  await telegramNotify("🎙️ Fuoconero Social — poesia in attesa\n"+(rec.title||("Post "+postId))+(poemLink?"\n"+poemLink:"")+"\n\n"+lines.join("\n")+"\n\nOra puoi scegliere una versione oppure scartare la poesia senza bloccare il flusso.",buttons);
  console.log("AUTO_POETRY pending controls resent",postId);
 }catch(e){console.warn("AUTO_POETRY resend controls failed",e.message);}
}
setTimeout(()=>void resendPendingPoetryControls(),7000);

async function resendFiondeOnce(){
 try{
  const state=await autoState();
  if(Number(state.fionde_resend_version||0)>=1)return;
  const rec=state?.poetry_pipeline?.["5227"];
  state.fionde_resend_version=1;
  await saveAutoState(state);
  if(!rec||!Array.isArray(rec.songs)||!rec.songs.length)return;
  const post=await publishedPostById(5227).catch(()=>null);
  const poemLink=post?.link||"";
  const lines=rec.songs.map((s,i)=>"Versione "+(i+1)+" ("+(s.duration?Number(s.duration).toFixed(1)+" s":"durata n/d")+"): "+s.audio_url);
  const buttons=[
   rec.songs.slice(0,2).map((s,i)=>({text:"✅ VERSIONE "+(i+1),callback_data:"poetrypick:5227:"+(i+1)})),
   poemLink?[{text:"🔗 APRI POESIA",url:poemLink}]:[],
   [{text:"❌ SCARTA POESIA",callback_data:"poetryskip:5227"}]
  ].filter(row=>row.length);
  await telegramNotify("🎙️ Fuoconero Social — riprova Fionde…"+(poemLink?"\n"+poemLink:"")+"\n\n"+lines.join("\n")+"\n\nRiproponiamo la poesia con il flusso aggiornato.",buttons);
  console.log("AUTO_POETRY Fionde controls resent");
 }catch(e){console.warn("AUTO_POETRY Fionde resend failed",e.message);}
}
setTimeout(()=>void resendFiondeOnce(),10000);

async function autoPoetryBranch(state){
 const pipeline=(state.poetry_pipeline&&typeof state.poetry_pipeline==="object")?state.poetry_pipeline:{};
 const rendered=new Set(Array.isArray(state.poetry_rendered)?state.poetry_rendered.map(String):[]);
 let manualCleanup=false;
 for(const doneId of AUTO_POETRY_MANUAL_DONE){
  const key=String(doneId);
  if(pipeline[key]){delete pipeline[key];manualCleanup=true;}
  rendered.add(key);
 }
 if(manualCleanup)state.poetry_last_scan=0;
 state.poetry_pipeline=pipeline;
 state.poetry_rendered=[...rendered].slice(-1000);
 if(Number(state.poetry_resume_version||0)<1){
  state.poetry_last_scan=0;
  state.poetry_resume_version=1;
  await saveAutoState(state);
  console.log("AUTO_POETRY resume migration applied");
 }
 if(Number(state.poetry_extractor_version||0)<2){
  for(const [id,rec] of Object.entries(pipeline)){
   if(rec?.status==="skipped_too_long"){delete pipeline[id];rendered.delete(String(id));}
  }
  state.poetry_extractor_version=2;
  state.poetry_pipeline=pipeline;
  state.poetry_rendered=[...rendered].slice(-1000);
  await saveAutoState(state);
  console.log("AUTO_POETRY extractor migration v2 applied");
 }
 const now=Date.now();
 const lastScan=Number(state.poetry_last_scan||0);
 if(now-lastScan<15*60*1000){console.log("AUTO_POETRY throttled",Math.round((15*60*1000-(now-lastScan))/1000),"s");return;}
 state.poetry_last_scan=now;

 let poems;
 try{poems=await recentPublishedPoems();}
 catch(e){
  if(/HTTP 429/.test(String(e.message||""))){
   console.warn("AUTO_POETRY WordPress rate limited — retry next scheduled scan");
   state.poetry_pipeline=pipeline;state.poetry_rendered=[...rendered].slice(-1000);return;
  }
  throw e;
 }

 // If one poem is already in-flight or awaiting approval, only inspect that one.
 const waitingEntry=Object.entries(pipeline).find(([,x])=>x&&["running","generated","approved"].includes(String(x.status||"")));
 if(waitingEntry){
  const [id,rec]=waitingEntry;
  if(rec.status==="approved"&&/^https:\/\//i.test(String(rec.selected_audio_url||""))){
   const post=poems.find(p=>Number(p.id)===Number(id));
   if(post){
    const scenes=autoScenes(post),publication=autoPublicationMeta(post,"poesie");
    const payload={
     request_id:"fuoconero-auto-poetry-v2-post-"+post.id+"-reel-story",
     post_id:Number(post.id),category:"poesie",
     music_id:process.env.FNS_AUTO_MUSIC_ID||"1Tf5mgp47tL7Gx1DB_yh0j39p06xIl62B",
     outputs:{reel:{preset:"poesia",scene_texts:scenes.reel},story:{preset:"story",scene_texts:scenes.story}},
     publication,publication_authorized:false
    };
    const created=await wp("POST","/reel-maker/render-jobs",payload);
    console.log("AUTO_POETRY enqueue Telegram-approved",post.id,created.status,JSON.stringify(created.data));
    if(created.status>=200&&created.status<300||isExistingRender(created)){
     rendered.add(String(post.id));rec.status="rendered";rec.updated_at=Date.now();
     state.poetry_rendered=[...rendered].slice(-1000);
     if(created.status>=200&&created.status<300){
      await telegramNotify("🖋️ Fuoconero Social\nRecitazione approvata:\n"+post.title+"\n\n⚙️ Reel + Story accodati automaticamente.");
      void pump();
     }
    }
   }
  }else if(rec.status==="generated"){
   const audio=await poetryAudioStatus(Number(id));
   if(audio.status==="approved"&&/^https:\/\//i.test(audio.audio_url)){
    const post=poems.find(p=>Number(p.id)===Number(id));
    if(post){
     const scenes=autoScenes(post),publication=autoPublicationMeta(post,"poesie");
     const payload={
      request_id:"fuoconero-auto-poetry-v1-post-"+post.id+"-reel-story",
      post_id:Number(post.id),category:"poesie",
      music_id:process.env.FNS_AUTO_MUSIC_ID||"1Tf5mgp47tL7Gx1DB_yh0j39p06xIl62B",
      outputs:{reel:{preset:"poesia",scene_texts:scenes.reel},story:{preset:"story",scene_texts:scenes.story}},
      publication,publication_authorized:false
     };
     const created=await wp("POST","/reel-maker/render-jobs",payload);
     console.log("AUTO_POETRY enqueue approved",post.id,created.status,JSON.stringify(created.data));
     if(created.status>=200&&created.status<300||isExistingRender(created)){
      rendered.add(String(post.id));rec.status="rendered";rec.updated_at=Date.now();
      state.poetry_rendered=[...rendered].slice(-1000);
      if(created.status>=200&&created.status<300){
       await telegramNotify("🖋️ Fuoconero Social\nAudio poesia approvato:\n"+post.title+"\n\n⚙️ Reel + Story accodati con la recitazione scelta.");
       void pump();
      }
     }
    }
   }
  }
  state.poetry_pipeline=pipeline;state.poetry_rendered=[...rendered].slice(-1000);
  console.log("AUTO_POETRY waiting",id,rec.status,rec.title||"");
  return;
 }

 // Process exactly one new poem per scan to avoid hammering WordPress and the audio API.
 // Multi-source duplicate guard: durable history + previous manual spoken commands + current pipeline.
 let post=null;
 for(const candidate of poems){
  const candidateId=String(candidate.id);
  if(rendered.has(candidateId)||AUTO_POETRY_MANUAL_DONE.has(Number(candidate.id))||pipeline[candidateId])continue;
  const evidence=poetryHistoryEvidence(state,candidate);
  if(evidence){
   rendered.add(candidateId);
   rememberPoetryHistory(state,candidate.id,candidate.title,"manual_done",{dedupe_source:evidence.source,dedupe_status:evidence.status});
   console.log("AUTO_POETRY duplicate guard skip",candidate.id,evidence.source,evidence.status,candidate.title);
   continue;
  }
  post=candidate;
  break;
 }
 if(!post){
  state.poetry_pipeline=pipeline;state.poetry_rendered=[...rendered].slice(-1000);await saveAutoState(state);
  console.log("AUTO_POETRY no candidate");
  return;
 }

 const id=String(post.id);
 const audio=await poetryAudioStatus(post.id);
 if(audio.status==="approved"&&/^https:\/\//i.test(audio.audio_url)){
  const scenes=autoScenes(post),publication=autoPublicationMeta(post,"poesie");
  const payload={
   request_id:"fuoconero-auto-poetry-v1-post-"+post.id+"-reel-story",
   post_id:Number(post.id),category:"poesie",
   music_id:process.env.FNS_AUTO_MUSIC_ID||"1Tf5mgp47tL7Gx1DB_yh0j39p06xIl62B",
   outputs:{reel:{preset:"poesia",scene_texts:scenes.reel},story:{preset:"story",scene_texts:scenes.story}},
   publication,publication_authorized:false
  };
  const created=await wp("POST","/reel-maker/render-jobs",payload);
  console.log("AUTO_POETRY enqueue existing approved",post.id,created.status,JSON.stringify(created.data));
  if(created.status>=200&&created.status<300||isExistingRender(created)){
   rendered.add(id);state.poetry_rendered=[...rendered].slice(-1000);
   if(created.status>=200&&created.status<300){await telegramNotify("🖋️ Fuoconero Social\nPoesia con audio approvato rilevata:\n"+post.title+"\n\n⚙️ Reel + Story accodati.");void pump();}
  }
  state.poetry_pipeline=pipeline;return;
 }

 const poem=await publishedPoemText(post.id);
 if(!poem||poem.length<20){
  console.warn("AUTO_POETRY empty poem",post.id);
  rendered.add(id);state.poetry_rendered=[...rendered].slice(-1000);state.poetry_pipeline=pipeline;return;
 }
 if(poem.length>4800){
  console.warn("AUTO_POETRY poem too long",post.id,"chars",poem.length,post.title);
  pipeline[id]={post_id:Number(post.id),title:post.title,status:"skipped_too_long",chars:poem.length,updated_at:Date.now()};
  rendered.add(id);state.poetry_pipeline=pipeline;state.poetry_rendered=[...rendered].slice(-1000);await saveAutoState(state);
  await telegramNotify("⏭️ Fuoconero Social — poesia saltata\n"+post.title+"\n\nTesto troppo lungo per la generazione automatica ("+poem.length+" caratteri; limite API 5000). Cercherò automaticamente la prossima poesia adatta.");
  return;
 }
 const created=await sunoCreateMusic({
  title:post.title,prompt:poem,
  tags:"spoken word, Italian male voice, poetry recital, dark ambient, intimate, slow, expressive narration, no singing, no melodic vocal, natural pauses, emotional but restrained",
  mv:"chirp-v6"
 });
 pipeline[id]={post_id:Number(post.id),title:post.title,task_id:created.task_id,status:"running",created_at:Date.now(),updated_at:Date.now()};
 state.poetry_pipeline=pipeline;state.poetry_rendered=[...rendered].slice(-1000);await saveAutoState(state);
 console.log("AUTO_POETRY generation started",post.id,created.task_id,post.title);
 await telegramNotify("🖋️ Fuoconero Social\nPoesia trovata automaticamente:\n"+post.title+"\n"+(post.link||"")+"\n\n🎙️ Sto preparando 2 recitazioni. Nessun Reel verrà creato prima della tua scelta.");
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
  const recoverNonce=String(process.env.FNS_RECOVER_NONCE||"default");
  const recoverPostIds=String(process.env.FNS_RECOVER_POST_IDS||process.env.FNS_RECOVER_POST_ID||"")
   .split(",").map(x=>Number(x.trim())).filter(Boolean);
  const recoveryFor=postId=>{
   const id=Number(postId),key="recovered_"+id+"_"+recoverNonce;
   return recoverPostIds.includes(id)&&!state[key]?{id,key}:null;
  };
  const now=Date.now(),firstRun=!state.initialized;
  for(const post of posts){
   const recovery=recoveryFor(post.id);
   if(seen.has(String(post.id))&&!recovery){console.log("AUTO_REEL skip seen",post.id,post.title);continue;}
   const age=now-postTime(post);
   // On first startup ignore future/scheduled posts WITHOUT marking them seen,
   // otherwise a post discovered a few minutes before its publish time would
   // never be rendered when it actually becomes public.
   if(firstRun&&age<0){console.log("AUTO_REEL skip future first-run",post.id,Math.round(age/60000),post.title);continue;}
   // Old archive posts are marked seen to prevent backfill.
   if(firstRun&&age>120*60*1000){console.log("AUTO_REEL skip first-run age",post.id,Math.round(age/60000),post.title);seen.add(String(post.id));continue;}
   const cats=(post.categories||[]).map(String),category=cats.map(x=>AUTO_REEL_CATEGORY_IDS[x]).find(Boolean);
   if(!category){console.log("AUTO_REEL skip category",post.id,cats,post.title);seen.add(String(post.id));continue;}
   if(category==="poesie"){console.log("AUTO_REEL poetry delegated",post.id,post.title);continue;}
   console.log("AUTO_REEL eligible",post.id,category,Math.round(age/60000),post.title);
   const scenes=autoScenes(post),publication=autoPublicationMeta(post,category);
   const songTitle=category==="canzoni"?cleanAutoTitle(post.title):null;
   const musicTitleOverride=autoMusicTitleOverride(post.id);
   const musicIdOverride=autoMusicIdOverride(post.id);
   const payload={
    request_id:(recovery?"fuoconero-auto-v6-recovery-"+recoverNonce+"-post-"+post.id+"-reel-story":"fuoconero-auto-v5-post-"+post.id+"-reel-story"),post_id:Number(post.id),category,
    ...(musicIdOverride?{music_id:musicIdOverride}:musicTitleOverride?{music_title:musicTitleOverride}:category==="canzoni"?{music_title:songTitle}:{music_id:process.env.FNS_AUTO_MUSIC_ID||"1Tf5mgp47tL7Gx1DB_yh0j39p06xIl62B"}),
    outputs:(recovery&&process.env.FNS_RECOVER_REEL_ONLY==="1"
      ?{reel:{preset:"articolo",scene_texts:scenes.reel}}
      :{reel:{preset:"articolo",scene_texts:scenes.reel},story:{preset:"story",scene_texts:scenes.story}}),
    publication,
    publication_authorized:!!(recovery&&process.env.FNS_RECOVER_AUTO_PUBLISH==="1")
   };
   const created=await wp("POST","/reel-maker/render-jobs",payload);
   console.log("AUTO_REEL enqueue",post.id,created.status,JSON.stringify(created.data));
   if(recovery&&created.status>=200&&created.status<300){
    state[recovery.key]=true;
    state["recovery_job_"+post.id]=created?.data?.render_job_id||"";
    console.log("AUTO_REEL recovery job created",post.id,created?.data?.render_job_id||"",recoverNonce);
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
    const jobCreatedAt=Number(created?.data?.created_at)||0;
    const jobAgeMs=jobCreatedAt?Date.now()-jobCreatedAt*1000:0;
    const isExistingSuccessfulJob=jobCreatedAt&&jobAgeMs>120000;
    if(isExistingSuccessfulJob){
     console.log("AUTO_REEL existing successful job — suppress duplicate notification",post.id,created?.data?.render_job_id||"",Math.round(jobAgeMs/1000));
    }else{
     await telegramNotify("🔥 Fuoconero Social\nNuovo articolo rilevato:\n"+post.title+"\n\n⚙️ Reel + Story accodati. Nessuna pubblicazione social senza approvazione.");
    }
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
  await autoPoetryBranch(state);
  state.initialized=true;state.seen=[...seen].slice(-1000);await saveAutoState(state);
 }catch(e){console.warn("AUTO_REEL tick failed",e.message);await telegramNotify("⚠️ Fuoconero Social\nControllo nuovi articoli/poesie fallito: "+e.message);}
 finally{autoReelBusy=false;}
}
setInterval(()=>void autoReelTick(),300000).unref();

let archiveSuggestionBusy=false;
function archiveAlreadyWorkedIds(state){
 const ids=new Set();
 for(const x of Array.isArray(state?.seen)?state.seen:[])ids.add(String(x));
 for(const x of Array.isArray(state?.poetry_rendered)?state.poetry_rendered:[])ids.add(String(x));
 for(const item of Array.isArray(state?.approval_queue)?state.approval_queue:[]){
  if(item?.post_id)ids.add(String(item.post_id));
 }
 const archiveHist=(state?.telegram_archive_history&&typeof state.telegram_archive_history==="object")?state.telegram_archive_history:{};
 for(const k of Object.keys(archiveHist))ids.add(String(k).split(":")[0]);
 const poetryHist=(state?.poetry_history&&typeof state.poetry_history==="object")?state.poetry_history:{};
 for(const k of Object.keys(poetryHist))ids.add(String(k).split(":")[0]);
 const pipeline=(state?.poetry_pipeline&&typeof state.poetry_pipeline==="object")?state.poetry_pipeline:{};
 for(const k of Object.keys(pipeline))ids.add(String(k));
 return ids;
}
async function nextArchiveSuggestion(chatId,{force=false,excludeId=null}={}){
 if(archiveSuggestionBusy)return false;
 archiveSuggestionBusy=true;
 try{
  const state=await autoState(),now=Date.now();
  const pending=state.archive_pending_suggestion;
  if(!force&&pending?.post_id)return false;
  const last=Number(state.archive_last_suggestion_at||0);
  if(!force&&last&&now-last<6*60*60*1000)return false;

  const proposed=new Set(Array.isArray(state.archive_suggested_ids)?state.archive_suggested_ids.map(String):[]);
  if(excludeId)proposed.add(String(excludeId));
  const archiveHist=(state.telegram_archive_history&&typeof state.telegram_archive_history==="object")?state.telegram_archive_history:{};
  const alreadyRequested=new Set(Object.keys(archiveHist).map(k=>String(k).split(":")[0]));
  const alreadyWorked=archiveAlreadyWorkedIds(state);
  let page=Math.max(1,Number(state.archive_page_cursor||1));
  let posts=await archivePublishedPage(page);
  if(!posts.length&&page>1){page=1;posts=await archivePublishedPage(page);}
  const cutoff=now-7*24*60*60*1000;

  let candidate=null;
  for(const post of posts){
   const id=String(post.id),ts=new Date((post.date_gmt||post.date)+"Z").getTime();
   const category=publicationCategory(post);
   if(!category||ts>cutoff||proposed.has(id)||alreadyRequested.has(id)||alreadyWorked.has(id))continue;
   if(category==="poesie"){
    if(AUTO_POETRY_MANUAL_DONE.has(Number(post.id))||poetryHistoryEvidence(state,post))continue;
    const pipe=(state.poetry_pipeline&&typeof state.poetry_pipeline==="object")?state.poetry_pipeline:{};
    if(pipe[id])continue;
   }
   candidate=post;break;
  }
  if(!candidate){
   state.archive_page_cursor=page+1;
   state.archive_last_suggestion_at=force?0:now;
   await saveAutoState(state);
   if(force)await telegramSend(chatId,"📚 In questa parte dell’archivio non ho trovato candidati nuovi. Al prossimo giro passo alla pagina successiva.");
   return false;
  }

  state.archive_page_cursor=page;
  const id=String(candidate.id),category=publicationCategory(candidate);
  proposed.add(id);
  state.archive_suggested_ids=[...proposed].slice(-2000);
  state.archive_pending_suggestion={post_id:Number(candidate.id),title:candidate.title,category,created_at:now};
  state.archive_last_suggestion_at=now;
  await saveAutoState(state);

  const label=category==="poesie"?"🖋️ POESIA DA RECUPERARE":"📚 DAL VECCHIO ARCHIVIO";
  const body=label+"\n\n"+candidate.title+"\n\nQuesto non risulta ancora lavorato dal nuovo sistema. Lo trasformiamo in Reel?";
  const buttons=[
   [{text:category==="poesie"?"🎙️ RECITA + REEL":"🎬 CREA REEL",callback_data:"archiverender:"+candidate.id+":reel"}],
   [{text:category==="poesie"?"🎙️ RECITA + REEL + STORY":"🎬 + 📱 REEL + STORY",callback_data:"archiverender:"+candidate.id+":both"}],
   [{text:"⏭️ SALTA",callback_data:"archiveskip:"+candidate.id},{text:"🔄 DAMMENE UN ALTRO",callback_data:"archivenext:"+candidate.id}]
  ];
  await telegramSend(chatId,body,buttons);
  console.log("ARCHIVE suggestion sent",candidate.id,category,candidate.title);
  return true;
 }catch(e){
  console.warn("ARCHIVE suggestion failed",e.message);
  return false;
 }finally{archiveSuggestionBusy=false;}
}
async function archiveSuggestionTick(){
 const chatId=process.env.FNS_TELEGRAM_CHAT_ID||await telegramChatId();
 if(!chatId)return;
 await nextArchiveSuggestion(chatId);
}
async function telegramArchiveRender(post,mode,chatId){
 const state=await autoState(),category=publicationCategory(post),key=String(post.id)+":"+mode;
 if(Number(state.archive_pending_suggestion?.post_id)===Number(post.id))state.archive_pending_suggestion=null;
 const hist=(state.telegram_archive_history&&typeof state.telegram_archive_history==="object")?state.telegram_archive_history:{};
 const previous=hist[key];
 const alreadyWorked=archiveAlreadyWorkedIds(state);
 if(alreadyWorked.has(String(post.id))&&!previous?.render_job_id){
  await telegramSend(chatId,"♻️ Questo contenuto risulta già lavorato dal sistema Reel:\n"+post.title+"\n\nNon creo un doppione. Cerco il prossimo candidato.");
  await nextArchiveSuggestion(chatId,{force:true,excludeId:post.id});
  return;
 }
 if(previous?.render_job_id){
  const out=await wp("GET","/reel-maker/render-jobs/"+encodeURIComponent(previous.render_job_id)+"/output");
  const reelId=findDriveFileId(out.data,"reel"),storyId=findDriveFileId(out.data,"story"),buttons=[];
  if(reelId)buttons.push({text:"🎬 APRI REEL",url:"https://drive.google.com/file/d/"+encodeURIComponent(reelId)+"/preview"});
  if(storyId)buttons.push({text:"📱 APRI STORY",url:"https://drive.google.com/file/d/"+encodeURIComponent(storyId)+"/preview"});
  await telegramSend(chatId,"♻️ Questo contenuto era già stato richiesto dal Reel Maker archivio:\n"+post.title+"\n\nNon creo un doppione.",buttons.length?[buttons]:null);
  return;
 }
 if(category==="poesie"){
  const audio=await poetryAudioStatus(post.id);
  if(!(audio.status==="approved"&&/^https:\/\//i.test(audio.audio_url))){
   const pipeline=(state.poetry_pipeline&&typeof state.poetry_pipeline==="object")?state.poetry_pipeline:{};
   const busy=Object.entries(pipeline).find(([,x])=>x&&["running","generated","approved"].includes(String(x.status||"")));
   if(busy&&Number(busy[0])!==Number(post.id)){
    await telegramSend(chatId,"🎙️ C’è già una poesia in lavorazione: "+(busy[1]?.title||("post "+busy[0]))+".\n\nPrima scegli/chiudi quella, poi rilancia /reel per "+post.title+".");
    return;
   }
   if(!pipeline[String(post.id)]){
    const poem=await publishedPoemText(post.id);
    if(!poem||poem.length<20||poem.length>4800){
     await telegramSend(chatId,"⚠️ Non posso avviare la recitazione automatica di questa poesia: testo non disponibile o troppo lungo.");
     return;
    }
    const created=await sunoCreateMusic({title:post.title,prompt:poem,tags:"spoken word, Italian male voice, poetry recital, dark ambient, intimate, slow, expressive narration, no singing, no melodic vocal, natural pauses, emotional but restrained",mv:"chirp-v6"});
    pipeline[String(post.id)]={post_id:Number(post.id),title:post.title,task_id:created.task_id,status:"running",created_at:Date.now(),updated_at:Date.now()};
    state.poetry_pipeline=pipeline;
    rememberPoetryHistory(state,post.id,post.title,"running",{task_id:created.task_id,source:"telegram_archive"});
    state.poetry_last_scan=Date.now();
    await saveAutoState(state);
   }
   await telegramSend(chatId,"🎙️ È una poesia e non ha ancora un audio approvato.\n\nHo avviato due recitazioni per “"+post.title+"”. Quando sono pronte scegli VERSIONE 1 o VERSIONE 2: dopo la scelta il flusso creerà Reel + Story.");
   return;
  }
 }
 const scenes=autoScenes(post),publication=autoPublicationMeta(post,category);
 const outputs=mode==="reel"?{reel:{preset:category==="poesie"?"poesia":"articolo",scene_texts:scenes.reel}}:{reel:{preset:category==="poesie"?"poesia":"articolo",scene_texts:scenes.reel},story:{preset:"story",scene_texts:scenes.story}};
 const musicTitleOverride=autoMusicTitleOverride(post.id),musicIdOverride=autoMusicIdOverride(post.id);
 const payload={
  request_id:"fuoconero-telegram-archive-v1-post-"+post.id+"-"+mode,
  post_id:Number(post.id),category,
  ...(musicIdOverride?{music_id:musicIdOverride}:musicTitleOverride?{music_title:musicTitleOverride}:category==="canzoni"?{music_title:cleanAutoTitle(post.title)}:{music_id:process.env.FNS_AUTO_MUSIC_ID||"1Tf5mgp47tL7Gx1DB_yh0j39p06xIl62B"}),
  outputs,publication,publication_authorized:false
 };
 const created=await wp("POST","/reel-maker/render-jobs",payload);
 if(!(created.status>=200&&created.status<300)&&!isExistingRender(created))throw new Error(String(created?.data?.message||created?.data?.error||"render non creato"));
 const jobId=created?.data?.render_job_id||previous?.render_job_id||null;
 hist[key]={post_id:Number(post.id),title:post.title,mode,render_job_id:jobId,status:"requested",created_at:Date.now()};
 state.telegram_archive_history=hist;await saveAutoState(state);
 await telegramSend(chatId,"⚙️ Reel Maker archivio\n\n"+post.title+"\n\n"+(mode==="reel"?"Creo il Reel.":"Creo Reel + Story.")+" Ti mando l’anteprima su Telegram appena il render è pronto.");
 void pump();
}
let telegramOffset=null,telegramApprovalBusy=false,telegramKnownChatId=process.env.FNS_TELEGRAM_CHAT_ID||null;
let telegramOffsetLoaded=false,telegramPollFailures=0;
const telegramHandled=new Set();
async function loadTelegramOffset(){
 if(telegramOffsetLoaded)return;
 telegramOffsetLoaded=true;
 try{
  const st=await autoState();
  const saved=Number(st?.telegram_update_offset);
  if(Number.isInteger(saved)&&saved>=0){telegramOffset=saved;console.log("TELEGRAM offset restored",saved);}
 }catch(e){console.warn("TELEGRAM offset restore failed",e.message);}
}
async function saveTelegramOffset(){
 if(telegramOffset===null)return;
 try{
  const st=await autoState();
  if(Number(st?.telegram_update_offset)===telegramOffset)return;
  st.telegram_update_offset=telegramOffset;
  await saveAutoState(st);
 }catch(e){console.warn("TELEGRAM offset persist failed",e.message);}
}
async function telegramGetUpdates(url,attempts=3){
 let lastError=null;
 for(let attempt=1;attempt<=attempts;attempt++){
  try{
   const r=await fetch(url,{signal:AbortSignal.timeout(12000)});
   if(!r.ok){
    const body=await r.text().catch(()=>"");
    throw new Error("HTTP "+r.status+(body?" "+body.slice(0,180):""));
   }
   const j=await r.json();
   if(j?.ok===false)throw new Error("Telegram "+String(j?.error_code||"API")+" "+String(j?.description||"error"));
   telegramPollFailures=0;
   return j;
  }catch(e){
   lastError=e;
   if(attempt<attempts)await new Promise(r=>setTimeout(r,500*attempt));
  }
 }
 telegramPollFailures++;
 throw new Error((lastError?.message||"getUpdates failed")+" (after "+attempts+" attempts, consecutive failures "+telegramPollFailures+")");
}
async function telegramAnswerCallback(token,id,textValue){
 try{await fetch("https://api.telegram.org/bot"+token+"/answerCallbackQuery",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({callback_query_id:id,text:textValue,show_alert:false}),signal:AbortSignal.timeout(15000)});}catch(e){console.warn("TELEGRAM callback answer failed",e.message);}
}
async function telegramApprovalTick(){
 if(telegramApprovalBusy)return;telegramApprovalBusy=true;
 try{
  const token=process.env.FNS_TELEGRAM_BOT_TOKEN;if(!token)return;
  await loadTelegramOffset();
  const qs=new URLSearchParams({timeout:"0",limit:"20",allowed_updates:JSON.stringify(["message","callback_query"])});
  if(telegramOffset!==null)qs.set("offset",String(telegramOffset));
  const j=await telegramGetUpdates("https://api.telegram.org/bot"+token+"/getUpdates?"+qs),updates=Array.isArray(j?.result)?j.result:[];
  if(!telegramKnownChatId){for(let i=updates.length-1;i>=0;i--){const id=updates[i]?.message?.chat?.id||updates[i]?.callback_query?.message?.chat?.id;if(id){telegramKnownChatId=String(id);console.log("TELEGRAM chat learned");try{const st=await autoState();st.telegram_chat_id=telegramKnownChatId;await saveAutoState(st);console.log("TELEGRAM chat persisted");}catch(e){console.warn("TELEGRAM chat persist failed",e.message);}break;}}}
  const allowed=String(process.env.FNS_TELEGRAM_CHAT_ID||telegramKnownChatId||"");
  if(telegramOffset===null){
   telegramOffset=updates.length?Math.max(...updates.map(x=>Number(x.update_id)||0))+1:0;
   await saveTelegramOffset();
   return;
  }
  for(const update of updates){
   telegramOffset=Math.max(telegramOffset,(Number(update.update_id)||0)+1);
   const msg=update?.message,msgChat=String(msg?.chat?.id||""),msgText=String(msg?.text||"").trim();
   if(msg&&msgChat===allowed){
    const reelCmd=msgText.match(/^\/(?:reel|archivio)(?:@\w+)?(?:\s+(.+))?$/i);
    if(reelCmd){
     const query=String(reelCmd[1]||"").trim();
     if(!query){
      await telegramSend(msgChat,"🎬 Reel Maker archivio\n\nScrivi:\n/reel titolo o parole da cercare\n\noppure:\n/reel 1234\n\nse conosci l’ID del post.");
     }else{
      try{
       const hits=await searchPublishedPosts(query);
       if(!hits.length)await telegramSend(msgChat,"🔎 Non ho trovato contenuti pubblicati per: “"+query+"”.");
       else{
        const rows=hits.slice(0,6).map(p=>[{text:"🎬 "+String(p.title||("Post "+p.id)).slice(0,52),callback_data:"archivepick:"+p.id}]);
        await telegramSend(msgChat,"🔎 Ho trovato "+hits.length+" risultat"+(hits.length===1?"o":"i")+" per “"+query+"”.\nScegli cosa vuoi trasformare in Reel:",rows);
       }
      }catch(e){console.warn("TELEGRAM archive search failed",e.message);await telegramSend(msgChat,"⚠️ Ricerca archivio non riuscita: "+e.message);}
     }
     continue;
    }
   }

   const q=update?.callback_query,data=String(q?.data||""),chat=String(q?.message?.chat?.id||"");
   if(!q||chat!==allowed||telegramHandled.has(data))continue;

   const archivePick=data.match(/^archivepick:(\d+)$/);
   if(archivePick){
    telegramHandled.add(data);
    try{
     const post=await publishedPostById(Number(archivePick[1]));
     if(!post)throw new Error("contenuto non trovato o non pubblicato");
     await telegramAnswerCallback(token,q.id,"Selezionato.");
     await telegramSend(chat,"🎬 Reel Maker archivio\n\n"+post.title+"\n\nCosa preparo?",[
      [{text:"🎬 SOLO REEL",callback_data:"archiverender:"+post.id+":reel"}],
      [{text:"🎬 + 📱 REEL + STORY",callback_data:"archiverender:"+post.id+":both"}]
     ]);
    }catch(e){await telegramAnswerCallback(token,q.id,"Non disponibile.");await telegramSend(chat,"⚠️ "+e.message);}
    continue;
   }

   const archiveSkip=data.match(/^archiveskip:(\d+)$/);
   if(archiveSkip){
    telegramHandled.add(data);
    try{
     const state=await autoState();
     if(Number(state.archive_pending_suggestion?.post_id)===Number(archiveSkip[1]))state.archive_pending_suggestion=null;
     const skipped=new Set(Array.isArray(state.archive_skipped_ids)?state.archive_skipped_ids.map(String):[]);
     skipped.add(String(archiveSkip[1]));state.archive_skipped_ids=[...skipped].slice(-2000);
     await saveAutoState(state);
     await telegramAnswerCallback(token,q.id,"Saltato.");
     await telegramSend(chat,"⏭️ Saltato. Non te lo ripropongo.");
    }catch(e){await telegramAnswerCallback(token,q.id,"Errore.");}
    continue;
   }

   const archiveNext=data.match(/^archivenext:(\d+)$/);
   if(archiveNext){
    telegramHandled.add(data);
    try{
     const state=await autoState();
     if(Number(state.archive_pending_suggestion?.post_id)===Number(archiveNext[1]))state.archive_pending_suggestion=null;
     await saveAutoState(state);
     await telegramAnswerCallback(token,q.id,"Cerco il prossimo.");
     await nextArchiveSuggestion(chat,{force:true,excludeId:Number(archiveNext[1])});
    }catch(e){console.warn("ARCHIVE next failed",e.message);await telegramAnswerCallback(token,q.id,"Errore.");}
    continue;
   }

   const archiveRender=data.match(/^archiverender:(\d+):(reel|both)$/);
   if(archiveRender){
    telegramHandled.add(data);
    try{
     const post=await publishedPostById(Number(archiveRender[1]));
     if(!post)throw new Error("contenuto non trovato o non pubblicato");
     await telegramAnswerCallback(token,q.id,"Avvio il render.");
     await telegramArchiveRender(post,archiveRender[2],chat);
    }catch(e){console.warn("TELEGRAM archive render failed",e.message);await telegramAnswerCallback(token,q.id,"Render non avviato.");await telegramSend(chat,"⚠️ Reel Maker archivio: "+e.message);}
    continue;
   }

   const poetrySkip=data.match(/^poetryskip:(\d+)$/);
   if(poetrySkip){
    const postId=Number(poetrySkip[1]);
    try{
     const state=await autoState();
     const pipeline=(state.poetry_pipeline&&typeof state.poetry_pipeline==="object")?state.poetry_pipeline:{};
     const rec=pipeline[String(postId)];
     const title=rec?.title||("Post "+postId);
     delete pipeline[String(postId)];
     const rendered=new Set(Array.isArray(state.poetry_rendered)?state.poetry_rendered.map(String):[]);
     rendered.add(String(postId));
     state.poetry_pipeline=pipeline;
     state.poetry_rendered=[...rendered].slice(-1000);
     state.poetry_last_scan=0;
     rememberPoetryHistory(state,postId,title,"skipped_by_user",{source:"telegram",skipped_at:Date.now()});
     await saveAutoState(state);
     telegramHandled.add(data);
     await telegramAnswerCallback(token,q.id,"Poesia scartata.");
     await telegramNotify("⏭️ Fuoconero Social — poesia scartata\n"+title+"\n\nNon la ripropongo. Cerco la prossima poesia.");
     setTimeout(()=>void autoReelTick(),1000);
    }catch(e){
     await telegramAnswerCallback(token,q.id,"Non sono riuscito a scartarla.");
     console.warn("TELEGRAM poetry skip failed",postId,e.message);
    }
    continue;
   }

   const poetryPick=data.match(/^poetrypick:(\d+):([12])$/);
   if(poetryPick){
    const postId=Number(poetryPick[1]),choice=Number(poetryPick[2]);
    try{
     const state=await autoState();
     const pipeline=(state.poetry_pipeline&&typeof state.poetry_pipeline==="object")?state.poetry_pipeline:{};
     const rec=pipeline[String(postId)];
     if(!rec||!Array.isArray(rec.songs)||!rec.songs[choice-1]?.audio_url)throw new Error("versione audio non disponibile");
     const selected=rec.songs[choice-1];
     rec.selected_index=choice;
     rec.selected_audio_url=selected.audio_url;
     rec.selected_audio_id=selected.id||null;
     rec.selected_duration=selected.duration||null;
     rec.selected_at=Date.now();
     rec.status="approving";
     pipeline[String(postId)]=rec;
     state.poetry_pipeline=pipeline;
     await saveAutoState(state);
     try{
      const saved=await wp("POST","/reel-maker/article/"+encodeURIComponent(postId)+"/audio",{
       approved:true,audio_url:selected.audio_url,drive_file_id:"",audio_source:"suno"
      });
      const official=saved?.data?.poetry_audio;
      if(saved.status<200||saved.status>=300||official?.status!=="approved"||!/^https:\/\//i.test(String(official?.audio_url||""))){
       throw new Error(String(saved?.data?.message||saved?.data?.error||"salvataggio audio ufficiale non riuscito"));
      }
      rec.selected_audio_url=official.audio_url;
      rec.official_audio_url=official.audio_url;
      rec.official_audio_revision=official.revision||null;
      rec.status="render_queued";
      pipeline[String(postId)]=rec;state.poetry_pipeline=pipeline;await saveAutoState(state);
      telegramHandled.add(data);
      await telegramAnswerCallback(token,q.id,"Versione "+choice+" approvata. Audio salvato nel blog; creo Reel + Story.");
      const post=await publishedPostById(postId);
      if(!post)throw new Error("poesia pubblicata non disponibile");
      const scenes=autoScenes(post),publication=autoPublicationMeta(post,"poesie");
      const payload={
       request_id:"fuoconero-auto-poetry-v3-post-"+post.id+"-reel-story",
       post_id:Number(post.id),category:"poesie",
       music_id:process.env.FNS_AUTO_MUSIC_ID||"1Tf5mgp47tL7Gx1DB_yh0j39p06xIl62B",
       outputs:{reel:{preset:"poesia",scene_texts:scenes.reel},story:{preset:"story",scene_texts:scenes.story}},
       publication,publication_authorized:false
      };
      const created=await wp("POST","/reel-maker/render-jobs",payload);
      if(!(created.status>=200&&created.status<300)&&!isExistingRender(created))throw new Error(String(created?.data?.message||created?.data?.error||"render non creato"));
      rec.status="rendered";rec.updated_at=Date.now();rec.render_job_id=created?.data?.render_job_id||rec.render_job_id||null;
      const rendered=new Set(Array.isArray(state.poetry_rendered)?state.poetry_rendered.map(String):[]);
      rendered.add(String(postId));
      state.poetry_rendered=[...rendered].slice(-1000);
      rememberPoetryHistory(state,postId,post.title,"rendered",{render_job_id:rec.render_job_id,selected_audio_id:rec.selected_audio_id||null});
      await saveAutoState(state);
      await telegramNotify("✅ Fuoconero Social — poesia\nVersione "+choice+" approvata per:\n"+post.title+"\n\n⚙️ Reel + Story accodati subito con questa recitazione.");
      void pump();
     }catch(renderError){
      const official=await poetryAudioStatus(postId).catch(()=>({status:"missing"}));
      rec.status=official?.status==="approved"?"approved":"generated";
      rec.updated_at=Date.now();rec.last_error=renderError.message;
      state.poetry_last_scan=0;state.poetry_pipeline=pipeline;await saveAutoState(state);
      if(rec.status==="approved"){
       telegramHandled.add(data);
       await telegramAnswerCallback(token,q.id,"Audio salvato; render da riprovare.");
       await telegramNotify("⚠️ Fuoconero Social — poesia\nL’audio scelto è già nel blog, ma il render non è partito subito: "+renderError.message+"\nRiproverò automaticamente.");
      }else{
       telegramHandled.delete(data);
       await telegramAnswerCallback(token,q.id,"Salvataggio audio non riuscito. Puoi riprovare.");
       await telegramNotify("⚠️ Fuoconero Social — poesia\nNon sono riuscito a salvare la recitazione nel blog: "+renderError.message+"\nPuoi premere di nuovo VERSIONE "+choice+".");
      }
      console.warn("TELEGRAM poetry approval/render failed",postId,renderError.message);
     }
    }catch(e){
     await telegramAnswerCallback(token,q.id,"Non sono riuscito a registrare la scelta.");
     console.warn("TELEGRAM poetry pick failed",postId,choice,e.message);
    }
    continue;
   }

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
  await saveTelegramOffset();
 }catch(e){console.warn("TELEGRAM approval poll failed",e.message);}
 finally{telegramApprovalBusy=false;}
}
setInterval(()=>void telegramApprovalTick(),5000).unref();
setTimeout(()=>void telegramApprovalTick(),3000);
setTimeout(()=>void telegramConfigureCommands(),5000);
setInterval(()=>void archiveSuggestionTick(),15*60*1000).unref();
setTimeout(()=>void archiveSuggestionTick(),15000);

async function runCommand(){
 let c; try{c=JSON.parse(await readFile(new URL("./command.json",import.meta.url),"utf8"));}catch(e){console.error("COMMAND read error",e.message);return;}
 if(!c||c.action==="noop"){console.log("COMMAND idle",c?.id||"none");return;}
 if(!c.id){console.error("COMMAND invalid: missing id");return;}

 if(c.action==="suno_spoken_test"||c.action==="suno_poll_existing"){
  const title=String(c.title||"Spoken poem").trim();
  const state=await autoState();
  const spoken=(state.spoken_commands&&typeof state.spoken_commands==="object")?state.spoken_commands:{};
  let record=(spoken[c.id]&&typeof spoken[c.id]==="object")?spoken[c.id]:{};
  const persist=async patch=>{
   record={...record,...patch,updated_at:Date.now()};
   spoken[c.id]=record;
   const entries=Object.entries(spoken).sort((a,b)=>Number(b[1]?.updated_at||0)-Number(a[1]?.updated_at||0)).slice(0,100);
   state.spoken_commands=Object.fromEntries(entries);
   await saveAutoState(state);
  };
  try{
   let taskId=String(c.task_id||record.task_id||"").trim();
   let songs=Array.isArray(record.songs)?record.songs:[];
   if(record.status==="succeeded"&&songs.length){
    console.log("COMMAND spoken already completed",c.id,"task",taskId||"stored");
    if(!record.notified){
     const lines=songs.map((s,i)=>"Versione "+(i+1)+": "+(s.audio_url||"(audio URL mancante)"));
     await telegramNotify("🎙️ Poesia recitata pronta\n"+title+"\n\n"+lines.join("\n"));
     await persist({notified:true});
    }
    return;
   }
   if(c.action==="suno_spoken_test"&&!taskId){
    const poem=String(c.poem||"").trim();
    if(!poem){console.error("COMMAND suno_spoken_test missing poem");return;}
    console.log("COMMAND suno_spoken_test creating",title);
    const created=await sunoCreateMusic({
     title,
     prompt:poem,
     tags:String(c.tags||"spoken word, Italian male vocal, poetry recital, dark ambient, cinematic, slow, intimate, expressive narration, no singing, no melodic vocal"),
     mv:String(c.mv||"chirp-v6")
    });
    taskId=created.task_id;
    await persist({task_id:taskId,status:"running",title,created_at:record.created_at||Date.now(),notified:false});
    console.log("SUNO SPOKEN task_id="+taskId);
   }else{
    console.log("COMMAND spoken resume",c.id,taskId,title);
    if(taskId&&!record.task_id)await persist({task_id:taskId,status:"running",title,created_at:record.created_at||Date.now(),notified:false});
   }
   if(!taskId)throw new Error("missing task id");
   let payload=null;
   for(let i=0;i<36;i++){
    await new Promise(r=>setTimeout(r,15000));
    const t=await sunoTask(taskId);
    payload=t.data;
    const rawSongs=Array.isArray(payload?.data)?payload.data:[];
    songs=rawSongs.map(s=>({
     id:s.clip_id||s.id||null,
     audio_url:s.audio_url||s.stream_audio_url||null,
     title:s.title||title,
     duration:s.duration||null,
     state:s.state||null
    }));
    const states=rawSongs.map(s=>String(s?.state||"").toLowerCase()).filter(Boolean);
    const stateName=states.length?(states.every(s=>s==="succeeded")?"succeeded":states.some(s=>s==="failed")?"failed":"running"):String(payload?.state||payload?.status||"").toLowerCase();
    console.log("SUNO SPOKEN poll",i+1,"state="+stateName,"clips="+rawSongs.length);
    if(stateName==="succeeded"||stateName==="failed")break;
   }
   const finalStates=songs.map(s=>String(s?.state||"").toLowerCase()).filter(Boolean);
   const finalState=finalStates.length?(finalStates.every(s=>s==="succeeded")?"succeeded":finalStates.some(s=>s==="failed")?"failed":"running"):String(payload?.state||payload?.status||"").toLowerCase();
   if(finalState!=="succeeded")throw new Error("spoken task ended with state "+finalState);
   songs=songs.map(({state,...s})=>s);
   await persist({task_id:taskId,status:"succeeded",songs,notified:false});
   console.log("SUNO SPOKEN success "+JSON.stringify(songs));
   const lines=songs.map((s,i)=>"Versione "+(i+1)+": "+(s.audio_url||"(audio URL mancante)"));
   await telegramNotify("🎙️ Poesia recitata pronta\n"+title+"\n\n"+lines.join("\n"));
   await persist({notified:true});
  }catch(e){
   console.error("COMMAND "+c.action+" failed",e.message);
   await persist({status:"failed",last_error:e.message});
   await telegramNotify("❌ Poesia recitata fallita\n"+title+"\n"+e.message);
  }
  return;
 }
 if(c.action==="metricool_watch_batch"){
  const items=Array.isArray(c.items)?c.items:[];
  if(!items.length||items.length>50){console.error("COMMAND invalid metricool_watch_batch");return;}
  let ok=0;
  for(const item of items){
   try{await registerMetricoolWatch(item);ok++;}
   catch(e){console.error("COMMAND metricool_watch_batch item failed",item?.id||"unknown",e.message);}
  }
  console.log("COMMAND metricool_watch_batch",c.id,"registered",ok,"of",items.length);
  void metricoolWatchTick();
  return;
 }
 if(c.action==="metricool_watch"){
  try{await registerMetricoolWatch(c.watch||c);console.log("COMMAND metricool_watch registered",c.id);void metricoolWatchTick();}
  catch(e){console.error("COMMAND metricool_watch failed",c.id,e.message);}
  return;
 }
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


const METRICOOL_WATCH_WINDOW_BEFORE_MS=15*60*1000;
const METRICOOL_WATCH_WINDOW_AFTER_MS=3*60*60*1000;
const METRICOOL_WATCH_EXPIRE_MS=24*60*60*1000;
let metricoolWatchBusy=false;

function normalizeMetricoolText(value=""){
 return decodeHtml(value).toLowerCase().normalize("NFD").replace(/\p{Diacritic}/gu,"").replace(/https?:\/\/\S+/g," ").replace(/[^a-z0-9#]+/g," ").replace(/\s+/g," ").trim();
}
function metricoolTextTokens(value=""){
 const stop=new Set(["fuoconero","com","www","https","http","reel","tiktok","video"]);
 return normalizeMetricoolText(value).split(" ").map(x=>x.replace(/^#/,"")).filter(x=>x.length>=3&&!stop.has(x));
}
function metricoolTextScore(expected,actual){
 const e=[...new Set(metricoolTextTokens(expected))],a=new Set(metricoolTextTokens(actual));
 if(!e.length)return 0;
 let hit=0;for(const x of e)if(a.has(x))hit++;
 return hit/e.length;
}
async function tiktokListRecentVideos(){
 const token=await tiktokOauthToken();
 const url=new URL("https://open.tiktokapis.com/v2/video/list/");
 url.searchParams.set("fields","id,create_time,title,video_description,duration,share_url");
 const r=await fetch(url,{
  method:"POST",
  headers:{authorization:"Bearer "+token,"content-type":"application/json; charset=UTF-8"},
  body:JSON.stringify({max_count:20}),
  signal:AbortSignal.timeout(30000)
 });
 const data=await r.json();
 if(!r.ok||(data?.error?.code&&data.error.code!=="ok")){
  throw new Error("TikTok video.list failed: "+(data?.error?.message||data?.error?.code||r.status));
 }
 return Array.isArray(data?.data?.videos)?data.data.videos:[];
}
async function registerMetricoolWatch(input){
 if(!input||typeof input!=="object")throw new Error("watch payload missing");
 const id=String(input.id||"").trim();
 const scheduledAt=Date.parse(input.scheduled_at||input.publish_at||"");
 if(!id||!Number.isFinite(scheduledAt))throw new Error("watch id/scheduled_at invalid");
 const st=await autoState();
 if(!Array.isArray(st.metricool_tiktok_watches))st.metricool_tiktok_watches=[];
 const existing=st.metricool_tiktok_watches.find(x=>x?.id===id);
 const next={
  id,
  title:String(input.title||"Fuoconero").slice(0,180),
  caption:String(input.caption||input.title||"").slice(0,2200),
  scheduled_at:new Date(scheduledAt).toISOString(),
  drive_file_id:input.drive_file_id?String(input.drive_file_id):"",
  storage_id:input.storage_id?String(input.storage_id):"",
  metricool_post_id:input.metricool_post_id?String(input.metricool_post_id):"",
  status:"pending",
  created_at:existing?.created_at||new Date().toISOString(),
  updated_at:new Date().toISOString(),
  matched_video_id:existing?.matched_video_id||"",
  matched_share_url:existing?.matched_share_url||"",
  published_notified:existing?.published_notified===true,
  deleted_notified:existing?.deleted_notified===true,
  expiry_notified:existing?.expiry_notified===true,
  cleanup_attempted_at:existing?.cleanup_attempted_at||"",
  last_error:""
 };
 if(existing)Object.assign(existing,next);
 else st.metricool_tiktok_watches.push(next);
 // Keep completed history bounded but long enough to prevent duplicate matching.
 st.metricool_tiktok_watches=st.metricool_tiktok_watches
  .sort((a,b)=>Date.parse(b?.scheduled_at||0)-Date.parse(a?.scheduled_at||0))
  .slice(0,120);
 await saveAutoState(st);
 console.log("METRICOOL watch registered",id,next.scheduled_at,next.title);
 return next;
}
function metricoolVideoMatch(watch,videos,claimed){
 const scheduled=Date.parse(watch.scheduled_at);
 const expected=watch.caption||watch.title||"";
 const titleNorm=normalizeMetricoolText(watch.title||"");
 const ranked=[];
 for(const v of videos){
  if(!v?.id||claimed.has(String(v.id)))continue;
  const created=Number(v.create_time||0)*1000;
  const delta=created-scheduled;
  if(!created||delta<-METRICOOL_WATCH_WINDOW_BEFORE_MS||delta>METRICOOL_WATCH_WINDOW_AFTER_MS)continue;
  const actual=[v.title,v.video_description].filter(Boolean).join(" ");
  const score=metricoolTextScore(expected,actual);
  const actualNorm=normalizeMetricoolText(actual);
  const titleHit=titleNorm.length>=12&&(actualNorm.includes(titleNorm)||titleNorm.includes(actualNorm.slice(0,Math.min(60,actualNorm.length))));
  const prefix=normalizeMetricoolText(expected).slice(0,42);
  const prefixHit=prefix.length>=18&&actualNorm.includes(prefix);
  const strong=titleHit||prefixHit||score>=0.55;
  ranked.push({v,delta,score,strong,rank:(strong?10:0)+score-Math.abs(delta)/(6*60*60*1000)});
 }
 const strong=ranked.filter(x=>x.strong).sort((a,b)=>b.rank-a.rank);
 if(strong.length)return strong[0].v;
 // Conservative fallback: only accept a single post very close to the expected time.
 const near=ranked.filter(x=>Math.abs(x.delta)<=45*60*1000);
 return near.length===1?near[0].v:null;
}
async function deleteMetricoolWatchFile(watch){
 let storageId=watch.storage_id||"";
 if(!storageId&&watch.drive_file_id){
  const imp=await wp("POST","/storage/drive",{
   request_id:"metricool-watch-import-"+watch.id,
   drive_file_id:watch.drive_file_id
  });
  if(imp.status<200||imp.status>=300||!imp.data?.storage_id)throw new Error("Drive file resolve failed HTTP "+imp.status);
  storageId=String(imp.data.storage_id);
  watch.storage_id=storageId;
 }
 if(!storageId)throw new Error("No storage_id or drive_file_id for cleanup");
 const del=await wp("POST","/storage/delete",{request_id:"metricool-watch-delete-"+watch.id,storage_id:storageId});
 if(del.status<200||del.status>=300)throw new Error("Drive delete failed HTTP "+del.status);
 return true;
}
async function metricoolWatchTick(){
 if(metricoolWatchBusy)return;metricoolWatchBusy=true;
 try{
  const st=await autoState();
  const watches=Array.isArray(st.metricool_tiktok_watches)?st.metricool_tiktok_watches:[];
  if(!watches.length)return;
  const now=Date.now();
  let dirty=false;
  // Retry cleanup independently of TikTok listing.
  for(const w of watches){
   if(!["published","cleanup_failed"].includes(w?.status))continue;
   const last=Date.parse(w.cleanup_attempted_at||0)||0;
   if(last&&now-last<15*60*1000)continue;
   w.cleanup_attempted_at=new Date().toISOString();dirty=true;
   try{
    await deleteMetricoolWatchFile(w);
    w.status="done";w.updated_at=new Date().toISOString();w.last_error="";
    if(!w.deleted_notified){
     await telegramNotify("🧹 Fuoconero Social\nFile TikTok eliminato da Drive dopo pubblicazione confermata — "+(w.title||w.id)+".");
     w.deleted_notified=true;
    }
   }catch(e){
    w.status="cleanup_failed";w.updated_at=new Date().toISOString();w.last_error=e.message;
    console.warn("METRICOOL cleanup failed",w.id,e.message);
   }
  }
  const due=watches.filter(w=>w?.status==="pending"&&Number.isFinite(Date.parse(w.scheduled_at))&&now>=Date.parse(w.scheduled_at)-2*60*1000);
  if(due.length){
   let videos=[];
   try{videos=await tiktokListRecentVideos();}
   catch(e){
    console.warn("METRICOOL TikTok list failed",e.message);
    for(const w of due){w.last_error=e.message;w.updated_at=new Date().toISOString();dirty=true;}
    if(dirty)await saveAutoState(st);
    return;
   }
   const claimed=new Set(watches.map(w=>w?.matched_video_id).filter(Boolean).map(String));
   for(const w of due){
    const scheduled=Date.parse(w.scheduled_at);
    if(now>scheduled+METRICOOL_WATCH_EXPIRE_MS){
     w.status="expired";w.updated_at=new Date().toISOString();w.last_error="TikTok post not safely matched within 24h";dirty=true;
     if(!w.expiry_notified){
      await telegramNotify("⚠️ Fuoconero Social\nNon ho trovato con certezza su TikTok il reel programmato: "+(w.title||w.id)+".\nIl file resta su Drive: nessuna cancellazione automatica.");
      w.expiry_notified=true;
     }
     continue;
    }
    const match=metricoolVideoMatch(w,videos,claimed);
    if(!match)continue;
    claimed.add(String(match.id));
    w.matched_video_id=String(match.id);
    w.matched_share_url=String(match.share_url||"");
    w.status="published";w.updated_at=new Date().toISOString();w.last_error="";dirty=true;
    if(!w.published_notified){
     const link=w.matched_share_url?("\n"+w.matched_share_url):"";
     await telegramNotify("✅ Fuoconero Social\nTikTok conferma la pubblicazione: "+(w.title||w.id)+link);
     w.published_notified=true;
    }
    w.cleanup_attempted_at=new Date().toISOString();
    try{
     await deleteMetricoolWatchFile(w);
     w.status="done";w.last_error="";
     if(!w.deleted_notified){
      await telegramNotify("🧹 Fuoconero Social\nFile TikTok eliminato da Drive dopo pubblicazione confermata — "+(w.title||w.id)+".");
      w.deleted_notified=true;
     }
    }catch(e){
     w.status="cleanup_failed";w.last_error=e.message;
     console.warn("METRICOOL cleanup failed",w.id,e.message);
    }
   }
  }
  if(dirty)await saveAutoState(st);
 }catch(e){console.warn("METRICOOL watch tick failed",e.message);}
 finally{metricoolWatchBusy=false;}
}
setInterval(()=>void metricoolWatchTick(),3*60*1000).unref();
setTimeout(()=>void metricoolWatchTick(),20000);

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
async function prepareTikTokVideo(inputUrl){
 const dir=await mkdtemp(join(tmpdir(),"fns-tiktok-"));
 const input=join(dir,"input.mp4"),output=join(dir,"output.mp4");
 try{
  const url=driveDownloadUrl(inputUrl);
  const r=await fetch(url,{redirect:"follow",signal:AbortSignal.timeout(120000)});
  if(!r.ok)throw new Error("Drive download failed HTTP "+r.status);
  const ct=(r.headers.get("content-type")||"").toLowerCase();
  if(ct.includes("text/html"))throw new Error("Drive returned an HTML page instead of the MP4");
  if(!r.body)throw new Error("Drive download returned no body");
  await pipeline(Readable.fromWeb(r.body),createWriteStream(input));
  const inputStat=await stat(input);
  if(inputStat.size<1000)throw new Error("Downloaded file is unexpectedly small");
  if(inputStat.size>128*1024*1024)throw new Error("Video exceeds 128 MB test limit");
  await runProcess(ffmpegPath,[
   "-y","-i",input,
   "-vf","fps=30,scale='min(1080,iw)':-2:force_original_aspect_ratio=decrease,pad=ceil(iw/2)*2:ceil(ih/2)*2",
   "-c:v","libx264","-preset","ultrafast","-crf","21","-pix_fmt","yuv420p","-r","30","-vsync","cfr",
   "-threads","1",
   "-c:a","aac","-b:a","128k","-ar","48000",
   "-movflags","+faststart",output
  ],{timeoutMs:240000});
  const outStat=await stat(output);
  if(outStat.size<1000)throw new Error("Normalized TikTok video is unexpectedly small");
  console.log("TIKTOK normalize",inputStat.size,"->",outStat.size,"bytes CFR 30fps H264/AAC disk-streamed");
  return {path:output,size:outStat.size,contentType:"video/mp4",cleanup:()=>rm(dir,{recursive:true,force:true}).catch(()=>{})};
 }catch(e){
  await rm(dir,{recursive:true,force:true}).catch(()=>{});
  throw e;
 }
}

async function runProcess(cmd,args,{timeoutMs=120000}={}){
 return await new Promise((resolve,reject)=>{
  const p=spawn(cmd,args,{stdio:["ignore","pipe","pipe"]});
  let out="",err="";const timer=setTimeout(()=>{p.kill("SIGKILL");reject(new Error("Process timed out"));},timeoutMs);
  p.stdout.on("data",d=>{if(out.length<4000)out+=d.toString();});
  p.stderr.on("data",d=>{err=(err+d.toString()).slice(-8000);});
  p.on("error",e=>{clearTimeout(timer);reject(e);});
  p.on("close",code=>{clearTimeout(timer);code===0?resolve({out,err}):reject(new Error("Process failed "+code+": "+err.slice(-1200)));});
 });
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
 const prepared=await prepareTikTokVideo(videoUrl);
 const contentType=prepared.contentType;
 const token=await tiktokOauthToken();
 const size=prepared.size;
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
 let up;
 try{
  up=await fetch(uploadUrl,{
   method:"PUT",
   redirect:"manual",
   headers:{"Content-Type":contentType,"Content-Length":String(size),"Content-Range":"bytes 0-"+(size-1)+"/"+size},
   body:createReadStream(prepared.path),
   duplex:"half",
   signal:AbortSignal.timeout(120000)
  });
 }finally{
  await prepared.cleanup();
 }
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

const pump=worker(wp,{
 getTelegramChatId:async()=>process.env.FNS_TELEGRAM_CHAT_ID||telegramKnownChatId||await telegramChatId()||null,
 afterRender:async(job,readyOutput)=>{
  const recoverId=Number(process.env.FNS_RECOVER_POST_ID||0);
  if(process.env.FNS_RECOVER_AUTO_PUBLISH!=="1"||!recoverId||Number(job?.post_id)!==recoverId)return;
  const st=await autoState(),doneKey="recovery_published_"+recoverId;
  if(st[doneKey]){console.log("RECOVERY publish already completed",recoverId);return;}
  const post=await publishedPostById(recoverId);if(!post)throw new Error("recovery article unavailable");
  const category=publicationCategory(post),pub=autoPublicationMeta(post,category);
  const reelId=findDriveFileId(readyOutput,"reel");if(!reelId)throw new Error("recovery reel output unavailable");
  await telegramNotify("🚀 Fuoconero Social\nReel rigenerato. Pubblico ora: "+pub.title);
  await executeScheduledPublication({
   id:"recovery-"+job.render_job_id,
   attempt_id:"recovery-"+job.render_job_id+"-"+Date.now(),
   render_job_id:job.render_job_id,
   publish_at:new Date().toISOString(),
   title:pub.title,caption:pub.caption,facebook_caption:pub.facebook_caption,
   reel:{targets:["ig_reel","fb_reel","youtube_short"]},
   story:false,
   youtube_privacy:"public",made_for_kids:"no",synthetic_media:"no"
  });
  st[doneKey]=true;st["recovery_published_job_"+recoverId]=job.render_job_id;await saveAutoState(st);
  await telegramNotify("✅ Fuoconero Social\nReel pubblicato: "+pub.title);
 }
});
const server=http.createServer(async(req,res)=>{
 try{
  const u=new URL(req.url,"http://localhost");
  if(req.method==="GET"&&u.pathname==="/oauth/tiktok"){
   const key=process.env.TIKTOK_CLIENT_KEY,secret=process.env.TIKTOK_CLIENT_SECRET;
   if(!key||!secret)return html(res,503,"TikTok non configurato","Mancano le credenziali TikTok sul server.");
   const reset=u.searchParams.get("reset")==="1";
   if(reset){
    try{
     const st=await autoState();
     delete st.tiktok_oauth;
     await saveAutoState(st);
     console.log("TIKTOK OAuth reset requested");
    }catch(e){console.warn("TIKTOK OAuth reset failed",e.message);}
   }
   const state=crypto.randomBytes(24).toString("hex");
   const authUrl=new URL("https://www.tiktok.com/v2/auth/authorize/");
   authUrl.searchParams.set("client_key",key);
   authUrl.searchParams.set("response_type","code");
   authUrl.searchParams.set("scope","user.info.basic,video.publish,video.upload,video.list");
   authUrl.searchParams.set("redirect_uri",TIKTOK_REDIRECT_URI);
   authUrl.searchParams.set("state",state);
   if(reset)authUrl.searchParams.set("disable_auto_auth","1");
   const cookies=["tiktok_oauth_state="+encodeURIComponent(state)+"; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=600"];
   if(reset)cookies.push("tiktok_admin_session=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0");
   res.writeHead(302,{location:authUrl.toString(),"set-cookie":cookies,"cache-control":"no-store"});
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
  if(req.method==="GET"&&u.pathname==="/health"){void pump();return json(res,200,{ok:true,service:"fuoconero-social-bridge",version:"0.4.28",mode:"authenticated-remote-render"});}
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
 void verifySunoApi();
 void pump();
 setTimeout(()=>void autoReelTick(),15000);
 runCommand().catch(e=>console.error("COMMAND error",e.message));
});

