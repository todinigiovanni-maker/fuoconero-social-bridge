import {mkdtemp,mkdir,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {render,download} from './renderer.js';
async function telegramReady(job){
 const token=process.env.FNS_TELEGRAM_BOT_TOKEN;if(!token)return;
 try{
  let chatId=process.env.FNS_TELEGRAM_CHAT_ID;
  if(!chatId){const r=await fetch("https://api.telegram.org/bot"+token+"/getUpdates",{signal:AbortSignal.timeout(15000)});const j=await r.json();const a=Array.isArray(j?.result)?j.result:[];for(let i=a.length-1;i>=0;i--){if(a[i]?.message?.chat?.id){chatId=a[i].message.chat.id;break;}}}
  if(!chatId){console.warn("TELEGRAM ready no chat id");return;}
  const title=job?.plans&&Object.values(job.plans)[0]?.title||("Articolo "+(job?.post_id||""));
  const r=await fetch("https://api.telegram.org/bot"+token+"/sendMessage",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({chat_id:chatId,text:"✅ Fuoconero Social\\nReel + Story pronti su Drive.\\n\\n"+title+"\\n\\nIn attesa della tua approvazione.",disable_web_page_preview:true}),signal:AbortSignal.timeout(15000)});
  console.log(r.ok?"TELEGRAM ready notification sent":"TELEGRAM ready send failed "+r.status);
 }catch(e){console.warn("TELEGRAM ready send failed",e.message);}
}

export function worker(wp){
 let busy=false,last=0;
 async function call(path,data){const r=await wp('POST','/reel-maker/render-worker'+path,data);if(r.status<200||r.status>=300)throw new Error(r.data?.message||'Worker HTTP '+r.status);return r.data;}
 async function publishTick(){try{
  const r=await wp('POST','/publish-worker/tick',{});
  if(r.status<200||r.status>=300){console.warn('PUBLISH tick failed HTTP',r.status,JSON.stringify(r.data));return false;}
  console.log('PUBLISH tick',r.status,JSON.stringify(r.data));return true;
 }catch(e){console.warn('PUBLISH tick failed',e.message);return false;}}
 async function pump(){
  if(busy||Date.now()-last<20000)return;busy=true;last=Date.now();let job,dir;
  try{
   ({job}=await call('/claim',{}));if(!job)return;
   dir=await mkdtemp(join(tmpdir(),'fns-render-'));console.log('RENDER start',job.render_job_id);
   const sharedDir=join(dir,'shared');await mkdir(sharedDir);const shared={images:{}};
   const plans=Object.values(job.plans),first=plans[0];
   if(first?.music?.url){shared.music=join(sharedDir,'music.audio');await download(first.music.url,shared.music);}
   const imageUrls=[...new Set(plans.flatMap(p=>p.images||[]).map(x=>x.url))];for(let i=0;i<imageUrls.length;i++){const file=join(sharedDir,'image-'+i);await download(imageUrls[i],file);shared.images[imageUrls[i]]=file;}
   console.log('RENDER shared assets ready',imageUrls.length,'images');
   for(const [kind,plan] of Object.entries(job.plans)){
    if(job.outputs[kind].status==='completed')continue;
    const folder=join(dir,kind);await mkdir(folder);let beat=Promise.resolve(),leaseError=null;
    const timer=setInterval(()=>{beat=beat.then(()=>call('/'+job.render_job_id+'/heartbeat',{lease:job.lease})).catch(e=>{leaseError=e;});},20000);
    // Render Free decides idleness from inbound traffic, while this worker spends
    // most of its time doing CPU work and outbound calls. Keep the web service
    // awake only for the lifetime of an active FFmpeg output.
    const keepAliveUrl=process.env.RENDER_EXTERNAL_URL||process.env.FNS_SELF_URL;
    const keepAlive=keepAliveUrl?setInterval(()=>{fetch(keepAliveUrl.replace(/\/$/,'')+'/health',{signal:AbortSignal.timeout(15000)}).catch(()=>{});},240000):null;
    let result;try{result=await render(plan,folder,download,shared,kind);}finally{clearInterval(timer);if(keepAlive)clearInterval(keepAlive);await beat;}
    if(leaseError)throw new Error('Lease non rinnovato: output non caricato.');
    // One request only. Never retry an upload after a lost/uncertain response.
    const {path,...metadata}=result;
    const output=await call('/'+job.render_job_id+'/output',{lease:job.lease,kind,sha256:result.sha256,metadata,mp4_base64:(await readFile(path)).toString('base64')});
    console.log('RENDER output',job.render_job_id,kind,output.status,result.sha256);await rm(folder,{recursive:true,force:true});
   }
   console.log('RENDER complete',job.render_job_id);await telegramReady(job);
  }catch(e){
   console.error('RENDER failed',job?.render_job_id||'claim',e.message.replace(/https?:\/\/\S+/g,'[url]'));
   if(job)try{await call('/'+job.render_job_id+'/fail',{lease:job.lease,error:e.message.replace(/https?:\/\/\S+/g,'[url]')});}catch{/* Durable lease expiry handles recovery; no upload retry. */}
  }finally{if(dir)await rm(dir,{recursive:true,force:true});busy=false;}
 }
 const timer=setInterval(()=>{void pump();void publishTick();},20000);timer.unref();void publishTick();return pump;
}
