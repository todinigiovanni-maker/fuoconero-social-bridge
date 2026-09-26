import {mkdtemp,mkdir,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {render,download} from './renderer.js';
export function worker(wp){
 let busy=false,last=0;
 async function call(path,data){const r=await wp('POST','/reel-maker/render-worker'+path,data);if(r.status<200||r.status>=300)throw new Error(r.data?.message||'Worker HTTP '+r.status);return r.data;}
 async function publishTick(){try{await wp('POST','/publish-worker/tick',{});}catch(e){console.warn('PUBLISH tick failed',e.message);}}
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
   console.log('RENDER complete',job.render_job_id);
  }catch(e){
   console.error('RENDER failed',job?.render_job_id||'claim',e.message.replace(/https?:\/\/\S+/g,'[url]'));
   if(job)try{await call('/'+job.render_job_id+'/fail',{lease:job.lease,error:e.message.replace(/https?:\/\/\S+/g,'[url]')});}catch{/* Durable lease expiry handles recovery; no upload retry. */}
  }finally{if(dir)await rm(dir,{recursive:true,force:true});busy=false;}
 }
 const timer=setInterval(()=>{void pump();void publishTick();},20000);timer.unref();void publishTick();return pump;
}
