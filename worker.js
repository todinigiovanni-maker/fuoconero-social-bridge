import {mkdtemp,mkdir,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {render} from './renderer.js';
export function worker(wp){
 let busy=false,last=0;
 async function call(path,data){const r=await wp('POST','/reel-maker/render-worker'+path,data);if(r.status<200||r.status>=300)throw new Error(r.data?.message||'Worker HTTP '+r.status);return r.data;}
 async function pump(){
  if(busy||Date.now()-last<20000)return;busy=true;last=Date.now();let job,dir;
  try{
   ({job}=await call('/claim',{}));if(!job)return;
   dir=await mkdtemp(join(tmpdir(),'fns-render-'));console.log('RENDER start',job.render_job_id);
   for(const [kind,plan] of Object.entries(job.plans)){
    if(job.outputs[kind].status==='completed')continue;
    const folder=join(dir,kind);await mkdir(folder);let beat=Promise.resolve(),leaseError=null;
    const timer=setInterval(()=>{beat=beat.then(()=>call('/'+job.render_job_id+'/heartbeat',{lease:job.lease})).catch(e=>{leaseError=e;});},60000);
    let result;try{result=await render(plan,folder);}finally{clearInterval(timer);await beat;}
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
 const timer=setInterval(()=>void pump(),20000);timer.unref();return pump;
}
