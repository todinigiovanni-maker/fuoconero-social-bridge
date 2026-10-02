import {mkdtemp,mkdir,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {render,download} from './renderer.js';
function driveFileId(output,kind){
 const roots=[output?.[kind],output?.outputs?.[kind],output?.data?.[kind],output?.data?.outputs?.[kind]];
 for(const x of roots){const id=x?.drive_file_id||x?.drive?.file_id||x?.file_id;if(typeof id==="string"&&id)return id;}
 return null;
}
function previewUrl(output,kind){
 const roots=[output?.[kind],output?.outputs?.[kind],output?.data?.[kind],output?.data?.outputs?.[kind]];
 for(const x of roots){const url=x?.preview_url;if(typeof url==="string"&&/^https:\/\/drive\.usercontent\.google\.com\//.test(url))return url;}
 return null;
}
async function telegramReady(job,output){
 const token=process.env.FNS_TELEGRAM_BOT_TOKEN;if(!token)return;
 try{
  let chatId=process.env.FNS_TELEGRAM_CHAT_ID||(await options.getTelegramChatId?.());
  if(!chatId){console.warn("TELEGRAM ready waiting for shared chat id");return;}
  if(!chatId){console.warn("TELEGRAM ready no chat id");return;}
  const title=job?.plans&&Object.values(job.plans)[0]?.title||("Articolo "+(job?.post_id||""));
  const reelId=driveFileId(output,"reel"),storyId=driveFileId(output,"story");
  const reelPreview=reelId?"https://drive.google.com/file/d/"+encodeURIComponent(reelId)+"/preview":previewUrl(output,"reel");
  const storyPreview=storyId?"https://drive.google.com/file/d/"+encodeURIComponent(storyId)+"/preview":previewUrl(output,"story");
  const previewButtons=[
   reelPreview?{text:"🎬 APRI REEL",url:reelPreview}:null,
   storyPreview?{text:"📱 APRI STORY",url:storyPreview}:null
  ].filter(Boolean);
  const keyboard=[];
  if(previewButtons.length)keyboard.push(previewButtons);
  keyboard.push([{text:"🕒 APPROVA E ACCODA",callback_data:"queue:"+job.render_job_id+":"+job.post_id}]);
  keyboard.push([{text:"🚀 PUBBLICA ORA",callback_data:"approve:"+job.render_job_id+":"+job.post_id},{text:"❌ RIFIUTA",callback_data:"reject:"+job.render_job_id+":"+job.post_id}]);
  const r=await fetch("https://api.telegram.org/bot"+token+"/sendMessage",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({chat_id:chatId,text:"✅ Fuoconero Social\\n"+(reelPreview&&storyPreview?"Reel + Story pronti":reelPreview?"Reel pronto":storyPreview?"Story pronta":"Contenuto pronto")+" su Drive.\\n\\n"+title+"\\n\\nApri le anteprime, poi scegli se accodare, pubblicare subito o rifiutare.",disable_web_page_preview:true,reply_markup:{inline_keyboard:keyboard}}),signal:AbortSignal.timeout(15000)});
  console.log(r.ok?"TELEGRAM ready notification sent":"TELEGRAM ready send failed "+r.status);
 }catch(e){console.warn("TELEGRAM ready send failed",e.message);}
}

function renderMusicFileIdOverride(postId){
 const raw=String(process.env.FNS_RENDER_MUSIC_FILE_ID_BY_POST||"");
 for(const item of raw.split(";")){
  const i=item.indexOf(":");if(i<1)continue;
  if(Number(item.slice(0,i).trim())===Number(postId)){
   const id=item.slice(i+1).trim();
   return id||null;
  }
 }
 return null;
}
function renderMusicUrlOverride(postId){
 const defaults={
  4820:"https://fuoconero.com/wp-content/uploads/2026/10/parole-vuote-audio-poesia.mp3",
  7443:"https://fuoconero.com/wp-content/uploads/2026/10/pruriti-audio-poesia.mp3",
  7359:"https://fuoconero.com/wp-content/uploads/2026/10/quando-muore-un-gigante-audio-poesia.mp3"
 };
 const raw=String(process.env.FNS_RENDER_MUSIC_URL_BY_POST||"");
 for(const item of raw.split(";")){
  const i=item.indexOf(":");if(i<1)continue;
  if(Number(item.slice(0,i).trim())===Number(postId)){
   const url=item.slice(i+1).trim();
   if(/^https:\/\//i.test(url))return url;
  }
 }
 const fallback=defaults[Number(postId)];
 return /^https:\/\//i.test(String(fallback||""))?fallback:null;
}
function renderMusicUrlByCategory(category){
 const key=String(category||"").trim().toLowerCase();
 const defaults={
  animale:"https://fuoconero.com/wp-content/uploads/2026/10/ani-male-jingle.mp3",
  fisicamente:"https://fuoconero.com/wp-content/uploads/2026/10/fisica-mente-jingle.mp3",
  naturalmente:"https://fuoconero.com/wp-content/uploads/2026/10/natural-mente-jingle.mp3",
  mondo:"https://fuoconero.com/wp-content/uploads/2026/10/il-mondo-visto-dal-nero-jingle.mp3"
 };
 const raw=String(process.env.FNS_RENDER_MUSIC_URL_BY_CATEGORY||"");
 for(const item of raw.split(";")){
  const i=item.indexOf(":");if(i<1)continue;
  const name=item.slice(0,i).trim().toLowerCase();
  const url=item.slice(i+1).trim();
  if(name===key&&/^https:\/\//i.test(url))return url;
 }
 return defaults[key]||null;
}
function renderJobCategory(job,plans=[]){
 const candidates=[
  job?.category,job?.article?.category,job?.source?.category,
  ...plans.flatMap(p=>[p?.category,p?.article?.category,p?.source?.category])
 ];
 return String(candidates.find(x=>typeof x==="string"&&x.trim())||"").trim().toLowerCase();
}
function driveDirectAudioUrl(id){
 return "https://drive.usercontent.google.com/download?id="+encodeURIComponent(id)+"&export=download&confirm=t";
}
async function approvedPoetryAudio(wp,postId){
 try{
  const r=await wp('GET','/reel-maker/article/'+encodeURIComponent(postId)+'/audio');
  const a=r?.data?.poetry_audio;
  if(r?.status===200&&a?.status==='approved'&&typeof a?.audio_url==='string'&&/^https:\/\//i.test(a.audio_url)){
   return {url:a.audio_url,drive_file_id:typeof a.drive_file_id==='string'?a.drive_file_id:'',audio_source:a.audio_source||'poetry_audio',revision:a.revision||''};
  }
 }catch(e){console.warn('RENDER poetry audio lookup failed',postId,e.message);}
 return null;
}
export function worker(wp,options={}){
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
   const obsoleteJobs=new Set([
    'c7717dcb-d41d-4869-8257-266ef4a87d4f',
    'bfe20a82-d0ca-4e2d-8728-0227ac391cad',
    '2fb78464-d715-4368-ad10-6bb11bd67cc2'
   ]);
   if(obsoleteJobs.has(String(job.render_job_id||''))){
    console.warn('RENDER obsolete job skipped',job.render_job_id);
    try{await call('/'+job.render_job_id+'/fail',{lease:job.lease,error:'Obsolete superseded render job'});}catch(e){console.warn('RENDER obsolete fail mark failed',job.render_job_id,e.message);}
    return;
   }
   dir=await mkdtemp(join(tmpdir(),'fns-render-'));console.log('RENDER start',job.render_job_id);
   const sharedDir=join(dir,'shared');await mkdir(sharedDir);const shared={images:{}};
   const plans=Object.values(job.plans),musicOverrideId=renderMusicFileIdOverride(job?.post_id),musicOverrideUrl=renderMusicUrlOverride(job?.post_id);
   const category=renderJobCategory(job,plans),categoryMusicUrl=renderMusicUrlByCategory(category);
   const approvedAudio=await approvedPoetryAudio(wp,job?.post_id);
   if(approvedAudio){
    for(const p of plans)if(p?.music)p.music={...p.music,url:approvedAudio.url,drive_file_id:approvedAudio.drive_file_id||"",source:"poetry_audio",audio_source:approvedAudio.audio_source,audio_revision:approvedAudio.revision,reuse_existing:true};
    console.log('RENDER poetry audio from WordPress',job.post_id);
   }else if(musicOverrideUrl){
    for(const p of plans)if(p?.music)p.music={...p.music,url:musicOverrideUrl,drive_file_id:"",source:"approved_poetry_url"};
    console.log('RENDER music URL override',job.post_id);
   }else if(musicOverrideId){
    for(const p of plans)if(p?.music)p.music={...p.music,url:driveDirectAudioUrl(musicOverrideId),drive_file_id:musicOverrideId,source:"render_override"};
    console.log('RENDER music override',job.post_id,musicOverrideId);
   }else if(categoryMusicUrl){
    for(const p of plans)if(p?.music)p.music={...p.music,url:categoryMusicUrl,drive_file_id:"",source:"category_jingle",category};
    console.log('RENDER category jingle',job.post_id,category);
   }
   const first=plans[0];
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
    let result;try{
     try{result=await render(plan,folder,download,shared,kind);}
     catch(e){
      if(/paginazione leggibile richiede/i.test(String(e?.message||""))){
       console.warn('RENDER duration auto-extend',job.render_job_id,kind,e.message);
       result=await render({...plan,allow_extended_duration:true},folder,download,shared,kind);
      }else throw e;
     }
    }finally{clearInterval(timer);if(keepAlive)clearInterval(keepAlive);await beat;}
    if(leaseError)throw new Error('Lease non rinnovato: output non caricato.');
    // One request only. Never retry an upload after a lost/uncertain response.
    const {path,...metadata}=result;
    const output=await call('/'+job.render_job_id+'/output',{lease:job.lease,kind,sha256:result.sha256,metadata,mp4_base64:(await readFile(path)).toString('base64')});
    console.log('RENDER output',job.render_job_id,kind,output.status,result.sha256);await rm(folder,{recursive:true,force:true});
   }
   console.log('RENDER complete',job.render_job_id);
   let readyOutput=null;
   try{
    const ready=await wp('GET','/reel-maker/render-jobs/'+encodeURIComponent(job.render_job_id)+'/output');
    if(ready.status>=200&&ready.status<300)readyOutput=ready.data;
    else console.warn('RENDER ready output unavailable',job.render_job_id,ready.status);
   }catch(e){console.warn('RENDER ready output lookup failed',job.render_job_id,e.message);}
   await telegramReady(job,readyOutput);
   if(options.afterRender)await options.afterRender(job,readyOutput);
  }catch(e){
   console.error('RENDER failed',job?.render_job_id||'claim',e.message.replace(/https?:\/\/\S+/g,'[url]'));
   if(job)try{await call('/'+job.render_job_id+'/fail',{lease:job.lease,error:e.message.replace(/https?:\/\/\S+/g,'[url]')});}catch{/* Durable lease expiry handles recovery; no upload retry. */}
  }finally{if(dir)await rm(dir,{recursive:true,force:true});busy=false;}
 }
 const timer=setInterval(()=>{void pump();void publishTick();},20000);timer.unref();void publishTick();return pump;
}
