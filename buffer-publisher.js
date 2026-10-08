// Buffer is used only after the existing Telegram publication approval.
// A durable claim is never removed after an uncertain HTTP response.
export function createBufferPublisher({redis,notify,env=process.env,fetchFn=fetch,service="tiktok"}){
 const channel=env.BUFFER_TIKTOK_CHANNEL_ID,key=env.BUFFER_API_KEY;
 const configured=!!(channel&&key);
 const pending='fuoconero:buffer:pending:v1';
 const recordKey=id=>'fuoconero:buffer:render:v1:'+channel+':'+id;
 let polling=false;
 async function db(){const r=redis();if(!r)throw new Error('Buffer richiede Redis persistente per evitare doppioni');if(r.status==='wait')await r.connect();return r;}
 async function api(query,variables={}){
  const r=await fetchFn('https://api.buffer.com',{method:'POST',headers:{'Content-Type':'application/json',Authorization:'Bearer '+key},body:JSON.stringify({query,variables}),signal:AbortSignal.timeout(45000)});
  if(!r.ok)throw new Error('Buffer HTTP '+r.status);
  const data=await r.json();if(data.errors?.length)throw new Error('Buffer: '+data.errors.map(x=>x.message).join('; ').slice(0,300));return data.data;
 }
 async function check(){
  if(!configured)return {configured:false};
  const data=await api('query($id: ChannelId!){channel(input:{id:$id}){id service}}',{id:channel});
  if(data.channel?.id!==channel||String(data.channel?.service).toLowerCase()!==service)throw new Error('Il canale Buffer configurato non è TikTok');
  await db();return {configured:true,connected:true,channelId:channel};
 }
 async function publish({renderJobId,videoUrl,text='',aiGenerated=false,mode='shareNow',dueAt=null}){
  if(!configured)return {disabled:true};
  const u=new URL(videoUrl);if(u.protocol!=='https:')throw new Error('Buffer richiede un video HTTPS pubblico');
  const r=await db(),rk=recordKey(renderJobId);
  if(!['shareNow','customScheduled'].includes(mode))throw new Error('Invalid Buffer scheduling mode');
  if(mode==='customScheduled'&&(!dueAt||Date.parse(dueAt)<=Date.now()))throw new Error('Invalid Buffer publication date');
  const record={renderJobId,status:'submitting',createdAt:Date.now(),dueAt,service,caption:String(text).split('\n')[0].slice(0,140)};
  const claimed=await r.set(rk,JSON.stringify(record),'NX');
  if(!claimed){const prior=JSON.parse(await r.get(rk)||'{}');return {...prior,existing:true};}
  try{
   const data=await api('mutation($input: CreatePostInput!){createPost(input:$input){... on PostActionSuccess{post{id status}} ... on MutationError{message}}}',{input:{channelId:channel,text:String(text).slice(0,service==='threads'?500:2200),schedulingType:'automatic',mode,...(dueAt?{dueAt}:{}),assets:[{video:{url:videoUrl}}],...(service==='tiktok'?{metadata:{tiktok:{isAiGenerated:!!aiGenerated}}}:{})}});
   const result=data.createPost;if(!result?.post?.id)throw new Error(result?.message||'Buffer non ha restituito un ID del post');
   Object.assign(record,{postId:result.post.id,status:result.post.status,updatedAt:Date.now()});
   await r.set(rk,JSON.stringify(record));
   if(record.status!=='sent')await r.sadd(pending,rk);
   await notify(record.status==='sent'?'✅ TikTok: Buffer conferma la pubblicazione.':'📤 TikTok: video inviato a Buffer; pubblicazione in elaborazione.');
   return record;
  }catch(e){
   // Keep the claim even if Buffer may have accepted a timed-out request.
   Object.assign(record,{status:'uncertain',updatedAt:Date.now()});
   try{await r.set(rk,JSON.stringify(record));}catch{}
   await notify('⚠️ TikTok/Buffer: invio non confermato. Non riprovo automaticamente per evitare doppioni. Gli altri social proseguono.');
   throw e;
  }
 }
 async function poll(){
  if(!configured||polling)return;polling=true;
  try{
   const r=await db(),keys=(await r.smembers(pending)).slice(0,20);if(!keys.length)return;
   const rows=[];
   for(const rk of keys){const rec=JSON.parse(await r.get(rk)||'{}');if(!rec.postId){await r.srem(pending,rk);continue;}rows.push({rk,rec});}
   const now=Date.now();
   const dueRows=rows.filter(({rec})=>now>=(Date.parse(rec.dueAt)||rec.createdAt)+180000&&now-(rec.lastCheckedAt||0)>=3600000);
   rows.splice(0,rows.length,...dueRows);
   if(!rows.length)return;
   const query='query{'+rows.map(({rec},i)=>'p'+i+':post(input:{id:'+JSON.stringify(rec.postId)+'}){id status}').join(' ')+'}';
   const data=await api(query);
   for(let i=0;i<rows.length;i++){
    const {rk,rec}=rows[i],post=data['p'+i];if(!post)continue;
    rec.status=post.status;rec.updatedAt=Date.now();rec.lastCheckedAt=Date.now();await r.set(rk,JSON.stringify(rec));
    if(['sent','error'].includes(rec.status)){
     await r.srem(pending,rk);
     const label=(rec.service==='threads'||rk.includes(':6900bdcc669affb4c98cc170:'))?'Threads':'TikTok';
     await notify((rec.status==='sent'?'✅ '+label+': video pubblicato.':'⚠️ '+label+': pubblicazione fallita; il video resta su Drive.')+(rec.caption?'\n'+rec.caption:''));
    }else if(Date.now()-Math.max(rec.createdAt,Date.parse(rec.dueAt)||0)>86400000){
     await r.srem(pending,rk);await notify('⚠️ TikTok: Buffer non ha confermato la pubblicazione entro 24 ore. Controllare il post in Buffer; nessun reinvio automatico.');
    }
   }
  }finally{polling=false;}
 }
 async function queueInfo(){
  const a=await api('query{account{organizations{id}}}');
  const org=a.account.organizations[0]?.id;if(!org)throw new Error('Buffer organization missing');
  const d=await api('query($org:OrganizationId!,$channel:ChannelId!){posts(first:100,input:{organizationId:$org,filter:{channelIds:[$channel],status:[scheduled,sending]}}){edges{node{id dueAt}}}}',{org,channel});
  return d.posts.edges.map(x=>x.node);
 }
 return {configured,check,publish,poll,queueInfo};
}
