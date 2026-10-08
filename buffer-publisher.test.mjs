import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createBufferPublisher} from './buffer-publisher.js';
function setup(response){
 const values=new Map(),pending=new Set(),calls=[],notifications=[];
 const redis={status:'ready',async set(k,v,nx){if(nx==='NX'&&values.has(k))return null;values.set(k,v);return 'OK';},async get(k){return values.get(k);},async sadd(k,v){pending.add(v);},async smembers(){return [...pending];},async srem(k,v){pending.delete(v);}};
 const publisher=createBufferPublisher({redis:()=>redis,notify:async text=>notifications.push(text),env:{BUFFER_API_KEY:'test',BUFFER_TIKTOK_CHANNEL_ID:'tiktok-channel'},fetchFn:async(url,options)=>{calls.push(JSON.parse(options.body));return response(calls.length);}});
 return {publisher,calls,notifications,pending};
}
const video={renderJobId:'render-1',videoUrl:'https://example.com/video.mp4',text:'caption'};
test('concurrent approval submits once, preserves caption and media',async()=>{
 const s=setup(()=>({ok:true,json:async()=>({data:{createPost:{post:{id:'post-1',status:'sending'}}}})}));
 await Promise.all([s.publisher.publish(video),s.publisher.publish(video)]);
 assert.equal(s.calls.length,1);assert.equal(s.calls[0].variables.input.text,'caption');assert.equal(s.calls[0].variables.input.mode,'shareNow');assert.equal(s.pending.size,1);
 await s.publisher.publish(video);assert.equal(s.calls.length,1);
 assert.ok(!s.notifications.some(x=>x.includes('conferma la pubblicazione')));
});
test('lost response is never automatically retried',async()=>{
 const s=setup(()=>{throw Error('timeout');});
 await assert.rejects(s.publisher.publish(video),/timeout/);
 assert.equal((await s.publisher.publish(video)).status,'uncertain');assert.equal(s.calls.length,1);
});
test('mutation errors are not mistaken for accepted posts',async()=>{
 const s=setup(()=>({ok:true,json:async()=>({data:{createPost:{message:'queue limit'}}})}));
 await assert.rejects(s.publisher.publish(video),/queue limit/);assert.equal(s.pending.size,0);
});
test('status polling confirms sent and clears pending',async()=>{
 const s=setup(n=>({ok:true,json:async()=>n===1?{data:{createPost:{post:{id:'post-1',status:'sending'}}}}:{data:{p0:{id:'post-1',status:'sent'}}}}));
 await s.publisher.publish(video);await s.publisher.poll();assert.equal(s.pending.size,0);assert.ok(s.notifications.some(x=>x.includes('video pubblicato')));
});
test('no persistent storage means no publishing request',async()=>{
 let calls=0;const p=createBufferPublisher({redis:()=>null,notify:async()=>{},env:{BUFFER_API_KEY:'test',BUFFER_TIKTOK_CHANNEL_ID:'id'},fetchFn:async()=>{calls++;}});
 await assert.rejects(p.publish(video),/Redis persistente/);assert.equal(calls,0);
});
