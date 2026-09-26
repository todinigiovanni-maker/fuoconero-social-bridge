import {spawn} from 'node:child_process';
import {createHash} from 'node:crypto';
import {readFile,writeFile,stat} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {createCanvas,GlobalFonts,loadImage} from '@napi-rs/canvas';
import sharp from 'sharp';
import ffmpeg from 'ffmpeg-static';
import probe from 'ffprobe-static';
GlobalFonts.registerFromPath(new URL('./node_modules/dejavu-fonts-ttf/ttf/DejaVuSans-Bold.ttf',import.meta.url).pathname,'FNS Sans');
GlobalFonts.registerFromPath(new URL('./node_modules/dejavu-fonts-ttf/ttf/DejaVuSerif-Bold.ttf',import.meta.url).pathname,'FNS Serif');
// Render keeps the same visual pipeline/output contract. Render currently has one effective CPU,\n// so avoid oversubscribing libvips/FFmpeg; the main optimization is one FFmpeg encode per output.\nsharp.concurrency(Math.max(1,Number(process.env.FNS_SHARP_THREADS||1)));sharp.cache(false);
export function run(exe,args,timeout=1200000){return new Promise((resolve,reject)=>{
 const p=spawn(exe,args,{stdio:['ignore','pipe','pipe'],shell:false});let out='',err='';
 const timer=setTimeout(()=>{p.kill('SIGKILL');reject(new Error('Tempo massimo renderer superato.'));},timeout);
 p.stdout.on('data',c=>{out=(out+c).slice(-1048576)});p.stderr.on('data',c=>{err=(err+c).slice(-16000)});
 p.on('error',e=>{clearTimeout(timer);reject(e)});p.on('close',code=>{clearTimeout(timer);code===0?resolve(out):reject(new Error('FFmpeg non riuscito ('+code+'): '+err.slice(-1500)))});
});}
const allowedHost=h=>h==='fuoconero.com'||h.endsWith('.fuoconero.com')||h.endsWith('.wordpress.com')||h.endsWith('.wp.com')||h==='drive.usercontent.google.com'||h==='drive.google.com'||h.endsWith('.googleusercontent.com');
export async function download(url,path,max=67108864){
 for(let n=0;n<5;n++){
  const u=new URL(url);if(u.protocol!=='https:'||u.username||u.password||u.port||!allowedHost(u.hostname))throw new Error('Host asset non autorizzato.');
  const r=await fetch(u,{redirect:'manual',signal:AbortSignal.timeout(90000)});
  if([301,302,303,307,308].includes(r.status)){url=new URL(r.headers.get('location'),u).href;await r.body?.cancel();continue;}
  if(!r.ok)throw new Error('Download asset: HTTP '+r.status);
  const chunks=[];let size=0;for await(const chunk of r.body){size+=chunk.length;if(size>max)throw new Error('Asset oltre il limite di 64 MiB.');chunks.push(chunk);}
  await writeFile(path,Buffer.concat(chunks));return path;
 }throw new Error('Troppi redirect asset.');
}
export function wrap(ctx,text,width){
 const lines=[];for(const para of text.split('\n')){
  let line='';for(const word of para.split(/\s+/u).filter(Boolean)){
   if(ctx.measureText(word).width>width)throw new Error('Parola troppo lunga per la safe area: modifica il testo.');
   const trial=line?line+' '+word:word;if(ctx.measureText(trial).width>width){lines.push(line);line=word;}else line=trial;
  }lines.push(line);
 }return lines;
}
function family(p,brand){return /Georgia|serif/i.test(p.typography.font_family||brand.font_family)&&!/sans/i.test(p.typography.font_family||brand.font_family)?'FNS Serif':'FNS Sans';}
export function cards(plan){
 const p=plan.preset,ctx=createCanvas(1080,1920).getContext('2d'),size=p.typography.body_size,f=family(p,plan.brand);ctx.font=`${size}px "${f}"`;
 const count=Math.floor(p.layout.body.height/(size*1.2));if(count<1)throw new Error('Zona testo troppo piccola.');
 const out=[];plan.scene_texts.forEach((text,index)=>{const lines=wrap(ctx,text,p.layout.body.width);for(let k=0;k<lines.length;k+=count){const part=lines.slice(k,k+count);out.push({lines:part,image_index:index,text:part.join(' ')});}});
 const mins=out.map(c=>Math.max(2,c.text.length/16+0.6));let duration=Math.max(mins.reduce((a,b)=>a+b,0),p.duration_min);
 if(duration>p.duration_max&&!plan.allow_extended_duration)throw new Error('La paginazione leggibile richiede '+Math.ceil(duration)+' secondi: approvare allow_extended_duration oppure ridurre il testo.');
 if(duration>600)throw new Error('Durata superiore a 600 secondi.');
 const total=mins.reduce((a,b)=>a+b,0);out.forEach((c,i)=>{c.duration=Math.ceil(mins[i]*duration/total*30)/30;});return out;
}
function text(ctx,lines,rect,size,color,align,font){
 ctx.font=`${size}px "${font}"`;ctx.fillStyle=color;ctx.textAlign=align;ctx.textBaseline='top';
 if(lines.length*size*1.2>rect.height||lines.some(s=>ctx.measureText(s).width>rect.width))throw new Error('Testo fuori dalla zona del preset.');
 const x=align==='center'?rect.x+rect.width/2:align==='right'?rect.x+rect.width:rect.x;
 const y=rect.y+(rect.height-lines.length*size*1.2)/2;lines.forEach((s,i)=>ctx.fillText(s,x,y+i*size*1.2));
}
export async function render(plan,dir,fetchAsset=download,shared={}){
 dir=resolve(dir);const p=plan.preset,sceneCards=cards(plan),font=family(p,plan.brand);let duration=sceneCards.reduce((a,b)=>a+b.duration,0);
 console.log('RENDER phase music-download start');const music=shared.music||join(dir,'music.audio');if(!shared.music)await fetchAsset(plan.music.url,music);console.log('RENDER phase music-download end',shared.music?'cached':'downloaded');
 // Decode locally downloaded files only. FFmpeg network protocols are disabled.
 const musicInfo=JSON.parse(await run(probe.path,['-v','error','-protocol_whitelist','file,pipe','-show_streams','-show_format','-of','json',music],30000));
 if(!musicInfo.streams.some(s=>s.codec_type==='audio'))throw new Error('La base non contiene audio valido.');
 console.log('RENDER phase images-download start',plan.images.length);const assets=[];for(let i=0;i<plan.images.length;i++){
  const file=shared.images?.[plan.images[i].url]||join(dir,`source-${i}`);if(!shared.images?.[plan.images[i].url])await fetchAsset(plan.images[i].url,file);const meta=await sharp(file,{limitInputPixels:40000000}).metadata();
  if(!['png','jpeg','webp'].includes(meta.format))throw new Error('Formato immagine non valido.');assets.push(file);console.log('RENDER phase image ready',i+1,'of',plan.images.length);
 }console.log('RENDER phase images-download end');
 let logo=null;if(plan.brand.assets.logo_primary?.url){const file=join(dir,'logo');await fetchAsset(plan.brand.assets.logo_primary.url,file);logo=await loadImage(await sharp(file).resize(p.logo.size,p.logo.size,{fit:'inside'}).png().toBuffer());}
 let brandBackground=null;if(p.render.background_source==='brand_background'){
  if(!plan.brand.assets.background?.url)throw new Error('Sfondo brand non configurato.');brandBackground=join(dir,'brand-background');await fetchAsset(plan.brand.assets.background.url,brandBackground);
 }
 console.log('RENDER phase scene-build start',sceneCards.length);const sceneParts=[];const threads=Math.max(1,Number(process.env.FNS_FFMPEG_THREADS||1));for(let i=0;i<sceneCards.length;i++){
  const c=sceneCards[i],source=assets[c.image_index%assets.length],bg=join(dir,`bg-${i}.png`),vis=join(dir,`vis-${i}.png`),overlay=join(dir,`text-${i}.png`);
  const canvas=createCanvas(1080,1920),ctx=canvas.getContext('2d');ctx.fillStyle=p.style.background_color;ctx.fillRect(0,0,1080,1920);
  if(p.render.background_source!=='solid'){
   let pipe=sharp(brandBackground||source,{limitInputPixels:40000000}).resize(1080,1920,{fit:'cover'});if(p.render.background_blur>0)pipe=pipe.blur(Math.max(.3,p.render.background_blur));
   ctx.drawImage(await loadImage(await pipe.png().toBuffer()),0,0);ctx.fillStyle=`rgba(0,0,0,${p.style.overlay_opacity})`;ctx.fillRect(0,0,1080,1920);
  }await writeFile(bg,canvas.toBuffer('image/png'));
  const v=p.layout.visual;await sharp(source,{limitInputPixels:40000000}).resize(v.width,v.height,{fit:p.style.image_fit==='contain'?'contain':'cover',background:p.style.background_color}).png().toFile(vis);
  ctx.clearRect(0,0,1080,1920);ctx.font=`${p.typography.category_size}px "${font}"`;
  text(ctx,wrap(ctx,plan.category_label,p.layout.header.width),p.layout.header,p.typography.category_size,p.style.accent_color,p.style.text_align,font);
  text(ctx,c.lines,p.layout.body,p.typography.body_size,p.style.text_color,p.style.text_align,font);
  ctx.font=`${p.typography.cta_size}px "${font}"`;text(ctx,wrap(ctx,p.cta,p.layout.cta.width),p.layout.cta,p.typography.cta_size,p.style.accent_color,p.style.text_align,font);
  if(logo){ctx.globalAlpha=p.logo.opacity;ctx.drawImage(logo,p.logo.x,p.logo.y);ctx.globalAlpha=1;}await writeFile(overlay,canvas.toBuffer('image/png'));
  console.log('RENDER phase scene ready',i+1,'of',sceneCards.length);
  const part=join(dir,`scene-${i}.mp4`);
  const filter=`[0:v][1:v]overlay=${v.x}:${v.y}[b];[b][2:v]overlay=0:0,format=yuv420p,trim=duration=${c.duration},setpts=PTS-STARTPTS[out]`;
  console.log('RENDER phase scene-video start',i+1,'of',sceneCards.length);await run(ffmpeg,['-nostdin','-v','error','-y','-filter_complex_threads',String(threads),'-threads',String(threads),'-loop','1','-framerate','2','-i',bg,'-loop','1','-framerate','2','-i',vis,'-loop','1','-framerate','2','-i',overlay,'-filter_complex',filter,'-map','[out]','-c:v','libx264','-threads',String(threads),'-preset','ultrafast','-crf','25','-maxrate','3500k','-bufsize','7000k','-pix_fmt','yuv420p','-r','2','-movflags','+faststart',part]);console.log('RENDER phase scene-video end',i+1,'of',sceneCards.length);sceneParts.push(part);
 }
 console.log('RENDER phase scene-build end');const concatFile=join(dir,'concat.txt');await writeFile(concatFile,sceneParts.map(x=>"file '"+x.replaceAll("'","'\\''")+"'").join('\n')+'\n');const videoOnly=join(dir,'video.mp4');
 console.log('RENDER phase ffmpeg-video start');await run(ffmpeg,['-nostdin','-v','error','-y','-f','concat','-safe','0','-protocol_whitelist','file,pipe','-i',concatFile,'-c','copy','-movflags','+faststart',videoOnly]);console.log('RENDER phase ffmpeg-video end');const output=join(dir,'output.mp4');const fadeIn=Math.min(p.render.fade_in,duration/2),fadeOut=Math.min(p.render.fade_out,duration/2);
 console.log('RENDER phase ffmpeg-audio start');await run(ffmpeg,['-nostdin','-v','error','-y','-threads','1','-protocol_whitelist','file,pipe','-i',videoOnly,'-stream_loop','-1','-ss',String(p.render.audio_start),'-protocol_whitelist','file,pipe','-i',music,'-map','0:v:0','-map','1:a:0','-t',String(duration),'-c:v','copy','-c:a','aac','-b:a','128k','-ar','48000','-ac','2','-af',`volume=${p.render.music_gain_db}dB,afade=t=in:st=0:d=${fadeIn},afade=t=out:st=${duration-fadeOut}:d=${fadeOut}`,'-movflags','+faststart',output]);
 console.log('RENDER phase ffmpeg-audio end');const size=(await stat(output)).size;if(size>33554432)throw new Error('MP4 superiore al limite storage di 32 MiB. Nessun upload eseguito.');
 console.log('RENDER phase verify start');const info=JSON.parse(await run(probe.path,['-v','error','-show_streams','-show_format','-of','json',output],30000));const video=info.streams.find(s=>s.codec_type==='video'),audio=info.streams.find(s=>s.codec_type==='audio');
 if(video?.codec_name!=='h264'||audio?.codec_name!=='aac'||video.width!==1080||video.height!==1920||video.pix_fmt!=='yuv420p')throw new Error('Verifica codec o dimensioni non superata.');
 console.log('RENDER phase verify end',size);return {path:output,sha256:createHash('sha256').update(await readFile(output)).digest('hex'),size,duration:Number(info.format.duration),video_codec:'h264',audio_codec:'aac',width:1080,height:1920,pixel_format:'yuv420p',scene_count:sceneCards.length,font_used:font};
}
