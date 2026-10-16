import test from 'node:test';
import assert from 'node:assert/strict';
import {isExistingRender} from '../render-status.js';

test('the exact WordPress request-already-started response does not trigger a retry',()=>{
 const response={status:400,data:{code:'fns_remote_render',message:'Richiesta già avviata. Controlla lo stato; non ripetere automaticamente un upload incerto.'}};
 assert.equal(isExistingRender(response),true);
});
test('other duplicate/previously requested render responses are recognized',()=>{
 for(const response of [{status:409,data:{}},{status:400,data:{message:'Request already exists'}},{status:400,data:{message:'Il render già esiste'}},{status:400,data:{message:'request_id already used'}},{status:400,data:{message:'Richiesta gia avviata'}}])assert.equal(isExistingRender(response),true,JSON.stringify(response));
});
test('other WordPress errors are not misidentified as existing render',()=>{
 for(const response of [{status:400,data:{message:'Articolo pubblicato e non protetto'}},{status:400,data:{message:'Asset non valido'}},{status:500,data:{message:'Servizio non disponibile'}}])assert.equal(isExistingRender(response),false,JSON.stringify(response));
});
