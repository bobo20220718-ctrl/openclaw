import { spawn } from 'node:child_process';
import { randomUUID, generateKeyPairSync, createHash, sign } from 'node:crypto';
import { buildDeviceAuthPayloadV3 } from '../packages/gateway-client/src/device-auth.ts';
import { once } from 'node:events';
import { createWriteStream, mkdirSync, mkdtempSync, writeFileSync, renameSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { setTimeout as delay } from 'node:timers/promises';
import { applyMockOpenAiModelConfig } from '../scripts/e2e/lib/fixtures/mock-openai-config.mjs';
import { BASE_GATEWAY_BENCH_CONFIG, buildGatewayBenchChildArgs, createGatewayBenchEnv, waitForInitialProbe, writeGatewayBenchConfig } from '../scripts/lib/gateway-bench-runtime.ts';
import { getFreePort } from '../scripts/lib/gateway-bench-probes.ts';
import { createGatewayWsClient } from '../scripts/lib/gateway-ws-client.ts';
import { stopChild } from '../scripts/lib/gateway-bench-child.ts';
import { PROTOCOL_VERSION } from '../packages/gateway-protocol/src/version.ts';
const output = mkdtempSync(path.join(process.env.PROOF_DIR ?? tmpdir(),'sqlmeasure-run-'));
const fixture=path.join(output,'fixture'); mkdirSync(fixture);
const children=[]; const evidence={output,head:process.env.CANDIDATE_SHA, phases:[],turns:[],rpcs:[], events:[],children:[]};
let phase='01.boot'; let phaseStart=Date.now(); let client; let gateway;
function setPhase(next){ evidence.phases.push({phase,start:phaseStart,end:Date.now()}); phase=next; phaseStart=Date.now(); writeFileSync(path.join(output,'phase.new'),phase); renameSync(path.join(output,'phase.new'),path.join(output,'phase')); console.log('PHASE',phase);save(); }
function save(){writeFileSync(path.join(output,'evidence.json'),JSON.stringify(evidence,null,2));}
writeFileSync(path.join(output,'phase'),phase); console.log('OUTPUT',output);
function launch(args,env,name){ const out=createWriteStream(path.join(output,name+'.log')); const c=spawn(process.execPath,args,{cwd:process.cwd(),env,stdio:['ignore','pipe','pipe'],detached:true}); c.stdout.pipe(out); c.stderr.pipe(out); children.push(c); evidence.children.push({name,pid:c.pid});save();return c; }
async function rpc(method,params={},timeout=120000){const result=await client.request(method,params,timeout);evidence.rpcs.push({phase,method,params,result});save();if(!result.ok)throw new Error(method+': '+JSON.stringify(result.error));return result.payload;}
async function turn(key,message){const started=await rpc('chat.send',{sessionKey:key,message,idempotencyKey:randomUUID(),deliver:false});if(!started.runId)throw new Error('Missing run ID');const result=await rpc('agent.wait',{runId:started.runId,timeoutMs:120000},125000);evidence.turns.push({phase,key,runId:started.runId,result});save();if(result.status!=='ok')throw new Error('Turn failed: '+JSON.stringify(result));await delay(1000);return result;}
const reply='Synthetic SQL measurement response. The Gateway receives this streamed answer from the local mock provider and persists it as ordinary assistant conversation content. No external model or service is used.';
const control=path.join(output,'mock-control.json');const setControl=(value)=>{writeFileSync(control+'.new',JSON.stringify(value));renameSync(control+'.new',control);};setControl({text:reply,chunkDelayMs:5});
try {
 const port=await getFreePort();const mockPort=await getFreePort(); evidence.ports={port,mockPort};
 const cfg=structuredClone(BASE_GATEWAY_BENCH_CONFIG);cfg.tools={codeMode:false};
 applyMockOpenAiModelConfig(cfg,{mockPort,modelRef:'openai/gpt-5.6-luna'});
 cfg.agents.defaults.heartbeat={every:'0m'};cfg.agents.defaults.skipBootstrap=true;
 cfg.agents.entries={main:{workspace:path.join(fixture,'workspace')}};mkdirSync(cfg.agents.entries.main.workspace);
 cfg.plugins.entries['memory-core']={config:{dreaming:{enabled:false}}};
 const configPath=writeGatewayBenchConfig(fixture,cfg,{});
 const mock=launch(['scripts/e2e/mock-openai-server.mjs'],{PATH:process.env.PATH,HOME:fixture,MOCK_PORT:String(mockPort),MOCK_RESPONSE_CONTROL:control,MOCK_REQUEST_LOG:path.join(output,'provider-requests.jsonl')},'mock');
 const mockReady=await waitForInitialProbe({port:mockPort,path:'/health',startAt:performance.now(),deadlineAt:performance.now()+30000});if(mockReady.status!==200)throw new Error('mock not ready');
 gateway=launch(buildGatewayBenchChildArgs('dist/entry.js',port),{...createGatewayBenchEnv(fixture,configPath,{caseEnv:{OPENCLAW_SKIP_CHANNELS:'1'}}),OPENAI_API_KEY:'synthetic-sqlmeasure-fixture',SQLMEASURE_OUT:output,NODE_OPTIONS:`--require=${process.cwd()}/.sqlmeasure/observer.cjs --enable-source-maps`},'gateway');
 const ready=await waitForInitialProbe({port,path:'/readyz',startAt:performance.now(),deadlineAt:performance.now()+300000,isDone:()=>gateway.exitCode!==null||gateway.signalCode!==null});evidence.ready=ready;if(ready.status!==200)throw new Error('Gateway not ready');setPhase('01.connect');
 let resolveChallenge;const challenge=new Promise(resolve=>{resolveChallenge=resolve;});
 client=createGatewayWsClient({url:`ws://127.0.0.1:${port}`,onEvent:(event)=>{if(event.event==='connect.challenge')resolveChallenge(event.payload.nonce);evidence.events.push({phase,time:Date.now(),...event});}});
 await client.waitOpen();const nonce=await challenge;const pair=generateKeyPairSync('ed25519');const raw=pair.publicKey.export({type:'spki',format:'der'}).subarray(-32);const deviceId=createHash('sha256').update(raw).digest('hex');const scopes=['operator.read','operator.write','operator.admin'];const signedAt=Date.now();const payload=buildDeviceAuthPayloadV3({deviceId,clientId:'gateway-client',clientMode:'ui',role:'operator',scopes,signedAtMs:signedAt,nonce,platform:process.platform});const device={id:deviceId,publicKey:raw.toString('base64url'),signature:sign(null,Buffer.from(payload),pair.privateKey).toString('base64url'),signedAt,nonce};await rpc('connect',{minProtocol:PROTOCOL_VERSION,maxProtocol:PROTOCOL_VERSION,client:{id:'gateway-client',displayName:'sqlmeasure',version:'1.0.0',platform:process.platform,mode:'ui'},role:'operator',scopes,caps:[],device});
 await delay(1000);
 const key='agent:main:sqlmeasure-primary';
 setPhase('02.first-turn');await rpc('sessions.create',{key,agentId:'main'});await turn(key,'Please give a short synthetic status report for measurement turn 1.');
 for(let i=1;i<=10;i++){setPhase('03.turn-'+String(i).padStart(2,'0'));await turn(key,`Continue our synthetic status conversation, measurement turn ${i+1}.`);}
 setPhase('04.second-session');const second='agent:main:sqlmeasure-secondary';await rpc('sessions.create',{key:second,agentId:'main'});await turn(second,'Start a second synthetic conversation.');
 // Script the next provider response as the real sessions_spawn tool, then ordinary text.
 const args=JSON.stringify({id:'sessions_spawn',args:{task:'Return a short synthetic child status.',mode:'run',cleanup:'keep',expectsCompletionMessage:false}});const item={type:'function_call',id:'fc_sqlmeasure_spawn',call_id:'call_sqlmeasure_spawn',name:'tool_call',arguments:args};
 setControl({scriptVersion:'spawn-once',responses:[{events:[{type:'response.output_item.added',output_index:0,item:{...item,arguments:''}},{type:'response.function_call_arguments.delta',output_index:0,item_id:item.id,delta:args},{type:'response.output_item.done',output_index:0,item},{type:'response.completed',response:{id:'resp_sqlmeasure_spawn',status:'completed',output:[item],usage:{input_tokens:50,output_tokens:30,total_tokens:80,input_tokens_details:{cached_tokens:0}}}}]}],default:{text:reply,chunkDelayMs:5}});
 try{await turn(second,'Delegate a short synthetic status check to one subagent.');
 function findSpawn(value,depth=0){if(depth>8)return;if(typeof value==='string'){try{return findSpawn(JSON.parse(value),depth+1);}catch{return;}}if(!value||typeof value!=='object')return;if(value.childSessionKey&&value.runId)return value;for(const v of Object.values(value)){const result=findSpawn(v,depth+1);if(result)return result;}}
 const requests=readFileSync(path.join(output,'provider-requests.jsonl'),'utf8').trim().split('\n').map(l=>JSON.parse(l));let spawned;for(const request of requests){const candidate=findSpawn(request);if(candidate)spawned=candidate;}
 if(!spawned)throw new Error('No successful subagent spawn receipt in provider tool results');evidence.spawn=spawned;evidence.childTerminal=await rpc('agent.wait',{runId:spawned.runId,timeoutMs:120000},125000);if(evidence.childTerminal.status!=='ok')throw new Error('Child did not complete successfully');await delay(3000);}catch(error){evidence.spawnError=String(error);}
 setControl({text:reply,chunkDelayMs:5});
 setPhase('05.control-plane');await rpc('sessions.list',{limit:100});await rpc('chat.history',{sessionKey:key,limit:100});
 try{const inbox=await rpc('mentions.list',{});await rpc('mentions.dismiss',{ids:inbox.items?.length?[inbox.items[0].id]:['synthetic-absent-mention']});}catch(error){evidence.mentionsError=String(error);}await delay(1000);
 setPhase('06.cron');
 try{const job=await rpc('cron.add',{name:'SQL measurement one-shot',agentId:'main',schedule:{kind:'at',at:new Date(Date.now()+3000).toISOString()},sessionTarget:'isolated',wakeMode:'now',payload:{kind:'agentTurn',message:'Give a short synthetic cron status.'},delivery:{mode:'none'},deleteAfterRun:false});evidence.cronJob=job;await delay(5000);let done=false;for(let i=0;i<24;i++){const runs=await rpc('cron.runs',{id:job.id,limit:5});if(runs.entries?.some(x=>x.status==='ok'||x.status==='error')){evidence.cronRuns=runs;done=true;break;}await delay(1000);}evidence.cronCompleted=done;}catch(error){evidence.cronError=String(error);}
 setPhase('07.compaction');setControl({text:'## Decisions\n- Completed eleven synthetic status conversation turns through the local streaming provider.\n\n## Open TODOs\n- Continue any next synthetic status request.\n\n## Constraints/Rules\n- Use only synthetic data in this isolated measurement.\n\n## Pending user asks\nNone.\n\n## Exact identifiers\n- agent:main:sqlmeasure-primary',chunkDelayMs:5});try{evidence.compaction=await rpc('sessions.compact',{key},180000);if(!evidence.compaction.compacted)throw new Error('Compaction not committed: '+JSON.stringify(evidence.compaction));}catch(error){evidence.compactionError=String(error);}await delay(1000);
 setPhase('08.shutdown');client.close();client=undefined;
 const closed=once(gateway,'close');gateway.kill('SIGTERM');evidence.shutdown=await Promise.race([closed,delay(120000,undefined,{ref:false}).then(()=>{throw new Error('shutdown timeout');})]);
 setPhase('done');evidence.ok=Boolean(evidence.spawn&&!evidence.spawnError&&!evidence.mentionsError&&!evidence.cronError&&!evidence.compactionError&&evidence.cronCompleted&&evidence.cronRuns?.entries?.some(x=>x.status==='ok')&&evidence.compaction?.compacted&&evidence.rpcs.every(x=>x.result.ok)&&evidence.shutdown?.[0]===0&&evidence.shutdown?.[1]===null);if(!evidence.ok)process.exitCode=1;save();console.log('COMPLETE',output,'ok='+evidence.ok);
} catch(error){evidence.error=String(error);console.error(error);save();process.exitCode=1;}
finally {client?.close();for(const child of children.reverse())await stopChild(child,{teardownGraceMs:10000,killGraceMs:2000});evidence.phases.push({phase,start:phaseStart,end:Date.now()});save();}
