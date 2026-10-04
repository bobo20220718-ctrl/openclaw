// Scratch port of observeHostDataSql's exact native boundaries; includes cached statements.
const fs = require('node:fs');
const { DatabaseSync, StatementSync } = require('node:sqlite');
const { threadId, isMainThread } = require('node:worker_threads');
const path = require('node:path');
const root = process.env.SQLMEASURE_OUT;
if (!root) throw new Error('SQLMEASURE_OUT missing');
Error.stackTraceLimit = 35;
const fd = fs.openSync(path.join(root, `sql-${process.pid}-${threadId}.jsonl`), 'a');
fs.writeSync(fd, JSON.stringify({type:'process',pid:process.pid,ppid:process.ppid,threadId,isMainThread,argv:process.argv})+'\n');
const keys = new Map();
const prepared = new WeakMap();
function capture() { return new Error().stack.split('\n').slice(2).filter(s=>!s.includes('/.sqlmeasure/observer.cjs')); }
function record(method,sql,stack,database,ok,error) {
  const phase = fs.readFileSync(path.join(root,'phase'),'utf8').trim();
  const value = {method,sql,stack,database,ok,...(error?{error}: {})};
  const key = JSON.stringify(value);
  let id = keys.get(key);
  if(id===undefined) { id=keys.size; keys.set(key,id); fs.writeSync(fd,JSON.stringify({type:'site',id,...value})+'\n'); }
  fs.writeSync(fd,JSON.stringify([phase,id])+'\n');
}
for (const method of ['prepare','exec']) {
  const original=DatabaseSync.prototype[method];
  DatabaseSync.prototype[method]=function(sql,...args) {
    const stack=capture(); const database=this.location();
    try {
      const result=Reflect.apply(original,this,[sql,...args]);
      if(method==='prepare') prepared.set(result,{database,sql});
      record(method,sql,stack,database,true);
      return result;
    } catch(error) { record(method,sql,stack,database,false,error.message); throw error; }
  };
}
for (const method of ['get','all','run','iterate']) {
  const original=StatementSync.prototype[method];
  StatementSync.prototype[method]=function(...args) {
    const stack=capture(); const sql=this.sourceSQL; const database=prepared.get(this)?.database ?? 'unknown';
    if(method==='iterate') {
      const iterator=Reflect.apply(original,this,args);
      const next=iterator.next.bind(iterator); let started=false;
      iterator.next=(...nextArgs)=> {
        try { const result=next(...nextArgs); if(!started){ started=true; record(method,sql,stack,database,true); } return result; }
        catch(error){ if(!started){started=true;record(method,sql,stack,database,false,error.message);}throw error; }
      };
      return iterator;
    }
    try { const result=Reflect.apply(original,this,args); record(method,sql,stack,database,true); return result; }
    catch(error){record(method,sql,stack,database,false,error.message);throw error;}
  };
}
