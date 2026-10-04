import json, pathlib, collections, sqlite3, re, sys
root=pathlib.Path(sys.argv[1]); meta=json.loads((root/'evidence.json').read_text());gateway=next(x['pid'] for x in meta['children'] if x['name']=='gateway')
skip=['/node_modules/','/.sqlmeasure/', 'node:','src/infra/']
def site(stack):
    frames=[]
    for frame in stack:
        match=re.search(r'((?:src|extensions|packages)/[^():]+):(\d+):(\d+)', frame)
        if match and not any(s in frame for s in skip):
            return f'{match[1]}:{match[2]}'
    for frame in stack:
        match=re.search(r'((?:src|extensions|packages)/[^():]+):(\d+):(\d+)',frame)
        if match and not any(s in frame for s in ['src/infra/kysely-sync','src/infra/sqlite-schema-facts.ts','src/infra/node-sqlite.ts']):return f'{match[1]}:{match[2]} [infra-only stack]'
    return 'unknown'
def statements(sql):
    result=[];buffer=''
    for ch in sql:
        buffer+=ch
        if ch==';' and sqlite3.complete_statement(buffer):
            if re.sub(r'/\*.*?\*/|--[^\n]*','',buffer,flags=re.S).strip(' ;\n\t\r'):result.append(buffer.strip())
            buffer=''
    if re.sub(r'/\*.*?\*/|--[^\n]*','',buffer,flags=re.S).strip(' ;\n\t\r'):result.append(buffer.strip())
    return result
phases={};allsites=collections.Counter();runSites=collections.Counter();full=collections.defaultdict(dict);processes=[];databases=collections.Counter();errors=[]
for file in sorted(root.glob('sql-*.jsonl')):
    sites={};proc=None
    for line in file.read_text().splitlines():
        item=json.loads(line)
        if isinstance(item,dict):
            if item['type']=='process':proc=item;processes.append(proc)
            elif item['type']=='site':sites[item['id']]=item
            continue
        phase,id=item;s=sites[id];bucket='main' if proc['pid']==gateway and proc['threadId']==0 else 'worker'
        row=phases.setdefault(phase,{'main':0,'worker':0,'worker_threads':0,'child_processes':0,'main_api':0,'worker_api':0,'main_prepare':0,'worker_prepare':0,'main_errors':0,'worker_errors':0,'main_prepare_errors':0,'worker_prepare_errors':0,'sites':collections.Counter(),'worker_sites':collections.Counter(),'methods':collections.Counter()})
        row[bucket+'_api']+=1;row['methods'][bucket+'.'+s['method']]+=1
        if s['method']=='prepare':
            row[bucket+'_prepare']+=1
            if not s['ok']:row[bucket+'_prepare_errors']+=1
            continue
        count=len(statements(s['sql'])) if s['method']=='exec' else 1
        if not s['ok']:row[bucket+'_errors']+=1;errors.append({'phase':phase,'bucket':bucket,**s});continue
        row[bucket]+=count
        if bucket=='worker':row['worker_threads' if proc['threadId']>0 else 'child_processes']+=count
        loc=site(s['stack']);row['sites' if bucket=='main' else 'worker_sites'][loc]+=count
        full[loc][s['sql']]=s
        databases[(bucket,str(s['database']))]+=count
        if bucket=='main':
            allsites[loc]+=count
            if phase.startswith(('02','03','04','05','06','07')):runSites[loc]+=count
out={'gatewayPid':gateway,'phases':phases,'topAll':allsites.most_common(),'topRuntime':runSites.most_common(),'processes':processes,'databases':[[*k,v] for k,v in databases.items()],'errors':errors,'sites':dict(full)}
(root/'analysis.json').write_text(json.dumps(out,indent=2))
for name,row in phases.items():print(name,row['main'],row['worker'],'prepare',row['main_prepare'],'errors',row['main_errors'],row['worker_errors'])
print('TOP ALL',allsites.most_common(15));print('TOP RUNTIME',runSites.most_common(15))
