#!/usr/bin/env python3
"""Own one isolated local topology; never clean databases or signal foreign PIDs."""
import argparse
from contextlib import contextmanager
import fcntl
from functools import wraps
import tempfile
import threading
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

REPO=Path(__file__).resolve().parents[2]
VERSION=1
LOCAL_HTTP=urllib.request.build_opener(urllib.request.ProxyHandler({}))

class LifecycleCancelled(BaseException):
    pass

def cancel_lifecycle(signum,frame):
    raise LifecycleCancelled()

_lock_state=threading.local()

@contextmanager
def runtime_lock(directory):
    # Keep the lock outside the runtime, so taking it cannot turn an unknown
    # empty directory into a nonempty directory that configure must refuse.
    key=hashlib.sha256(str(directory.resolve()).encode()).hexdigest()
    held=getattr(_lock_state,'held',set())
    if key in held:
        yield;return
    locks=Path(tempfile.gettempdir())/('fable-local-locks-'+str(os.getuid()))
    locks.mkdir(mode=0o700,exist_ok=True)
    info=locks.lstat()
    if locks.is_symlink() or info.st_uid!=os.getuid() or info.st_mode&0o077:
        raise ValueError('unsafe local lifecycle lock directory')
    fd=os.open(locks/key,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
    try:
        fcntl.flock(fd,fcntl.LOCK_EX)
        held.add(key);_lock_state.held=held
        try:yield
        finally:held.remove(key)
    finally:os.close(fd)

def serialized(function):
    @wraps(function)
    def locked(directory,*args,**kwargs):
        with runtime_lock(directory):return function(directory,*args,**kwargs)
    return locked

@contextmanager
def cleanup_signals():
    previous={s:signal.signal(s,signal.SIG_IGN) for s in [signal.SIGINT,signal.SIGTERM]}
    try:yield
    finally:
        for s,handler in previous.items():signal.signal(s,handler)

def write_json(path,value):
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    fd,raw=tempfile.mkstemp(prefix='.'+path.name+'.',dir=path.parent)
    temp=Path(raw)
    try:
        with os.fdopen(fd,'w') as f:json.dump(value,f,indent=2)
        temp.replace(path)
    finally:temp.unlink(missing_ok=True)

def load_state(directory):
    private=directory/'private.json'
    state=json.loads((private if private.exists() else directory/'state.json').read_text())
    if state.get('version')!=VERSION or state.get('owner')!=str(REPO):
        raise ValueError('runtime directory belongs to another project/version')
    return state

def save_state(directory,state):
    write_json(directory/'private.json',state)
    public=json.loads(json.dumps(state))
    public.pop('secrets',None)
    public['database'].pop('password',None)
    public['redis'].pop('password',None)
    for kind in ['logic','comet','job']:
        for role in public[kind]:role.pop('env',None)
    write_json(directory/'state.json',public)

def free_port():
    with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]

def configure(directory,port_base=18080,external=False):
    directory=directory.resolve()
    if directory in [REPO,Path.home(),Path('/')]:raise ValueError('select an isolated runtime directory')
    if (directory/'state.json').exists():
        previous=load_state(directory)
        if any(process_matches(p) for p in previous.get('processes',{}).values()):raise ValueError('topology is already running')
    else:
        if directory.exists() and any(directory.iterdir()):raise ValueError('refusing an unowned nonempty directory')
        previous={}
    directory.mkdir(parents=True,exist_ok=True,mode=0o700)
    os.chmod(directory,0o700)
    run_id=previous.get('run_id',uuid.uuid4().hex[:12])
    key=previous.get('secrets') or {k:secrets.token_urlsafe(36) for k in ['jwt','job','kick','admin','comet1','comet2','mysql']}
    selected_ports=set()
    def port(offset):
        value=port_base+offset if port_base else free_port()
        while value in selected_ports:value=free_port()
        if value<1 or value>65535:raise ValueError('local port range is invalid')
        selected_ports.add(value)
        return value
    proxy_port=port(0)
    mysql_port=int(os.environ.get('FABLE_MYSQL_PORT',3306 if external else 13306))
    redis_port=int(os.environ.get('FABLE_REDIS_PORT',6379 if external else 16379))
    kafka_brokers=os.environ.get('FABLE_KAFKA_BROKERS','127.0.0.1:9092' if external else '127.0.0.1:19094').split(',')
    state={'version':VERSION,'owner':str(REPO),'run_id':run_id,'external':external,'secrets':key,'processes':{},
      'database':{'host':os.environ.get('FABLE_MYSQL_HOST','127.0.0.1'),'port':mysql_port,'user':os.environ.get('FABLE_MYSQL_USER','root'),'password':os.environ.get('FABLE_MYSQL_PASSWORD','' if external else key['mysql']),'name':os.environ.get('FABLE_LOCAL_DATABASE','fable_local_'+run_id)},
      'redis':{'host':os.environ.get('FABLE_REDIS_HOST','127.0.0.1'),'port':redis_port,'password':os.environ.get('FABLE_REDIS_PASSWORD',''),'prefix':'fable:local:'+run_id+':'},
      'kafka':{'brokers':kafka_brokers,'topic':os.environ.get('FABLE_LOCAL_TOPIC','fable.push.v1.'+run_id),'dlq_topic':os.environ.get('FABLE_LOCAL_DLQ_TOPIC','fable.push.dlq.v1.'+run_id),'group':os.environ.get('FABLE_LOCAL_GROUP','fable-push-job-v1.'+run_id)},
      'proxy':{'url':f'http://127.0.0.1:{proxy_port}','port':proxy_port},'logic':[],'comet':[],'job':[]}
    if not state['database']['name'].startswith(('fable_local_','fable_e2e_')):raise ValueError('select a fable_local_ or fable_e2e_ database')
    for i in range(2):
        boot=str(uuid.uuid4())
        state['logic'].append({'http':f'127.0.0.1:{port(1+i*2)}','rpc':f'127.0.0.1:{port(2+i*2)}'})
        state['comet'].append({'http':f'127.0.0.1:{port(5+i*2)}','rpc':f'127.0.0.1:{port(6+i*2)}','instance_id':f'comet-{run_id}-{i+1}-{boot[:8]}','boot_id':boot})
        state['job'].append({'http':f'127.0.0.1:{port(9+i)}'})
    grants=[{'identity':{'instance_id':c['instance_id'],'boot_id':c['boot_id']},'rpc_address':c['rpc'],'token':key[f'comet{i+1}']} for i,c in enumerate(state['comet'])]
    db=state['database'];r=state['redis'];k=state['kafka']
    for i,c in enumerate(state['logic']):
        path=directory/f'logic-{i+1}.toml'
        path.write_text(f'''[mysqlConfig]
host = {json.dumps(db['host'])}
port = {db['port']}
user = {json.dumps(db['user'])}
databaseName = {json.dumps(db['name'])}
[redisConfig]
enabled = true
host = {json.dumps(r['host'])}
port = {r['port']}
db = 0
[kafkaConfig]
enabled = true
mode = "kafka"
brokers = {json.dumps(k['brokers'])}
topic = {json.dumps(k['topic'])}
[logic]
httpAddress = {json.dumps(c['http'])}
rpcAddress = {json.dumps(c['rpc'])}
redisPrefix = {json.dumps(r['prefix'])}
''')
        c['config']=str(path)
        c['env']={'FABLE_MYSQL_PASSWORD':db['password'],'FABLE_JWT_SECRET':key['jwt'],'FABLE_REDIS_PASSWORD':r['password'],'FABLE_LOGIC_JOB_TOKEN':key['job'],'FABLE_LOGIC_KICK_TOKEN':key['kick'],'FABLE_LOGIC_COMET_GRANTS_JSON':json.dumps(grants)}
    for i,c in enumerate(state['comet']):
        path=directory/f'comet-{i+1}.toml';path.write_text('[comet]\n')
        c['config']=str(path)
        c['env']={'FABLE_COMET_INSTANCE_ID':c['instance_id'],'FABLE_COMET_BOOT_ID':c['boot_id'],'FABLE_COMET_HTTP_ADDR':c['http'],'FABLE_COMET_RPC_ADDR':c['rpc'],'FABLE_COMET_LOGIC_ADDR':state['logic'][i]['rpc'],'FABLE_COMET_LOGIC_TOKEN':key[f'comet{i+1}'],'FABLE_COMET_JOB_TOKEN':key['job'],'FABLE_COMET_KICK_TOKEN':key['kick'],'FABLE_COMET_ADMIN_TOKEN':key['admin'],'FABLE_COMET_ALLOWED_ORIGINS':state['proxy']['url'],'FABLE_JWT_SECRET':key['jwt']}
    for i,c in enumerate(state['job']):
        path=directory/f'job-{i+1}.toml';path.write_text('[job]\n[kafka]\n')
        c['config']=str(path)
        c['env']={'FABLE_JOB_HTTP_ADDR':c['http'],'FABLE_JOB_LOGIC_ADDR':state['logic'][i]['rpc'],'FABLE_JOB_TOKEN':key['job'],'FABLE_KAFKA_BROKERS':','.join(k['brokers']),'FABLE_KAFKA_TOPIC':k['topic'],'FABLE_JOB_GROUP':k['group'],'FABLE_JOB_DLQ_TOPIC':k['dlq_topic']}
    for path in directory.glob('*.toml'):os.chmod(path,0o600)
    save_state(directory,state)
    return state

def process_record(pid,marker):
    started=subprocess.check_output(['ps','-p',str(pid),'-o','lstart='],text=True).strip()
    return {'pid':pid,'started':started,'marker':str(Path(marker).resolve())}

def process_matches(record):
    try:
        started=subprocess.check_output(['ps','-p',str(record['pid']),'-o','lstart='],text=True,stderr=subprocess.DEVNULL).strip()
        command=subprocess.check_output(['ps','-p',str(record['pid']),'-o','command='],text=True,stderr=subprocess.DEVNULL).strip()
        return bool(started and started==record['started'] and record['marker'] in command)
    except (subprocess.SubprocessError,KeyError):return False

def stop_process(record):
    try:os.kill(record['pid'],0)
    except ProcessLookupError:return
    try:
        status=subprocess.check_output(['ps','-p',str(record['pid']),'-o','stat='],text=True).strip()
        if status.startswith('Z'):
            try:os.waitpid(record['pid'],os.WNOHANG)
            except ChildProcessError:pass
            return
    except subprocess.CalledProcessError:return
    if not process_matches(record):raise ValueError('refusing to signal an unowned or reused PID')
    os.kill(record['pid'],signal.SIGTERM)
    until=time.monotonic()+18
    while time.monotonic()<until:
        if not process_matches(record):return
        # Children of this invocation can be reaped here; foreign processes are
        # never waited on or signalled because the ownership check above fails.
        try:
            if os.waitpid(record['pid'],os.WNOHANG)[0]:return
        except ChildProcessError:pass
        time.sleep(.1)
    if process_matches(record):os.kill(record['pid'],signal.SIGKILL)

def child_env(overrides):
    # Caller FABLE_* overrides cannot silently replace the selected manifest.
    env={k:v for k,v in os.environ.items() if not k.startswith('FABLE_')}
    env.update(overrides)
    return env

def invoke(args,env=None):
    process=None
    blocked=signal.pthread_sigmask(signal.SIG_BLOCK,{signal.SIGINT,signal.SIGTERM})
    try:
        try:
            process=subprocess.Popen(args,cwd=REPO,env=env,preexec_fn=lambda:signal.pthread_sigmask(signal.SIG_SETMASK,blocked))
        finally:signal.pthread_sigmask(signal.SIG_SETMASK,blocked)
        code=process.wait()
        if code:raise subprocess.CalledProcessError(code,args)
    except BaseException:
        # Let one-shot local-admin honor its canceled context and clean fixtures.
        # Popen owns and has not reaped this PID, so it cannot have been reused.
        if process is not None and process.poll() is None:
            process.terminate()
            try:process.wait(timeout=12)
            except subprocess.TimeoutExpired:process.kill();process.wait()
        raise

def docker_command(state):
    args=['docker']
    if os.environ.get('FABLE_LOCAL_DOCKER_CONTEXT'):args+=['--context',os.environ['FABLE_LOCAL_DOCKER_CONTEXT']]
    return args+['compose','-p','fable-'+state['run_id'],'-f',str(REPO/'deploy/local/infra.yml')]

def infrastructure_env(state):
    return {**os.environ,'FABLE_LOCAL_MYSQL_PORT':str(state['database']['port']),'FABLE_LOCAL_MYSQL_PASSWORD':state['database']['password'],'FABLE_LOCAL_REDIS_PORT':str(state['redis']['port']),'FABLE_LOCAL_KAFKA_PORT':str(int(state['kafka']['brokers'][0].rsplit(':',1)[1]))}

def wait_ready(address,record,timeout=45):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        if not process_matches(record):raise ValueError('role exited during startup; inspect its private runtime log')
        try:
            with LOCAL_HTTP.open(address,timeout=2) as response:
                if response.status==200:return
        except (urllib.error.URLError,TimeoutError):pass
        time.sleep(.15)
    raise ValueError('role did not become ready; inspect its private runtime log')

def start_role(directory,state,name,role,args=None,extra=None):
    binary=str(directory/'bin'/role)
    if role=='proxy':
        command=[shutil.which('node') or 'node',str(REPO/'deploy/local/proxy.mjs')];marker=command[1]
    else:command=[binary,'-config',name['config'],'serve'];marker=binary
    env=child_env(name.get('env',{}) if extra is None else extra)
    role_name=args or role
    log=directory/(role_name+'.log')
    # Defer cancellation until the spawned child has an ownership record.
    blocked=signal.pthread_sigmask(signal.SIG_BLOCK,{signal.SIGINT,signal.SIGTERM})
    try:
        with log.open('ab',buffering=0) as output:
            p=subprocess.Popen(command,cwd=REPO,env=env,stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT,start_new_session=True,preexec_fn=lambda:signal.pthread_sigmask(signal.SIG_SETMASK,blocked))
        record=process_record(p.pid,marker);record['log']=str(log)
        state['processes'][role_name]=record;save_state(directory,state)
    finally:signal.pthread_sigmask(signal.SIG_SETMASK,blocked)
    wait_ready(('http://'+name['http']+'/readyz') if role!='proxy' else state['proxy']['url']+'/healthz',record)

@serialized
def down(directory,infra=False):
    if not (directory/'state.json').exists():return
    state=load_state(directory)
    errors=[]
    for name,record in reversed(list(state['processes'].items())):
        try:stop_process(record)
        except ValueError:errors.append(name)
    if errors:raise ValueError('unowned PID records preserved: '+','.join(errors))
    state['processes']={};save_state(directory,state)
    if infra and not state['external']:invoke(docker_command(state)+['stop'],infrastructure_env(state))

@serialized
def up(directory,external=False,skip_web=False,port_base=18080):
    if (directory/'state.json').exists():
        old=load_state(directory)
        live=[p for p in old.get('processes',{}).values() if process_matches(p)]
        if live:
            if len(live)==7:
                selected_smoke(directory,old)
                print('already running; selected smoke passed: '+old['proxy']['url']);return old
            raise ValueError('partial topology still running; use dev-down before restarting')
    state=configure(directory,port_base,external)
    try:
        if not external:invoke(docker_command(state)+['up','-d','--wait','--wait-timeout','120'],infrastructure_env(state))
        (directory/'bin').mkdir(exist_ok=True)
        for role in ['logic','comet','job','local-admin']:
            invoke(['go','build','-p','1','-o',str(directory/'bin'/role),'./cmd/'+role])
        invoke([str(directory/'bin/local-admin'),'-config',state['logic'][0]['config'],'-dlq',state['kafka']['dlq_topic']],child_env(state['logic'][0]['env']))
        if not skip_web:
            subprocess.run(['npm','ci','--no-audit','--no-fund'],cwd=REPO/'web/chat-server',check=True)
            subprocess.run(['npm','run','build'],cwd=REPO/'web/chat-server',check=True)
        for role in ['logic','comet','job']:
            for i,config in enumerate(state[role]):start_role(directory,state,config,role,role+'-'+str(i+1))
        proxy_env={'FABLE_PROXY_HOST':'127.0.0.1','FABLE_PROXY_PORT':str(state['proxy']['port']),'FABLE_PROXY_LOGIC':json.dumps(['http://'+c['http'] for c in state['logic']]),'FABLE_PROXY_COMET':json.dumps(['http://'+c['http'] for c in state['comet']]),'FABLE_PROXY_ROOT':str(REPO/'web/chat-server/dist')}
        start_role(directory,state,{},'proxy',extra=proxy_env)
        selected_smoke(directory,state)
        print('ready; selected smoke passed: '+state['proxy']['url'])
        return state
    except BaseException:
        with cleanup_signals():down(directory)
        raise

def selected_smoke(directory,state):
    logic=state['logic'][0]
    invoke([str(directory/'bin/local-admin'),'-config',logic['config'],'-smoke','-entry',state['proxy']['url']],child_env(logic['env']))

@serialized
def smoke(directory):
    state=load_state(directory)
    selected_smoke(directory,state)
    print('selected topology smoke passed')

def status(directory):
    if not (directory/'state.json').exists():print('not initialized');return False
    state=load_state(directory);healthy=len(state.get('processes',{}))==7
    for name,record in state.get('processes',{}).items():
        live=process_matches(record);ready=False
        if live:
            config=state[name.split('-')[0]][int(name.split('-')[1])-1] if name!='proxy' else None
            url='http://'+config['http']+'/readyz' if config else state['proxy']['url']+'/healthz'
            try:
                with LOCAL_HTTP.open(url,timeout=2) as r:ready=r.status==200
            except (urllib.error.URLError,TimeoutError):pass
        details=' pid='+str(record['pid'])
        if name!='proxy':
            kind,index=name.split('-');config=state[kind][int(index)-1]
            details+=' http='+config['http']
            if kind=='comet':details+=' boot='+config['boot_id']
            if kind=='job' and ready:
                try:
                    import re
                    with LOCAL_HTTP.open('http://'+config['http']+'/metrics',timeout=2) as m:metrics=m.read(65536).decode()
                    lag=re.findall(r'^fable_job_partition_lag\{[^}]+\} (\d+)$',metrics,re.M)
                    details+=' lag='+str(sum(map(int,lag)))+' consumer=alive'
                except (urllib.error.URLError,TimeoutError):details+=' lag=unknown'
        print(name+': '+('ready' if ready else 'not ready')+details)
        healthy=healthy and ready
    print('browser: '+state['proxy']['url'])
    return healthy

@serialized
def stop_jobs(directory):
    state=load_state(directory)
    for name in ['job-2','job-1']:
        record=state['processes'].get(name)
        if record:stop_process(record);del state['processes'][name]
    save_state(directory,state)

@serialized
def restart_job(directory,index):
    state=load_state(directory);name='job-'+str(index)
    if name in state['processes']:stop_process(state['processes'][name])
    start_role(directory,state,state['job'][index-1],'job',name)
    print(name+': ready')

def main():
    for s in [signal.SIGINT,signal.SIGTERM]:signal.signal(s,cancel_lifecycle)
    parser=argparse.ArgumentParser()
    parser.add_argument('command',choices=['up','down','status','stop-jobs','restart-job','replay-dlq','smoke'])
    parser.add_argument('--external',action='store_true',default=os.environ.get('FABLE_LOCAL_EXTERNAL')=='1')
    parser.add_argument('--skip-web',action='store_true')
    parser.add_argument('--infra',action='store_true')
    parser.add_argument('--index',type=int,choices=[1,2],default=1)
    args,remaining=parser.parse_known_args()
    directory=Path(os.environ.get('FABLE_LOCAL_RUNTIME_DIR',REPO/'.fable/local')).resolve()
    if args.command=='replay-dlq':
        state=load_state(directory);job=state['job'][0]
        invoke([str(directory/'bin/job'),'-config',job['config'],'replay-dlq',*remaining],child_env(job['env']))
    else:
        if remaining:parser.error('unexpected arguments')
        if args.command=='up':up(directory,args.external,args.skip_web,int(os.environ.get('FABLE_LOCAL_PORT_BASE','18080')))
        elif args.command=='down':down(directory,args.infra)
        elif args.command=='smoke':smoke(directory)
        elif args.command=='stop-jobs':stop_jobs(directory)
        elif args.command=='restart-job':restart_job(directory,args.index)
        elif not status(directory):raise SystemExit(1)

if __name__=='__main__':
    try:main()
    except LifecycleCancelled:
        print('local topology operation cancelled',file=sys.stderr)
        raise SystemExit(130)
    except (ValueError,subprocess.SubprocessError,OSError) as e:
        # Errors contain no expanded environment, tokens, database DSNs or URLs.
        print('local topology operation failed: '+(str(e) if isinstance(e,ValueError) else 'inspect selected dependencies and private role logs'),file=sys.stderr)
        raise SystemExit(1)
