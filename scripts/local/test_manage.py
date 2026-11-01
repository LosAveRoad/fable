import importlib.util
import json
import os
from pathlib import Path
import subprocess
import shutil
import sys
import tempfile
import unittest
import http.server
import threading
import signal
import time
import concurrent.futures
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('manage',Path(__file__).with_name('manage.py'))
manage=importlib.util.module_from_spec(spec)
spec.loader.exec_module(manage)

class LifecycleTest(unittest.TestCase):
    def test_loopback_readiness_bypasses_user_http_proxy(self):
        class Probe(http.server.BaseHTTPRequestHandler):
            def do_GET(self):self.send_response(200);self.end_headers()
            def log_message(self,*args):pass
        server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Probe)
        thread=threading.Thread(target=server.serve_forever);thread.start()
        child=subprocess.Popen([shutil.which('sleep'),'10'])
        try:
            record=manage.process_record(child.pid,shutil.which('sleep'))
            with patch.dict(os.environ,{'http_proxy':'http://127.0.0.1:1','HTTP_PROXY':'http://127.0.0.1:1','no_proxy':'','NO_PROXY':''}):
                manage.wait_ready('http://127.0.0.1:'+str(server.server_port),record,timeout=.5)
        finally:child.terminate();child.wait();server.shutdown();server.server_close();thread.join()
    def test_private_manifest_reuses_secrets_but_rotates_boots_for_restart(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'owned'
            state=manage.configure(path,port_base=0,external=True)
            self.assertEqual(len(state['logic']),2)
            self.assertEqual(len(state['comet']),2)
            self.assertEqual(len(state['job']),2)
            self.assertEqual(os.stat(path/'state.json').st_mode&0o777,0o600)
            public=json.loads((path/'state.json').read_text())
            self.assertFalse('secrets' in public,'public metadata contains secret fields')
            self.assertFalse('password' in public['database'],'public metadata contains database credentials')
            self.assertFalse('env' in public['logic'][0],'public metadata contains role environment')
            original=state['secrets']['jwt']
            old_boots=[c['boot_id'] for c in state['comet']]
            old_instances=[c['instance_id'] for c in state['comet']]
            restarted=manage.configure(path,port_base=0,external=True)
            self.assertEqual(restarted['secrets']['jwt'],original)
            self.assertNotEqual([c['boot_id'] for c in restarted['comet']],old_boots)
            self.assertNotEqual([c['instance_id'] for c in restarted['comet']],old_instances)
            for role in restarted['logic']:
                grants=json.loads(role['env']['FABLE_LOGIC_COMET_GRANTS_JSON'])
                self.assertEqual([g['identity']['boot_id'] for g in grants],[c['boot_id'] for c in restarted['comet']])

    def test_refuses_unknown_directory_and_never_signals_reused_pid(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'valuable').write_text('keep')
            with self.assertRaises(ValueError):manage.configure(root,port_base=0,external=True)
            executable=shutil.which('sleep')
            child=subprocess.Popen([executable,'30'])
            try:
                record=manage.process_record(child.pid,executable)
                self.assertTrue(manage.process_matches(record))
                stale={**record,'started':'not this process'}
                with self.assertRaises(ValueError):manage.stop_process(stale)
                self.assertIsNone(child.poll())
                manage.stop_process(record)
                child.wait(timeout=5)
            finally:
                if child.poll() is None:child.terminate();child.wait()

    def test_state_does_not_include_legacy_topic_or_database(self):
        with tempfile.TemporaryDirectory() as d:
            state=manage.configure(Path(d)/'owned',port_base=0,external=True)
            self.assertTrue(state['database']['name'].startswith('fable_local_'))
            self.assertTrue(state['kafka']['topic'].startswith('fable.push.v1'))
            self.assertTrue(state['kafka']['dlq_topic'].startswith('fable.push.dlq.v1'))
            self.assertNotIn('fable.chat.message',json.dumps(state))

    def test_exited_owned_child_can_be_reaped_without_signalling(self):
        child=subprocess.Popen([shutil.which('sleep'),'.05'])
        record=manage.process_record(child.pid,shutil.which('sleep'))
        import time
        time.sleep(.1)
        manage.stop_process(record)
        child.wait(timeout=1)

    def test_random_service_ports_do_not_repeat_when_kernel_reuses_a_port(self):
        with tempfile.TemporaryDirectory() as d:
            with patch.object(manage,'free_port',side_effect=[31001,31002,31002,*range(31003,31012)]):
                state=manage.configure(Path(d)/'owned',port_base=0,external=True)
            addresses=[state['proxy']['port']]+[int(role[field].split(':')[1]) for kind in ['logic','comet','job'] for role in state[kind] for field in ['http','rpc'] if field in role]
            self.assertEqual(len(set(addresses)),len(addresses))

class ReviewedLifecycleTest(unittest.TestCase):
    def test_atomic_private_writes_do_not_share_temporary_filename(self):
        with tempfile.TemporaryDirectory() as d:
            target=Path(d)/'private.json'
            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
                list(pool.map(lambda n:manage.write_json(target,{'n':n}),range(100)))
            self.assertIn('n',json.loads(target.read_text()))
            self.assertEqual(target.stat().st_mode&0o777,0o600)
            self.assertEqual(list(Path(d).iterdir()),[target])

    def fixture(self, root, mode, command='up'):
        # Exercise real CLI dispatch, ownership records and OS children while
        # replacing only unavailable backend/build/readiness boundaries.
        code = r"""
import importlib.util, pathlib, os, subprocess, shutil, time
spec=importlib.util.spec_from_file_location('manage',os.environ['MANAGER']);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
root=pathlib.Path(os.environ['FIXTURE_ROOT'])
original=m.configure
def configure(*a,**kw):
    value=original(*a,**kw);time.sleep(.2);return value
m.configure=configure
def invoke(args,env=None):
    if args[0]=='go':
        path=pathlib.Path(args[-2]);path.write_text('#!/usr/bin/env python3\nimport time\ntime.sleep(40)\n');path.chmod(0o700)
m.invoke=invoke
m.selected_smoke=lambda *a,**kw:None
def start(directory,state,name,role,args=None,extra=None):
    child=subprocess.Popen([shutil.which('sleep'),'40'],start_new_session=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    with (root/'children').open('a') as f:f.write(str(child.pid)+'\n')
    state['processes'][args or role]=m.process_record(child.pid,shutil.which('sleep'));m.save_state(directory,state)
    if os.environ['FIXTURE_MODE']=='cancel':
        (root/'started').write_text(str(child.pid));time.sleep(40)
    time.sleep(.03)
if os.environ['FIXTURE_MODE']=='cancel':
    def ready(address,record,timeout=45):
        (root/'started').write_text(str(record['pid']));time.sleep(40)
    m.wait_ready=ready
else:m.start_role=start
m.main()
"""
        env={**os.environ,'MANAGER':str(Path(manage.__file__)),'FIXTURE_ROOT':str(root),'FIXTURE_MODE':mode,'FABLE_LOCAL_RUNTIME_DIR':str(root/'runtime'),'FABLE_LOCAL_EXTERNAL':'1','FABLE_LOCAL_PORT_BASE':'0'}
        return subprocess.Popen([sys.executable,'-c',code,command,'--skip-web'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)

    def cleanup_children(self, root):
        if (root/'started').exists():
            state=manage.load_state(root/'runtime')
            for record in state['processes'].values():
                try:manage.stop_process(record)
                except ValueError:pass
        if (root/'children').exists():
            for pid in map(int,(root/'children').read_text().splitlines()):
                try:
                    record=manage.process_record(pid,shutil.which('sleep'))
                    if manage.process_matches(record):os.kill(pid,signal.SIGKILL)
                except (ProcessLookupError,subprocess.CalledProcessError):pass

    def test_simultaneous_cli_up_serializes_the_entire_lifecycle(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);children=[]
            try:
                children=[self.fixture(root,'parallel') for _ in range(2)]
                outputs=[c.communicate(timeout=15) for c in children]
                self.assertEqual([c.returncode for c in children],[0,0],outputs)
                pids=(root/'children').read_text().splitlines()
                self.assertEqual(len(pids),7,'concurrent CLI starts created duplicate topology children')
                self.assertEqual(len(manage.load_state(root/'runtime')['processes']),7)
            finally:
                for c in children:
                    if c.poll() is None:c.kill();c.wait()
                self.cleanup_children(root)

    def test_cli_down_waits_for_concurrent_up(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);children=[]
            try:
                children.append(self.fixture(root,'parallel'))
                until=time.monotonic()+10
                while not (root/'children').exists() and time.monotonic()<until:time.sleep(.01)
                self.assertTrue((root/'children').exists())
                children.append(self.fixture(root,'parallel','down'))
                outputs=[c.communicate(timeout=15) for c in children]
                self.assertEqual([c.returncode for c in children],[0,0],outputs)
                self.assertEqual(manage.load_state(root/'runtime')['processes'],{})
                self.assertEqual(len((root/'children').read_text().splitlines()),7)
            finally:
                for c in children:
                    if c.poll() is None:c.kill();c.wait()
                self.cleanup_children(root)

    def test_runtime_lock_keeps_unknown_empty_runtime_empty(self):
        with tempfile.TemporaryDirectory() as d:
            directory=Path(d)/'runtime';directory.mkdir()
            with manage.runtime_lock(directory):self.assertEqual(list(directory.iterdir()),[])

    def test_new_up_runs_selected_smoke_after_all_roles_before_ready(self):
        with tempfile.TemporaryDirectory() as d:
            directory=Path(d)/'runtime';started=[]
            def smoke(directory,state):
                self.assertEqual(started,['logic-1','logic-2','comet-1','comet-2','job-1','job-2','proxy'])
                raise ValueError('selected smoke failed')
            def start(directory,state,name,role,args=None,extra=None):started.append(args or role)
            with patch.object(manage,'invoke'),patch.object(manage,'start_role',side_effect=start),patch.object(manage,'selected_smoke',side_effect=smoke),patch.object(manage,'down') as down:
                with self.assertRaisesRegex(ValueError,'selected smoke failed'):manage.up(directory,True,True,0)
                down.assert_called_once_with(directory)

    def test_sigterm_during_startup_cleans_only_owned_children(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);child=self.fixture(root,'cancel')
            unrelated=subprocess.Popen([shutil.which('sleep'),'40'])
            try:
                until=time.monotonic()+10
                while not (root/'started').exists() and time.monotonic()<until:time.sleep(.02)
                self.assertTrue((root/'started').exists())
                record=next(iter(manage.load_state(root/'runtime')['processes'].values()))
                child.terminate();child.communicate(timeout=10)
                self.assertFalse(manage.process_matches(record),'SIGTERM leaked the startup child')
                self.assertIsNone(unrelated.poll())
            finally:
                if child.poll() is None:child.kill();child.wait()
                self.cleanup_children(root);unrelated.terminate();unrelated.wait()

    def test_up_runs_selected_smoke_and_existing_failure_preserves_topology(self):
        with tempfile.TemporaryDirectory() as d:
            directory=Path(d)/'runtime';state=manage.configure(directory,0,True)
            state['processes']={str(n):{'pid':n} for n in range(7)}
            manage.save_state(directory,state)
            with patch.object(manage,'process_matches',return_value=True),patch.object(manage,'selected_smoke',create=True,side_effect=ValueError('selected smoke failed')) as smoke,patch.object(manage,'down') as down:
                with self.assertRaisesRegex(ValueError,'selected smoke failed'):manage.up(directory,True,True,0)
                smoke.assert_called_once();down.assert_not_called()

    def test_smoke_script_is_executable(self):
        self.assertTrue(os.access(manage.REPO/'scripts/smoke.sh',os.X_OK))

if __name__=='__main__':unittest.main()
