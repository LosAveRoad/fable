import {test} from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import net from 'node:net';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createProxy} from './proxy.mjs';

const listen = server => new Promise(resolve => server.listen(0,'127.0.0.1',()=>resolve(`http://127.0.0.1:${server.address().port}`)));
const close = server => new Promise(resolve=>server.close(resolve));
test('proxy serves the built client, preserves authenticated API and streams SSE',async t=>{
  const root=await mkdtemp(join(tmpdir(),'fable-proxy-'));
  await writeFile(join(root,'index.html'),'<main>Fable v2</main>');
  const peers=[];
  for(const id of ['one','two']){
    const server=http.createServer((req,res)=>{
      if(req.url==='/mcp'){res.writeHead(200,{'content-type':'text/event-stream'});res.end('data: ready\n\n');return;}
      res.setHeader('content-type','application/json');res.end(JSON.stringify({id,auth:req.headers.authorization,url:req.url}));
    });
    peers.push({server,url:await listen(server)});
  }
  const proxy=createProxy({logic:peers.map(p=>p.url),comet:peers.map(p=>p.url),root});
  const address=await listen(proxy);
  t.after(async()=>{await close(proxy);for(const p of peers)await close(p.server);await rm(root,{recursive:true});});
  assert.match(await (await fetch(address)).text(),/Fable v2/);
  const responses=[];
  for(let i=0;i<2;i++)responses.push(await(await fetch(address+'/api/v2/messages/sync?cursor=safe',{headers:{authorization:'Bearer sample'}})).json());
  assert.deepEqual(responses.map(r=>r.id).sort(),['one','two']);
  assert.ok(responses.every(r=>r.auth==='Bearer sample'&&r.url.endsWith('cursor=safe')));
  assert.equal(await(await fetch(address+'/mcp')).text(),'data: ready\n\n');
  assert.equal((await fetch(address+'/%2e%2e/%2e%2e/etc/passwd')).status,404);
});

test('upgrade balances Comets and preserves both initial tunnel byte buffers',{timeout:5000},async t=>{
  const servers=[];
  for(const id of ['one','two']){
    const server=http.createServer();
    server.on('upgrade',(req,socket,head)=>{
      assert.equal(req.headers.origin,'http://local.test');
      socket.write('HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n'+id);
      if(head.length)socket.write(head);
      socket.on('data',b=>socket.write(b));
      socket.on('end',()=>socket.end());
    });
    servers.push({server,url:await listen(server)});
  }
  const proxy=createProxy({logic:servers.map(p=>p.url),comet:servers.map(p=>p.url),root:tmpdir()});
  const address=new URL(await listen(proxy));
  const sockets=[];
  t.after(async()=>{for(const s of sockets)s.destroy();proxy.shutdown();for(const p of servers)await close(p.server);});
  const results=[];
  for(let i=0;i<2;i++){
    results.push(await new Promise((resolve,reject)=>{
      const socket=net.connect(Number(address.port),'127.0.0.1');sockets.push(socket);let data='';
      socket.setTimeout(2000,()=>reject(Error('tunnel timed out')));
      socket.on('error',reject);socket.on('data',b=>{data+=b;if(data.includes('hello')){socket.destroy();resolve(data);}});
      socket.on('connect',()=>socket.write('GET /wss?protocol=v2 HTTP/1.1\r\nHost: local.test\r\nOrigin: http://local.test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\nhello'));
    }));
  }
  assert.match(results[0],/101 Switching Protocols/);
  assert.ok(results.some(r=>r.includes('one'))&&results.some(r=>r.includes('two')));
});

test('malformed HTTP and upgrade targets return 400 and keep ingress alive',{timeout:5000},async t=>{
  const proxy=createProxy({logic:['http://127.0.0.1:1'],comet:['http://127.0.0.1:1'],root:tmpdir()});
  const address=new URL(await listen(proxy));
  t.after(()=>proxy.shutdown());
  for(const upgrade of [false,true]){
    const reply=await new Promise((resolve,reject)=>{
      const socket=net.connect(Number(address.port),'127.0.0.1');let data='';
      socket.setTimeout(1000,()=>{socket.destroy();reject(Error('request timed out'));});
      socket.on('error',reject);socket.on('data',b=>data+=b);socket.on('end',()=>resolve(data));
      socket.on('connect',()=>socket.write('GET //[ HTTP/1.1\r\nHost: local.test\r\nConnection: '+(upgrade?'Upgrade\r\nUpgrade: websocket':'close')+'\r\n\r\n'));
    });
    assert.match(reply,/^HTTP\/1\.1 400 /);
    assert.equal((await fetch(address+'healthz')).status,200);
  }
});
