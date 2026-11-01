import http from 'node:http';
import {createReadStream} from 'node:fs';
import {stat} from 'node:fs/promises';
import {resolve,extname,sep} from 'node:path';
import {pathToFileURL} from 'node:url';

const types={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.png':'image/png','.svg':'image/svg+xml','.ico':'image/x-icon'};
const apiPath=/^\/(?:api(?:\/|$)|mcp(?:\/|$)|register$|login$|user(?:\/|$)|session(?:\/|$)|group(?:\/|$)|message(?:\/|$)|contact(?:\/|$))/;
function targets(values){
  if(!Array.isArray(values)||!values.length)throw Error('upstream addresses required');
  return values.map(raw=>{
    const u=new URL(raw);
    if(u.protocol!=='http:'||!['127.0.0.1','localhost','[::1]'].includes(u.hostname)||u.username||u.password||u.pathname!=='/'||u.search||u.hash)throw Error('local upstream required');
    return u;
  });
}

// Local ingress only: business and socket ownership remain in the three roles.
export function createProxy({logic,comet,root}){
  const apis=targets(logic),sockets=targets(comet),base=resolve(root);
  let apiIndex=0,wsIndex=0;
  const tunnels=new Set();
  const server=http.createServer(async(req,res)=>{
    let url;
    try{url=new URL(req.url,'http://local');}catch{res.writeHead(400);res.end();return;}
    if(url.pathname==='/healthz'){res.writeHead(200,{'content-type':'application/json'});res.end('{"live":true}');return;}
    if(apiPath.test(url.pathname)){
      const target=apis[apiIndex++%apis.length];
      const upstream=http.request({hostname:target.hostname,port:target.port,path:req.url,method:req.method,headers:{...req.headers,host:target.host}},reply=>{
        res.writeHead(reply.statusCode,reply.headers);reply.pipe(res);
        reply.on('error',()=>res.destroy());
      });
      upstream.on('error',()=>{if(!res.headersSent){res.writeHead(502);res.end('upstream unavailable');}else res.destroy();});
      req.on('aborted',()=>upstream.destroy());
      res.on('close',()=>{if(!res.writableEnded)upstream.destroy();});
      req.pipe(upstream);return;
    }
    if(req.method!=='GET'&&req.method!=='HEAD'){res.writeHead(405);res.end();return;}
    let pathname;
    try{pathname=decodeURIComponent(url.pathname);}catch{res.writeHead(400);res.end();return;}
    const path=resolve(base,'.'+(pathname==='/'?'/index.html':pathname));
    if(!path.startsWith(base+sep)){res.writeHead(404);res.end();return;}
    try{
      const s=await stat(path);
      if(!s.isFile())throw Error('not a file');
      res.writeHead(200,{'content-type':types[extname(path)]||'application/octet-stream','content-length':s.size,'cache-control':'no-cache'});
      if(req.method==='HEAD'){res.end();return;}
      const input=createReadStream(path);input.on('error',()=>res.destroy());input.pipe(res);
    }catch{res.writeHead(404);res.end('not found');}
  });
  server.on('upgrade',(req,client,head)=>{
    let url;
    try{url=new URL(req.url,'http://local');}catch{client.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');return;}
    if(url.pathname!=='/wss'){client.end('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');return;}
    const target=sockets[wsIndex++%sockets.length];
    tunnels.add(client);client.on('close',()=>tunnels.delete(client));
    const upstream=http.request({hostname:target.hostname,port:target.port,path:req.url,method:'GET',headers:{...req.headers,host:target.host}});
    const timer=setTimeout(()=>upstream.destroy(),10000);timer.unref();
    upstream.on('upgrade',(reply,peer,peerHead)=>{
      clearTimeout(timer);
      tunnels.add(peer);peer.on('close',()=>tunnels.delete(peer));
      client.write(`HTTP/1.1 ${reply.statusCode} ${reply.statusMessage}\r\n`);
      for(let i=0;i<reply.rawHeaders.length;i+=2)client.write(`${reply.rawHeaders[i]}: ${reply.rawHeaders[i+1]}\r\n`);
      client.write('\r\n');
      if(peerHead.length)client.write(peerHead);
      if(head.length)peer.write(head);
      client.on('error',()=>peer.destroy());peer.on('error',()=>client.destroy());
      client.on('close',()=>peer.destroy());peer.on('close',()=>client.destroy());
      peer.pipe(client);client.pipe(peer);
    });
    upstream.on('response',reply=>{
      clearTimeout(timer);
      client.write(`HTTP/1.1 ${reply.statusCode} Rejected\r\nConnection: close\r\n\r\n`);
      reply.resume();reply.on('end',()=>client.end());
    });
    upstream.on('error',()=>{clearTimeout(timer);client.end('HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n');});
    client.on('error',()=>upstream.destroy());client.on('close',()=>upstream.destroy());upstream.end();
  });
  server.shutdown=()=>{for(const socket of tunnels)socket.destroy();server.close();server.closeAllConnections();};
  return server;
}

if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href){
  try{
    const host=process.env.FABLE_PROXY_HOST||'127.0.0.1';
    const port=Number(process.env.FABLE_PROXY_PORT||18080);
    if(host!=='127.0.0.1'||!Number.isInteger(port)||port<1||port>65535)throw Error('invalid local listen address');
    const server=createProxy({logic:JSON.parse(process.env.FABLE_PROXY_LOGIC),comet:JSON.parse(process.env.FABLE_PROXY_COMET),root:process.env.FABLE_PROXY_ROOT});
    server.on('error',()=>{console.error('local ingress unavailable');process.exitCode=1;});
    server.listen(port,host);
    for(const signal of ['SIGINT','SIGTERM'])process.on(signal,()=>server.shutdown());
  }catch{console.error('invalid local ingress configuration');process.exitCode=1;}
}
