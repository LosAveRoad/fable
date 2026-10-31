package routing

import "github.com/redis/go-redis/v9"

// Every key uses the same hash tag, so instance fences and route mutations are
// atomic even on Redis Cluster. Dynamic instance keys stay in that same slot.
var leaseScript = redis.NewScript(`
local op = ARGV[1]
local p = cjson.decode(ARGV[2])
local ip = ARGV[3]
local rp = ARGV[4]
local ittl = tonumber(ARGV[5])
local rttl = tonumber(ARGV[6])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local function answer(v) return cjson.encode(v) end
local function err(code) return answer({err=code}) end
local function uuid(v)
 if type(v)~='string' or #v~=36 or v=='00000000-0000-0000-0000-000000000000' then return false end
 if v:sub(9,9)~='-' or v:sub(14,14)~='-' or v:sub(19,19)~='-' or v:sub(24,24)~='-' then return false end
 local hex=v:gsub('-','')
 return #hex==32 and hex:match('^%x+$')~=nil
end
local function instanceid(v)
 return type(v)=='string' and #v>=1 and #v<=64 and v:match('^[a-z0-9%-]+$')~=nil
end
local function timestamp(v)
 return type(v)=='number' and v>0 and v<=253402300799999 and v==math.floor(v)
end
local function token(v)
 if type(v)~='string' or #v<22 or #v>128 or not v:match('^[A-Za-z0-9_%-]+$') then return false end
 local mod=#v%4
 if mod==1 then return false end
 if mod==2 then return v:sub(-1):match('^[AQgw]$')~=nil end
 if mod==3 then return v:sub(-1):match('^[AEIMQUYcgkosw048]$')~=nil end
 return true
end
local function read(key)
 local raw = redis.call('GET',key)
 if not raw then return nil end
 local v=cjson.decode(raw)
 if type(v)~='table' or not instanceid(v.instance) or not uuid(v.boot) or not timestamp(v.deadline) then error('invalid stored lease') end
 if key:sub(1,#ip)==ip then
  if key~=ip..v.instance or type(v.address)~='string' or #v.address==0 or type(v.draining)~='boolean' then error('invalid stored instance') end
 else
  if type(v.user)~='string' or #v.user~=37 or v.user:sub(1,1)~='U' or not uuid(v.user:sub(2)) or key~=rp..v.user or not uuid(v.connection) or not token(v.token) or not timestamp(tonumber(v.auth)) or v.deadline>tonumber(v.auth) then error('invalid stored route') end
 end
 return v
end
local function live(v) return v and v.deadline > now end
local function save(key,v)
 redis.call('SET',key,cjson.encode(v),'PX',v.deadline-now)
end
local function instance(id) return read(ip..id) end
local function same(a,b)
 return a.user==b.user and a.instance==b.instance and a.connection==b.connection and a.token==b.token
end
local function validinstance(r)
 local i=instance(r.instance)
 return live(i) and i.boot==r.boot
end
if op=='register' then
 local old=read(KEYS[1])
 if live(old) then
  if old.boot~=p.boot or old.address~=p.address then return err('conflict') end
  p.draining=old.draining
 else p.draining=false end
 p.deadline=now+ittl
 save(KEYS[1],p)
 return answer({instance=p})
elseif op=='renew' then
 local old=read(KEYS[1])
 if not live(old) or old.boot~=p.boot then return err('not_registered') end
 old.draining=old.draining or p.draining
 old.deadline=now+ittl
 save(KEYS[1],old)
 return answer({instance=old})
elseif op=='unregister' then
 local old=read(KEYS[1])
 if not live(old) then return answer({outcome=3}) end
 if old.boot~=p.boot then return answer({outcome=2}) end
 redis.call('DEL',KEYS[1])
 return answer({outcome=1})
elseif op=='connect' then
 local i=read(KEYS[2])
 if not live(i) or i.boot~=p.boot then return err('not_registered') end
 if i.draining then return err('draining') end
 if tonumber(p.auth)-now<=5000 then return err('expired') end
 p.deadline=math.min(now+rttl,tonumber(p.auth))
 local old=read(KEYS[1])
 if not live(old) or tonumber(old.auth)<=now then old=nil end
 save(KEYS[1],p)
 return answer({route=p,previous=old})
elseif op=='check' or op=='heartbeat' then
 local old=read(KEYS[1])
 if not old then return err('expired') end
 if not same(old,p) then return err('superseded') end
 if not live(old) or tonumber(old.auth)<=now then return err('expired') end
 if not validinstance(old) then return err('not_registered') end
 if op=='heartbeat' then
  old.deadline=math.min(now+rttl,tonumber(old.auth))
  save(KEYS[1],old)
 end
 return answer({route=old})
elseif op=='disconnect' then
 local old=read(KEYS[1])
 if not live(old) then return answer({outcome=3}) end
 if not same(old,p) then return answer({outcome=2}) end
 redis.call('DEL',KEYS[1])
 return answer({outcome=1})
elseif op=='lookup' then
 local results={}
 for n,id in ipairs(p.ids) do
  local r=read(KEYS[n])
  if not r then results[n]={id=id,reason=1}
  elseif not live(r) or tonumber(r.auth)<=now then results[n]={id=id,reason=2}
  elseif not validinstance(r) then results[n]={id=id,reason=3}
  else results[n]={id=id,route=r} end
 end
 return answer({results=results})
elseif op=='resolve' then
 local results={}
 for n,id in ipairs(p.ids) do
  local i=read(KEYS[n])
  if not i then results[n]={id=id,reason=1}
  elseif not live(i) then results[n]={id=id,reason=2}
  else results[n]={id=id,instance=i} end
 end
 return answer({results=results})
end
return err('invalid')
`)
