# Fable Chat 面试准备

本文档收集 Fable Chat 开发过程中可能被追问的设计问题。回答重点不是背诵某个框架的写法，而是说明：最初方案解决了什么问题、为什么继续演进、新方案付出了什么代价，以及代码当前是否真正满足这套设计。

## 使用方式

每个问题分为三部分：

- **30 秒回答**：先给结论，适合面试现场直接回答。
- **深入追问**：面试官继续追问时展开。
- **避免误答**：容易说得不严谨或与代码不一致的地方。

## 当前代码状态

当前 `ChatServer` 已经完成连接表的单一所有者改造：

1. `clients` 只由 `Server.Start` 所在的 goroutine 访问，不再使用 `sync.RWMutex`。
2. `OnlineCount` 读取独立的 `atomic.Int64`，不会从事件循环外读取 map。
3. `Close` 只发送关闭信号；事件循环清理 `clients`、关闭全部 Client，并通过 `stopped` 通知调用方清理完成。

Server 生命周期固定为 `NewServer → go Start → Close`。`Start` 只允许调用一次，因此不使用 `startOnce` 掩盖重复启动；未启动就关闭、关闭后重新启动都属于调用方错误。

目前仍有一个后续优化点：`gormservice.SendMessage` 还在主事件循环中执行，数据库延迟会阻塞后续注册、注销和消息路由。因此可以说明连接表已经采用单一所有者模型，但暂时不能声称事件循环内只有 O(1) 的内存操作。

---

## 1. 为什么不用 `gin.Engine.Run`，而是显式创建 `http.Server`

### 30 秒回答

`gin.Engine.Run` 是对 `http.ListenAndServe` 的便捷封装，适合只有 Gin 的简单服务。项目现在需要在同一个端口挂载 Gin 和 MCP，因此最外层 Handler 是 `http.ServeMux`，Gin 只是其中一个子 Handler。显式持有 `http.Server` 还可以调用 `Shutdown`，统一管理 HTTP、WebSocket 和数据库的退出生命周期。

### 深入追问

当前路由关系是：

```text
http.Server
└── http.ServeMux
    ├── /mcp → MCP Handler
    └── /    → Gin Engine
```

如果直接调用：

```go
ginHandler.Run(":8080")
```

真正启动的是以 Gin 为根 Handler 的 HTTP Server，挂在外层 `ServeMux` 上的 MCP Handler 不会生效。

显式创建 Server：

```go
server := &http.Server{
    Addr:    ":8080",
    Handler: root,
}
```

不仅解决组合 Handler 的问题，还保留了 `Shutdown`、超时和底层连接管理能力。

### 避免误答

- 不是 Gin 不能挂 MCP，而是当前项目选择让标准库 `ServeMux` 作为统一根路由。
- 不是使用 `http.Server` 后就不再使用 Gin；Gin 仍然实现了 `http.Handler`。
- 只说“这样更高级”没有意义，要落到多 Handler 组合和优雅关闭两个具体需求。

---

## 2. 为什么在 goroutine 中运行 `ListenAndServe`

### 30 秒回答

`ListenAndServe` 会一直阻塞到服务器退出。如果在主 goroutine 直接调用，程序无法同时等待操作系统退出信号。把它放入后台 goroutine，并通过带一个缓冲位的 `errCh` 返回结果，主 goroutine 就可以同时等待服务器错误和上层 context 取消。

### 深入追问

```go
errCh := make(chan error, 1)
go func() {
    errCh <- server.ListenAndServe()
}()
```

主 goroutine 随后等待两个事件：

```go
select {
case err := <-errCh:
    // Server 自己退出，例如监听端口失败。
case <-ctx.Done():
    // 收到 SIGINT、SIGTERM，开始主动关闭。
}
```

`errCh` 使用容量 1，是因为 Server 只会返回一次退出结果；即使接收方已经进入其他退出路径，发送 goroutine 也可以完成一次写入后退出。

### 避免误答

- 应该说“启动一个 goroutine”，不是“启动一个 Go runtime”。Go runtime 是调度所有 goroutine 的运行时系统。
- goroutine 只让 `ListenAndServe` 不阻塞主控制流，本身不等于优雅关闭。
- `ListenAndServe` 返回 `http.ErrServerClosed` 通常代表主动关闭，不应作为异常记录。

---

## 3. `Shutdown` 为什么需要一个新的 5 秒 context

### 30 秒回答

上层 context 取消只表示“应用应该退出”，不会自动停止 HTTP Server。`Shutdown` 会停止接收新请求，并等待正在执行的请求结束。因为上层 context 此时已经取消，不能直接拿它做关闭期限，所以基于 `context.Background` 创建一个独立的 5 秒 context，给服务器有限的收尾时间，同时避免请求卡住导致进程永远无法退出。

### 深入追问

```go
shutdownCtx, cancel := context.WithTimeout(
    context.Background(),
    5*time.Second,
)
defer cancel()

return server.Shutdown(shutdownCtx)
```

关闭顺序是：

```text
上层 ctx 取消
    ↓
停止接收新 HTTP 请求
    ↓
等待正在执行的 Handler
    ├── 5 秒内完成 → Shutdown 返回 nil
    └── 超过 5 秒   → 返回 context deadline exceeded
```

`cancel` 用于在 Server 提前关闭时释放 timeout context 内部的定时器资源。

### 为什么不直接 `defer server.Shutdown(ctx)`

可以把关闭过程封装在 defer 中，但不能直接使用已经取消的上层 `ctx`。否则 `Shutdown` 几乎没有机会等待现有请求结束。另外，将 `Shutdown` 显式放在返回路径上，更容易把关闭错误交给调用方。

### 避免误答

- `Shutdown` 不是等待 5 秒后再关闭，而是最多等待 5 秒。
- `Shutdown` 不会主动关闭被 Hijack 的 WebSocket 长连接，WebSocket Client/Server 必须有自己的关闭流程。
- `Close` 会直接关闭连接，语义不同于等待请求完成的 `Shutdown`。

---

## 4. 为什么把在线连接表交给单一 Server 管理

### 30 秒回答

第一版使用共享在线用户表和 `RWMutex`，并发上可以正确运行，但状态操作权分散在多个 Client goroutine 中，每增加一个操作都要遵守同一套加锁规则。后续将连接表收拢到 ChatServer，由一个事件循环通过注册、注销和路由事件管理，是从“共享状态加锁”演进到“单一所有者”模型。主要收益是状态所有权和事件顺序更清晰，而不是简单地宣称性能更高。

### 演进过程

第一版：

```text
Client A ─┐
Client B ─┼─→ RWMutex → clients map
Client C ─┘
```

目标设计：

```text
Client A ─┐
Client B ─┼─→ event channel → Server goroutine → clients map
Client C ─┘
```

Client 不再决定连接表如何修改，只提交事件：

- 注册连接；
- 注销连接；
- 请求路由消息。

Server 统一决定：

- 同一用户新旧连接如何替换；
- 旧连接退出时是否还能删除当前连接；
- 接收方离线时如何处理；
- 慢消费者队列满时如何处理；
- Server 关闭时如何清理连接。

### 为什么不是直接调用带锁方法

下面的设计同样可以正确：

```go
server.Register(client)
server.Unregister(client)
server.Deliver(message)
```

每个方法内部通过 mutex 保护 map。选择事件循环不是因为 mutex 错了，而是连接注册、注销和路由天然表现为一组生命周期事件。将事件交给一个所有者串行处理，可以减少锁规则向不同调用方扩散，并为这些事件建立统一顺序。

### 避免误答

- 不要说“channel 没有锁”。channel 自身也有同步成本；这里是业务代码不再显式共享并加锁连接表。
- 不要说“单 goroutine 一定比 mutex 快”。选择它主要是为了状态所有权和可维护性。
- 不要把 `Server` 设计成公开 `clients` 字段。Client 应提交事件，而不是直接修改 map。
- 当前连接表已经完成单一所有者改造；数据库持久化仍需从主事件循环的阻塞路径中拆出。

---

## 5. `inbound` channel 是什么，为什么叫这个名字

### 30 秒回答

`inbound` 表示相对于 ChatServer 而言进入 Server 的消息。Client 从 WebSocket 读出消息后写入该 channel，Server 再查询接收方并投递到接收方 Client 的 `outbound`。这个名字方向上正确，但职责不够明确；`routeQueue` 更能说明它是等待 Server 路由的消息队列。

### 消息路径

```text
发送方 WebSocket
    ↓ Client.read
Server routeQueue
    ↓ 查找接收方
接收方 Client.outbound
    ↓ Client.write
接收方 WebSocket
```

两个队列的边界不同：

- `Server.routeQueue`：多个 Client 提交给 Server、等待路由的消息。
- `Client.outbound`：已经分配给某个 Client、等待写入 WebSocket 的消息。

### 命名选择

```go
inbound    chan wschat.Message // 方向正确，但依赖阅读者先确定参照物
messages   chan wschat.Message // 简单，但职责仍然宽泛
routeQueue chan wschat.Message // 最直接地表达用途
```

### 避免误答

- 它不是数据库消息队列，也不是跨实例可靠消息队列。
- 有缓冲不代表消息可靠；进程退出时，内存队列中的消息仍然可能丢失。
- 如果消息持久化成功后才允许路由，需要明确持久化与路由的先后关系和失败语义。

---

## 6. 所有连接都由一个 Server goroutine 管理，能扩展吗

### 30 秒回答

单一 goroutine 只应该管理连接表和路由决策，不负责所有用户的网络读写。每个 Client 仍有独立的读写 goroutine。只要 Server 循环只做 O(1) 的 map 操作和非阻塞队列投递，当前规模可以支撑；如果压测证明它成为瓶颈，可以按 User UUID 对连接表分片，再通过 Redis 实现跨实例路由。

### 单实例内的并发关系

```text
Client A：read goroutine + write goroutine
Client B：read goroutine + write goroutine
Client C：read goroutine + write goroutine

Server：连接状态和路由事件循环
```

Server 循环不应该同步执行：

- 数据库读写；
- Redis 网络请求；
- WebSocket `WriteJSON`；
- 任何不受控的外部 RPC；
- 长时间业务计算。

否则一个慢操作会形成队头阻塞，影响所有连接事件。

### 单机分片

当单循环成为瓶颈时，可以根据 User UUID 选择 shard：

```text
hash(userUUID) % shardCount
```

每个 shard 独占自己的连接表和事件循环。不同用户可以并行处理，同一个用户仍然落到同一 shard，从而保留该用户生命周期事件的顺序。

### 多实例与 Redis

每个 ChatServer 实例只管理本机 WebSocket 连接：

```text
用户 A → Server 1
用户 B → Server 2

Server 1 → Redis Pub/Sub → Server 2 → 用户 B
```

Redis 解决跨实例消息发现和转发，不能替代每个实例内部的本地连接表。

### 避免误答

- 不要把“管理所有连接”说成“执行所有连接的网络 I/O”。
- mutex 方案同样可能竞争同一把锁，并不会自动获得无限扩展性。
- 是否需要分片应由压测和指标决定，当前项目不应为了假设的规模提前增加复杂度。

---

## 7. 为什么接收方 Client 还需要独立的 `outbound` channel

### 30 秒回答

Server 只负责决定消息应该交给哪个 Client，不应该直接执行 `WriteJSON`。每个 Client 的 `outbound` 将路由和网络写入解耦，并确保同一 WebSocket 连接只有一个 write goroutine 顺序写入，避免慢连接阻塞整个 Server，也避免多个 goroutine 并发写同一连接。

### 深入追问

```text
Server 路由
    ↓ 非阻塞投递
Client.outbound
    ↓ 单一 write goroutine
WebSocket connection
```

当 `outbound` 队列已满时，Server 必须有明确的慢消费者策略，例如：

- 断开该 Client；
- 丢弃消息并记录指标；
- 将可靠投递交给持久化和离线消息机制。

不能让 Server 事件循环无限等待某个 Client 的队列出现空间。

### 避免误答

- `outbound` 不是为了增加 goroutine 数量，而是为了建立单连接写入所有权和背压边界。
- 内存 channel 不能保证进程崩溃后的消息可靠性。
- “队列满就断开”是一种策略，不是唯一答案，需要结合产品语义说明。

---

## 8. 为什么旧连接注销时不能只按 UUID 删除

### 30 秒回答

同一用户可能快速重连。新连接注册后，旧连接的 read/write goroutine 才退出。如果旧连接按照 UUID 无条件删除，会把刚注册的新连接从在线表中删掉。因此注销事件必须携带具体 Client，并确认 map 中的当前指针仍然等于这个旧 Client 后才能删除。

### 竞态时序

```text
旧连接 A 在线
    ↓
新连接 B 注册，clients[user] = B
    ↓
旧连接 A 延迟退出
    ↓
如果直接 delete(clients, user)，B 被误删
```

正确判断是：

```go
current, ok := clients[client.userUUID]
if ok && current == client {
    delete(clients, client.userUUID)
}
```

### 避免误答

- UUID 能唯一标识用户，但不能唯一标识该用户某一次连接。
- 单一事件循环能保证事件串行执行，但不能自动消除这个业务时序问题；仍然必须比较具体连接身份。

---

## 9. MCP 为什么不复用 WebSocket 生命周期

### 30 秒回答

MCP 和 IM WebSocket 是两条不同的传输和生命周期链路。MCP 通过 HTTP Handler 接收工具调用，WebSocket 用于长期聊天连接。两者可以复用认证、会话权限和消息 Service，但不应该让 MCP 为了发送消息伪装成一个 WebSocket Client。

### 深入追问

合理的复用边界是业务层：

```text
WebSocket Handler ─┐
                   ├→ message/session service → database
MCP Tool Handler ──┘
```

如果 MCP 最终实现发送消息，它可以调用同一业务 Service 完成鉴权和持久化；在线推送再交给 ChatServer 或后续 Redis 路由。这样 AI 功能与 IM 传输层保持解耦。

### 避免误答

- “复用业务 Service”不等于“复用 WebSocket 连接”。
- MCP 的认证和会话授权必须在工具调用边界重新校验，不能因为内部调用就跳过权限检查。

---

## 一分钟项目设计总结

> Fable Chat 的 HTTP 层使用标准库 `ServeMux` 统一挂载 Gin 和 MCP，并显式持有 `http.Server` 完成信号驱动的优雅关闭。WebSocket 部分将每个连接封装为 Client，由独立 read/write goroutine 负责网络 I/O，ChatServer 负责在线连接生命周期和消息路由。
>
> 在线连接表最初采用共享 map 加 `RWMutex`，随后演进为单一所有者事件循环：Client 只提交注册、注销和路由事件，由 Server 串行修改连接表；在线人数使用原子计数，关闭清理由事件循环完成。这次演进的核心不是宣称 channel 比 mutex 更快，而是明确状态所有权、统一连接生命周期顺序。事件循环还需要进一步移出数据库持久化等阻塞 I/O；规模增长后可以按 UUID 分片，并通过 Redis 支持跨实例路由。
>
> MCP 与 WebSocket 保持传输层解耦，只复用认证、会话授权和消息业务 Service，避免 AI 功能侵入 IM 连接生命周期。

## 后续补充规则

每次新增面试题时，至少记录：

1. 当前代码是怎么做的。
2. 为什么没有选择更简单的方案。
3. 新设计解决了什么具体问题。
4. 新设计带来了什么代价。
5. 如果规模继续增长，下一步如何演进。
