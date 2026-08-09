# WebSocket 生命周期升级说明

## 1. 升级目标

本次升级参考 KamaChat 的 Server/Client 分层，但不逐字段复制它的实现。

旧结构：

~~~text
Controller
→ RegisterUser
→ ReadPump / WritePump
→ 直接访问 onlineUsers
→ ReadPump 内查询 Session、落库和在线转发
~~~

目标结构：

~~~text
Controller
→ Upgrade
→ Client.Start
→ Server.register

Client.read
→ gormservice.SendMessage
→ Server.inbound
→ Server.deliver
→ Client.outbound
→ Client.write
~~~

本轮不增加群聊、Redis、Kafka、消息 ACK 或新的 WebSocket 协议。

## 2. 升级意义

### 2.1 明确连接所有权

一个 Client 代表一条 WebSocket 连接，并显式记录所属 Server。

Client 不再隐式访问可被替换的全局 Server，不会向错误的 Server 提交消息或注销连接。

### 2.2 统一在线连接管理

Server 是本进程在线连接的唯一管理者。

其他包不能直接修改在线 Map，也不能直接向注册、注销和入站 Channel 写数据。

### 2.3 分开入站和出站

~~~text
入站：
WebSocket → Client.read → gormservice.SendMessage → Server.inbound

出站：
Server.deliver → Client.outbound → Client.write → WebSocket
~~~

Client 只需要一个出站队列。KamaChat 的 SendTo 没有保留，因为当前入站消息直接进入 Server 的公共队列。

### 2.4 修复重复连接

当前策略是同一用户只保留最新 Client：

~~~text
旧 Client 注册
→ 新 Client 使用相同 userUUID 注册
→ Server 将 Map 指向新 Client
→ Server 关闭旧 Client
~~~

旧 Client 随后注销时，只有 Map 当前值仍然等于旧 Client 才能删除。比较的是 userUUID 与 Client 身份，而不是只按 userUUID 删除。

### 2.5 隔离慢客户端

Client.outbound 是有界队列。队列满时不能阻塞唯一的 Server 事件循环。

当前策略：

~~~text
outbound 有空间 → 入队
outbound 已满   → 关闭慢 Client
消息已经落库   → 不回滚 MySQL
~~~

## 3. 当前文件结构

~~~text
cmd/my_chat_server/
└── main.go

internal/api/v1/
├── ws_controller.go
└── ws_controller_test.go

internal/https_server/
├── https_server.go
└── auth_middleware.go

internal/dto/wschat/
└── chat.go

internal/service/chatservice/
├── client.go
├── server.go
└── server_test.go

internal/service/gormservice/
├── message_service.go
├── message_service_test.go
└── errors.go

internal/model/
└── message.go

internal/integration/
├── test_helper_test.go
└── websocket_integration_test.go
~~~

## 4. 分层职责与签名

### 4.1 main.go

负责加载配置、初始化 GORM、启动 ChatServer、启动 HTTP Server，以及收到 SIGINT/SIGTERM 后依次关闭 HTTP、ChatServer 和数据库。

~~~go
func main()
func run(ctx context.Context) error
~~~

### 4.2 auth_middleware.go

WsAuth 从 Query Token 或 Authorization Header 读取 Token，验证 JWT，并将 Token 用户写入 user_uuid。

~~~go
func WsAuth(jwtKey []byte) gin.HandlerFunc
~~~

身份完全来自 Token，不再接受冗余 client_id。

### 4.3 ws_controller.go

Controller 负责读取 Gin Context、执行 WebSocket Upgrade、创建并启动 Client。

~~~go
func WsController(c *gin.Context)
~~~

Controller 不访问数据库，不维护在线 Map，不解析聊天消息。websocket.Upgrader 保留在 Controller，chatservice 不依赖 Gin 或 HTTP。

### 4.4 client.go

Client 负责一条 WebSocket 连接：

- 保存连接和所属 Server；
- 保存认证用户 UUID；
- 读取入站 Frame；
- 写出站 Frame；
- 统一关闭连接；
- 通知所属 Server 注销。

~~~go
type Client struct {
    conn      *websocket.Conn
    server    *Server
    userUUID  string
    outbound  chan wschat.Message
    done      chan struct{}
    closeOnce sync.Once
}

func NewClient(
    server *Server,
    conn *websocket.Conn,
    userUUID string,
    outboundBuffer int,
) *Client

func (c *Client) Start() bool
func (c *Client) Close()
func (c *Client) read()
func (c *Client) write()
~~~

read 和 write 是内部 Pump，不对其他包暴露。

### 4.5 server.go

Server 管理本地在线 Client、注册、注销和在线投递。消息持久化由 `Client.read` 调用消息 Service 完成，Server 事件循环不访问数据库。

~~~go
type Server struct {
    clients map[string]*Client
    online  atomic.Int64

    inbound    chan wschat.Message
    register   chan *Client
    unregister chan *Client
    done       chan struct{}
    stopped    chan struct{}
    closeOnce  sync.Once
}

func NewServer(queueSize int) *Server
func (s *Server) Start()
func (s *Server) Register(client *Client) bool
func (s *Server) OnlineCount() int
func (s *Server) Close()

func (s *Server) unregisterClient(client *Client)
func (s *Server) submit(message wschat.Message) bool
func (s *Server) removeClient(client *Client)
func (s *Server) deliver(client *Client, message wschat.Message) bool
~~~

事件 Channel 和在线 Map 全部不导出，防止其他包绕过生命周期方法。`clients` 只由 `Start` 的事件循环访问；`OnlineCount` 读取独立的原子计数；`Close` 等待事件循环完成清理，因此连接表不需要 `RWMutex`。生命周期固定为 `NewServer → go Start → Close`，未启动就关闭或关闭后重新启动都不属于合法用法。

### 4.6 message_service.go

SendMessage 验证用户对和正文、查询 Session、创建 Message，并返回已创建消息。

~~~go
func SendMessage(
    senderUUID string,
    receiverUUID string,
    content string,
) (response.MessageResponse, error)
~~~

错误语义：

- ErrInvalidUserPair：Sender/Receiver 非法或相同；
- ErrInvalidMessageContent：正文非法；
- ErrInvalidSession：双方不存在 Session；
- ErrDatabase：数据库操作失败。

文本消息使用 model.MessageTypeText，不直接使用魔法值 0。

## 5. 完整生命周期

### 5.1 服务启动

~~~text
main
→ 初始化 GORM
→ go ChatServer.Start()
→ 启动 HTTP Server
~~~

### 5.2 建立连接

~~~text
GET /wss?token=...
→ WsAuth
→ Context.user_uuid
→ WsController
→ websocket.Upgrade
→ NewClient(ChatServer, conn, userUUID)
→ Client.Start
→ Server.Register
~~~

### 5.3 注册连接

~~~text
Server.register
→ 查找相同 userUUID 的旧 Client
→ Map 指向新 Client
→ 如果存在旧 Client，关闭旧 Client
~~~

### 5.4 读取消息

~~~text
Client.read
→ conn.ReadJSON
→ 验证 message.SendID == client.userUUID
→ gormservice.SendMessage
→ MySQL
→ server.submit
→ Server.inbound
~~~

Sender 必须与 Token 用户一致。

### 5.5 落库和在线转发

~~~text
Server.inbound
→ Server.deliver(receiver)
~~~

只有数据库成功后才执行在线推送。Receiver 离线时停止实时推送，但数据库消息保留。

### 5.6 写消息

~~~text
Server.deliver
→ Client.outbound
→ Client.write
→ conn.WriteJSON
~~~

Client.write 是该连接唯一 Writer。

### 5.7 关闭连接

~~~text
read/write 返回
→ Client.Close
→ close(done)
→ conn.Close
→ Server.unregisterClient
→ Server.removeClient
~~~

closeOnce 保证两个 Pump 同时退出时只关闭一次。removeClient 只有在 Map 当前值仍等于该 Client 时才删除。

### 5.8 进程退出

~~~text
SIGINT/SIGTERM
→ http.Server.Shutdown
→ ChatServer.Close
→ close(Server.done)
→ Server.Start 清空 clients 并关闭全部 Client
→ close(Server.stopped)
→ dao.CloseGorm
~~~

## 6. 命名规则

| 名称 | 含义 |
|---|---|
| register | 注册 WebSocket Client，不表示用户登录 |
| unregister | 注销 WebSocket Client，不表示用户登出 |
| inbound | 从 WebSocket Client 进入 Server 的消息 |
| outbound | 从 Server 发往某个 WebSocket Client 的消息 |
| OnlineCount | 当前实例在线 Client 数量 |
| DefaultQueueSize | 队列默认容量 |
| SendID/ReceiveID | Go Initialism 使用 ID，不使用 Id |

已经删除：

- SendTo；
- SendBack；
- Login；
- Logout；
- Transmit；
- ClientLogout；
- client_id；
- 包外可写的 Clients Map 和事件 Channel。

## 7. 与 KamaChat 的对应关系

| KamaChat | Fable |
|---|---|
| Server.Clients | 私有 Server.clients |
| Server.Login | 私有 Server.register |
| Server.Logout | 私有 Server.unregister |
| Server.Transmit | 私有 Server.inbound |
| Client.SendBack | 私有 Client.outbound |
| Client.SendTo | 删除 |
| Client.Read | 私有 Client.read |
| Client.Write | 私有 Client.write |
| NewClientInit 接收 Gin Context | 不复制；Upgrade 留在 Controller |

参考位置：

- ZChat/internal/service/chat/server.go；
- ZChat/internal/service/chat/client.go；
- ZChat/api/v1/ws_controller.go；
- ZChat/cmd/kama_chat_server/main.go。

ZChat 使用 GPLv3。Fable 只参考职责和流程，自行实现代码，不逐行复制。

## 8. MCP 边界

MCP 不走 WebSocket，也不进入 Server.inbound。

~~~text
WebSocket：
Client.read → gormservice.SendMessage → Server.inbound

MCP：
MCP HTTP → Tool → aiservice 权限检查 → gormservice.SendMessage
~~~

如果 MCP 消息落库后需要通知在线 Receiver，可以调用单独的实时投递入口。这是使用 WebSocket 通知结果，不表示 MCP 请求通过 WebSocket。

## 9. Redis 扩展边界

本地连接继续由 Server.clients 和 Client.outbound 管理。

Redis 未来负责：

- userUUID 到 serverID 的在线路由；
- Presence TTL；
- 跨实例 Pub/Sub。

~~~text
消息落库
→ Redis 查询 Receiver 所在 serverID
→ Pub/Sub 通知目标 Server
→ 目标 Server.deliver
→ Client.outbound
~~~

Redis 不保存 websocket.Conn，也不替代 Client.outbound。

## 10. 测试要求

Server 单元测试：

- 初始化私有状态和 Channel；
- 注册、注销；
- 同 UUID 新连接替换旧连接；
- 旧连接注销不会删除新连接；
- 离线用户查询；
- outbound 满时关闭慢 Client；
- 慢 Client 不阻塞后续注册。

Message Service 单元测试：

- 合法 Session 成功落库；
- Session 不存在；
- Sender/Receiver 非法；
- 正文非法；
- 数据库失败。

联合测试：

- 两个在线用户双向发送；
- 消息写入 MySQL；
- Receiver 离线时仍然落库；
- Receiver 离线不导致 Sender 断开；
- 缺少 Token 无法 Upgrade；
- 连接关闭后 OnlineCount 恢复；
- 测试 URL 不携带 client_id。

## 11. 完成标准

- chatservice 不依赖 Gin；
- Client 只有一个出站队列；
- Client 显式绑定所属 Server；
- Server 状态字段不导出；
- 连接事件使用 register/unregister；
- 消息方向使用 inbound/outbound；
- 旧连接不能删除新连接；
- 慢客户端不能阻塞中心事件循环；
- 身份只来自 Token；
- 消息错误语义准确；
- 没有消息类型魔法值；
- MCP、WebSocket 和 Redis 边界清晰；
- 进程退出会关闭 HTTP、WebSocket 和数据库资源；
- 单元测试、静态检查和可用的联合测试通过。
