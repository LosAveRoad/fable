# MVP0 我的设计：缩小版 KamaChat

## 1. 设计目标

本阶段复刻 KamaChat 的项目结构、包名、结构体命名、DTO 命名、路由命名和主要调用链，但只实现最小单聊闭环。

```text
注册/登录
→ 获取用户信息
→ 打开单聊会话
→ 轮询会话列表
→ WebSocket 发送文本消息
→ MySQL 保存消息
→ 查询历史消息
```

本阶段不实现：联系人申请、群聊、Redis、Kafka、短信登录、管理员、文件消息、头像上传、音视频和 AI/MCP。

## 2. 项目结构

目录与 KamaChat 保持一致，但只在功能做到时手动创建需要的文件，不提前生成完整骨架。

```typescript
api/v1/ // 实现具体的handler
├── controller.go
├── user_info_controller.go
├── session_controller.go
├── message_controller.go
└── ws_controller.go

cmd/kama_chat_server/
└── main.go // 启动后端服务器

configs/
└── config.toml

internal/
├── config/config.go
├── dao/gorm.go
├── dto/request/
├── dto/respond/
├── https_server/ //配置路由&实现中间件
│   ├── https_server.go
│   └── auth_middleware.go
├── model/ // gorm 数据结构
│   ├── user_info.go
│   ├── session.go
│   └── message.go
└── service/ //业务逻辑
    ├── chat/
    │   ├── client.go
    │   └── server.go
    └── gorm/
        ├── user_info_service.go
        ├── session_service.go
        └── message_service.go

pkg/
├── constants/constants.go
├── enum/message/
├── util/random/random_int.go
└── zlog/logger.go

web/chat-server/
└── src/
    ├── router/index.js
    ├── store/index.js
    └── views/
        ├── access/Login.vue
        ├── access/Register.vue
        ├── chat/session/SessionList.vue
        └── chat/contact/ContactChat.vue
```

## 3. 调用链

HTTP 请求保持 KamaChat 的分层：

```text
Vue
→ internal/https_server 路由
→ api/v1 Controller
→ internal/service/gorm Service
→ internal/dao.GormDB
→ internal/model
→ MySQL
```

实时消息保持 KamaChat 的分层：

```text
ContactChat.vue
→ GET /wss 升级 WebSocket
→ api/v1.WsLogin
→ chat.NewClientInit
→ Client.Read
→ Server.Transmit
→ 消息落库
→ Client.Write
→ ContactChat.vue
```

## 4. REST API

接口路径和 Controller 命名与 KamaChat 一致。

| Method | Path                          | Controller           | 鉴权 | 用途                       |
| ------ | ----------------------------- | -------------------- | ---- | -------------------------- |
| POST   | `/register`                   | `Register`           | 否   | 注册用户                   |
| POST   | `/login`                      | `Login`              | 否   | 登录并签发 Token           |
| POST   | `/user/getUserInfo`           | `GetUserInfo`        | 是   | 获取指定用户的基础资料     |
| POST   | `/session/openSession`        | `OpenSession`        | 是   | 创建或取得单聊会话         |
| POST   | `/session/getUserSessionList` | `GetUserSessionList` | 是   | 获取当前用户会话列表       |
| POST   | `/message/getMessageList`     | `GetMessageList`     | 是   | 获取两名用户之间的历史消息 |
| GET    | `/wss`                        | `WsLogin`            | 是   | 建立 WebSocket             |

本阶段保持 KamaChat 的 POST 路由风格，不改成另一套 RESTful 命名。后续复盘时可以讨论 GET/资源式路由的取舍。

### 统一响应

沿用 KamaChat 的 `JsonBack` 结构：

```json
{
  "code": 200,
  "message": "登录成功",
  "data": {}
}
```

改进点：HTTP 状态码应与结果对应，参数错误、未认证、禁止访问和系统错误不能全部返回 HTTP 200。

## 5. Request/Respond DTO

命名沿用 KamaChat，只保留当前功能需要的字段。

### RegisterRequest

```text
telephone
password
nickname
```

不保留 `sms_code`，因为 MVP0 不做短信注册。

### LoginRequest

```text
telephone
password
```

### LoginRespond

```text
uuid
nickname
telephone
avatar
status
token
```

`token` 只存在于登录响应，不写入 `user_info` 表。

### GetUserInfoRequest

```text
uuid
```

### OpenSessionRequest

```text
send_id
receive_id
```

为了和 KamaChat DTO 一致保留 `send_id`，但后端必须检查它与 Token 中的用户一致，不能直接信任请求体。

### OwnlistRequest

```text
owner_id
```

为了和 KamaChat 一致保留 `owner_id`，但实际当前用户以鉴权中间件写入的身份为准。

### GetMessageListRequest

```text
user_one_id
user_two_id
```

后端必须确认当前用户是其中一方。

### ChatMessageRequest

```text
session_id
type
content
send_id
send_name
send_avatar
receive_id
```

MVP0 的 `type` 只允许文本类型。`url`、文件字段和 `av_data` 暂不加入。

### UserSessionListRespond

```text
session_id
avatar
user_id
user_name
```

### GetMessageListRespond

```text
send_id
send_name
send_avatar
receive_id
type
content
created_at
```

## 6. GORM 模型

结构体和表名与 KamaChat 保持一致，但字段少于 KamaChat。

### UserInfo / `user_info`

```text
Id           int64
Uuid         string
Nickname     string
Telephone    string
Password     string
Avatar       string
Status       int8
CreatedAt    time.Time
LastOnlineAt sql.NullTime
LastOfflineAt sql.NullTime
```

约束：

- `Uuid` 唯一。
- `Telephone` 唯一。
- `Password` 字段保存密码哈希，不保存明文。
- Token 不属于 UserInfo，不写入数据库。

### Session / `session`

```text
Id            int64
Uuid          string
SendId        string
ReceiveId     string
ReceiveName   string
Avatar        string
LastMessage   string
LastMessageAt sql.NullTime
CreatedAt     time.Time
DeletedAt     gorm.DeletedAt
```

沿用 KamaChat 的会话展示模型。`SendId` 表示该会话列表所属用户，`ReceiveId` 表示聊天对象。

打开会话时需要避免重复创建同一所属用户与接收者的记录，至少对 `(send_id, receive_id)` 建立唯一约束。

### Message / `message`

```text
Id         int64
Uuid       string
SessionId  string
Type       int8
Content    string
SendId     string
SendName   string
SendAvatar string
ReceiveId  string
Status     int8
CreatedAt  time.Time
SendAt     sql.NullTime
```

约束：

- `Uuid` 唯一。
- `SessionId` 建索引。
- `SendId`、`ReceiveId` 建查询索引。
- `Type` 在 MVP0 只允许 Text。
- `Status` 第一版沿用 KamaChat 的 Unsent/Sent。

暂不加入文件、URL、文件大小和 `AVdata` 字段。

## 7. Gin 鉴权设计

新增 `internal/https_server/auth_middleware.go`，仍属于 KamaChat 已有的 `https_server` 包，不增加新的顶层结构。

流程：

```text
Authorization: Bearer <token>
→ 校验 Token 签名和有效期
→ 解析 uuid
→ 查询或确认用户状态
→ c.Set("uuid", uuid)
→ c.Next()
```

路由分组：

```text
公开：/register、/login
受保护：/user/*、/session/*、/message/*
WebSocket：/wss 在 Upgrade 前完成相同身份校验
```

浏览器原生 WebSocket 不方便自定义 Authorization Header。MVP0 可以使用：

```text
/wss?client_id=Uxxx&token=xxx
```

保留 KamaChat 的 `client_id` 命名，但服务端必须验证 `client_id` 与 Token 中的 uuid 相同。后续可以改成安全 Cookie 或短期 WebSocket Ticket。

## 8. WebSocket 设计

保持 KamaChat 的核心结构：

```text
Client
├── Conn
├── Uuid
├── SendTo
└── SendBack

Server
├── Clients
├── Transmit
├── Login
└── Logout
```

每个 Client：

- 一个 `Read` goroutine 读取前端消息。
- 一个 `Write` goroutine 向前端写消息。
- 所有写操作都经过 `Write`，避免并发写同一个 WebSocket。

MVP0 只实现：登录连接、退出清理、文本单聊、消息落库、发送方回显和在线接收方推送。

## 9. 客户端网页设计

页面命名与 KamaChat 一致：

- `Register.vue`：注册。
- `Login.vue`：登录并保存 Token 和用户信息。
- `SessionList.vue`：显示会话列表。
- `ContactChat.vue`：显示历史消息和 WebSocket 实时消息。

### SessionList 轮询

第一版每 5 秒调用：

```text
POST /session/getUserSessionList
```

要求：

- 页面挂载时立即请求一次，再启动定时器。
- 页面卸载时清理定时器。
- 上一次请求未完成时不重复发起下一次。
- 401 时停止轮询并跳转登录。
- 轮询只负责更新会话列表，不负责实时聊天消息。

### ContactChat

进入会话时：

1. 调用 `/message/getMessageList` 获取历史消息。
2. 使用 `/wss` 建立或复用 WebSocket。
3. 发送 `ChatMessageRequest`。
4. 收到消息后追加到当前消息列表。

## 10. 推荐实现顺序

1. 手动初始化 Go Module 和 KamaChat 同名目录。
2. 完成 `config.toml`、`config.go`、`gorm.go` 和 `main.go`。
3. 完成 `JsonBack` 和 Gin 路由。
4. 完成 `UserInfo`、Register、Login 和 Token 鉴权。
5. 完成 GetUserInfo。
6. 完成 `Session`、OpenSession、GetUserSessionList。
7. 完成 `Message` 和 GetMessageList。
8. 前端完成注册、登录和 SessionList 轮询。
9. 完成 WebSocket Client/Server 和文本单聊。
10. 前端完成 ContactChat 历史消息与实时消息。
11. 测试离线消息、伪造身份、重复会话和连接断开。

## 11. KamaChat 参考文件

只参考当前功能对应文件：

- 工程：`go.mod`、`configs/config.toml`、`internal/config/config.go`、`cmd/kama_chat_server/main.go`。
- 数据库：`internal/dao/gorm.go`。
- 路由：`internal/https_server/https_server.go`、`api/v1/controller.go`。
- 用户：`user_info.go`、`user_info_controller.go`、`user_info_service.go` 和登录/注册 DTO。
- 会话：`session.go`、`session_controller.go`、`session_service.go` 和用户会话 DTO。
- 消息：`message.go`、`message_controller.go`、`message_service.go` 和单聊消息 DTO。
- WebSocket：`ws_controller.go`、`chat/client.go`、`chat/server.go` 的 Text + User 分支。
- 前端：`Login.vue`、`Register.vue`、`SessionList.vue`、`ContactChat.vue`。

不要参考：联系人、群聊、Redis、Kafka、文件上传、短信、管理员和 AV 分支。

## 12. MVP0 验收标准

- 用户可以注册、登录并取得 Token。
- 未登录不能访问用户、会话和消息接口。
- 请求体伪造 `send_id` 或 `owner_id` 不能冒充其他用户。
- 两个用户可以打开单聊会话，重复打开不会创建重复会话。
- SessionList 每 5 秒更新，并在页面卸载时停止请求。
- 两个在线用户可以通过 WebSocket 发送文本消息。
- 消息成功写入 MySQL 后才返回发送成功。
- 接收方离线时消息不会丢失，进入会话后能通过历史接口读取。
- 断开 WebSocket 后，Server 能清理对应 Client。
- 群聊、Redis、Kafka、文件和音视频代码没有提前出现。
