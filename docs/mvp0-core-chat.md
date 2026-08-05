# MVP0：用户与可靠单聊

## 1. 阶段目标

使用 Go、Gin、GORM、MySQL 和 WebSocket 完成一个单机、可持久化、可鉴权的单聊系统。完成后即使 Redis、Kafka、MCP 和 WebRTC 都不存在，两个用户也能可靠交换文本消息并查询历史记录。

代码组织尽量沿用 KamaChat：`cmd` 负责入口，`api/v1` 负责 Controller，`internal/model` 负责 GORM 模型，`internal/service/gorm` 负责业务与数据库操作，`internal/service/chat` 负责 WebSocket，`internal/https_server` 负责路由注册。所有目录和文件由你按实现进度手动创建。

## 2. 必须完成

### 工程基础

- 配置从环境变量或本地配置文件读取，敏感值不提交 Git。
- Gin 路由、统一响应、错误分类、请求日志和 panic 恢复。
- GORM 连接 MySQL，设置连接池，提供健康检查。
- 服务接收退出信号后停止接收新请求并释放资源。

### 用户与鉴权

- 用户注册、登录、查询自己的资料。
- 用户名或邮箱唯一。
- 密码使用 bcrypt/argon2 等专用密码算法保存，不保存明文或可逆密文。
- 登录返回短期 Access Token；MVP0 可暂不实现 Refresh Token。
- HTTP 和 WebSocket 使用同一套身份语义。
- WebSocket 建连时验证 Token，并由服务端得到 userID。

### 会话与消息

- 创建或获取两个用户之间的唯一单聊会话。
- 发送文本消息。
- 消息先成功持久化，再向在线接收方推送。
- 发送方收到服务端确认，其中包含服务端消息 ID 和会话序号。
- 接收方不在线时，消息仍保留在 MySQL。
- 历史消息使用游标分页，不使用无限制全量查询。
- 接收方可以提交 delivered/read ACK。
- 客户端重试同一消息时不能重复入库。

### 最小前端

- 注册页、登录页、用户搜索/选择页、单聊页。
- 展示连接中、已连接、断开和重连状态。
- 消息至少展示 sending、sent、delivered、read、failed。
- 浏览器刷新后可以重新加载历史消息。

## 3. 明确不做

- 好友申请、拉黑、群聊。
- Redis 缓存与分布式在线状态。
- Kafka、多实例消息路由、K8s。
- 文件消息、音视频、AI 和 MCP。
- 复杂管理后台、短信登录和多端同步。

## 4. 与 KamaChat 对齐的数据模型

### user_info

关键字段：

```text
id, uuid, nickname, telephone/email, password_hash,
nickname, avatar_url, status, created_at, updated_at
```

约束提示：

- 数据库自增 id 用于内部关联，uuid 用于业务和 API 暴露。
- telephone/email 根据你的登录方案建立唯一约束。
- password_hash 长度为算法输出预留空间，不照搬 KamaChat 的短密码字段。

### session

关键字段：

```text
id, uuid, send_id, receive_id, receive_name,
avatar, last_message, last_message_at, created_at, deleted_at
```

先理解 KamaChat 为什么为每个用户维护面向接收方的会话展示数据。你可以保持这一模型，暂时不要提前抽象统一 conversation/group conversation。

### message

关键字段：

```text
id, uuid, client_message_id, session_id, type, content,
send_id, receive_id, status, sequence, created_at, send_at
```

关键约束：

- `(sender_id, client_message_id)` 唯一，用于客户端重试幂等。
- `(session_id, sequence)` 唯一，用于稳定排序。
- 历史查询索引优先考虑 `(session_id, sequence)`。

KamaChat 的 `message` 还包含 `send_name`、`send_avatar`、文件和 AV 字段。MVP0 可以保留字段以保持兼容，但只实现文本消息；你需要记录这些冗余字段的好处和一致性代价。

## 5. HTTP 契约草案

建议接口：

```text
POST /register
POST /login
POST /user/getUserInfo
POST /session/openSession
POST /session/getUserSessionList
POST /message/getMessageList
GET  /wss
GET  /health/live
GET  /health/ready
```

为贴近 KamaChat，业务接口命名先保持一致；你可以额外使用正确 HTTP 状态码和稳定错误码改进其响应语义。

统一错误格式示例：

```json
{
  "error": {
    "code": "USERNAME_ALREADY_EXISTS",
    "message": "username already exists",
    "request_id": "..."
  }
}
```

## 6. WebSocket 协议草案

所有帧使用信封结构：

```json
{
  "type": "message.send",
  "request_id": "req-001",
  "data": {}
}
```

最小事件集合：

```text
client → server
message.send
message.delivered
message.read
ping

server → client
connection.ready
message.accepted
message.created
message.status_changed
error
pong
```

`message.send` 不接收可信 sender_id。发送者必须来自当前 WebSocket Client 的鉴权身份。

## 7. 推荐实现顺序

1. 亲手执行 `go mod init`，创建 `cmd/kama_chat_server/main.go`。
2. 手动创建 `configs`、`internal/config`、`internal/dao`，完成配置和 GORM/MySQL。
3. 创建 `api/v1`、`internal/https_server`，完成路由与统一响应。
4. 创建用户 model、DTO、Controller、Service，贯通注册。
5. 完成登录与 HTTP 鉴权。
6. 创建 session model、DTO、Controller、Service，贯通打开会话与会话列表。
7. 创建 message model 与历史消息查询。
8. 创建 `internal/service/chat/client.go` 和 `server.go`，完成 WebSocket 鉴权升级。
9. 实现 Client 的单读单写和 Server 的在线用户表。
10. 实现文本消息持久化、发送方确认和在线接收方推送。
11. 完成历史分页、离线恢复、delivered/read ACK。
12. 完成断线、重连、并发和故障测试。

## 8. 本阶段参考 KamaChat 文件

严格按下面顺序参考，不要先打开 MVP1/MVP3 文件：

1. 工程入口：`go.mod`、`configs/config.toml`、`internal/config/config.go`、`cmd/kama_chat_server/main.go`。
2. 数据库与通用层：`internal/dao/gorm.go`、`pkg/constants/constants.go`、`pkg/util/random/random_int.go`、`pkg/zlog/logger.go`。
3. HTTP 路由：`internal/https_server/https_server.go`、`api/v1/controller.go`。
4. 用户链路：`internal/model/user_info.go`、`api/v1/user_info_controller.go`、`internal/service/gorm/user_info_service.go`、登录/注册相关 request/respond DTO。
5. 会话链路：`internal/model/session.go`、`api/v1/session_controller.go`、`internal/service/gorm/session_service.go`、open/get/delete session DTO。
6. 消息链路：`internal/model/message.go`、`api/v1/message_controller.go`、`internal/service/gorm/message_service.go`、message/get_message_list DTO 和消息枚举。
7. WebSocket：`api/v1/ws_controller.go`、`internal/service/chat/client.go`、`internal/service/chat/server.go`。
8. 前端启动与登录：`web/chat-server/package.json`、`src/main.js`、`src/App.vue`、`src/router/index.js`、`src/store/index.js`、`src/views/access/Login.vue`、`Register.vue`。
9. 前端单聊：`src/views/chat/session/SessionList.vue`、`src/views/chat/contact/ContactChat.vue`。

完整映射和每个文件的阅读任务见 [KamaChat 参考映射](kamachat-reference.md#mvp0用户会话与单聊)。

## 9. 技术提示，不是实现答案

- Hub 可维护 `map[userID]map[connectionID]*Client`；即使 MVP0 限制单端登录，也要明确替换旧连接的策略。
- Client 至少需要读循环、写循环和有界发送队列。
- 不要在 Hub 全局锁内调用 `WriteMessage`。
- 消息序号需要并发安全；先研究数据库事务、行锁或独立序列表的取舍。
- 数据库成功但 WebSocket 推送失败，不应回滚已经成立的聊天消息，而应保持可重新拉取。
- `message.accepted` 表示服务端已持久化，不等于接收方已读。
- 历史分页使用 sequence 游标比 `OFFSET` 更稳定。

## 10. 必须验收

- 两个用户可注册、登录并实时聊天。
- 伪造消息体 sender_id 不会冒充他人。
- 接收方离线后上线可以通过历史接口看到消息。
- 同一个 client_message_id 重试至少两次，数据库只有一条记录。
- 并发发送后 sequence 不重复，历史排序稳定。
- 一个慢连接不会让其他连接永久阻塞。
- 非法 Token、过期 Token、错误 JSON 和超大消息均被拒绝。
- `go test ./...` 和 `go test -race ./...` 通过。
- MySQL 不可用时客户端不会收到“发送成功”。

## 11. 面试追问

- 为什么消息要先落库再推送？反过来有什么问题？
- ACK 的 accepted、delivered、read 分别是谁确认的？
- 如何保证 client_message_id 幂等？
- 会话序号如何在并发下生成？
- 为什么 WebSocket 需要一个独立写协程？
- 服务重启后在线状态和未送达消息分别怎么办？
- 为什么使用游标分页而不是 OFFSET？
- JWT 被盗后如何处理？MVP0 有哪些不足？
