# KamaChat 分阶段参考地图

参考仓库：<https://github.com/youngyangyang04/KamaChat>

目标是复刻 KamaChat 的主干，而不是把完整版从头读一遍。每个 MVP 只开放对应文件：先根据需求自行设计和手写，再阅读参考文件对照，最后记录差异。除非当前实现被阻塞，不提前查看后续阶段文件。

## 使用方法

一个功能的参考循环：

```text
先读需求
→ 自己画调用链和数据表
→ 手动创建当前需要的目录/文件
→ 尝试完成最小实现
→ 阅读指定 KamaChat 文件
→ 对比并修改
→ 记录保留项、差异项和原因
```

阅读一个文件时只回答：

1. 它属于入口、Controller、DTO、Service、Model、基础设施还是前端？
2. 上游是谁调用它，下游又调用谁？
3. 它读写哪些 MySQL 表、Redis Key、Channel 或 WebSocket？
4. 正常路径、业务失败和系统失败分别如何返回？
5. 哪些地方保持一致，哪些地方为了安全或可靠性需要改进？

## 全局目录对齐

自己的项目优先沿用这些顶层目录和职责：

```text
api/v1/                    HTTP Controller
cmd/kama_chat_server/      程序入口
configs/                   配置文件
docs/                      项目说明
internal/config/           配置解析
internal/dao/              GORM/MySQL 初始化
internal/dto/request/      请求 DTO
internal/dto/respond/      响应 DTO
internal/https_server/     Gin Engine 与路由
internal/model/            GORM Model
internal/service/chat/     WebSocket 与消息转发
internal/service/gorm/     业务服务和数据库访问
internal/service/redis/    Redis 适配
internal/service/kafka/    Kafka 适配
pkg/constants/             常量
pkg/enum/                  业务枚举
pkg/util/                  通用工具
pkg/zlog/                  日志
test/                      测试
web/chat-server/           Vue 3 前端
```

不要一次性创建完整目录。MVP0 进行到哪个切片，就手动创建哪个目录和文件；MVP1 才创建 Redis 和群聊相关文件，MVP3 才创建 Kafka 文件。

## MVP0：用户、会话与单聊

### A. 工程入口与配置

| 阅读顺序 | 文件 | 需要理解 |
|---|---|---|
| 1 | `go.mod` | Go 版本、Gin/GORM/MySQL/WebSocket/Zap 等依赖 |
| 2 | `configs/config.toml` | 配置分组和各组件地址 |
| 3 | `internal/config/config.go` | TOML 如何映射到结构体，配置何时加载 |
| 4 | `cmd/kama_chat_server/main.go` | 服务启动、后台 goroutine、信号监听和资源关闭 |
| 5 | `internal/https_server/https_server.go` | Gin Engine、CORS、静态目录和路由注册 |
| 6 | `api/v1/controller.go` | 统一 JSON 响应约定 |
| 7 | `internal/dao/gorm.go` | GORM 初始化、MySQL DSN 和模型迁移 |
| 8 | `pkg/zlog/logger.go` | 日志初始化、级别和滚动策略 |
| 9 | `pkg/constants/constants.go` | Channel、文件、错误提示等全局常量 |
| 10 | `pkg/util/random/random_int.go` | 业务 UUID 的生成方法 |

第一次对照时只看 Gin、GORM、MySQL 和日志部分，不学习 Redis/Kafka 配置的实现。

### B. 注册与登录

| 层 | 文件 |
|---|---|
| Model | `internal/model/user_info.go` |
| Controller | `api/v1/user_info_controller.go` 中 Register、Login、GetUserInfo |
| Service | `internal/service/gorm/user_info_service.go` 中对应方法 |
| Request DTO | `register_request.go`、`login_request.go`、`get_userinfo_request.go` |
| Respond DTO | `register_respond.go`、`login_respond.go`、`get_userinfo_respond.go` |
| 枚举 | `pkg/enum/user_info/user_status_enum/user_status_enum.go` |
| 前端 | `src/views/access/Register.vue`、`Login.vue`、`src/store/index.js`、`src/router/index.js` |

暂不参考短信登录：`SmsLogin.vue`、`sms` Service 和阿里云 SMS 依赖不属于 MVP0 必做。

对照重点：KamaChat 的密码与身份方案较弱。保持接口和分层相似，但自己的版本应使用密码哈希和服务端鉴权，不能只相信 `client_id` 或消息体的 `send_id`。

### C. 会话

| 层 | 文件 |
|---|---|
| Model | `internal/model/session.go` |
| Controller | `api/v1/session_controller.go` |
| Service | `internal/service/gorm/session_service.go` |
| Request DTO | `open_session_request.go`、`create_session_request.go`、`delete_session_request.go`、`ownlist_request.go` |
| Respond DTO | `user_sessionlist_respond.go` |
| 前端 | `src/views/chat/session/SessionList.vue` |

MVP0 只看用户单聊分支，跳过 group session 分支。

### D. 消息历史

| 层 | 文件 |
|---|---|
| Model | `internal/model/message.go` |
| Controller | `api/v1/message_controller.go` 中 GetMessageList |
| Service | `internal/service/gorm/message_service.go` 中 GetMessageList |
| Request DTO | `get_message_list_request.go`、`message_request.go` |
| Respond DTO | `get_message_list_respond.go` |
| 枚举 | `pkg/enum/message/message_type_enum/*`、`message_status_enum/*` |

MVP0 只实现文本消息；文件上传、群消息、AV 字段留给后续。

### E. WebSocket 单聊

| 阅读顺序 | 文件 | 需要理解 |
|---|---|---|
| 1 | `api/v1/ws_controller.go` | HTTP 如何升级为 WebSocket |
| 2 | `internal/dto/request/chat_message_request.go` | 前端发来的消息结构 |
| 3 | `internal/service/chat/client.go` | Client、Read/Write goroutine、SendTo/SendBack |
| 4 | `internal/service/chat/server.go` | Clients map、Login/Logout/Transmit Channel、单聊转发 |
| 5 | `src/views/chat/contact/ContactChat.vue` | WebSocket 创建、发送、onmessage 和页面状态 |

阅读 `server.go` 时只跟踪 Text + User 分支。File、Group、AV 分支分别留给 MVP1/MVP3。

## MVP1：联系人、群聊与 Redis

### A. 联系人体系

| 层 | 文件 |
|---|---|
| Model | `internal/model/contact_apply.go`、`user_contact.go` |
| Controller | `api/v1/user_contact_controller.go` |
| Service | `internal/service/gorm/user_contact_service.go` |
| 枚举 | `pkg/enum/contact/*`、`pkg/enum/contact_apply/*` |
| Request DTO | `apply_contact_request.go`、`pass_contact_apply_request.go`、`delete_contact_request.go`、`black_contact_request.go`、`black_apply_request.go` |
| Respond DTO | `get_userlist_respond.go`、`get_contactinfo_respond.go`、`new_contact_list_respond.go` |
| 前端 | `src/views/chat/contact/ContactList.vue`、`src/components/ContactListModal.vue` |

把申请、通过、拒绝、删除、拉黑画成状态机后再写代码。

### B. 群聊体系

| 层 | 文件 |
|---|---|
| Model | `internal/model/group_info.go` |
| Controller | `api/v1/group_info_controller.go` |
| Service | `internal/service/gorm/group_info_service.go` |
| 枚举 | `pkg/enum/group_info/add_mode_enum/*`、`group_status_enum/*` |
| Request DTO | `create_group_request.go`、`enter_group_directly_request.go`、`leave_group_request.go`、`dissmiss_group_request.go`、`update_groupinfo_request.go`、`remove_groupmembers_request.go`、`get_groupmember_list_request.go` |
| Respond DTO | `get_groupinfo_respond.go`、`get_grouplist_respond.go`、`get_groupmember_list_respond.go`、`load_my_group_respond.go`、`load_my_joined_group_respond.go` |

前端群聊分散在 `SessionList.vue`、`ContactList.vue`、`ContactChat.vue` 和多个 Modal 中，应通过搜索接口名逐段阅读，不要从头通读整个大组件。

### C. 群会话与群消息

- `session_controller.go`、`session_service.go` 的 Group 分支。
- `message_controller.go`、`message_service.go` 的 GetGroupMessageList。
- `server.go` 的 Text/File + Group 分支。
- `get_group_message_list_request.go`、`get_group_messagelist_respond.go`、`group_sessionlist_respond.go`。

### D. Redis

| 文件 | 需要理解 |
|---|---|
| `internal/service/redis/redis_service.go` | Client 初始化、Get/Set/Del、TTL、KEYS/SCAN |
| `configs/config.toml`、`internal/config/config.go` | Redis 连接配置 |
| `session_service.go` | session list 和 group session list 缓存 |
| `message_service.go` | 单聊/群聊消息列表缓存 |
| `group_info_service.go` | 群资料、成员和用户群列表缓存 |
| `user_contact_service.go` | 联系人列表缓存和失效 |
| `user_info_service.go` | 用户资料、验证码相关缓存 |
| `server.go` | 新消息到达后的缓存追加 |

对照重点：列出所有 Key 后再实现。自己的版本不在业务请求中使用 `KEYS`，不在关机时清空全部 Redis；缓存必须可从 MySQL 重建。

### E. 可选后台管理

以下文件用于扩展，不属于 MVP1 门禁：

- `web/chat-server/src/views/manager/Manager.vue`
- `DeleteUserModal.vue`、`DisableUserModal.vue`、`SetAdminModal.vue`
- `DeleteGroupModal.vue`、`DisableGroupModal.vue`
- `user_info_controller.go`、`group_info_controller.go` 中管理员接口

## MVP2：AI 与自研 MCP

KamaChat 没有 MCP 模块。这一阶段参考的是“接入点”，不是实现答案。

| 接入任务 | 参考文件 | 复用内容 |
|---|---|---|
| AI 作为聊天参与者 | `user_info.go`、`message.go` | 用户和消息模型 |
| AI 消息收发 | `chat_message_request.go`、`client.go`、`server.go` | WebSocket 消息链路 |
| AI 消息持久化 | `message_service.go` | 历史记录和会话刷新 |
| 群权限 | `group_info_service.go` | 成员和群权限判断 |
| 私聊权限 | `user_contact_service.go` | 联系人关系判断 |
| AI 前端状态 | `ContactChat.vue`、`store/index.js` | 消息展示、处理中/失败状态 |
| 新路由 | `https_server.go`、对应 Controller | 任务查询或取消接口 |

需要自行新增和设计：

```text
MCP Server
AI Agent/Worker
ai_jobs
ai_tool_audits
chat_todos
工具 Schema
权限上下文
超时、取消、限流和审计
```

不得直接修改 KamaChat 的长消息分支来塞入同步模型调用。AI 任务应异步执行，最终结果重新进入普通消息链路。

## MVP3：音视频、Kafka 与 K8s

### A. 音视频与 WebRTC

| 阅读顺序 | 文件 | 需要理解 |
|---|---|---|
| 1 | `internal/dto/request/av_data_request.go` | AV 信令数据结构 |
| 2 | `internal/dto/respond/av_message_respond.go` | 服务端返回结构 |
| 3 | `internal/dto/request/get_cur_contactlist_in_chatroom_request.go` | 聊天室成员请求 |
| 4 | `internal/dto/respond/get_cur_contactlist_in_chatroom_respond.go` | 当前 Peer 列表 |
| 5 | `api/v1/chatroom_controller.go` | 当前聊天室成员接口 |
| 6 | `internal/service/gorm/chatroom_service.go` | 在线 Client 到 Peer 列表的映射 |
| 7 | `internal/model/message.go`、`chat_message_request.go` | `av_data` 和 AV 消息类型 |
| 8 | `internal/service/chat/server.go` | CURRENT_PEERS、PEER_JOIN、PEER_LEAVE、PROXY 转发 |
| 9 | `ContactChat.vue` | RTCPeerConnection、getUserMedia、SDP、ICE、呼叫状态 |

阅读前端时搜索这些关键词定位，不要从头读两千多行组件：

```text
RTCPeerConnection
getUserMedia
start_call
receive_call
reject_call
sdp
candidate
```

### B. Kafka

| 阅读顺序 | 文件 | 需要理解 |
|---|---|---|
| 1 | `go.mod` | `segmentio/kafka-go` 依赖 |
| 2 | `configs/config.toml` | messageMode、broker、topic、partition、timeout |
| 3 | `internal/config/config.go` | KafkaConfig |
| 4 | `internal/service/kafka/kafka_service.go` | Writer、Reader、Topic、GroupID、Balancer |
| 5 | `internal/service/chat/client.go` | WebSocket 消息如何选择 Channel 或 Kafka Producer |
| 6 | `internal/service/chat/kafka_server.go` | Consumer 如何读取并转发消息 |
| 7 | `cmd/kama_chat_server/main.go` | 初始化、启动模式和关闭资源 |

必须将 `server.go` 和 `kafka_server.go` 并排比较，并记录：

- 为什么出现大量重复消息处理逻辑？
- Key 和 partition 是否能保证会话内顺序？
- `RequireNone` 有什么可靠性影响？
- offset 何时提交？处理失败会怎样？
- 多个实例如何定位真正持有接收方 WebSocket 的节点？
- 如何实现幂等和失败重试？

先复刻最小 Kafka 模式，再将传输层和消息处理层解耦作为自己的改进。

### C. K8s

KamaChat 仓库没有 K8s 文件，这部分完全由你扩展。只有本地双进程 + Redis 路由 + Kafka 跨实例消息通过后，才开始创建：

```text
Dockerfile
Deployment
Service/Ingress
ConfigMap/Secret
liveness/readiness probe
优雅退出和滚动更新策略
```

## 保持一致与允许改进的边界

优先保持一致：

- 顶层目录和主要包职责。
- `api/v1 → service → model/dao` 的主调用链。
- `Client.Read/Write + Server.Clients + Channel` 的单机 WebSocket 思路。
- `user_info`、`session`、`message`、`contact_apply`、`user_contact`、`group_info` 的业务概念。
- Vue 3 的登录、会话、联系人和聊天页面划分。
- Channel/Kafka 两种消息模式的演进顺序。

必须理解后再改进：

- WebSocket 和消息发送者身份鉴权。
- 密码存储。
- 消息幂等、稳定排序和 ACK 语义。
- 持锁期间阻塞投递的问题。
- Redis `KEYS`、全量清理和缓存一致性。
- 群成员 JSON 的查询与并发修改成本。
- Kafka Key、ACK、offset、幂等、跨实例路由和重复业务代码。
- API 总是返回 HTTP 200 的错误语义。

每个改进必须在复盘中写清：KamaChat 原实现、观察到的问题、自己的方案、代价和验证结果。这样既能说明项目确实基于 KamaChat，又能证明不是简单照抄。
