# Fable 接口契约与文档维护

状态：已接受三角色架构的接口细化，2026-10-28；Comet 投递端的实现进度见[角色契约](../../docs/contracts/comet.md)，其他未交付能力不因出现在 spec 中成为可用服务。架构与交付依赖见[总设计](../../docs/design/goim-migration.md)，各角色的职责见 [Logic](../../docs/contracts/logic.md)、[Comet](../../docs/contracts/comet.md)、[Job](../../docs/contracts/job.md)。

## 权威来源与边界

| 结构 | 权威文件 | 用途 |
| --- | --- | --- |
| RPC 公共类型、枚举、错误详情 | [common.proto](fable/v1/common.proto) | 消息视图、实例/连接身份、租约、发送结果 |
| LogicService | [logic.proto](fable/v1/logic.proto) | 9 个 unary RPC，连接管理、发送和寻址 |
| CometService | [comet.proto](fable/v1/comet.proto) | 3 个 unary RPC，Deliver、Kick、Drain |
| JSON / Redis DTO | [internal/dto/im](../../internal/dto/im) | v2 WS、同步页、Kafka、内部租约的 Go 类型声明 |
| 当前 HTTP 接口 | [路由](../../internal/https_server/https_server.go)、[Handler](../../internal/api/v1)、[请求](../../internal/dto/request)/[响应 DTO](../../internal/dto/response) | code-first 的维护源；生成工具尚未接入 |

RPC 直接使用生成的 [mychat/api/gen/fable/v1](../gen/fable/v1) 类型，不再手写第二份 RPC request/response。已提交生成代码及投递所需的 gRPC runtime；Comet 投递内核和 Job 单任务内核已实现，尚无独立服务监听入口、真实 Logic 或 Job 消费运行链路。JSON DTO 使用 `encoding/json`，不是 ProtoJSON；Comet 写出端和 Job 投递端显式转换，其他适配由相应实现交付。

三个角色契约长期保留，作为实施前的职责和协作参考；它们不承担逐字段 Schema 的另一份维护。RPC spec 维护在 `.proto` 和就近注释中；角色边界、调用目的、业务前置条件或跨角色流程变化时才同步角色文档。HTTP 维护按以下 code-first 规则进行。

## HTTP：code-first

HTTP 文档从实际路由注册、Handler、请求/响应 DTO、校验标签和代码中的接口注释生成，不以手写 Markdown 或 YAML 作为另一份接口定义。生成工具不能凭 Go 类型推断鉴权、条件校验和全部错误分支；这些语义在 Handler 附近显式记录，并由契约测试核对。代码及受测行为为事实来源，生成结果用于阅读和评审。

每次 HTTP 变更在同一个 PR 完成：

1. 修改实际路由、Handler 与 DTO；字段/格式/必填校验随代码维护，记录路径、方法、参数位置、鉴权、成功和错误响应。
2. 使用固定版本的工具和统一命令重新生成 HTTP 文档；目标生成物可放在 `api/openapi/http.json`，不能直接手工修改。工具接入时明确 OpenAPI/Swagger 的实际版本，不把两者混称为已支持的格式。
3. 验证真实路由的 method/path 与生成文档覆盖一致；校验请求、响应与状态码，包含认证/授权失败。生成用的路由注释不能独立于真实注册漂移。
4. CI 重新生成并比较提交产物，发现差异即失败；同时运行路由/响应契约测试。字段变化不要求另改角色 Markdown，角色职责或语义约束变化才改对应文档。

保留现有公开 HTTP 行为，不为了生成文档而统一或改写历史返回格式。例如当前 `/message/getMessageList` 返回消息数组，错误返回 `{"error":"..."}`；生成文档必须如实表达，不能假设所有接口都包在 `data` 中。需要显式响应类型来支持生成时，只描述既有 wire shape，不借此改变业务响应。

角色契约可以提出待实现的 HTTP 能力，但生成文档只发布已注册实现的接口，不能把设计中的同步接口当已交付。WebSocket 帧和 MCP tools 各有自己的协议/Schema，不把它们的全部消息结构塞进 REST 文档。

**当前状态：** 仓库已有 Gin 路由、Handler、DTO 和测试；HTTP 生成工具、生成产物、路由覆盖检查及 CI 漂移门禁尚未接入。`scripts/check-contracts.sh` 只检查 RPC spec/Go DTO 编译，不能作为 HTTP 文档一致性的证明。先完成这个维护闭环，再随各业务块持续更新，不写固定 PR 编号。

## 公共值与存储结构

| 值 | 约束 |
| --- | --- |
| user / group / session / message ID | 现有 `U` / `G` / `S` / `M` 加 UUID 的格式；不能互相代用 |
| Message.session_id | 单聊为对应 S+UUID；群消息沿用 G+UUID，而非某个成员的 S+UUID 群会话记录 |
| client_message_id / connection_id / boot_id | 合法 UUID 字符串；client_message_id 在同一 sender 下唯一，重试不更换 |
| instance_id | 每次启动唯一的命名标识，建议别名加 boot_id；1–64 个 ASCII 小写字母、数字或连字符 |
| route_token | 每连接新生成，至少 128 位随机量的无填充 base64url；内部凭据，不记日志或发往客户端 |
| request_id | 可省略；存在时 1–128 个可打印 ASCII 字符，只用于关联，不用于幂等或认证 |
| conversation_id | 单聊 `direct:<session_uuid>`；群聊 `group:<group_uuid>`；服务端从已授权会话推导 |
| position | 正的消息行 ID，最大 int64；RPC uint64，JSON 为十进制字符串，排序须按整数而非字典序 |
| 时间 | RPC 使用 Timestamp；JSON 使用 UTC RFC3339，可带小数秒；禁止零时间与无时区字符串 |
| content | 文本、非空、UTF-8 最多 16 KiB；普通消息不新增 TrimSpace 语义；MCP 仍先 TrimSpace 且最多 4000 字符 |

新开发库直接按目标模型建表，没有生产数据迁移、旧行回填或兼容 DDL 阶段。目标 `message` 保留既有字段，新增非空 client_message_id、32 字节 request_fingerprint 和 JSON recipient_ids；唯一索引 `(send_id, client_message_id)`，单聊按 session_id、群聊按现有群标识查询并以 id 分页。旧客户端/MCP 没有幂等 ID 时由入口生成一次 UUID，因此新表也不需要 NULL 幂等 ID。

指纹是入口规范化后的 destination、正文、type、origin 的 SHA-256，使用固定版本和确定性字段编码；不包含 request_id、连接身份、路由或发布时间。实现 PR 必须固定编码与跨入口用例，不能依赖 map 遍历顺序。recipient_ids 是发送事务内排序去重的授权目标快照，不是 Outbox。DTO 是传输视图，不直接作为 GORM Model。

同一会话的所有写入必须先持有对应 session/group_info 行锁，覆盖消息行 ID 分配到提交。客户端 `position` 只用于定位/排序；只有 sync 页合并后可推进同步游标。Sync 游标采用无填充 base64url 编码的版本化 JSON：`{"v":1,"conversation_id":"...","after_position":"123"}`；它不保密、不授予权限，每页重新鉴权，禁止跨会话复用或接受负数/溢出位置。

### 枚举映射

| 含义 | legacy / 现有存储 | 新 JSON DTO | Protobuf |
| --- | --- | --- | --- |
| 单聊目标 | receive_type=0 | `user` | RECEIVE_TYPE_USER=1 |
| 群聊目标 | receive_type=1 | `group` | RECEIVE_TYPE_GROUP=2 |
| 文本 | type=0 | `text` | MESSAGE_TYPE_TEXT=1 |
| 用户消息 | origin=0 | `user` | MESSAGE_ORIGIN_USER=1 |
| AI 消息 | origin=1 | `ai` | MESSAGE_ORIGIN_AI=2 |
| 发布全部确认 | 无旧对应项 | `queued` | DISPATCH_STATE_QUEUED=1 |
| 确定未发布 | 无旧对应项 | `not_queued` | DISPATCH_STATE_NOT_QUEUED=2 |
| 部分发布/结果不确定 | 无旧对应项 | `unknown` | DISPATCH_STATE_UNKNOWN=3 |

所有 Proto 枚举 0/UNSPECIFIED 和 JSON 空枚举均无效，未知非零枚举也拒绝。不得把 legacy 数字直接强转为新枚举。proto3 不自动验证这些业务约束，生成类型也不替代验证器。

## RPC 共同行为

所有 RPC 为内部 unary；无流式转发、广播 RPC、通用存库 RPC 或 JobService。WebSocket 仍由 Comet 持有。每个调用设置 deadline：发送 10 秒，其余 2 秒；下游消耗同一预算。批量最多 100 个且至少 1 个，重复 ID/连接目标返回 INVALID_ARGUMENT，不偷偷去重改变响应基数。

服务凭据通过 gRPC metadata `authorization: Bearer <service-token>` 传递，按角色独立配置并授权方法。Connect 的 user_jwt 是另一个经过用户鉴权的 Token，不能替代服务凭据。CometIdentity 必须与该凭据允许的实例/boot 绑定，注册地址必须在允许网络范围内；共享角色凭据也不能省略实例授权。开发默认 loopback，跨主机前补 TLS/mTLS。所有身份、Token、正文不得进入错误描述。

缺少必需 message、oneof 未选择、空 ID、非法枚举/时间、目标类型与 ID 前缀矛盾都在边界拒绝。Unknown Protobuf 字段按扩展兼容保留/忽略，不能因此跳过已知字段验证。删除字段或枚举值必须保留其编号与名字为 reserved，不能复用。源文件注释中的“必须”是语义约束，编译成功不证明服务已执行它。

### Logic 方法约束

| 方法 | 调用者 / 校验 | 成功与重试 |
| --- | --- | --- |
| RegisterComet | Comet；合法 identity/address | 相同有效 boot/address 返回当前租约并续期；不同地址冲突；不得把 draining 改回 active |
| RenewComet | Comet；身份匹配且租约有效 | 返回新租约；draining 只能 false→true；失效先停止连接服务，再重新注册 |
| UnregisterComet | Comet；条件匹配 | DELETED / NOT_CURRENT / NOT_FOUND 均为正常结果；不删除其他实例的用户路由 |
| Connect | Comet；验证 JWT、实例、connection_id、route_token | Logic 从 JWT 得到 user/expiry，替换路由并负责 Kick；首次调用不配置应用级自动重试，超时关闭 pending 连接后条件清理 |
| Heartbeat | Comet；四元组仍为当前路由 | 租约取 `min(now+TTL, auth_expires_at)`，实例租约也须有效；被替换返回 SUPERSEDED |
| Disconnect | Comet；条件匹配四元组 | 三种删除结果均幂等，不得删新连接 |
| SubmitMessage | Comet；当前路由、auth expiry、会话/成员权限 | 只接受用户文本，sender 从 identity 得到，origin 固定 user；AI 走 Logic 内部授权方法 |
| LookupRoutes | Job；1–100 个不同 user_id | 按输入顺序/数量返回；每项 oneof 是 route 或非零 offline 原因；依赖错误失败整个 RPC |
| ResolveComets | Job；1–100 个不同 instance_id | 按输入顺序/数量返回；每项 oneof 是有效 lease 或 missing 原因；draining 的有效实例仍可解析 |

缺少/过期实例不能被当作有效用户路由。Connect/Heartbeat 的剩余租约不超过 JWT 过期时间；Comet 在有效截止前 5 秒停止服务，剩余不足安全余量的新连接直接拒绝。新 Connect 只阻止之后才开始校验的旧连接发送，已通过校验的请求允许完成提交，不承诺 Redis 与 MySQL 原子 fencing。

SubmitMessage 的成功结果必须 `stored=true`，带完整的持久 MessageView 和非零 dispatch；HTTP/WS 的 SendResult 使用同一结构。相同 ID 返回原 message，但仍先检查当前权限；权限撤销后返回 PERMISSION_DENIED，不返回旧内容或重推。Kafka 发布失败/响应不确定使用正常成功结果中的 not_queued/unknown；一旦已知提交成功，不能用普通 gRPC 失败冒充“没保存”。连接超时导致结果不可知时，调用方只以同一 ID 重试。

### Comet 方法约束

| 方法 | 调用者 / 校验 | 成功与重试 |
| --- | --- | --- |
| Deliver | Job；本机 instance_id/boot_id、task/message 对应、每个 target 属于本实例 | 同顺序同数量返回每个完整 target 与非零 outcome；半成功仍为 gRPC OK；未返回结果的目标必须当结果不确定 |
| Kick | Logic；本机身份、目标四元组与非零 reason | 精确匹配后启动关闭；NOT_CURRENT/NOT_FOUND 幂等；不按 user_id 误关新连接 |
| Drain | 本机 admin 专用凭据；本机身份，未来且不超过配置上限的 deadline | 接受后立即返回，停止新连接并登记 draining；重复调用只取更早截止时间；不会等待所有 Socket 关闭 |

Drain 默认最大宽限 30 秒；截止前可向已有 active 连接投递，截止后返回 DRAINING 并关闭。Deliver 的 ENQUEUED/DUPLICATE 只表明曾入队，NOT_FOUND/STALE 需要刷新路由，QUEUE_FULL 关闭慢连接并有界重试；整个 RPC 的旧 boot 错误返回 FAILED_PRECONDITION 并要求 REFRESH_ROUTE。实例正在启动、Hub 阻塞、请求取消不能被伪装成成功入队。整批校验失败发生在任何目标入队前；一旦开始执行而响应丢失，Job 必须接受存在部分成功的可能。

## gRPC 错误与 JSON 错误

RPC 错误使用非 OK status，可附 `fable.v1.ErrorDetail`（status details 中的 Any），不在正常响应加 success/error 两套字段。运行时实现可用 grpc status details API；传输层错误可能没有 detail，不能依赖字符串 message 判断保存结果。`ErrorReason` 的 UNSPECIFIED 也不是已知错误。

| ErrorReason / 新 JSON code | gRPC code | 默认处理 |
| --- | --- | --- |
| INVALID_ARGUMENT / `invalid_argument` | INVALID_ARGUMENT | DO_NOT_RETRY，修改输入 |
| IDEMPOTENCY_CONFLICT / `idempotency_conflict` | ALREADY_EXISTS | DO_NOT_RETRY，同 ID 不得改请求 |
| UNAUTHENTICATED / `unauthenticated` | UNAUTHENTICATED | REAUTHENTICATE |
| PERMISSION_DENIED / `permission_denied` | PERMISSION_DENIED | DO_NOT_RETRY；不证明先前请求没保存 |
| SUPERSEDED / `superseded` | FAILED_PRECONDITION | Comet 关闭旧连接，RECONNECT |
| LEASE_EXPIRED / `lease_expired` | FAILED_PRECONDITION | RECONNECT |
| INSTANCE_NOT_REGISTERED / `instance_not_registered` | FAILED_PRECONDITION | Comet 停止服务后重注册；Job REFRESH_ROUTE |
| DEPENDENCY_UNAVAILABLE / `dependency_unavailable` | UNAVAILABLE | 依方法同 ID 重试，不自动重试 Connect |
| RESOURCE_EXHAUSTED / `resource_exhausted` | RESOURCE_EXHAUSTED | 同请求有界退避 |
| DRAINING / `draining` | UNAVAILABLE | 重连或刷新路由 |

ErrorDetail.retry 明确 DO_NOT_RETRY、SAME_REQUEST、RECONNECT、REAUTHENTICATE 或 REFRESH_ROUTE；JSON 用相应小写字符串。未附详情的 DEADLINE_EXCEEDED/UNAVAILABLE/CANCELLED/UNKNOWN 映射为公开 `result_unknown`，不推断 stored=false；INTERNAL/DATA_LOSS 等未识别服务错误也按不确定结果安全处理并上报诊断。对 SubmitMessage 仅允许同 ID 重试，对 Connect 关闭 pending，对 Deliver 刷新后有界重试。正常保存后的 dispatch=unknown 与 RPC 错误 result_unknown 是不同状态。

## WS、同步与 Kafka DTO 细节

legacy `/wss` 继续使用原 [wschat.Message](../../internal/dto/wschat/chat.go)，原字段和数字枚举不变。新 `protocol=v2` 是显式新协议：输入为 SendFrame，不携带任意 sender/origin；输出为嵌套 MessageFrame/SendResultFrame，不把控制帧交给旧客户端。SendResultFrame 的 result/error **恰有一个**非空，内外 client_message_id 必须一致；Message/SendResult 不包含内部 route_token。

```json
{"event":"send_message","client_message_id":"e1668bf9-e028-454b-bb87-d3905dc4d29c","receive_id":"U46a1891b-e5a2-4a2e-8f94-c79044717e92","receive_type":"user","content":"hello"}
```

```json
{"event":"send_result","client_message_id":"e1668bf9-e028-454b-bb87-d3905dc4d29c","error":{"code":"result_unknown","retry":"same_request"}}
```

SyncQuery 来自 query 参数而非 JSON；limit=0/省略用 100，1–500 合法，负数或超过 500 拒绝。SyncPage 的 messages 空时编码 `[]` 而非 null；无新数据时 next_cursor 保留输入游标，首次空页可为 `""`。有消息时为最后一条已合并页的合法位置游标；has_more 按多查一条确定。分页、实时推送、发送结果使用同一 Message，实时 position 不推进 sync 游标。

PushTask 不复制顶层 message_id/conversation_id/created_at，以 `message` 内字段为唯一来源；task_id=`push:v1:<message.message_id>:<chunk_index>`。schema_version=1、kind=message_push、chunk_count≥1、chunk_index<chunk_count；recipients 为持久快照排序去重后的稳定分片，1–100 个；targets 为这些用户的有效内部连接提示，可为空但编码 `[]`。

同一 task_id 的业务 message、recipients 和分片信息不变；targets、request_id 与重放上下文可刷新。pending_recipients 省略表示完整分片，存在时必须非空、无重复、为 recipients 子集，并附非空 replay_run_id；Logic 常规生产者不得填写它。JSON 中显式 null/空数组不能用来伪装省略的 pending_recipients，未来消费者须在解码边界区分并拒绝这类输入。

DLQRecord 的 task 与 raw_payload_base64 恰有一个：只有通过版本/结构/语义验证的有效任务在投递耗尽时，才保存完整 Task 加非空 pending_recipients。JSON 解码失败、未知 Schema、非法 ID/枚举/收件人及其他验证失败均保存原字节 base64，不转换为当前 PushTask，也不要求伪造 pending 子集；这类原始记录修复原因前不可直接自动重放。source_partition≥0，source_offset 为非负 int64 的十进制字符串；尝试时间有序。重放有效任务时保持原业务身份和完整 recipients，只改变 pending 子集与 replay_run_id。DTO 声明不自动验证这些跨字段规则。

## 契约检查与后续实现

从仓库根目录运行，使用支持 proto3 的 protoc 和 go.mod 要求的 Go：

```sh
sh scripts/check-contracts.sh
```

该命令把 descriptor 写到临时目录并清理，再编译 Go DTO 包；不生成仓库代码、不连接数据库或 Kafka，不证明运行时验证器或服务可用。当前核对使用 protoc 33.4。Go 生成器的固定版本与命令见下；禁止用 fmt/JSON 作为 Protobuf 二进制序列化的替代。

Go 绑定由固定版本生成：protoc 33.4、protoc-gen-go v1.36.12、protoc-gen-go-grpc v1.6.2。运行时 grpc-go v1.85.0；依赖以 go.mod/go.sum 为准。

```sh
sh scripts/generate-proto.sh
sh scripts/generate-proto.sh --check
go test -race ./internal/comet
```

生成脚本在临时目录安装固定插件；`--check` 只比较生成物，不改检出。脚本必须有对应 Go 模块的下载权限；生成绑定和通过测试不等于已有真实 Logic、租约或多实例部署。

实现者以本 spec 的语义建立边界用例：缺少/未知枚举、超长正文、64 位 JSON 位置、过期 JWT/租约、旧 boot、重复批量输入、部分投递、同 ID 不同正文、权限撤销后的重试、无详情超时、空 pending 与越权重放目标、同步游标被实时消息跳过。PR 按实际能力拆分，不固定数量或编号。

协议规则参考 [Protobuf proto3](https://protobuf.dev/programming-guides/proto3/)、[ProtoJSON](https://protobuf.dev/programming-guides/json/) 和 [gRPC 状态码](https://grpc.io/docs/guides/status-codes/)、[错误处理](https://grpc.io/docs/guides/error/)。这些规范不替本项目定义业务幂等与可靠性。
