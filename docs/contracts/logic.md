# Logic 角色契约

状态：架构已接受；Logic 独立服务与本契约能力已实现；完整三角色聊天仍待 Comet/Job 运行链路接入。Owner 为仓库维护者，修改 Logic 接口的 PR 作者同步本文。方法与字段以 [logic.proto](../../api/proto/fable/v1/logic.proto) 和 [DTO / RPC spec](../../api/proto/README.md) 为准，本文只维护职责和调用语义。

本文长期保留，作为写 Logic 代码前的参考。HTTP 以路由、Handler、DTO 和代码接口注释生成文档，具体见 [code-first 维护规则](../../api/proto/README.md)；不在本文重复维护字段表。纯字段变化更新代码/生成物，职责、授权边界或协作流程变化才同步本文。

## 已实现的存储组件

`internal/logic/messagestore` 在同一事务内保存消息、幂等指纹与收件人快照；先锁发送用户，再锁单聊会话或群，并在返回重复记录前重新校验当前成员/AI 白名单。相同 sender/client_message_id 跨目标或内容冲突被拒绝，普通文本保留空白，AI 发送维持 TrimSpace/4000 字符边界。群消息 session_id 保持 G UUID。Sync 绕过缓存，以整数位置分页并逐次鉴权，JSON 位置为十进制字符串。

该组件直接初始化新空库模型，旧 model 和单体写法保持不变；部署入口必须拒绝对已有表执行初始化，不做生产回填。真实 MySQL 用例验证并发重复只存一条、跨目标冲突、成员/白名单竞争、事务回滚、分页和大整数位置。该组件本身不发送 Kafka，也不开放 RPC 入口。

## 路由和生产组件

`internal/logic/routing` 以 Redis Lua 和服务器时间原子管理实例及用户路由，默认 TTL 60 秒，用户租约受 JWT 截止约束；四元组和内部 boot 共同隔离旧进程。draining 不可恢复为 active；旧续租/删除不能影响新路由。Lookup/Resolve 保留请求顺序与数量，依赖/损坏数据返回错误而不是假离线。命名空间隔离，原子相关 Key 使用同一 hash tag；当前只验收单 Redis，未声明集群部署完成。

`internal/logic/kafkapush` 同步 RequireAll 发布新 `fable.push.v1` 任务，使用会话 Key、明确 Topic 和最多 256 KiB 的原始 JSON。它不创建 Topic、不消费、不写消息数据库；写出前拒绝为 definitely-not-published，写出后的错误按结果不确定处理，不能由异常推断接收成功。Ping 检查显式 Topic 元数据，关闭会等待有界 writer 清理。真实 Redis 和 Kafka 用例验证竞争、期限、损坏数据、同 Key 分区、精确载荷和禁止自动建 Topic。

## 职责与业务归属

Logic 是原业务后端的新运行角色：HTTP API、MCP、用户/会话/群/联系人规则、AI 授权、消息持久化、Redis 路由和 Kafka 生产。它不持有 WebSocket，也不消费推送 Topic。

注册登录、查询及群管理继续调用现有 [api/v1](../../internal/api/v1)、[gormservice](../../internal/service/gormservice)；AI 能力继续由 [aiservice](../../internal/service/aiservice) 与 [MCP](../../internal/mcpserver) 承载。普通消息、群消息、MCP 发送统一由 `internal/logic.Service` 编排，并由 `messagestore` 事务持久化；原业务授权规则保持。旧 `gormservice` 消息写法仅供隔离单体回归，新 Logic 不挂载旧 WS、不启动旧 Kafka 消费者。

## 公共 HTTP / MCP

| 接口组 | 对外契约与迁移要求 |
| --- | --- |
| 注册/登录/用户/联系人 | 维持[现有路由](../../internal/https_server/https_server.go)、状态码与响应；仍签发/验证 JWT |
| 会话/群管理/历史 | 现有接口保留，群审批与成员权限不变 |
| `/api/v1/ai/setting` | 维持 AI 白名单设置，不转换成通用访问权限 |
| `/mcp` | 原四个工具保留；发送改统一编排，新增可选 client_message_id 与附加 dispatch 信息 |
| `GET /api/v2/messages/sync` | 新增：JWT、conversation_id、可选 cursor、limit；返回消息页、next_cursor、has_more；MySQL 权威查询 |
| `/healthz` / `/readyz` | 存活与分项依赖/服务就绪分开，不用旧常量 200 证明推送可用 |

sync 的消息以 message_id 表示原消息 UUID，附 conversation_id 与用于排序的 position；调用者必须属于该会话，群权限取当前权威状态。游标绑定会话，不能换会话复用；无效游标返回 INVALID_ARGUMENT。普通用户同步不自动授予 AI；MCP 查询另走白名单。

同步游标仅由成功读取并合并的 sync 页推进，不能按实时推送或 send_result 中最大的消息位置推进；详见设计中的客户端合并规则。MCP 发送继续保留原有去除首尾空白和最多 4000 字符限制。

## LogicService 内部 RPC

所有方法需要按角色及实例授权的服务凭据；Connect 另外验证用户 JWT。方法与类型已在 Protobuf 固定，后续采用增量兼容修改；九个方法已实现并通过真实后端集成验证。

| 方法 | 调用方 | 输入 | 成功结果 / 副作用 | 重试语义 |
| --- | --- | --- | --- | --- |
| RegisterComet | Comet | instance_id、boot_id、rpc_address | 验证实例身份/地址并创建实例租约 | 同 boot_id 同地址可重试；身份冲突拒绝 |
| RenewComet | Comet | instance_id、boot_id、draining | 条件续租，返回 lease_deadline | 租约丢失必须重新注册；不冒用旧 boot |
| UnregisterComet | Comet | instance_id、boot_id | 条件删除实例租约 | 幂等，不删除用户路由的其他实例 |
| Connect | Comet | JWT、instance_id、connection_id、route_token | 验证用户/实例，将 JWT 的 auth_expires_at 写入路由，原子替换当前路由，返回 user_id、lease_deadline、旧路由 | 首次注册不自动重试；不确定则关闭并条件清理 |
| Heartbeat | Comet | 连接身份四元组 | 仅匹配当前路由时续租 | 可重复；不匹配返回 SUPERSEDED，不覆盖 |
| Disconnect | Comet | 连接身份四元组 | 条件删除该连接路由 | 幂等；不存在或已替换均不能影响新路由 |
| SubmitMessage | Comet | 连接身份、client_message_id、目标、receive_type、正文、request_id | 校验身份和业务；事务保存或返回原记录；生产任务；返回 SendResult | 权限仍有效时同 ID 同内容返回原消息且可重发；不同内容 CONFLICT |
| LookupRoutes | Job | 最多 100 个 user_id | 每个用户返回有效 route 或 offline；过滤失效实例 | 只读；依赖出错不得返回假 offline |
| ResolveComets | Job | 最多 100 个 instance_id | 已注册、未过期实例的地址/boot_id/draining | 只读；不接受调用方提供任意目标地址 |

连接身份为 user_id、instance_id、connection_id、route_token；Comet 不能仅提交任意 user_id 来发消息。SubmitMessage 在落库前重新验证当前路由与由 Logic 在 Connect 写入的 auth_expires_at；续租不能超过它，不信任 Comet 自报时间。此校验是发送受理时刻的检查；检查后发生新 Connect 的在途请求允许完成，不承诺 Redis 与 MySQL 间的原子 fencing。

每次重试先检查当前权限，再返回旧幂等结果或重发。若用户已退群或 MCP 白名单被撤销，返回 PERMISSION_DENIED，不回传旧记录、不重新推送；它不代表先前请求没有提交，不得因此改用新 ID 重发。权限仍有效时返回原记录，未保存时才创建新记录。

MCP 不经 Comet RPC：在同一 Logic 进程中完成 JWT/白名单校验后调用内部发送编排。内部调用也必须传明确来源，不能接受外部客户端设置 OriginAI。没有任何对外 RPC 用于跳过授权“直接保存消息”。

## SendResult 与错误

SendResult 包含 client_message_id、stored、完整 message、dispatch、replayed；message 内保存 message_id、created_at、conversation_id 与 position，不重复维护顶层别名。stored 只有数据库提交后为 true；dispatch 的 queued/not_queued/unknown 只描述 Kafka 发布，不是对方接收。业务拒绝用明确错误返回；数据库提交后的推送失败以成功保存结果附 dispatch 降级，不能返回一条会诱导新 ID 重发的笼统数据库失败。

| 错误 | 调用方行为 |
| --- | --- |
| UNAUTHENTICATED / PERMISSION_DENIED | 不重试发送，关闭或重新登录；MCP 显示安全错误 |
| INVALID_ARGUMENT / CONFLICT | 修正输入；同 ID 不可改正文或目标 |
| SUPERSEDED | Comet 关闭对应旧连接 |
| UNAVAILABLE / DEADLINE_EXCEEDED，结果不确定 | 可在有效连接上以同 client_message_id 重试；不得自动换 ID |
| RESOURCE_EXHAUSTED | 有界退避；不无界堆积 |

RPC 初始 deadline 提案：路由/续租 2 秒、发送 10 秒；Kafka publish 占用同一发送预算。超时可能发生在提交后，须按不确定结果处理。Connect 注册用独立 2 秒预算，丢失响应后关闭连接而非盲目重复替换。

## 状态与持久化不变量

1. 消息、幂等字段和收件人快照在同一事务中写入；所有新发送入口共享事务写法。
2. Job 重放不调用这些写方法；Kafka 中已经是推送任务。
3. 路由与实例租约只有 Logic 修改；缓存失败与路由失败分开处理。
4. 同一会话的行锁覆盖新消息 ID 分配到提交，才能安全提供增量游标。
5. 首期没有自动恢复数据库提交后未发布任务的后台扫描器；完整边界见设计第 7 节。

## 独立运行与验证

入口为 `cmd/logic`，配置参考 [Logic 示例](../../config/logic.example.toml)。`init-schema` 只接受显式选择且没有任何表的数据库，正常 `serve` 只校验目标结构，不执行 AutoMigrate。没有生产回填或旧 backlog 迁移。HTTP/MCP 使用同一个业务数据库；新 Logic 关闭旧查询缓存，历史和权限读权威数据库，Redis 客户端专用于路由。

服务凭据为显式配置的 Comet token→instance/boot/address 授权，以及独立 Job/Kick token；开发监听和注册地址限制 loopback。Connect 再次验证 JWT 和用户存在；Heartbeat/Submit 检查当前路由。替换路由后通过有界队列/工作协程尽力 Kick，完全相同 Connect 四元组不踢自己。新逻辑上线规则以路由为准，踢旧连接不承诺瞬时完成。

`/healthz` 表示进程存活；`/readyz` 分别检查 MySQL、Redis 路由和显式 Kafka Topic，任一失败为 503。Redis/Kafka 故障不阻止数据库历史接口服务；发送依赖或 dispatch 降级按上述契约表达。关闭先排空 HTTP/RPC，超出 5 秒则强制关闭，再关闭工作协程及客户端；Kafka writer 清理还可能等待其有界写超时，不能把 5 秒宣称为总退出上限。

测试说明见[测试环境](../test-environment.md)。已验证真实 MySQL 下并发幂等/事务回滚/成员与白名单竞争、真实 Redis 下租约和条件更新、真实 Kafka 下显式 Topic/分区/载荷，并以实际 Logic RPC 连接两个真实 Comet 验证注册、续租、Kick、旧连接清理与持久化。HTTP/MCP 验证已授权发送、撤权重试、历史读取及大整数同步；跨组件编排的故障用例用捕获 Publisher 定位提交后降级，Kafka adapter 另以真实 Broker 验证。

公开 `/wss`、Comet 用户连接循环、Job Kafka Fetch/Commit/DLQ、前端 v2 和全拓扑脚本仍属于其他角色/能力，不能据 Logic 完成宣称整个迁移完成。

## 逐块交付与验证

普通单聊、群消息和 MCP 可按业务块独立评审；RPC 装配、路由、Producer 与同步按实际依赖推进，不固定 PR 数量或编号。当前无生产数据，直接初始化新空库，不做旧行回填。每个 PR 说明调用方与协议变化；验收覆盖同 ID 并发、幂等冲突、越权、群成员竞争、MCP 白名单、路由失效与提交后发布失败。
