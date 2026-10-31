# Job 与 Kafka 推送契约

状态：架构已接受；单任务投递内核已实现，Kafka 消费运行链路尚未接入。Owner 为仓库维护者，生产/消费任务的 PR 作者共同维护本文。Logic 是 Producer；Job 是 Consumer；Comet 不连接 Kafka。精确 JSON 结构以 [PushTask DTO](../../internal/dto/im/push.go) 与 [spec](../../api/proto/README.md) 为准。

本文长期保留，作为写 Job 代码前的消费、投递与恢复参考。任务字段以 DTO、RPC 方法与局部语义以 `.proto`/注释为准；消费流程、提交边界和重试行为变化时同步本文，不为纯字段变化复制另一份清单。

## 当前交付边界

[internal/job](../../internal/job) 的 `DecodeTask` 校验原始 JSON；拒绝重复/大小写歧义字段、未知字段、未配对 Unicode 转义、非法版本、消息、收件人、路由提示及重放上下文。失败只返回安全的错误类别；后续消费者必须保存原始字节，不能把无效任务重新编码成当前 DTO 后写 DLQ。普通任务可用 hints，重放任务只处理 pending 子集且重新查路由。

`NewProcessor` 接受 Logic 查询客户端、Comet 客户端选择器和 Job 角色凭据。`Process(ctx, raw)` 只处理一条任务：有序校验 Logic 的路由/实例租约，按实例解析地址并调用 Comet，保留已入队、去重和明确离线的终结结果。已知租约在投递前及客户端选择后重新检查，过期目标进入有界修复/pending；不因 RPC 在途过期而丢弃已确认入队结果。响应缺项、错序、非法 oneof/枚举或过期租约不能冒充成功或离线；身份比较忽略未知 Protobuf 扩展。错误描述不透传上游地址、身份、令牌或正文。

默认最多 3 轮，共用 10 秒预算；每次 RPC（含客户端选择）最多 2 秒，轮间退避 200 ms、1 s。`Options` 可缩短预算和退避，不能延长默认上限。初次无提示时的路由查询不算修复；遇到失效实例、旧 boot、stale 连接或不确定投递时，整个任务周期最多追加一批路由修复，之后仍不确定的目标保留 pending。明确 SAME_REQUEST 的错误可按原路由有界重试；权限、认证及 DO_NOT_RETRY 错误停止对应目标。实例批次先顺序处理，所有 RPC 和等待均受任务/调用方截止时间限制。

`Result` 保留完整任务及本次处理范围内的有序逐用户结果，`PendingRecipients()` 给出未终结子集；`Attempts` 计入查询/解析失败的任务轮次。存在 pending 时返回 `ErrUnresolved`，取消/截止同时保留 context 错误和已知进展。nil error 只说明全部本次目标已终结，不等于客户端收到，也不执行 offset 提交。

当前未实现 Kafka Fetch/Commit、DLQ 写入/重放命令、跨任务冲突检测/报警、管理端点或独立 Job 进程；不接入旧命令 Topic、不访问数据库。RPC 客户端的连接池、关闭和传输安全由调用方负责；选择器必须遵守 context，不从任务接收地址。Job 内核测试使用真实 gRPC 与两个真实 Comet 内核，并以测试服务器隔离 Logic 依赖；真实 Logic 服务已在独立入口实现，但尚未通过 Job 消费入口接通完整聊天链路。

## 角色接口

Job 没有用户业务 API，也没有持久化消息 RPC。它暴露本机 `/healthz`、`/readyz` 和指标，消费 PushTaskV1，调用 [Logic](logic.md) 的 LookupRoutes/ResolveComets 与 [Comet](comet.md) 的 Deliver。单次 DLQ 重放使用 Job 的命令行模式，不增加第四个常驻服务。

## Topic 与消息

| 配置 | 初始提案 |
| --- | --- |
| 新推送 Topic | `fable.push.v1`；测试运行加隔离后缀 |
| Consumer Group | `fable-push-job-v1`，所有 Job 副本共用，测试运行隔离 |
| DLQ | `fable.push.dlq.v1`；初始保留 7 天，需受限访问 |
| 分区 | 本地初始化 6 个；脚本显式创建并验证，不依赖自动创建 |
| 生产确认 | RequireAll；单 Broker 开发环境不意味着副本容灾 |
| Key | canonical conversation_id，单聊 `direct:<session_uuid>`，群聊 `group:<group_uuid>` |

旧 `fable.chat.message` 是旧消费者处理的待落库命令，不能让 Job 接入。新开发环境用独立 Topic/Group，不迁移旧开发 backlog，禁止混写两种语义。

PushTaskV1 使用版本化 JSON，内部 RPC 使用 Protobuf；不复用旧 ChatEvent 来改变其含义。每条任务包含：

| 字段 | 含义与约束 |
| --- | --- |
| schema_version、kind | 固定 `1`、`message_push`；未知版本不能按成功跳过 |
| task_id | `push:v1:<message.message_id>:<chunk_index>`；重复发布保持不变 |
| request_id | 可选追踪 ID，不用于幂等 |
| message | 完整 Message：message_id、conversation_id、created_at、position、sender、目标、正文、来源；顶层不复制这些字段，不能再次存库 |
| recipients | 该稳定分片的 user_id；必须包含离线用户，用于消费时重新查路由 |
| targets | Logic 发布前查询到的 instance_id、connection_id、route_token 提示；无路由可为空 |
| chunk_index、chunk_count | 收件人快照排序去重后，每 100 个一片；编号从 0 开始 |
| pending_recipients、replay_run_id | 仅重放填写：非空未完成目标子集与重放标识；省略子集表示完整分片，显式 null/空数组非法 |

单条任务上限 256 KiB，Logic 在落库前验证单消息内容可容纳；快照持久化后可还原同一分片。一次请求多片发布可能部分成功，SendResult 必须表达 unknown，不假装原子发布。相同 task_id 的消息业务内容和 recipients 不改变，targets 允许在重新发布时刷新；task_id 冲突但业务内容不同必须报警。

DLQ 与重放保留原 message 和完整 recipients，不修改 task_id 的业务身份；另附 pending_recipients 来缩小本次投递范围。Job 校验它是完整 recipients 的子集，缺少原任务或含额外用户时拒绝并报警；目标提示按子集重新查询。该字段不参与业务内容冲突判断，普通 Logic Producer 不填写它。

同一 Key 只约束 Broker 分区，不保证多个 Logic 的数据库提交顺序与发布顺序一致。扩分区会改变 Key 映射，不在本阶段运行时随意修改。Job 的并发单位先是分区，批量目标内部可有界并发，但需等该任务所有结果终结。

## 消费算法与提交

1. Fetch 后检查 Schema、大小和 ID；无法处理的任务写 DLQ，成功后才可提交原 offset。
2. 使用 targets 作为首次路由提示，经 Logic 解析有效 Comet 地址后按实例聚合调用 Deliver；没有提示、实例失效或目标 stale 时，对对应 recipients 调用 LookupRoutes。
3. LookupRoutes 明确无有效连接的用户记为 offline；网络/Redis 错误不能当 offline。连接可能正在迁移，stale 后最多做一次最新路由修复，再进入统一有界重试预算。
4. 逐目标保留 ENQUEUED/DUPLICATE/offline 终结结果；仅重试尚未终结的目标，避免整批反复发送。RPC 响应丢失无法确定入队，允许重复尝试。
5. 一次任务初始重试预算 3 次投递尝试，总时长最多 10 秒，退避 200 ms、1 s；路由修复也计入预算。预算耗尽，把未解决目标与原任务写 DLQ。
6. 所有目标均入队、去重、确认离线，或成功写入 DLQ 后，才提交该原 offset。DLQ 或 commit 失败不跳过任务，退避并使健康状态反映故障；进程重启允许重放。

“最多一次路由修复”按一次任务处理周期计；消费者重启后可重新执行整个周期。3 次/10 秒是默认配置提案，不能同时层层重试让总时间无限放大。DLQ 保存完整原任务和单独的未终结目标子集，重放用 pending_recipients 指定这些用户，保留原 task_id/message_id 与完整 recipients，不主动重复处理已知成功的目标。

同一分区一次只处理一个未提交任务；提交成功才处理后续任务。不能在 partition 的 offset 10 失败时提交 offset 11，让 10 被间接跳过。遇到 rebalance/取消，停止接收新任务，在截止前完成当前任务并尝试提交；超过期限取消，由新持有者重放。不得在 Deliver 尚未返回时提前提交。

## DLQ 与人工重放

有效任务的死信包含 task_id、原 Topic/partition/offset、失败原因、未解决目标、首次/最后尝试时间及完整原任务。解码失败、未知 Schema 或语义验证失败都保存原始字节 base64，不把未知字段丢弃后塞进当前 DTO，也不为非法收件人伪造 pending 子集；这类记录不可直接自动重放。日志只保留摘要，DLQ 自身包含业务正文，按敏感数据管理。

`replay-dlq.sh` 显式选择 run_id、时间或 offset 范围，先预览数量，修复原因后再执行；只重发原推送语义，不调用 Logic SubmitMessage，不制造新的业务消息 ID。重放事件记录 replay_run_id 和来源，不自动删除原记录或重置整个 Consumer Group。重复重放可重复推送，客户端按 message_id 合并；旧 Socket 兼容期可能显示重复，不能把旧协议称为幂等交付。

## 故障与可观测性

消费循环停止必须导致 not ready，并退出或被明确监督重启；不能复用当前只打一行日志的行为。监测 lag、每分区最后处理/提交时间、重试、offline、DLQ、commit 错误和各 Comet RPC 结果。没有消息时不能仅凭“很久没消费”判故障，需要区分空队列与循环死亡。

Producer 和 Consumer 可分别评审，不固定 PR 数量或编号；接入实际使用前必须通过真实 Kafka 的联调：任意 Job 接到任务仍能投到目标 Comet；Job 数量变化不影响连接；重放不写 MySQL；部分成功不误提交；坏消息/DLQ 故障可见。端到端已读与强保证在线交付不在此契约内。
