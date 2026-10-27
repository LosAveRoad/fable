# Job 与 Kafka 推送契约

状态：Draft，与[迁移设计](../design/goim-migration.md)一起评审；尚未实现。Owner 为仓库维护者，生产/消费任务的 PR 作者共同维护本文。Logic 是 Producer；Job 是 Consumer；Comet 不连接 Kafka。

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

旧 `fable.chat.message` 仍是旧消费者处理的待落库命令，不能让 Job 接入。切换前清空旧链路，禁止往同一个 Topic 混写两种语义。

PushTaskV1 使用版本化 JSON，内部 RPC 使用 Protobuf；不复用旧 ChatEvent 来改变其含义。每条任务包含：

| 字段 | 含义与约束 |
| --- | --- |
| schema_version、kind | 固定 `1`、`message_push`；未知版本不能按成功跳过 |
| task_id、message_id | task_id=`push:v1:<message_id>:<chunk_index>`；重复发布保持不变 |
| conversation_id、created_at、request_id | 会话标识、数据库消息时间、可选追踪 ID |
| message | 已保存的 sender、receive_id/type、content、origin 与同步元数据；不能据此再创建消息 |
| recipients | 该稳定分片的 user_id；必须包含离线用户，用于消费时重新查路由 |
| targets | Logic 发布前查询到的 instance_id、connection_id、route_token 提示；无路由可为空 |
| chunk_index、chunk_count | 收件人快照排序去重后，每 100 个一片；编号从 0 开始 |
| pending_recipients | 仅 DLQ 重放可选：本次实际要处理的 recipients 子集；省略表示完整分片 |

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

死信包含 task_id、原 Topic/partition/offset、失败原因、未解决目标、首次/最后尝试时间及可重放的原消息。无法解析的任务保存原始字节及摘要并标记不可直接自动重放。日志只保留摘要，DLQ 自身包含业务正文，按敏感数据管理。

`replay-dlq.sh` 显式选择 run_id、时间或 offset 范围，先预览数量，修复原因后再执行；只重发原推送语义，不调用 Logic SubmitMessage，不制造新的业务消息 ID。重放事件记录 replay_run_id 和来源，不自动删除原记录或重置整个 Consumer Group。重复重放可重复推送，客户端按 message_id 合并；旧 Socket 兼容期可能显示重复，不能把旧协议称为幂等交付。

## 故障与可观测性

消费循环停止必须导致 not ready，并退出或被明确监督重启；不能复用当前只打一行日志的行为。监测 lag、每分区最后处理/提交时间、重试、offline、DLQ、commit 错误和各 Comet RPC 结果。没有消息时不能仅凭“很久没消费”判故障，需要区分空队列与循环死亡。

P9/P10 分开评审 Producer 和 Consumer，但切流前必须通过真实 Kafka 的联调：任意 Job 接到任务仍能投到目标 Comet；Job 数量变化不影响连接；重放不写 MySQL；部分成功不误提交；坏消息/DLQ 故障可见。端到端已读与强保证在线交付不在此契约内。
