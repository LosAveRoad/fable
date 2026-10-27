# Comet 角色契约

状态：架构已接受；接口已细化为可编译 spec，服务尚未实现。Owner 为仓库维护者，修改连接和投递行为的 PR 作者同步本文。方法及字段以 [comet.proto](../../api/proto/fable/v1/comet.proto) 和 [DTO / RPC spec](../../api/proto/README.md) 为准。

本文长期保留，作为写 Comet 代码前的连接生命周期与协作参考；不因 `.proto` 存在而删除。RPC 字段及局部语义维护在 `.proto`/注释，职责或连接行为变化时同步本文。

## 职责与依赖

Comet 持有 WebSocket、本机连接表、读写协程和有界发送队列。它调用 [Logic](logic.md) 的注册、连接、续租、发送方法，暴露定向投递和关闭连接方法。它不初始化 MySQL、Redis 或 Kafka，不含会话/群/MCP 业务逻辑。

本机 Hub 从 [chatservice](../../internal/service/chatservice) 提取，保留每连接读写协程、慢连接关闭和指针/令牌匹配注销；不把原 HandleMessage 的数据库调用一起搬来。首期保留单 Hub 事件循环，避免把分桶优化和职责拆分混为一笔。

## 外部 WebSocket

路径保持 `/wss`；Upgrade 前验证 JWT，再由 Logic Connect 确认身份与路由。Origin 允许列表、读写期限和帧大小应配置；不能沿用无条件允许所有 Origin 作为正式跨域策略。

| 协议 | 输入 | 输出与兼容 |
| --- | --- | --- |
| legacy，默认 | 原 send_id、receive_id、content、receive_type | 保留原消息字段，额外元数据可忽略；不发送控制帧 |
| `protocol=v2` | SendFrame：event、client_message_id、receive_id/type、content；sender 来自连接身份 | 嵌套 MessageFrame 与 SendResultFrame；Message 中为 message_id/conversation_id/position，result/error 恰有一个 |

legacy 的 send_id 必须与已认证用户相同；v2 不携带任意 send_id/origin。OriginAI 不由用户指定。v2 发送结果按 client_message_id 关联到乐观气泡；持久化成功后使用 message_id 合并。控制帧字段为 Logic SendResult 或业务错误码；Comet 不把 RPC 失败转换成“未保存”的确定结论。

实时消息中的 position 只用于消息定位/排序；客户端同步水位只由成功合并的 HTTP sync 页推进，不能采用实时消息或发送确认里的最大值。

legacy 每次接收时生成一次 ID；跨帧或跨连接自动重试没有幂等保证。v2 断线后以同一 ID 在新有效连接上重试；服务器对未知 protocol 版本明确拒绝，不静默降级。

## CometService 内部 RPC

| 方法 | 调用方 | 输入 | 输出 / 副作用 | 重试与权限 |
| --- | --- | --- | --- | --- |
| Deliver | Job | 本机 CometIdentity、task_id、MessageView、最多 100 个完整连接目标 | 每个目标单独返回结果，不用一个 bool 表示整批成功 | Job 凭据；重复调用可能重复入队，客户端按 message_id 去重 |
| Kick | Logic | 本机 CometIdentity、目标 ConnectionIdentity、非零 reason | 仅匹配本机当前连接时关闭；不存在为幂等结果 | Logic 凭据；不得只按 user_id 误关新连接 |
| Drain | 本机管理入口 | 本机授权与截止时间 | 停止接新连接，登记 draining，排空或关闭已有连接 | 不暴露公网；重复调用不恢复为 active |

Deliver 目标包含 user_id、instance_id、connection_id、route_token；响应按请求顺序逐项回显完整目标与结果。RPC 必须定向访问具体 instance_id 的地址；请求的 CometIdentity 与本机 boot 不匹配时，整次 RPC 返回 FAILED_PRECONDITION 并要求 REFRESH_ROUTE，不触碰 Hub。只有实例身份正确而某个目标连接/令牌不匹配时，才返回该目标的 STALE；不得按 user_id 找到另一条连接就发送。

| 每目标结果 | 含义 | Job 后续行为 |
| --- | --- | --- |
| ENQUEUED | 已放入该连接 outbound | 本次投递尝试完成；不是 Socket 写成功 |
| DUPLICATE | 有界近期去重命中，之前已经入队 | 与 ENQUEUED 相同；不能解释为客户端已收 |
| NOT_FOUND / STALE | 本机不存在该连接或令牌不匹配 | 通过 Logic 查询最新路由；依赖错误不当 offline |
| QUEUE_FULL | 慢连接队列已满，开始关闭连接 | 有界重试/路由修复，超限 DLQ |
| DRAINING | 排空截止时间已到，不能接收本批投递；截止前允许投递已有 active 连接 | 由 Job 按退避策略处理 |

RPC 完成前通过带有结果回传的 Hub 命令确认实际连接检查/入队结果；不能继续沿用“仅 routeQueue 入队就返回 true”的含义。请求取消后不得无限等待 Hub。初始投递 deadline 为 2 秒；队列容量初始沿用现有 256，可配置。

## 连接状态机与租约

`pending → active → draining/closing → closed`。pending 创建本机对象，但只有 Connect 成功后才读取业务帧。注册失败或结果不确定关闭并条件清理。active 期间续租；被替换或 JWT 到期立即关闭。不能成功续租时不得越过本地租约安全截止时间继续服务；租约受 auth_expires_at 限制，安全余量为截止前 5 秒，剩余不足余量时停止发送。

本机连接的 Close 幂等；旧连接 Close 只条件删除自己的对象，并向 Logic 条件注销自己的路由。实例租约失效时停止所有相关连接。进程崩溃依赖 TTL 清理，正常退出尽力注销，不能承诺断电时立即消失。

新路由决定逻辑上的当前连接，但旧 Socket 的关闭可能延迟；本阶段不提供跨实例瞬时单活屏障。暂时的旧连接在途投递与处理方式见设计，不用多个 Hub 或 Kafka 分区模拟连接所有权。

## 交付与验证

独立 Comet、Kafka 联调与前端协议适配可以按可验证边界拆分，不固定 PR 编号；只有对端就绪且测试通过才能接入实际链路。验证重复 Kick、旧 Disconnect 不删新路由、pending 阶段投递、客户端取消、慢连接、JWT 到期、租约丢失、Drain 和 legacy 协议。Comet 不承担客户端消息历史缓存。
