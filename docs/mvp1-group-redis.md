# MVP1：联系人、群聊与 Redis

## 1. 阶段目标

在 MVP0 的可靠消息核心上增加社交关系和群聊，并使用 Redis 解决已经测量到的热点读取、在线状态过期、未读计数和短期幂等问题。MySQL 仍是事实来源。

## 2. 必须完成

### 联系人

- 发起好友申请、同意、拒绝。
- 查询联系人列表。
- 删除联系人、拉黑和解除拉黑。
- 所有状态变化都有明确权限和幂等语义。

### 群聊

- 创建群、查询群信息。
- 邀请/申请加入群、退群、踢人、解散群。
- 群主、管理员、普通成员三种角色。
- 全员禁言或成员禁言至少实现一种。
- 群消息落库、实时广播、历史查询和未读数。
- 第一版沿用 KamaChat 的 `group_info` 和成员 JSON 表达完成复刻；必须记录其查询和并发修改问题，再决定是否拆分 `group_members`。

### Redis

- 在线状态使用 TTL，并通过心跳续期。
- 热点会话、群成员或历史页至少选择一个场景实现 Cache Aside。
- 未读数或 last_read_sequence 至少选择一种高频状态进行 Redis 优化。
- Redis 不可用时，核心收发消息可降级到 MySQL/本机状态。
- 禁止业务请求使用 `KEYS` 扫描；使用结构化 Key 和必要时的 SCAN。

## 3. 明确不做

- 多实例跨节点消息投递。
- Kafka 和 K8s。
- AI、MCP、音视频。
- 万人群和完整消息漫游。

## 4. 与 KamaChat 对齐的模型

```text
contact_apply
user_contact
group_info
```

重点约束：

- 好友双方关系应有唯一性策略，避免重复申请和相反方向的重复关系。
- 先读懂 KamaChat 在 `contact_apply` 和 `user_contact` 中如何表达申请与正式关系。
- 为保持复刻主线，第一版群资料和成员关系沿用 `group_info` 的表达方式。
- KamaChat 将 members/admins 存为 JSON；你应先实现并记录其查询和并发修改问题，再把拆分 `group_members` 表作为有证据支撑的增强项。
- 群消息继续复用 MVP0 的 `session/message`，不另外复制一套消息表。

## 5. Redis Key 草案

```text
presence:user:{user_id}                 String + TTL
conversation:{conversation_id}:summary Hash/String + TTL
group:{group_id}:members                Set/Hash + TTL
unread:user:{user_id}                   Hash
idempotency:{user_id}:{request_id}      String + TTL
```

每个 Key 必须记录：写入者、读取者、TTL、MySQL 来源、删除/重建方式。

## 6. 推荐实现顺序

1. 好友申请状态机。
2. 联系人权限对单聊的影响。
3. 按 KamaChat 模型实现群与群成员关系。
4. 群管理接口。
5. 群消息权限校验与广播。
6. 未读与 last_read_sequence。
7. 对 MVP0 做基准测试并选择缓存目标。
8. Redis 连接、Key 规范和 Cache Aside。
9. 心跳与在线状态 TTL。
10. Redis 故障降级和缓存一致性测试。

## 7. 本阶段参考 KamaChat 文件

1. 联系人模型与枚举：`internal/model/contact_apply.go`、`user_contact.go`，以及 `pkg/enum/contact*`。
2. 联系人后端：`api/v1/user_contact_controller.go`、`internal/service/gorm/user_contact_service.go`，以及 apply/pass/refuse/black/delete/get contact DTO。
3. 群模型与枚举：`internal/model/group_info.go`、`pkg/enum/group_info/*`。
4. 群后端：`api/v1/group_info_controller.go`、`internal/service/gorm/group_info_service.go`，以及 create/enter/leave/dismiss/update/remove/get group DTO。
5. 群会话与消息：继续参考 `session_controller.go`、`message_controller.go`、`session_service.go`、`message_service.go` 中 group 分支。
6. Redis：`internal/service/redis/redis_service.go`，并检索 `session_service.go`、`message_service.go`、`group_info_service.go`、`user_contact_service.go` 中所有 Redis Key。
7. 群广播：`internal/service/chat/server.go` 中 `ReceiveId` 为 Group 的分支。
8. 前端联系人：`src/views/chat/contact/ContactList.vue`、`ContactListModal.vue`。
9. 前端群聊：`SessionList.vue`、`ContactChat.vue` 中群会话、群消息和成员管理部分。
10. 后台管理属于可选：`src/views/manager/Manager.vue` 和各类 Admin Modal；不影响 MVP1 门禁。

完整映射见 [KamaChat 参考映射](kamachat-reference.md#mvp1联系人群聊与-redis)。

## 8. 技术提示

- 群消息主体保存一份，成员通过 last_read_sequence 表示阅读位置。
- 广播时先在锁内复制在线 Client 引用，释放锁后投递。
- Cache Aside 更新一般采用“更新 MySQL 后删除缓存”，不要声称实现了强一致。
- 在线状态必须有 TTL；Pod/进程崩溃时不能依赖正常退出清理。
- 热点群成员缓存需要版本或失效策略，成员变更后避免长期脏读。
- Redis 优化前后要记录查询次数、P95 延迟和缓存命中率。

## 9. 必须验收

- 好友和群成员状态机不能越权跳转。
- 非群成员不能发送或读取群消息。
- 并发加群/退群不会产生重复成员。
- 群消息对在线成员实时可见，对离线成员可通过历史记录恢复。
- 清空 Redis 后系统能从 MySQL 重建缓存。
- Redis 停止后单聊和群聊核心链路有明确降级行为。
- 心跳停止后 presence Key 自动过期。
- 慢成员不会阻塞整个群广播。
- 有缓存前后的性能数据，而不只是“感觉更快”。

## 10. 面试追问

- Redis 在系统中哪些数据是缓存，哪些是短期状态？
- 为什么在线状态不能只存在 Redis 且永不过期？
- 群消息为什么不按成员复制 N 份？
- 未读数如何计算，发生并发已读时怎么办？
- 删除好友是否删除历史会话？
- Cache Aside 会出现哪些不一致窗口？
- 大群广播如何进一步优化？
