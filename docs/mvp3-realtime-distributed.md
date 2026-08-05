# MVP3：音视频、Kafka 与 K8s

## 1. 阶段目标

完成两个最终能力：使用 WebRTC 建立一对一音视频通话；将聊天服务扩展为多个实例，通过 Redis 连接路由和 Kafka 消息流完成跨实例投递，最后使用 K8s 展示部署、扩容和故障恢复。

MVP3 对外是一个阶段，内部严格按 A/B/C 三个里程碑推进。

## 2. MVP3-A：WebRTC 音视频

必须完成：

- 一对一语音/视频呼叫。
- 邀请、接受、拒绝、忙线、取消、挂断、超时。
- WebSocket 仅传递信令：offer、answer、ICE candidate 和通话状态。
- WebRTC 传输媒体流。
- 配置 STUN；至少说明 TURN 在公网失败场景中的作用。
- 保存通话记录，但不保存媒体内容。

状态机建议：

```text
created → ringing → accepted → connected → ended
                  ↘ rejected
created/ringing   → cancelled/timed_out
任意活动状态       → failed
```

技术提示：

- 服务端不应把 SDP 和 ICE 当成普通聊天消息永久展示。
- 同一用户多设备和双方同时呼叫需要明确策略。
- 局域网直连成功不等于公网可用；记录 NAT/TURN 测试结果。
- 通话信令也必须鉴权，不能由消息体伪造 caller。

## 3. MVP3-B：Kafka 多实例投递

先在不使用 Kafka 的情况下启动两个聊天实例，复现：A 连接实例 1、B 连接实例 2 时，本机 Clients map 无法找到 B。

然后完成：

- 每个实例拥有唯一 server_id。
- Redis 保存 `user_id → server_id/connection_id`，并设置 TTL。
- 聊天消息持久化后发布到 Kafka。
- 目标实例消费并通过本机 WebSocket 推送。
- 使用稳定 Key 保证所需粒度的分区顺序。
- 消费处理幂等。
- 明确 offset 提交时机。
- 明确可重试错误、不可重试错误和死信处理。
- Kafka 不可用时有降级或待投递恢复方案。

Kafka 最小概念清单：

```text
broker, topic, producer, consumer,
consumer group, partition, key, offset,
ack, retry, idempotency
```

架构提示：

```text
Client A → Gateway 1 → MySQL/outbox → Kafka
                                      ↓
Client B ← Gateway 2 ← target routing/consumer
                     ↑
                   Redis
```

直接“落库后再发 Kafka”存在数据库成功但发布失败的窗口。基础版本可以先记录并演示该问题，进阶版本再实现 Transactional Outbox：同一数据库事务写 messages 和 outbox_events，由后台发布器可靠发送。

## 4. MVP3-C：K8s 部署

必须完成：

- Chat Server Deployment，至少两个副本。
- Service/Ingress 支持 WebSocket Upgrade。
- readiness 和 liveness 探针。
- ConfigMap/Secret 区分普通配置与敏感配置。
- 滚动更新时停止接收新连接并优雅关闭旧连接。
- Pod 删除后 Redis 连接路由能够过期或清理。
- 演示扩容、删除 Pod 和滚动发布。

项目演示可以将 Go 服务部署到 K8s，而 MySQL、Redis、Kafka 使用本地容器或托管服务。无需为了展示 K8s 而自行维护生产级 Kafka 集群。

## 5. 明确不做

- 多人音视频会议和 SFU/MCU。
- 端到端加密。
- 跨地域多活。
- Kafka 集群运维平台。
- 自动弹性扩缩容的完整生产参数调优。

## 6. 推荐实现顺序

1. WebRTC 浏览器点对点原型。
2. 复用 WebSocket 实现鉴权信令。
3. 通话状态机和异常恢复。
4. 手动启动两个 Chat Server 复现跨实例失败。
5. Redis 连接路由。
6. Kafka 单 Topic 最小投递。
7. 分区 Key、幂等、offset 和重试。
8. Outbox 可选增强。
9. 容器化 Chat Server。
10. K8s 双副本、探针、扩缩容和故障演示。

## 7. 本阶段参考 KamaChat 文件

### 音视频

1. AV 请求与响应：`internal/dto/request/av_data_request.go`、`internal/dto/respond/av_message_respond.go`。
2. 消息协议：`chat_message_request.go` 的 `av_data`，以及 `internal/model/message.go` 的 AV 字段和消息类型枚举。
3. 当前聊天室成员：`api/v1/chatroom_controller.go`、`internal/service/gorm/chatroom_service.go`、对应 request/respond DTO。
4. 服务端信令转发：`internal/service/chat/server.go` 中 AV 消息分支。
5. 浏览器 WebRTC：`web/chat-server/src/views/chat/contact/ContactChat.vue` 中 `RTCPeerConnection`、`getUserMedia`、offer/answer、candidate、start/reject/receive call 相关部分。

### Kafka

1. 依赖与配置：`go.mod`、`configs/config.toml`、`internal/config/config.go` 的 KafkaConfig。
2. 生命周期：`cmd/kama_chat_server/main.go` 中 Kafka 初始化、模式选择和关闭。
3. Producer：`internal/service/chat/client.go` 中 channel/kafka 分支。
4. Reader/Writer：`internal/service/kafka/kafka_service.go`。
5. Consumer 与消息处理：`internal/service/chat/kafka_server.go`。
6. 对照阅读：把 `kafka_server.go` 与 `server.go` 并排比较，找出重复业务逻辑、分区 Key、ACK、offset、路由和幂等方面的不足。

KamaChat 仓库没有 K8s 清单；容器化、探针、Ingress、滚动更新和 Pod 路由失效是本阶段的原创扩展。

完整映射见 [KamaChat 参考映射](kamachat-reference.md#mvp3音视频kafka-与-k8s)。

## 8. 必须验收

- 两个浏览器可以完成一次音视频呼叫全状态流程。
- 信令越权和伪造 caller 被拒绝。
- 两个 Chat Server 实例之间可以互发单聊和群聊消息。
- 同一会话消息顺序可解释并用测试验证。
- 重复消费不会重复生成业务消息或重复改变状态。
- 消费者在处理前后崩溃时，行为与 offset 策略一致。
- Kafka 暂停后消息不会被错误标记为已投递。
- 删除目标 Pod 后，旧路由最终失效。
- K8s 滚动更新不会造成数据库损坏，客户端能识别断线并重连。

## 9. 面试追问

- K8s Service 为什么不能直接解决跨 Pod WebSocket 投递？
- Redis 路由和 Kafka 分别解决什么问题？
- Kafka 为什么只能保证分区内有序？Key 如何选择？
- 消息落库成功但 Kafka 发布失败怎么办？
- offset 在什么时候提交？为什么？
- WebSocket、WebRTC、STUN、TURN 分别负责什么？
- Pod 被终止时如何优雅迁移长连接？

## WebSocket 生命周期与连接可靠性

WebSocket 生命周期优化归入 MVP3，和多实例消息路由一起完成，不在 MVP1 阶段提前扩展。

必须处理：

- 连接建立、认证、注册、替换旧连接和关闭的完整状态机
- 读循环、写循环和连接关闭之间的退出关系
- 用户断线、服务重启和 Pod 删除时的资源清理
- channel 关闭、发送超时、写失败和目标用户离线
- 心跳、读写超时和连接存活检测
- 同一用户多设备连接的策略
- 单连接只允许一个读协程和一个写协程
- WebSocket 连接与 Redis presence、实例路由信息的一致性
- 优雅停机时停止接收新连接，并等待或通知旧连接退出

验收场景：

```text
正常连接 → 收发消息 → 正常关闭
异常断线 → 服务端清理 → 重新连接
重复连接 → 按策略替换或保留旧连接
实例下线 → 路由过期 → 消息重新投递或恢复
```
