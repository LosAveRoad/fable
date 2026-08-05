# MVP2：AI 助手与自研 MCP Server

## 1. 阶段目标

把 AI 设计成受权限约束的聊天参与者，而不是普通的“调用模型返回文本”。自研 MCP Server 将聊天系统中的可授权能力暴露为工具，Agent 根据用户意图选择工具并把结果作为聊天消息异步返回。

## 2. 产品场景

必须完成三个高价值场景：

- 总结当前用户有权查看的最近一段群聊。
- 从群聊中提取待办并写入聊天系统的待办表。
- 在当前用户权限范围内搜索历史消息并回答问题。

可选：

- 查询群成员和用户资料。
- 生成会议纪要。
- 查询系统健康状态。
- 对长会话进行分段摘要。

## 3. MCP 工具草案

```text
get_recent_messages
search_messages
get_group_members
create_chat_todo
list_chat_todos
```

每个工具必须定义：

- 输入 JSON Schema。
- 输出结构。
- 调用者身份如何传递。
- 权限检查位置。
- 最大查询范围。
- 超时和错误码。
- 审计字段。

## 4. AI 调用链路

```text
用户 @AI
→ 普通消息先落库
→ 创建 ai_jobs 任务
→ 后台 Worker 调用模型
→ 模型选择 MCP Tool
→ MCP Server 二次鉴权并访问聊天 API/数据库
→ 模型组织答案
→ AI 消息落库
→ WebSocket 推送结果
```

AI 处理不得阻塞 WebSocket 读循环。

## 5. 推荐模型

```text
ai_jobs
ai_tool_audits
chat_todos
```

`ai_jobs` 至少记录：请求人、会话、触发消息、状态、超时、错误摘要和结果消息 ID。

`ai_tool_audits` 至少记录：job、tool、调用者、参数摘要、耗时、结果状态。不要记录完整 Token、密钥或不必要的私人消息正文。

## 6. 安全要求

- 模型传入的 user_id、group_id 都不可信。
- MCP 工具根据服务端身份重新验证会话和群权限。
- 限制单次消息数量、时间范围、结果长度和工具调用次数。
- 防范提示词注入：聊天消息只能作为数据，不能自动获得系统级权限。
- 敏感配置通过环境变量或密钥系统提供。
- 用户能够识别 AI 生成内容和失败状态。

## 7. 推荐实现顺序

1. 定义 AI 用户和消息类型。
2. 实现 ai_jobs 状态机和异步 Worker。
3. 手写一个只返回固定结果的最小 MCP Server，打通协议。
4. 实现 get_recent_messages 及权限检查。
5. 接入模型的工具调用。
6. 实现 search_messages。
7. 实现 create_chat_todo。
8. 增加超时、取消、限流和审计。
9. 注入越权和提示词攻击测试。

## 8. 本阶段参考 KamaChat 文件

KamaChat 没有 AI/MCP 成品，本阶段是你最主要的原创增量。不要另起一套聊天系统，而是复用它已有的消息链路：

1. 消息协议入口：`internal/dto/request/chat_message_request.go`、`message_request.go`。
2. 消息持久化：`internal/model/message.go`、`internal/service/gorm/message_service.go`。
3. 实时收发：`internal/service/chat/client.go`、`server.go`。
4. Controller 与路由：`api/v1/message_controller.go`、`internal/https_server/https_server.go`。
5. 用户和群权限：`user_contact_service.go`、`group_info_service.go`。
6. 前端展示：`web/chat-server/src/views/chat/contact/ContactChat.vue`、`src/store/index.js`。

参考目的不是找 MCP 答案，而是回答：AI 消息如何复用普通消息落库、会话列表、未读数、群权限和 WebSocket 推送。MCP Server、`ai_jobs`、工具审计和 Agent 编排均由你自行设计。

完整映射见 [KamaChat 参考映射](kamachat-reference.md#mvp2ai-与自研-mcp)。

## 9. 技术提示

- MCP 是工具协议，不负责替代你的业务权限层。
- MCP Server 优先调用受保护的内部应用服务，不要让模型直接拼接 SQL。
- AI 回复使用普通 messages 表，才能复用历史、未读和 WebSocket 推送链路。
- AI 任务状态建议为 pending、running、succeeded、failed、cancelled、timed_out。
- 长对话先做截断和分段摘要，不要把全部历史无边界塞进上下文。
- 测量端到端延迟、工具调用次数、Token 使用量和失败率。

## 10. 必须验收

- AI 能完成三个必做场景。
- 用户不能让 AI 查询自己无权访问的群或私聊。
- 恶意聊天文本不能诱导 MCP 绕过权限。
- 模型超时不影响普通聊天。
- 工具失败会产生明确的任务状态和用户提示。
- 每次工具调用都有可检索的审计记录。
- 相同 AI 请求重复提交具有幂等或明确的重复处理策略。

## 11. 面试追问

- MCP 和普通 Function Calling 有什么区别？
- 为什么权限检查不能交给模型？
- AI 调用为什么要异步化？
- 如何处理提示词注入和越权查询？
- 如何限制模型成本和上下文长度？
- MCP 工具失败后是否重试，哪些错误不能重试？
