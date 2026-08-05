# Development Log

记录每个 PR 周期中遇到的问题、原因、解决方案和验证结果。

## PR #1：MVP0 单聊链路与 CI

### 遇到的问题

- Go 环境中存在多个 `go.exe`，导致 VS Code 使用了错误的 `GOROOT`，出现 `package unsafe is not in std`。
- VS Code 无法稳定识别 `gopls`、`goimports` 和自定义结构体类型。
- WebSocket 鉴权、连接身份和 `user_id` 的传递链路不完整。
- WebSocket 消息使用全局 channel 分发，多个连接会竞争读取消息。
- 会话表使用排序后的用户 UUID 保存，但消息发送时曾按单向关系查询会话。
- 登录查询没有按手机号过滤，导致登录和测试结果不稳定。
- MySQL 初始化没有自动迁移数据表。
- `TEXT/BLOB` 类型字段不能直接建立唯一索引，导致 MySQL 建表失败。
- 历史消息接口缺失，前端无法恢复聊天记录。
- 前端 HTML 的引号、结束标签和 JavaScript 内容曾被编码破坏，导致页面控件和消息发送不可用。
- 前端使用绝对地址连接 API/WebSocket，静态托管到 Gin 后需要改为同源地址。
- 发送消息时 WebSocket 尚未连接会被静默忽略，用户看不到任何反馈。
- 项目缺少 HTTP、WebSocket 集成测试和持续集成流程。
- 将全局消息 channel 改为按用户保存的 channel 后，`onlineUsers` map 忘记初始化，CI 首次运行 WebSocket 测试时触发 `assignment to entry in nil map`。
- WebSocket 集成测试使用随机用户 UUID 时，发送方可能是排序后的会话 UUID 中较大的一方；当前 `ReadPump` 的单向会话查询会因此找不到会话，造成 CI 随机失败。

### 本次处理

- 配置 VS Code 的 Go 工具链，并统一使用正确的 Go 环境。
- 增加 WebSocket JWT 中间件，校验 token 和 `client_id`。
- 完成 MVP0 的 WebSocket 连接、读写循环和消息持久化基础链路。
- 增加历史消息查询 API：`POST /message/getMessageList`。
- 启动时自动迁移 `user_info`、`session` 和 `message` 表。
- 修正用户表字段类型，使 UUID、手机号和昵称可以建立索引。
- 修正登录查询和登录响应中的 UUID、token 字段。
- 修复前端登录、会话、历史消息、WebSocket 连接和发送后的本地回显。
- 增加测试数据库 `mychat_test`，编写 HTTP 和双用户 WebSocket 集成测试。
- 增加 GitHub Actions CI：启动 MySQL 8.4，执行格式检查、单元测试和集成测试。
- 初始化在线用户 map，增加 `RegisterUser` 单元测试，并让 WebSocket 集成测试覆盖新的按用户 channel 分发结构。
- 让集成测试按照当前会话排序规则选择发送方，消除随机 UUID 导致的测试不稳定。

### 验证结果

```text
go test ./...
go test -tags=integration -count=1 ./internal/integration
```

两组测试均通过。当前暂不包含 CD 或部署流程。

## 后续 PR 记录模板

```markdown
## PR #N：标题

### 遇到的问题

- 

### 本次处理

- 

### 验证结果

```text
测试命令
```
```
