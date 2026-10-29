# Fable Chat

Fable 是参考 KamaChat 业务组织方式的 Go 聊天学习项目，包含用户与会话、文本单聊、部分群聊功能，以及供外部 AI 客户端调用的 MCP Server。

**当前状态：** 单实例聊天与 MCP 链路已有实现和测试；Redis 用于缓存。Kafka 模式、Docker Compose 和 K8s 双副本清单已经存在，但跨实例消息路由、消息确认与重放幂等尚未完成，不能据此宣称可靠分布式聊天或高可用。

三角色迁移已交付独立的 [Comet 投递内核](internal/comet)，包含本地连接、WebSocket 写出和 RPC 适配；尚未接入现有入口与真实 Logic，当前启动方式不变。

## 文档入口

| 任务 | 入口 |
| --- | --- |
| 理解系统、数据流、设计取舍与已知缺口 | [总架构](docs/architecture.md) |
| 理解已接受的 goim 三角色设计、角色接口与交付依赖 | [迁移设计（已接受，实施进行中）](docs/design/goim-migration.md) |
| 写模块前核对职责与协作 | [Logic](docs/contracts/logic.md)、[Comet](docs/contracts/comet.md)、[Job](docs/contracts/job.md) 角色契约（长期保留） |
| 查看 RPC/DTO spec 与 HTTP code-first 维护规则 | [接口契约与文档维护](api/proto/README.md) |
| 准备测试依赖并运行测试 | [测试环境](docs/test-environment.md) |
| 理解部署样例、初始化前提与发布方式 | [部署说明](deploy/k8s/README.md) |
| 配置部署密钥 | [密钥说明](deploy/secrets/README.md) |
| 使用压测脚本并记录结果 | [压测说明](tests/load/README.md) |

接口字段、环境变量和 MCP Schema 以总架构链接的源码与测试为准，避免在多份文档中重复维护。旧 MVP 规划、个人设计、面试笔记和未填写的报告已从当前文档集中移除；需要研究演进过程时，可阅读[清理前的文档快照](https://github.com/LosAveRoad/fable/tree/2d85cdbf1f229deb4fa17518ff7a1c419a08a893/docs)。该快照不作为当前契约。

## 本地启动后端

从仓库根目录执行。准备满足 [go.mod](go.mod) 的 Go 工具链，以及可连接、已创建开发数据库的 MySQL。

```sh
cp config/config.example.toml config/config.toml
```

编辑本地配置中的 MySQL 用户名、密码和数据库名，并替换 JWT 示例密钥。配置加载顺序为 TOML 后叠加环境变量；完整覆盖项见 [config.go](internal/config/config.go)。本地配置已被 Git 忽略。

首次运行可关闭 Redis 和 Kafka，先验证单实例链路：

```sh
export FABLE_REDIS_ENABLED=false
export FABLE_KAFKA_ENABLED=false
go run ./cmd/my_chat_server
```

PowerShell 中用 `$env:FABLE_REDIS_ENABLED = 'false'` 和 `$env:FABLE_KAFKA_ENABLED = 'false'` 设置相同开关。进程启动时会连接 MySQL 并通过 GORM AutoMigrate 建表。

```sh
curl --fail http://localhost:8080/healthz
```

返回 `ok` 只说明 HTTP 入口可响应，不能证明消息消费、持久化或投递正常。主要入口为 HTTP API、`/wss` 和 `/mcp`，具体边界见总架构。

## 前端与学习约定

浏览器前端位于 [web/chat-server](web/chat-server)，使用 React/Vite，构建方式见其 [package.json](web/chat-server/package.json)。现有前端工作流单独发布静态资源；后端 Docker 镜像没有打包前端。

核心业务代码由项目作者手写，不由 AI 直接生成。AI 可用于解释、审查和文档整理；单个函数的补全必须能逐行解释并自行修改。参考 KamaChat 的业务边界和组织方式时，应记录差异与取舍，不把参考实现的能力当作 Fable 已完成的能力。

## License

Fable 的自有代码采用 [MIT License](LICENSE)。
