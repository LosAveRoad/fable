# Fable Chat

Fable 是参考 goim 职责划分的 Go 聊天学习项目。Logic 处理 HTTP/MCP、业务与持久化；Comet 持有 WebSocket；Job 消费 Kafka 并定向投递。保留用户、联系人、会话、单聊、群聊和用户授权的 AI 代发规则。

**当前能力：** 三个角色有独立入口；本地脚本启动两个 Logic、两个 Comet、两个 Job 和统一浏览器入口。v2 前端支持稳定发送 ID、消息合并、重连与分页同步修复。真实 MySQL/Redis/Kafka 验证了跨实例收发、幂等、MCP、连接迁移、Job 重启与 DLQ。可靠性是“持久消息 + 尽力在线推送 + 同步修复”，没有 Outbox 或端到端已读保证。

## 本地启动

准备满足 [go.mod](go.mod) 的 Go、Python 3、Node.js 22.12+ 和 Docker Compose。容器仅监听回环；首次启动会下载依赖镜像并构建服务/前端。

```sh
./scripts/dev-up.sh
./scripts/dev-status.sh
```

打开 `http://127.0.0.1:18080`，注册两个测试账户并创建会话。`dev-up` 只为本地开发前缀的数据库准备结构，已有表不自动迁移；服务凭据与运行状态保存在忽略的私有目录。详细的依赖复用、端口、日志、启停和死信操作见 [本地部署](deploy/local/README.md)。

```sh
./scripts/dev-down.sh          # 停止本轮应用进程，保留基础设施和数据
./scripts/dev-down.sh --infra  # 同时停止本轮 Compose 服务，保留数据卷
```

也可分别运行 `cmd/logic`、`cmd/comet`、`cmd/job`；配置与服务凭据必须匹配，不把示例占位符用于真实环境。Kubernetes、跨主机 TLS、基础设施高可用与远程发布不在本阶段范围；旧 K8s/CD 材料不能用于运行新拓扑。

## 文档与验证

| 任务 | 权威入口 |
| --- | --- |
| 理解当前运行架构与失败边界 | [总架构](docs/architecture.md) |
| 查阅已接受的决策与迁移约束 | [设计基线](docs/design/goim-migration.md) |
| 写模块前核对职责与协作 | [Logic](docs/contracts/logic.md)、[Comet](docs/contracts/comet.md)、[Job](docs/contracts/job.md) |
| 查看接口与维护规则 | [.proto / DTO spec](api/proto/README.md)、[HTTP code-first](api/openapi/README.md) |
| 启停、配置、探针与重放 | [本地部署](deploy/local/README.md) |
| 准备隔离依赖、执行测试 | [测试环境](docs/test-environment.md) |
| 查阅交付进度和实际证据 | [Issue #7](https://github.com/LosAveRoad/fable/issues/7) |

```sh
go test -p 1 ./...
go vet -p 1 ./...
go test -p 1 -race ./internal/comet/... ./internal/job/...
sh scripts/check-httpdocs.sh
npm --prefix web/chat-server test
npm --prefix web/chat-server run build
```

真实后端与多进程冒烟需要专用测试依赖，具体环境变量见测试环境。不能以 mock 通过、Kafka ACK 或 healthz 代替完整链路验收。

## 学习与旧环境

架构和业务决策由项目作者审核；AI 可辅助实现、测试、解释和审查，合并前应能够说明数据流、失败语义及代码取舍。参考实现的能力不自动成为本项目的能力，性能数据只能来自实际测量。

旧 `cmd/my_chat_server` 及旧前端构建/部署样例保留为隔离回归参照，不与新拓扑共用消息数据库、路由前缀或旧 Kafka 命令 Topic。旧规划和分析可查[历史文档快照](https://github.com/LosAveRoad/fable/tree/2d85cdbf1f229deb4fa17518ff7a1c419a08a893/docs)，不作为当前契约。

## License

Fable 自有代码采用 [MIT License](LICENSE)。
