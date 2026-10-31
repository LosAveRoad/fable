# 测试环境

本文说明本地测试依赖与执行方式。系统行为和测试覆盖边界见[总架构](architecture.md)。维护者为仓库维护者，测试入口变化时应同步本文。

## 前提

- 使用满足 [go.mod](../go.mod) 的 Go 工具链，从仓库根目录执行命令。
- 普通单元测试使用 mock/miniredis 等测试替身，不要求外部 MySQL、Redis 或 Kafka。
- 旧单体的 `internal/integration` 测试要求 MySQL 和 Redis；新 Logic 的后端和 Comet 联调见下方独立服务验证。
- race detector 需要平台支持的 C 工具链；Windows 可使用 64 位 MSYS2 UCRT64 GCC。

集成测试会清理所选数据库中的业务表，并扫描删除所选 Redis DB 中的 `fable:v1:*`。必须使用专用测试数据库和独立 Redis 实例；当前测试没有提供 Redis DB 编号覆盖。

## 准备集成测试依赖

以下容器只用于本地测试，绑定回环地址。MySQL 密码为公开测试值，不用于部署。

```sh
docker run -d --name fable-test-mysql -p 127.0.0.1:3306:3306 -e MYSQL_ROOT_PASSWORD=mychat-dev-password -e MYSQL_DATABASE=mychat_test mysql:8.4
docker run -d --name fable-test-redis -p 127.0.0.1:6379:6379 redis:7.4-alpine
```

若容器已创建，使用 `docker start fable-test-mysql fable-test-redis`。执行下列命令确认依赖已就绪；MySQL 尚在初始化时需等待后重试：

```sh
docker exec fable-test-mysql mysqladmin ping -h 127.0.0.1 -uroot -pmychat-dev-password --silent
docker exec fable-test-redis redis-cli ping
```

默认连接与覆盖方式以 [test_helper_test.go](../internal/integration/test_helper_test.go) 为准：

| 依赖 | 默认值 | 可覆盖项 |
| --- | --- | --- |
| MySQL | `127.0.0.1:3306`，`root`，`mychat-dev-password`，`mychat_test` | `TEST_MYSQL_HOST`、`TEST_MYSQL_PORT`、`TEST_MYSQL_USER`、`TEST_MYSQL_PASSWORD`、`TEST_MYSQL_DATABASE` |
| Redis | `127.0.0.1:6379`，无密码，DB 0 | `TEST_REDIS_HOST`、`TEST_REDIS_PORT`、`TEST_REDIS_PASSWORD` |

Redis DB 固定为 0；应用的 `FABLE_*` 配置不会替代旧单体测试配置。

## 执行测试

```sh
go test -count=1 ./...
go test -count=1 -tags=integration ./internal/integration
go test -count=1 -race ./...
```

第三条命令没有 `integration` 标签，不包含集成测试。每次记录结果时应保留提交 SHA、环境、命令和原始输出，不以测试文件存在代替执行结果。

Windows 在独立 Redis 已就绪、GCC 已安装后，也可以执行：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\test.ps1
```

[test.ps1](../scripts/test.ps1) 会准备或复用 MySQL，运行单元、集成和普通 race 测试，并恢复临时修改的工具链环境变量；它目前不准备 Redis，也不运行 Kafka 测试。

## CI 与结果边界

[CI](../.github/workflows/ci.yml) 配置 MySQL 8.4、Redis 7.4、Kafka 3.9 和 Go 工具链，执行格式与 HTTP 生成物检查、全量单测、旧单体集成回归、Comet/Job race 测试及新 Logic 真实后端 race 测试。它不代表完整三角色公开 WebSocket 链路或 K8s 故障验收；容量测试另见[压测说明](../tests/load/README.md)，需保存实际运行证据。

## Logic 独立服务验证

新 Logic 使用独立新空库，示例配置 `config/logic.example.toml`；显式创建数据库后运行 `go run ./cmd/logic -config config/logic.example.toml init-schema`，正常运行使用 `serve`。初始化拒绝已有表，服务启动只检查结构。JWT、Job/Kick Token、逐 Comet 的 instance/boot/address/token 授权由环境提供，不把示例占位符用于真实服务。Kafka `fable.push.v1` 须由运维脚本/管理命令显式创建（首期 6 分区），Producer 不自动建 Topic。

HTTP 文档生成和漂移检查：

```sh
go run ./cmd/httpdocs
sh scripts/check-httpdocs.sh
node --test .github/scripts/check-docs.test.cjs
```

真实后端测试由环境选择，只连接隔离测试服务；MySQL 账号须能创建/删除 `fable_logic_test_*` 库，测试只清理自己创建的库和 Redis 命名空间。Kafka 测试创建隔离后缀 Topic，不消费旧消息命令 Topic。

```sh
# 先配置 FABLE_LOGIC_TEST_MYSQL_DSN（parseTime=true/UTC）、
# FABLE_LOGIC_TEST_REDIS_ADDR、FABLE_LOGIC_TEST_KAFKA_BROKER。
go test -p 1 -race -tags='integration mysql' ./internal/logic/... -count=1
go test -p 1 -race ./internal/comet ./internal/mcpserver/... ./internal/httpdocs ./internal/https_server
```

Logic 集成测试使用实际 MySQL、Redis、TCP Logic 和两个真实 Comet RPC/实例客户端，验证业务持久化、并发重复/冲突、群成员与 AI 白名单撤权、连接替换/Kick/旧 Disconnect、HTTP/MCP 和权威同步。编排级推送失败使用仅替换 Broker 边界的捕获 Publisher，真实 Kafka adapter 测试另验证 RequireAll/会话分区 Key/字节载荷/禁止自动建 Topic；不把捕获器视为实际 Kafka 全链路。

旧单体回归测试现支持 TEST_MYSQL_PORT / TEST_REDIS_PORT；显式空 TEST_MYSQL_PASSWORD 可用于隔离本机无密码测试实例。旧回归入口会清空所选业务表，仅允许指向测试库。CI 保留该回归并追加新 Logic 的真实后端 race 测试。
