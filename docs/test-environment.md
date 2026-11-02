# 测试环境

本文说明本地测试依赖与执行方式。系统行为和测试覆盖边界见[总架构](architecture.md)。维护者为仓库维护者，测试入口变化时应同步本文。

## 前提

- 使用满足 [go.mod](../go.mod) 的 Go 工具链，从仓库根目录执行命令。
- 普通单元测试使用 mock/miniredis 等测试替身，不要求外部 MySQL、Redis 或 Kafka。
- 旧单体回归使用 MySQL 和 Redis；三角色后端测试与完整拓扑验收另外使用 Kafka，见下方分开的入口。
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

[CI](../.github/workflows/ci.yml) 使用 MySQL 8.4、Redis 7.4、Kafka 3.9，执行格式、固定 protoc 33.4 的 Proto 漂移检查与 HTTP 生成物检查、全量 Go 单测与 vet、Comet/Job race、旧单体业务回归、真实后端 race、前端状态测试与生产构建、本地入口和生命周期测试，以及下面的完整三角色验收。另起一套托管 Compose 验证 dev-up、status、选定环境 smoke、重复 up，再运行公开 HTTP/WS/MCP 黑盒流程与两对用户的短时消息负载，最后始终退出拓扑。

CI 保存公开黑盒与负载 JSON 报告，以及仅含安全关联字段的 Logic/Comet/Job 角色日志；不上传含凭据的 private.json 或生成配置。测试证明本地多进程的正确性，不代表 Kubernetes、跨主机安全、基础设施容灾或容量验收；压测另见[压测说明](../tests/load/README.md)，需保存实际运行证据。

## 三角色后端验证

新 Logic 使用独立新空库，示例配置 `config/logic.example.toml`；显式创建数据库后运行 `go run ./cmd/logic -config config/logic.example.toml init-schema`，正常运行使用 `serve`。初始化拒绝已有表，服务启动只检查结构。JWT、Job/Kick Token、逐 Comet 的 instance/boot/address/token 授权由环境提供，不把示例占位符用于真实服务。Kafka `fable.push.v1` 须由运维脚本/管理命令显式创建（首期 6 分区），Producer 不自动建 Topic。

Proto 和 HTTP 文档生成、漂移检查（Proto 要求 protoc 33.4；脚本固定 Go 插件版本）：

```sh
sh scripts/generate-proto.sh --check
go run ./cmd/httpdocs
sh scripts/check-httpdocs.sh
node --test .github/scripts/check-docs.test.cjs
```

真实后端测试由环境选择，只连接隔离测试服务；MySQL 账号须能创建/删除 `fable_logic_test_*` 库，测试只清理自己创建的库和 Redis 命名空间。Kafka 测试创建隔离后缀 Topic，不消费旧消息命令 Topic。

```sh
# 先配置 FABLE_LOGIC_TEST_MYSQL_DSN（parseTime=true/UTC）、
# FABLE_LOGIC_TEST_REDIS_ADDR、FABLE_LOGIC_TEST_KAFKA_BROKER。
go test -p 1 -race -tags='integration mysql' -count=1 -timeout=180s ./internal/logic/... ./internal/job/... ./internal/localdev
go test -p 1 -race ./internal/comet ./internal/mcpserver/... ./internal/httpdocs ./internal/https_server
```

Logic 集成测试使用实际 MySQL、Redis、TCP Logic 和两个真实 Comet RPC/实例客户端，验证业务持久化、并发重复/冲突、群成员与 AI 白名单撤权、连接替换/Kick/旧 Disconnect、HTTP/MCP 和权威同步。编排级推送失败使用仅替换 Broker 边界的捕获 Publisher，真实 Kafka adapter 测试另验证 RequireAll/会话分区 Key/字节载荷/禁止自动建 Topic；不把捕获器视为实际 Kafka 全链路。

旧单体回归测试现支持 TEST_MYSQL_PORT / TEST_REDIS_PORT；显式空 TEST_MYSQL_PASSWORD 可用于隔离本机无密码测试实例。旧回归入口会清空所选业务表，仅允许指向测试库。CI 保留该回归并追加新 Logic 的真实后端 race 测试。

## 完整本地拓扑验收

开发启动与运维命令见[本地部署](../deploy/local/README.md)。对已运行入口的独立公共流程测试与负载测量见[黑盒与压测命令](../tests/load/README.md)，该驱动不访问数据库、Redis、Kafka 或内部 RPC。以下两种 smoke 各有用途：

| 入口 | 验证对象 | 数据范围 |
| --- | --- | --- |
| `python3 scripts/local/manage.py smoke` | 当前运行目录选定的入口、两向消息及持久化 | 临时随机 UUID 用户/会话，精确清理；不初始化表、不改 Topic |
| `./scripts/smoke.sh -timeout=240s` | 新建两份每角色的完整隔离进程，测试后退出 | 新建 `fable_e2e_*` 库、独立 Redis 命名空间和带隔离后缀的 Kafka Topic/Group |

完整验收需要显式提供以下环境；MySQL DSN 不选择业务库，账号须能创建/删除测试前缀库。示例端口与密码需改成自己的隔离测试依赖。

```sh
export FABLE_LOGIC_TEST_MYSQL_DSN='root:test-password@tcp(127.0.0.1:3306)/?charset=utf8mb4&parseTime=true&loc=UTC'
export FABLE_LOGIC_TEST_REDIS_ADDR=127.0.0.1:6379
export FABLE_LOGIC_TEST_KAFKA_BROKER=127.0.0.1:9092
./scripts/smoke.sh -timeout=240s
```

[拓扑测试](../internal/e2e/topology_test.go) 启动真实角色命令，通过入口使用公开 HTTP、WebSocket v2 与 MCP，验证 100 条双向消息、群聊、相同 client_message_id 重试的一次持久化、AI 来源、Job 全部停止后排队与重启投递、换 Comet 后踢旧连接、离线同步、无效原始字节进入 DLQ、无效重放不发布，以及路由丢失后的租约关闭与整轮重启。测试清理自己创建的数据库、路由命名空间及隔离 Kafka Topic，不重置其他消费组。可设 `FABLE_E2E_ARTIFACT_DIR` 在失败时复制角色日志，禁止收集配置或私有清单。

本地脚本和浏览器状态的快速检查不需要外部依赖：

```sh
python3 -m unittest discover -s scripts/local -p 'test_*.py'
node --test deploy/local/proxy.test.mjs
cd web/chat-server
npm ci --no-audit --no-fund
npm test
npm run build
```

前端自动测试覆盖稳定重试 ID、超过 JavaScript 安全整数范围的位置、分页与实时消息合并、账户/会话切换、断线重连及 AI 来源。实际浏览器验收还应检查注册登录、双向聊天、群聊、同步去重和账户切换；自动状态测试不能替代界面检查。
