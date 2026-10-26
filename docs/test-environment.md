# 测试环境

本文说明本地测试依赖与执行方式。系统行为和测试覆盖边界见[总架构](architecture.md)。维护者为仓库维护者，测试入口变化时应同步本文。

## 前提

- 使用满足 [go.mod](../go.mod) 的 Go 工具链，从仓库根目录执行命令。
- 普通单元测试使用 mock/miniredis 等测试替身，不要求外部 MySQL、Redis 或 Kafka。
- 带 `integration` 标签的测试同时要求 MySQL 和 Redis；没有启动真实 Kafka 或第二个聊天进程。
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
| MySQL | `127.0.0.1:3306`，`root`，`mychat-dev-password`，`mychat_test` | `TEST_MYSQL_HOST`、`TEST_MYSQL_USER`、`TEST_MYSQL_PASSWORD`、`TEST_MYSQL_DATABASE` |
| Redis | `127.0.0.1:6379`，无密码，DB 0 | `TEST_REDIS_HOST`、`TEST_REDIS_PASSWORD` |

MySQL 端口、Redis 端口和 Redis DB 当前固定；应用的 `FABLE_*` 配置不会替代这里的测试配置。

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

[CI](../.github/workflows/ci.yml) 配置了 MySQL 8.4、Redis 7.4 和 Go 工具链，执行格式检查、单元测试和集成测试。现有工作流未执行 race detector、真实 Kafka、跨实例路由或 K8s 故障验收。

本次文档整理仅核对操作说明与源码，没有执行以上服务测试。需要容量或端到端结果时，另见[压测说明](../tests/load/README.md)，并保存实际运行证据。
