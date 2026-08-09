# 测试环境

项目的单元测试只依赖 Go；集成测试依赖 MySQL 8；Windows 上运行 Go race detector 还需要 64 位 GCC。

## 一键运行

在仓库根目录执行：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\test.ps1
```

脚本会完成以下工作：

1. 检查 Docker 引擎；必要时启动 Docker Desktop。
2. 复用或创建名为 `fable-test-mysql` 的 MySQL 8.4 容器。
3. 等待 `mychat_test` 测试库可用。
4. 运行普通单元测试和带 `integration` build tag 的集成测试。
5. 查找 MSYS2 UCRT64/MinGW64 GCC，并运行全项目 race detector 测试。

测试数据库只绑定 `127.0.0.1:3306`，默认连接信息与集成测试保持一致：

- 用户名：`root`
- 密码：`mychat-dev-password`
- 数据库：`mychat_test`

脚本不会永久修改系统 `PATH`、`CGO_ENABLED` 或 `CC`，也不会在测试结束后删除数据库容器。容器使用 `unless-stopped` 重启策略，后续运行会直接复用。

## 手动运行

数据库启动后，可以分别执行：

```powershell
go test -count=1 ./...
go test -count=1 -tags=integration ./internal/integration

$env:Path = "C:\msys64\ucrt64\bin;$env:Path"
$env:CGO_ENABLED = "1"
$env:CC = "C:\msys64\ucrt64\bin\gcc.exe"
go test -count=1 -race ./...
```

集成测试支持通过 `TEST_MYSQL_HOST`、`TEST_MYSQL_USER`、`TEST_MYSQL_PASSWORD` 和 `TEST_MYSQL_DATABASE` 覆盖默认连接信息。

若只想停止测试数据库而保留数据，可执行：

```powershell
docker stop fable-test-mysql
```
