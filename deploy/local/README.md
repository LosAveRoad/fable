# 本地三角色部署

本文维护已实现的开发运行入口，不替代[角色契约](../../docs/contracts/logic.md)或冻结的[设计基线](../../docs/design/goim-migration.md)。支持 macOS/Linux 的 POSIX shell；Windows 使用 WSL2。本阶段不上 Kubernetes。

## 快速开始

准备 Go（满足 go.mod）、Python 3.9+、Node.js 22.12+、运行中的 Docker 引擎及可用的 `docker compose` 插件。Homebrew 单独安装 Compose 时还需按其安装说明使 Docker 能找到插件；脚本不修改全局 Docker 配置或切换 context。

```sh
./scripts/dev-up.sh
./scripts/dev-status.sh
```

默认构建前端与三个业务二进制，启动 MySQL 8.4、Redis 7.4、Kafka 3.9，再启动两个 Logic、两个 Comet、两个 Job 和本地入口。启动完成前会通过该入口创建临时 UUID 夹具，实际双向发送、检查保存结果/接收帧/数据库记录，再精确清理夹具；通过后才输出 ready。重复 up 不重复启动进程，但仍验证选定运行环境的消息链路。

浏览器地址为 `http://127.0.0.1:18080`；默认 Origin 允许列表与该入口匹配。先注册账户，通过对方 UUID 创建会话。HTTP/MCP 轮询到 Logic，WebSocket 轮询到 Comet；同一用户只维护一条当前路由。新浏览器使用 v2，原 legacy 帧仍由 Comet 兼容。

| 组件 | 默认宿主机端口 |
| --- | --- |
| 入口 | 18080 |
| Logic HTTP / RPC | HTTP 18081、18083；RPC 18082、18084 |
| Comet HTTP/WS / RPC | HTTP 18085、18087；RPC 18086、18088 |
| Job 管理端点 | 18089、18090 |
| MySQL / Redis / Kafka | 13306 / 16379 / 19094 |

`FABLE_LOCAL_PORT_BASE` 改变应用起始端口，0 为测试分配互不重复的空闲端口；最终地址以本轮 state.json 为准。基础设施端口可由下方依赖配置覆盖。所有端口只绑定回环，不能把此明文开发配置直接暴露公网。

## 运行状态与配置

默认运行目录 `.fable/local` 被 Git 忽略，可用 `FABLE_LOCAL_RUNTIME_DIR` 指定其他独立目录。目录权限 0700；private.json、配置和临时写文件为 0600。private.json 含 JWT/服务凭据及子进程环境，不能分享或上传；state.json 是去掉秘密的诊断元数据。日志保留关联 ID 与有界结果，不包含消息正文或 Token。

启动生成新的 Comet instance/boot，并同步 Logic 的明确授权；正常退出后再次 up 仍使用原数据库、Topic、Group 与密钥。退出不成功留下的旧租约不会被当成新进程身份。脚本不会修改已有数据库结构：仅当显式本地前缀数据库没有任何表时调用 Logic init-schema，普通服务只检查结构。已有不兼容表会启动失败，请改用另一个本地库名，不自动删库或回填。

主要覆盖项：

| 环境变量 | 用途 |
| --- | --- |
| FABLE_LOCAL_RUNTIME_DIR、FABLE_LOCAL_PORT_BASE | 本轮运行目录、应用端口起点 |
| FABLE_LOCAL_DATABASE | 仅接受 fable_local_ / fable_e2e_ 前缀的新拓扑库 |
| FABLE_MYSQL_HOST、FABLE_MYSQL_PORT、FABLE_MYSQL_USER、FABLE_MYSQL_PASSWORD | MySQL；托管模式默认本机 13306/root/本轮随机密码 |
| FABLE_REDIS_HOST、FABLE_REDIS_PORT、FABLE_REDIS_PASSWORD | Redis；当前使用 DB 0 与独立路由前缀 |
| FABLE_KAFKA_BROKERS | 逗号分隔 Broker；托管模式默认本机 19094 |
| FABLE_LOCAL_TOPIC、FABLE_LOCAL_DLQ_TOPIC、FABLE_LOCAL_GROUP | 新推送、死信与消费组的隔离后缀名称 |
| FABLE_LOCAL_DOCKER_CONTEXT | 仅为本轮 Docker 命令选择 context，不修改全局选择 |

角色的独立 TOML/环境覆盖由 [Logic](../../config/logic.example.toml)、[Comet](../../config/comet.example.toml)、[Job](../../config/job.example.toml) 示例与实际加载代码维护；脚本将生成后的配置和匹配凭据传给子进程，不让调用者遗留的其他 FABLE_* 变量悄悄覆盖本轮选择。Job 不接收数据库密码。

## 复用隔离依赖

设 `FABLE_LOCAL_EXTERNAL=1` 或传 `--external`，脚本不会管理外部容器。外部 MySQL 账号需要为明确的本地前缀库建库/初始化权限，Kafka 账号需要创建指定的新 Topic。不要指向旧单体数据库或旧命令 Topic。

```sh
export FABLE_LOCAL_EXTERNAL=1
export FABLE_MYSQL_HOST=127.0.0.1
export FABLE_MYSQL_PORT=3306
export FABLE_MYSQL_USER=root
# FABLE_MYSQL_PASSWORD 由本机私有环境提供
export FABLE_REDIS_HOST=127.0.0.1
export FABLE_REDIS_PORT=6379
export FABLE_KAFKA_BROKERS=127.0.0.1:9092
./scripts/dev-up.sh
```

准备阶段显式创建新推送与死信 Topic，均为 6 分区，死信保留 7 天且使用 delete 清理。已有分区/保留配置不符合时拒绝，不擅自改写；Producer/Consumer 本身不自动建 Topic。托管 Compose 分开配置宿主机与容器内部 Kafka advertised listener。

已有前端 dist 且只需重建后端时可传 `--skip-web`；默认启动仍执行锁定依赖安装与生产构建。

## 检查、恢复与停止

```sh
./scripts/dev-status.sh
python3 scripts/local/manage.py smoke
python3 scripts/local/manage.py stop-jobs
python3 scripts/local/manage.py restart-job --index 1
python3 scripts/local/manage.py restart-job --index 2
./scripts/dev-down.sh
./scripts/dev-down.sh --infra
```

status 显示各角色 PID、地址、Comet boot、依赖就绪和 Job 当前 lag/consumer 存活；不以空闲队列误判消费者死亡。smoke 验证当前选定运行环境，创建/清理的仅是其随机夹具，不初始化表或改 Topic。stop-jobs 用于观察保存后排队与恢复；重启 Job 不改变连接归属，也不再次持久化业务消息。

生命周期操作按运行目录互斥；并发 up/down 不能覆盖别人的 PID 或失败回收。SIGINT/SIGTERM 中断启动时清理本轮子进程。停止先关闭入口，再有界停止 Job、Comet、Logic；按记录的 PID、启动时间和二进制标记核对归属，不按端口或进程名字批量杀进程。down 不删数据；--infra 也只停止本轮 Compose，不删除卷。独立运行的外部依赖不会被停止。

Comet 的 boot 与 Logic 授权绑定；需要整轮重启时先 down 再 up，不能只给旧进程套一个新 boot。角色也可直接使用各自 `-config FILE serve` 命令运行，由调用者负责匹配凭据、进程监督和关闭。

## 死信预览与重放

```sh
./scripts/replay-dlq.sh -run-id repair-20261031 -partition 0 -from-offset 0 -to-offset 10
# 核对修复原因与预览后，显式执行同一选择：
./scripts/replay-dlq.sh -run-id repair-20261031 -partition 0 -from-offset 0 -to-offset 10 -execute
```

选择是单分区闭区间，最多 10000 个 offset、16 MiB 记录；缺口、保留期推进或截断不能假装完整选择。无效原始任务只保留供修复，不能自动重放；有效任务只重放 pending 子集，保留原任务/消息/完整收件人身份。命令不写业务数据库、不删除死信、不重置整个消费组。重放仍可能重复推送，浏览器按 message_id 合并。

完整隔离验收入口是 [scripts/smoke.sh](../../scripts/smoke.sh)，它创建自己的临时数据库/命名空间/Topic 和两份每角色进程，不是上述选定环境 smoke。环境变量、实际故障用例和 CI 见[测试环境](../../docs/test-environment.md)。

## 旧环境与回退

旧 Dockerfile、Compose、K8s/CD 配置仅供旧单体回归，不能用于新三角色。停用本轮脚本后可在另一套独立库和命名空间运行旧入口；不让旧消息写法连接新表，不做旧数据/backlog 迁移。远程部署、基础设施容灾与跨主机传输安全需要另外设计。
