# 部署样例与发布说明

本文解释仓库现有部署文件及操作前提，由仓库维护者维护。架构、单点与跨实例投递限制统一见[总架构](../../docs/architecture.md)。这些样例尚不能作为生产可用性或开箱即用安装的证明。

## 文件选择

| 文件 | 表达的部署方式与限制 |
| --- | --- |
| [app-external.yaml](app-external.yaml) | 应用双副本和 NodePort；MySQL/Redis 在外部主机，MySQL 端口为 3306 |
| [kafka-only.yaml](kafka-only.yaml) | 命名空间、单 Broker Kafka、Service 和 PVC；内部 Topic 副本因子为 1 |
| [app.yaml](app.yaml) | 另一份应用清单；MySQL 端口为 3307，仍显式指定外部 MySQL/Redis 地址 |
| [infra.yaml](infra.yaml) | Redis 和 Kafka 的另一套清单；不能据此认为 app.yaml 自动连接集群内 Redis |
| [kafka.yaml](kafka.yaml) | 重复的 Kafka 样例，与另两份清单的镜像及参数有差异 |
| [host1 Compose](../host1/docker-compose.yml) | 外部 MySQL/Redis 主机样例，含环境专用的地址绑定 |
| [host2](../host2) | K3s 与 Nginx/TLS 的环境专用配置；Nginx 反代本机 30080 并配置 WebSocket Upgrade |
| [根目录 Compose](../../docker-compose.yml) | 两个应用进程、Redis 和 Kafka；需外部 MySQL、`.env` 和预先准备的 Topic |

两份应用清单操作相同的 Deployment/Service，三份 Kafka 清单操作同名资源。应选择并审查一种组合，不要把全部文件依次 apply。若沿用外部 MySQL/Redis 的拓扑，可从 `app-external.yaml` 与 `kafka-only.yaml` 开始评估；本次没有修改或验证这些清单。

## 首次部署前提

1. 确认网络地址、MySQL 端口、数据库与用户权限；清单中的地址是原环境样例。
2. 准备可用的 StorageClass/PVC、容器镜像拉取权限，以及命名空间 `fable`。
3. 将应用镜像的 `ghcr.io/OWNER/fable:latest` 占位值替换为实际仓库和提交标签；后续 CD 只更新镜像，不完成首次资源初始化。
4. 按[密钥说明](../secrets/README.md)准备 `fable-secret`。应用通过 envFrom 读取，但清单显式 env 优先，因此 Secret 内的地址不会覆盖同名显式值。
5. Kafka 就绪后，预先创建与应用配置一致的 Topic。默认名为 `fable.chat.message`；Broker 与应用 Writer 都关闭自动建 Topic。六个分区不是由应用的 `partitionCount` 自动创建。
6. 核对 Kafka 消费组、Broker advertised listener 可达性、应用环境和前端 API/WebSocket 地址。

命名空间可通过 `kubectl apply -f deploy/k8s/namespace.yaml` 创建。已有 Secret 环境文件时，可从仓库根目录执行：

```sh
kubectl create secret generic fable-secret -n fable --from-env-file=/etc/fable/secrets.env
```

该命令用于首次创建，已有 Secret 时应按维护流程更新。应用仍先读取镜像内的 TOML，再叠加受支持的环境变量；详见 [Dockerfile](../../Dockerfile) 和 [config.go](../../internal/config/config.go)。

资源是否启动、HTTP 是否可达和消息是否成功投递需分别检查。`/healthz` 为 200 不代表消费者仍在工作；双副本下应明确将两个用户连接到不同实例，验证单聊、群聊和 MCP，不能仅检查 rollout 成功。

## 当前发布工作流

| 工作流 | 作用与前提 |
| --- | --- |
| [后端 CD](../../.github/workflows/cd.yml) | 构建并推送提交 SHA 标签，再 SSH 执行 set image 和 rollout status；要求 Deployment 已存在 |
| [Edge CD](../../.github/workflows/edge-cd.yml) | 上传 host2 配置并调整 Nginx/K3s，包含原环境域名；不负责完整应用安装 |
| [前端 CD](../../.github/workflows/frontend-cd.yml) | npm 构建后发布到 Cloudflare Pages；后端镜像不包含前端 |

触发条件以各工作流的 `on` 为准，包括文档路径过滤。本次没有修改发布工作流或添加自动文档门禁。

## 回滚与验收

发布前记录当前镜像摘要或提交标签。应用镜像回滚可将 Deployment 的镜像改回已记录版本，再检查 rollout 和业务链路；`kubectl rollout undo` 依赖可用的 Deployment 历史，不能代替数据库回滚。

每个应用启动都会 AutoMigrate。当前缺少版本化数据库回滚、长连接排空和客户端自动恢复，回滚前需评估模式兼容性，发布后需验证重连、历史记录与消息投递。Kafka 是单 Broker，业务仍无跨实例寻址；这些缺口不会由 K8s 自动修复。
