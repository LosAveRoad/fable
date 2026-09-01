# Fable Chat

<p align="center">
  <img src="web/chat-server/fable-im-logo.png" alt="Fable IM" width="420">
</p>

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Fable Chat 是一个使用 Go、Gin、GORM、MySQL、Redis、Kafka、WebSocket、Docker 和 Kubernetes 构建的即时通信后端。项目支持本地双实例运行，也提供一套可复刻的双 VPS 部署方案。

## 部署架构

### 本地开发

```text
浏览器或 API 客户端
        │
        ├── localhost:8080 → fable-1 ┐
        └── localhost:8081 → fable-2 ├── MySQL（宿主机）
                                     ├── Redis（Compose）
                                     └── Kafka（Compose）
```

### 双 VPS

```text
                           VPS2：应用与公网入口
客户端 ── HTTPS/WSS ──> Nginx :443
                              │
                              ▼
                    127.0.0.1:30080 NodePort
                              │
                      Kubernetes Service
                         ┌────┴────┐
                         ▼         ▼
                      fable-0   fable-1
                         │         │
                         └────┬────┘
                              ▼
                      Kafka（Kubernetes）
                              │
                         私网/VPN 网络
                              │
                              ▼
                 VPS1：MySQL + Redis（Docker Compose）
```

VPS1 保存有状态数据，VPS2 运行 Nginx、K3s、Kafka 和两个 Fable 实例。NodePort 只监听回环地址，公网请求必须经过 Nginx。该方案用于项目演示和部署实践，不依赖公网暴露 MySQL、Redis 或 NodePort。

## 一、本地部署

### 1. 前置条件

- Git
- Docker Engine 24+ 和 Docker Compose v2
- 可用的 MySQL 8.4（下面给出 Docker 启动方式）
- 可选：Node.js 22，用于启动前端

项目 `Dockerfile` 使用 Go 1.26 构建镜像；使用 Docker 部署时不需要在宿主机安装 Go。

### 2. 获取代码

```bash
git clone https://github.com/LosAveRoad/fable.git
cd fable
```

### 3. 启动本地 MySQL

根目录的 `docker-compose.yml` 有意把 MySQL 视为外部依赖，并通过 `host.docker.internal:3306` 访问宿主机。可以用下面的容器提供本地 MySQL：

```bash
docker volume create fable-mysql-data
docker run -d \
  --name fable-mysql \
  --restart unless-stopped \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=fable-local-root \
  -e MYSQL_DATABASE=mychat \
  -e MYSQL_USER=fable \
  -e MYSQL_PASSWORD=fable-local-password \
  -v fable-mysql-data:/var/lib/mysql \
  mysql:8.4 \
  --character-set-server=utf8mb4 \
  --collation-server=utf8mb4_unicode_ci
```

等待 MySQL 就绪：

```bash
docker exec fable-mysql mysqladmin ping -uroot -pfable-local-root
```

如果本机已经安装 MySQL，只需创建 `mychat` 数据库和拥有该数据库权限的用户，并在下一步填写对应凭证。

### 4. 创建本地环境变量

```bash
cp .env.example .env
```

至少修改 JWT 和数据库、Redis 密码。`REDIS_PASSWORD` 是 Redis 容器密码，`FABLE_REDIS_PASSWORD` 是应用连接 Redis 时使用的密码，两者必须一致。`.env` 已被 Git 忽略，不要提交。

### 5. 启动两个应用实例、Redis 和 Kafka

```bash
docker compose up -d --build
docker compose ps
```

Compose 会完成以下工作：

- 构建 Fable 镜像并启动 `fable-1`、`fable-2`；
- 启动带 AOF 持久化的 Redis；
- 启动单 broker Kafka；
- 幂等创建 `fable.chat.message` topic；
- 将两个 Fable 实例分别映射到 `8080`、`8081`。

应用启动时，GORM 会对当前模型执行 `AutoMigrate`。这适合本地开发；正式生产演进建议改用版本化 migration。

### 6. 验证

```bash
curl --fail http://localhost:8080/livez
curl --fail http://localhost:8080/healthz
curl --fail http://localhost:8081/healthz
docker compose logs -f fable-1 fable-2
```

- `/livez` 只检查进程是否存活；
- `/healthz` 会检查必需的 MySQL、Redis 和 Kafka 依赖。

### 7. 可选：启动前端

```bash
cd web/chat-server
npm ci
npm run dev
```

开发服务器会把 HTTP API 和 `/wss` WebSocket 代理到 `localhost:8080`，不需要关闭浏览器 CORS。生产构建则通过 `VITE_API_BASE`、`VITE_WS_BASE` 指向公网 API。

### 8. 停止本地环境

```bash
docker compose down
docker stop fable-mysql
```

上述命令保留数据卷。只有确定不再需要本地数据时，才删除 `fable-mysql-data`、`redis-data` 和 `kafka-data` 卷。

## 二、复刻双 VPS 部署

以下命令以 Ubuntu/Debian 为例。请先用云厂商安全组和主机防火墙限制端口，再启动数据库服务。

### 1. 准备网络与域名

准备两台 VPS：

| 主机 | 作用 | 允许的入站流量 |
|---|---|---|
| VPS1 | MySQL、Redis | SSH；3306、6379 仅允许 VPS2 私网/VPN 地址 |
| VPS2 | Nginx、K3s、Kafka、Fable | SSH；公网 80、443 |

推荐让两台 VPS 通过云厂商私网或 WireGuard 通信。本文用以下占位符：

- `<VPS1_PRIVATE_IP>`：VPS1 的私网/VPN 地址；
- `<VPS2_PRIVATE_IP>`：VPS2 的私网/VPN 地址；
- `<VPS2_PUBLIC_IP>`：VPS2 的公网地址；
- `<API_DOMAIN>`：例如 `api.example.com`。

在 DNS 服务商处创建 A/AAAA 记录，把 API 域名解析到 VPS2。申请 TLS 证书前，域名必须已经解析且公网 80、443 可访问。

### 2. VPS1：安装并启动 MySQL、Redis

使用 Docker 官方文档安装 Docker Engine 和 Compose 插件，然后验证：

```bash
docker version
docker compose version
```

把仓库的 `deploy/host1/` 上传或克隆到 VPS1：

```bash
git clone https://github.com/LosAveRoad/fable.git
cd fable/deploy/host1
cp .env.example .env
chmod 600 .env
```

编辑 `.env`：

```dotenv
MYSQL_ROOT_PASSWORD=<随机强密码>
MYSQL_DATABASE=mychat
MYSQL_USER=fable
MYSQL_PASSWORD=<随机强密码>
REDIS_PASSWORD=<随机强密码>
MYSQL_BIND_ADDRESS=<VPS1_PRIVATE_IP>
REDIS_BIND_ADDRESS=<VPS1_PRIVATE_IP>
```

启动并检查基础设施：

```bash
docker compose up -d
docker compose ps
docker compose logs --tail=100 mysql redis
```

防火墙只允许 VPS2 访问数据库端口。以 UFW 为例，先确保 SSH 规则存在，再执行：

```bash
sudo ufw allow OpenSSH
sudo ufw allow from <VPS2_PRIVATE_IP> to any port 3306 proto tcp
sudo ufw allow from <VPS2_PRIVATE_IP> to any port 6379 proto tcp
sudo ufw enable
```

不要为 `0.0.0.0/0` 开放 3306 或 6379。从 VPS2 验证私网连通性：

```bash
nc -vz <VPS1_PRIVATE_IP> 3306
nc -vz <VPS1_PRIVATE_IP> 6379
```

### 3. VPS2：安装 K3s

先安装仓库中的 K3s 配置。复制前将 `node-name` 改成这台 VPS 的唯一名称；配置同时禁用内置 Traefik，并限制 NodePort 只监听回环地址。

```bash
git clone https://github.com/LosAveRoad/fable.git
cd fable
sudo install -d -m 0755 /etc/rancher/k3s
sudo install -m 0644 deploy/host2/k3s-config.yaml /etc/rancher/k3s/config.yaml
curl -sfL https://get.k3s.io | sudo sh -
sudo kubectl get nodes
```

如果后续使用非 root 部署用户，需要为它配置最小化的 `kubectl`、Nginx 和 systemd 权限。当前 `bootstrap-edge.sh` 会安装软件并写入 `/etc`，因此现有 Edge CD 要求 SSH 用户具有 root 权限；不要把这一点误写成已经实现的受限部署用户。

### 4. 创建 SSH 部署密钥

在可信的本地机器生成一对专用密钥：

```bash
ssh-keygen -t ed25519 -C "fable-github-actions" -f ./fable_cd_ed25519
```

- 把 `fable_cd_ed25519.pub` 内容追加到 VPS2 部署用户的 `~/.ssh/authorized_keys`；
- 把私钥 `fable_cd_ed25519` 的完整内容保存为 GitHub Secret `DEPLOY_SSH_KEY`；
- 不要把私钥提交到仓库，也不要复用个人日常 SSH 密钥。

验证登录：

```bash
ssh -i ./fable_cd_ed25519 -p <SSH_PORT> <DEPLOY_USER>@<VPS2_PUBLIC_IP>
```

### 5. 创建 Kubernetes 业务 Secret

业务 Secret 不在仓库中，也不会由当前 CD 自动生成。首次部署时在 VPS2 手动创建：

```bash
sudo kubectl apply -f deploy/k8s/namespace.yaml
sudo kubectl -n fable create secret generic fable-secret \
  --from-literal=FABLE_MYSQL_HOST=<VPS1_PRIVATE_IP> \
  --from-literal=FABLE_MYSQL_PORT=3306 \
  --from-literal=FABLE_MYSQL_USER=fable \
  --from-literal=FABLE_MYSQL_PASSWORD='<MYSQL_PASSWORD>' \
  --from-literal=FABLE_MYSQL_DATABASE=mychat \
  --from-literal=FABLE_REDIS_HOST=<VPS1_PRIVATE_IP> \
  --from-literal=FABLE_REDIS_PORT=6379 \
  --from-literal=FABLE_REDIS_PASSWORD='<REDIS_PASSWORD>' \
  --from-literal=FABLE_JWT_SECRET='<至少 32 位随机字符串>'
```

可以用下面的命令生成 JWT 密钥：

```bash
openssl rand -base64 48
```

如需更新 Secret，使用 `--dry-run=client -o yaml | kubectl apply -f -` 生成并应用，不要提交真实的 `secret.yaml`。

### 6. 配置 GitHub Actions

在仓库 `Settings → Secrets and variables → Actions` 中配置：

| 名称 | 内容 |
|---|---|
| `DEPLOY_HOST` | VPS2 公网 IP 或 SSH 域名 |
| `DEPLOY_PORT` | SSH 端口 |
| `DEPLOY_USER` | 当前有部署权限的 SSH 用户 |
| `DEPLOY_SSH_KEY` | 上一步生成的完整私钥 |
| `GHCR_PULL_USERNAME` | 能读取 GHCR 镜像的 GitHub 用户名 |
| `GHCR_PULL_TOKEN` | 具有 `read:packages` 权限的 token |

`cd.yml` 的 deploy job 使用 GitHub `production` Environment。可以为该 Environment 增加人工审批、受保护分支和环境级 Secrets。

前端部署到 Cloudflare Pages 时，还需要配置 `CLOUDFLARE_API_TOKEN`、`CLOUDFLARE_ACCOUNT_ID`，以及 `VITE_API_BASE`、`VITE_WS_BASE`、`CLOUDFLARE_PAGES_PROJECT` variables；它与后端双 VPS 初始化相互独立。

### 7. 首次发布顺序

仓库的 Nginx 配置和 `bootstrap-edge.sh` 默认使用 `api.fableim.lol`，CORS 允许来源默认是 `https://fableim.lol`。复刻部署时，先把三个 `deploy/host2/` 文件中的 API 域名替换为 `<API_DOMAIN>`，并把 `fable-api.conf` 中的前端来源替换为 `<WEB_ORIGIN>` 后提交；否则 Certbot 会为原项目域名申请证书，浏览器跨域请求也会被错误配置。

首次发布必须按依赖顺序进行：

1. VPS1 的 MySQL、Redis 已健康；
2. VPS2 已安装 K3s，且能通过私网访问 VPS1；
3. `fable-secret` 已创建；
4. GitHub Actions Secrets 已配置；
5. 运行 **CD** workflow，部署 Kafka、初始化 topic，并滚动发布 Fable；
6. 确认 Kubernetes Service 已创建后，运行 **Edge CD** workflow；
7. Edge CD 配置 Nginx、Certbot 和 NodePort，并从公网检查 `/healthz`。

可以在 GitHub Actions 页面手动运行两个 workflow。常规代码推送也会触发 CD；`deploy/host2/**` 或入口 Service 变化会触发 Edge CD。

### 8. 验证部署

在 VPS2 检查 Kubernetes：

```bash
sudo kubectl -n fable get pods,svc,statefulset,job
sudo kubectl -n fable rollout status statefulset/fable --timeout=300s
sudo kubectl -n fable logs statefulset/fable --tail=100
curl --fail http://127.0.0.1:30080/healthz
```

从外部网络检查公网入口：

```bash
curl --fail https://<API_DOMAIN>/livez
curl --fail https://<API_DOMAIN>/healthz
```

`/livez` 成功但 `/healthz` 失败，通常表示应用进程正常，但 MySQL、Redis 或 Kafka 至少有一个必需依赖不可用。

### 9. 后续发布与回滚

- 后端代码推送到 `main` 后，CD 先执行 gofmt、go vet、单元测试和 MySQL/Redis 集成测试；
- 测试通过后构建 `ghcr.io/losaveroad/fable:<commit-sha>`；
- CD 更新 Kubernetes 清单并等待 StatefulSet rollout；
- rollout 失败时执行 `kubectl rollout undo`；
- Nginx/K3s 入口配置由 Edge CD 独立发布，执行 `nginx -t` 后 reload，并检查公网 HTTPS 健康状态。

手动查看和回滚：

```bash
sudo kubectl -n fable rollout history statefulset/fable
sudo kubectl -n fable rollout undo statefulset/fable
sudo kubectl -n fable rollout status statefulset/fable --timeout=300s
```

应用镜像回滚不会自动回滚数据库结构。当前应用启动时使用 GORM `AutoMigrate`；生产演进应改用可审计、兼容新旧版本的数据库 migration。

### 10. 数据备份

MySQL、Redis 和 Kafka 当前都是单实例，数据卷只解决容器重建后的持久化，不等于备份。至少应为 MySQL 配置定期逻辑备份并把备份复制到另一台主机或对象存储，同时定期验证恢复流程。

## 三、常见问题

### 为什么有 Kubernetes 还需要 Nginx？

Kubernetes Service 负责找到 Ready Pod 并分配连接；Nginx 负责公网 80/443、TLS 终止、WebSocket Upgrade、CORS 和 HTTPS 重定向。当前只有一个公网应用入口且宿主机已有 Nginx，因此没有额外部署 Ingress Controller。

### 用户会固定连接到某一个 Fable 实例吗？

不会按用户 ID 固定分配。Nginx 把连接转发到 NodePort，Kubernetes Service 从 Ready Pod 中选择后端。WebSocket 建立后会一直保持在所选 Pod，断线重连可能进入另一个 Pod。每个实例使用独立 Kafka consumer group 接收投递事件，再只向本机连接执行 WebSocket fan-out。

### CI/CD 能从空白 VPS 完成所有初始化吗？

不能。当前流水线负责已有环境上的应用和边缘配置发布；首次部署仍需要安装 Docker、K3s，建立私网、防火墙、SSH 密钥、GitHub Secrets，并在 VPS1 启动 MySQL 和 Redis。若需要完全自动化，可进一步使用 Terraform 创建基础设施、使用 Ansible 完成主机初始化。

## 四、开发文档

- [总路线与验收规则](docs/roadmap.md)
- [MVP0：用户与可靠单聊](docs/mvp0-core-chat.md)
- [MVP1：联系人、群聊与 Redis](docs/mvp1-group-redis.md)
- [MVP2：AI 助手与 MCP](docs/mvp2-ai-mcp.md)
- [MVP3：音视频、Kafka 与 K8s](docs/mvp3-realtime-distributed.md)
- [Kubernetes 部署清单说明](deploy/k8s/README.md)
- [开发问题记录](docs/development-log.md)

## License

Fable Chat 的自有代码采用 [MIT License](LICENSE) 开源。
