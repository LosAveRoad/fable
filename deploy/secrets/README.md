# 部署密钥

由仓库维护者维护。实际消费项以[后端 CD](../../.github/workflows/cd.yml)、[Edge CD](../../.github/workflows/edge-cd.yml)和[前端 CD](../../.github/workflows/frontend-cd.yml)为准；部署前提见[部署说明](../k8s/README.md)。

## 应用配置

可以将 [production.env.example](production.env.example) 复制到服务器的 `/etc/fable/secrets.env`，填写实际值并限制文件读取权限。`.env`、本地 TOML 和真实部署环境文件不提交 Git。

后端从 `config/config.toml` 加载基础配置，再读取 [config.go](../../internal/config/config.go) 中支持的环境覆盖。K8s 通过 `fable-secret` 注入 MySQL 用户名/密码、JWT 密钥及其他运行配置。样例文件包含原环境地址，必须与选用的部署清单一起核对。

`production.env.example` 中的 `FABLE_KAFKA_PARTITION_COUNT` 当前未被应用读取；Topic 必须另行初始化。清单中的显式 env 会覆盖 Secret 中同名变量。

## GitHub Actions 配置

| 工作流 | 当前读取的 secrets / variables |
| --- | --- |
| 后端 CD、Edge CD | Secrets：`DEPLOY_HOST`、`DEPLOY_PORT`、`DEPLOY_USER`、`DEPLOY_PASSWORD` |
| 后端镜像发布 | 使用工作流提供的 `GITHUB_TOKEN`，要求 packages 写权限 |
| 前端 CD | Secrets：`CLOUDFLARE_API_TOKEN`、`CLOUDFLARE_ACCOUNT_ID`；Variables：`VITE_API_BASE`、`VITE_WS_BASE`、`CLOUDFLARE_PAGES_PROJECT` |
| 手动压测 | Secrets：`LOAD_TEST_AUTH_TOKEN`、`LOAD_TEST_WS_TOKEN`；Variable：`LOAD_TEST_GROUP_ID` |

当前 SSH 发布使用密码认证，没有读取 `DEPLOY_SSH_KEY` 或 `DEPLOY_ENV_FILE`，也没有自动将服务器环境文件更新为 Kubernetes Secret。此说明描述现状，不引入新的密钥管理方案。
