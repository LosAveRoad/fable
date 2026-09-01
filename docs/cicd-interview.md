# Fable Chat：CI/CD 面试追问

## 简历描述

> GitHub Actions 以测试作为发布门禁，构建 Git SHA 镜像并部署 Kubernetes；独立发布 Nginx 入口配置并执行 HTTPS 健康检查。

## 1. 流水线

### 问题

介绍一下项目的 CI/CD 流程。

### 回答

代码推送到主分支后，CD workflow 先运行 test job：检查 gofmt、执行 go vet、单元测试，以及依赖 MySQL、Redis service container 的集成测试。test 成功后 deploy job 才登录 GHCR，构建并推送 Git SHA 镜像。

随后流水线上传 Kubernetes 清单，通过 SSH 进入 VPS2，apply namespace、镜像拉取 Secret 和基础设施，等待 Kafka topic 初始化，再更新 Fable StatefulSet并检查 rollout 状态。

Nginx 等公网入口配置由独立的 Edge CD 管理。只有 `deploy/host2/**` 或入口 Service 发生变化时才触发，流水线更新 Nginx 与 K3s 边缘配置，执行配置校验和 reload，最后通过公网 HTTPS 健康检查确认整条访问链路可用。

### 追问

为什么 deploy job 需要显式依赖 test job？

### 追问的回答

如果 CI 和 CD 只是被同一次 push 独立触发，它们可能并行运行，测试失败也不能阻止部署。通过 `needs: test` 建立有向依赖，只有 test 成功后 deploy 才会获得执行资格。

这才形成发布门禁，而不是仅仅“仓库里同时存在测试 workflow 和部署 workflow”。

## 2. Nginx 边缘发布

### 问题

Nginx 配置是如何通过 CI/CD 发布的？

### 回答

项目把应用发布和边缘入口发布拆成两个 workflow。常规 CD 负责测试、构建 Git SHA 镜像和更新 Kubernetes 工作负载；Edge CD 只在 `deploy/host2/**` 或 `deploy/k8s/app.yaml` 变化时触发，将 Nginx、证书和 K3s 入口配置上传到 VPS2。

服务器上的部署脚本先同步 NodePort 和 Nginx 配置，执行 `nginx -t` 验证语法，通过后才 reload Nginx；最后请求 `https://api.fableim.lol/healthz`，从公网入口验证 TLS、Nginx、NodePort、Service 和 Pod 整条链路。

### 追问

为什么不把 Nginx 配置和应用镜像放在同一次发布中？

### 追问的回答

应用代码变化频繁，只需要重新构建镜像并滚动更新 Pod；Nginx、证书和宿主机入口配置变化频率低，而且错误可能直接影响整个公网入口。拆分后可以通过 path filter 避免每次应用发布都修改宿主机，并缩小流水线权限和故障范围。

两条流水线仍通过最终健康检查衔接：应用 CD 检查 Kubernetes rollout，Edge CD 从公网 HTTPS 地址检查端到端访问结果。

## 3. 测试环境

### 问题

集成测试依赖 MySQL 和 Redis，GitHub Actions 中怎么提供环境？

### 回答

test job 使用 GitHub Actions service containers 启动 MySQL 8.4 和 Redis 7.4，并配置端口和 health command。测试通过环境变量连接这些服务，MySQL 使用独立测试数据库。

service container 健康后 job 才开始执行步骤，减少数据库尚未启动完成导致的偶发失败。

### 追问

单元测试和集成测试有什么区别？

### 追问的回答

单元测试尽量隔离外部系统，验证单个函数或组件的逻辑，运行快、定位明确；集成测试连接真实 MySQL、Redis 和 HTTP/WebSocket 服务，验证模型、SQL、缓存和组件协作是否正确。

mock 能验证预期调用，但不能发现真实数据库方言、迁移、约束和连接配置问题，所以两类测试承担不同职责。

## 4. 镜像版本

### 问题

为什么镜像使用 Git commit SHA 标签？

### 回答

commit SHA 把源代码、构建镜像和部署版本关联起来。流水线构建 `ghcr.io/losaveroad/fable:<sha>`，部署后再读取 StatefulSet 镜像字段并核对 SHA。

这样可以确认线上运行的具体提交，出现问题时也能明确选择上一个已知镜像回滚。

### 追问

为什么不使用 `latest`？

### 追问的回答

`latest` 是可变标签，同一名称可以被后续构建覆盖，仅查看清单无法确定实际代码版本；节点还可能因拉取策略和缓存使用不同镜像内容。

SHA 标签保持版本不可变和可追踪，`latest` 可以作为人工浏览标记，但不适合作为生产部署的唯一版本依据。

## 5. 声明式部署与回滚

### 问题

项目如何完成 Kubernetes 发布和失败回滚？

### 回答

流水线把仓库中的清单上传到固定 release 目录，使用 `kubectl apply` 同步期望状态，再把 Fable 镜像替换为本次 SHA。随后执行 `kubectl rollout status` 等待新版本达到 Ready。

如果 rollout 超时或失败，则执行 `kubectl rollout undo` 并等待上一 revision 恢复，最终让 workflow 返回失败。

### 追问

应用回滚是否意味着数据库也能直接回滚？

### 追问的回答

不意味着。工作负载回滚只恢复容器镜像和 Pod 模板，已经执行的数据库 schema 或数据变更不会自动撤销。

数据库变更应使用版本化 migration，并采用向前、向后兼容的发布顺序，例如先增加兼容字段、发布新旧代码都可运行的版本，再清理旧字段。

## 6. Secret 与发布权限

### 问题

流水线中的 SSH、镜像仓库和业务凭证怎么管理？

### 回答

仓库只保留字段示例，SSH 私钥、GHCR pull token 和生产凭证存放在 GitHub Secrets 或 Kubernetes Secret 中。deploy job 绑定 production environment，只有进入部署阶段的 job 才使用相应 secrets。

流水线使用 SSH 私钥而不是密码登录，并创建专门的镜像拉取 Secret；业务容器通过 `envFrom` 注入 MySQL、Redis 和 JWT 配置。

### 追问

GitHub environment 相比普通 repository secret 有什么作用？

### 追问的回答

environment 表示 production、staging 等部署目标，可以配置审批、允许部署的分支、保护规则和该环境专属 secrets。job 只有通过 environment 保护规则后才能访问对应凭证。

它把“能读取仓库代码”和“能部署生产环境”分成不同权限边界，便于限制和审计发布操作。

## 7. CI/CD 独立八股

### 问题

CI、Continuous Delivery 和 Continuous Deployment 有什么区别？

### 回答

CI 是频繁合并代码并通过自动构建、检查和测试尽早发现集成问题。Continuous Delivery 保证通过流水线的软件随时具备发布条件，但生产发布可以保留人工审批。Continuous Deployment 则把所有通过门禁的变更自动发布到生产。

Fable 的 deploy job 可以由主分支触发，同时绑定 production environment；是否完全自动进入生产取决于 environment 是否配置人工审批。

### 追问

滚动发布、蓝绿发布和金丝雀发布有什么区别？

### 追问的回答

滚动发布逐步用新实例替换旧实例，资源开销较低；蓝绿发布同时维护两套完整环境，验证后一次切换流量，回切快但资源成本高；金丝雀发布先让少量用户或流量进入新版本，根据指标逐步扩大范围。

选择方式要结合系统容量、流量治理能力、数据库兼容性和回滚要求。Kubernetes 默认滚动更新不等于已经实现精细的金丝雀流量控制。
