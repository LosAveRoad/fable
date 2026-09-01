# Fable Chat：Docker 与 Kubernetes 面试追问

## 简历描述

> 使用 Docker 容器化 Go 服务，基于 Kubernetes 编排双实例并实现滚动发布

## 1. 部署方案

### 问题

介绍一下你项目中 Docker 和 Kubernetes 的使用。

### 回答

我先用多阶段 Dockerfile 构建 Go 服务，编译阶段使用 Go 工具链，运行阶段只保留静态二进制和配置，并使用非 root 的 distroless 镜像。

部署上，我把 MySQL、Redis 放在 VPS1，把公网入口和应用放在 VPS2。VPS2 运行 Nginx 和单节点 K3s；K3s 中运行两个 Fable 实例和一个 Kafka broker。Nginx 负责 TLS 和 WebSocket 代理，通过只监听回环地址的 NodePort 访问 Fable Service。

发布由 GitHub Actions 完成。代码通过格式检查、静态检查、单元测试和集成测试后，流水线构建带 Git commit SHA 的镜像，更新 K3s 工作负载，等待新实例通过 readiness，再完成滚动发布；发布失败则执行回滚。

### 追问

为什么选择 K3s，而不是完整的 Kubernetes？

### 追问的回答

项目只有一台应用 VPS，我的目标是实践 Kubernetes 的 Pod、Service、探针和滚动更新，而不是维护多节点控制面。K3s 兼容 Kubernetes API，但安装和资源开销更低，适合当前规模。

## 2. Docker 镜像构建

### 问题

你刚才提到使用 Docker 容器化 Go 服务，Dockerfile 是怎么写的？

### 回答

Dockerfile 分为构建和运行两个阶段。构建阶段先复制 `go.mod`、`go.sum` 并下载依赖，再复制源码，通过 `CGO_ENABLED=0` 编译 Linux 静态二进制。运行阶段使用 distroless nonroot 镜像，只复制最终二进制和基础配置，通过 JSON 数组形式的 `ENTRYPOINT` 启动服务。

这样生产镜像不包含 Go 编译器、源码、包管理器和 Shell，可以减小镜像体积和攻击面。

### 追问

为什么先复制 `go.mod`、`go.sum`，再复制业务源码？

### 追问的回答

Docker 构建会按层复用缓存。Go 依赖文件的变化频率通常低于业务源码，先复制依赖文件并执行 `go mod download`，业务代码改变时仍然可以复用依赖层，避免每次重新下载全部依赖。

如果 `go.mod` 或 `go.sum` 发生变化，依赖层才会失效；后面的源码复制和编译层仍会根据新内容重新构建。

## 3. 容器运行安全

### 问题

你刚才提到使用 distroless 和非 root 用户，为什么这样设计？

### 回答

distroless 只保留应用运行需要的最小文件，避免把 Shell、包管理器和编译工具带进生产镜像。非 root 用户则降低应用漏洞被利用后的权限范围。

Kubernetes 清单中还配置了 `runAsNonRoot`、禁止提权、删除全部 Linux capabilities 和只读根文件系统，进一步限制容器内进程的权限。

### 追问

只读根文件系统会不会影响 Go 服务运行？

### 追问的回答

取决于程序是否需要在容器文件系统中写临时文件、日志或上传内容。Fable 主要把状态放在 MySQL、Redis 和 Kafka，日志写标准输出，因此应用运行时不需要修改根文件系统，可以启用只读根文件系统。

如果后续确实需要临时写入，应显式挂载 `emptyDir` 到指定目录，而不是把整个根文件系统改回可写。这样能保留最小写权限边界。

## 4. 双副本与请求分配

### 问题

你刚才说 K3s 中运行两个 Fable 实例，请求是怎么分配的？

### 回答

两个实例使用相同的 Pod label，Fable Service 通过 selector 选择它们。只有 readiness 成功的 Pod 才会进入 Service 对应的 EndpointSlice。宿主机 Nginx 把请求代理到 NodePort，NodePort 后面的 Service 再把新连接分配给可用实例。

普通 HTTP 请求可以按这种方式分配，但 WebSocket 建立后会长期绑定具体 Pod。每个 Pod 只知道连接在自己进程内的用户，所以项目另外使用 Kafka 把实时投递事件发送给每个 Fable 实例，再由实例查找本机连接。

### 追问

Service 已经能负载均衡，为什么还不能解决跨 Pod 的 WebSocket 消息投递？

### 追问的回答

Service 只负责把网络连接转发到某个 Pod，不理解用户身份，也不会同步各 Pod 的进程内状态。用户 A 的 WebSocket 可能连接在 `fable-0`，用户 B 可能连接在 `fable-1`；连接建立后，Service 不知道哪个 Pod 持有用户 B。

因此 Service 解决的是“请求如何进入 Pod”，Kafka 解决的是“投递事件如何被所有应用实例看到”，每个实例内部的连接表再解决“消息应写入哪个本地 WebSocket”。

## 5. Kafka 跨实例投递

### 问题

你刚才提到 Kafka 会把投递事件发送给每个 Fable 实例，具体是怎么消费的？

### 回答

每个 Fable 实例使用独立且稳定的 consumer group，例如 `fable-chat-fable-0` 和 `fable-chat-fable-1`。这样同一条投递事件会被两个 group 分别消费，每个实例都能检查目标用户是否连接在本机。

消息只在接收发送方请求的实例中持久化一次，然后发布规范化投递事件。其他实例消费事件时只执行本地 WebSocket fan-out，不再写数据库，因此不会因为每个实例都消费而产生重复消息记录。

### 追问

如果两个 Fable 实例共用同一个 consumer group，会发生什么？

### 追问的回答

同一个 consumer group 中，一条 Kafka 消息只会被其中一个成员消费。如果消息被 `fable-0` 消费，但目标用户连接在 `fable-1`，`fable-0` 找不到本地连接，而 `fable-1` 又没有收到事件，就会丢失这次实时投递。

因此这里需要的是 fan-out，而不是用同一个 group 分摊消息处理。独立 group 解决所有实例都能看到事件的问题；MySQL 中已经持久化的消息负责离线历史，Kafka负责在线实时通知。

## 6. StatefulSet 的选择

### 问题

你刚才说 consumer group 中包含 `fable-0`、`fable-1`，Pod 身份是怎么保持稳定的？

### 回答

当前 Fable 使用 StatefulSet，Pod 名称固定为 `fable-0`、`fable-1`。应用通过 Downward API读取 Pod 名，并把它加入 consumer group 名称。滚动更新后 Pod 仍然恢复相同实例身份，可以继续使用原 consumer group 的 offset。

Fable 本身没有把业务数据保存在 Pod 本地，使用 StatefulSet 主要是为了稳定的消息订阅身份，而不是为了本地存储。

### 追问

无状态 Go 服务一般使用 Deployment，为什么这里使用 StatefulSet？

### 追问的回答

一般无状态 HTTP 服务确实应该优先使用 Deployment。Deployment 的 Pod 名随机变化，如果直接把随机 Pod 名作为独立 group 身份，每次滚动更新都会产生新 group，消费身份和 offset 难以稳定管理。

因此项目使用 StatefulSet 保留稳定实例身份，让滚动更新后的实例继续使用原 consumer group 和 offset。这里使用 StatefulSet 是由当前 Kafka fan-out 方案决定的，不是因为 Go 服务需要本地持久化。

## 7. 滚动发布与健康检查

### 问题

你刚才提到滚动发布会等待 readiness，通过什么方式判断新实例可以接收流量？

### 回答

应用提供 `/livez` 和 `/healthz` 两个端点。startupProbe 和 livenessProbe 使用 `/livez`，判断进程及 HTTP 事件循环能否响应；readinessProbe 使用 `/healthz`，除了检查进程，还检查 MySQL、必需 Redis、Kafka topic 和 consumer 状态。

新 Pod 启动后先通过 startupProbe，随后 readiness 成功才进入 Service 端点。这样旧实例不会在新实例尚未具备业务能力时过早退出。

### 追问

为什么不能让 livenessProbe 也检查 MySQL、Redis 和 Kafka？

### 追问的回答

liveness 失败会触发容器重启，而外部依赖短暂不可用不一定能通过重启应用解决。如果所有 Pod 都因为同一个数据库故障而不断重启，反而会形成重启风暴，并在依赖恢复时同时建立大量连接。

readiness 失败只会把 Pod 从 Service 流量端点中摘除，进程仍有机会等待依赖恢复。因此进程自身无法继续工作才应影响 liveness，暂时不能提供完整业务能力则影响 readiness。

## 8. CI/CD 与版本回滚

### 问题

你刚才提到 GitHub Actions 会完成发布，具体流程是什么？

### 回答

CD 的 deploy job 依赖 test job。测试阶段执行 gofmt、`go vet`、单元测试和集成测试；全部通过后才构建镜像并推送到 GHCR。镜像使用 Git commit SHA 作为标签，随后流水线上传 Kubernetes 清单，通过 SSH 在 VPS2 执行声明式 apply，并等待 StatefulSet rollout 完成。

如果 rollout 在规定时间内失败，流水线执行 `rollout undo`，等待旧版本恢复，并把本次发布标记为失败。最后还会读取工作负载中的镜像地址，确认运行版本与本次 commit SHA 一致。

### 追问

为什么使用 commit SHA，而不是直接使用 `latest`？

### 追问的回答

`latest` 是可变标签，同一个标签可能在不同时间指向不同镜像，也容易受到节点缓存影响。仅看到 `latest` 无法确认 Pod 对应哪次代码提交。

commit SHA 是不可变版本标识，可以建立代码提交、镜像和运行工作负载之间的对应关系。出现问题时能够定位具体版本，也可以明确回滚到上一份已知镜像。

## 9. Nginx 与 NodePort

### 问题

你刚才说宿主机 Nginx 通过 NodePort 访问 Fable，为什么这样设计？

### 回答

当前是单节点 K3s，宿主机已经使用 Nginx 管理域名、TLS 证书、CORS 和 WebSocket 反向代理，所以没有再引入 Ingress Controller。K3s 的 NodePort 只监听 `127.0.0.0/8`，只有本机 Nginx 能访问，用户不能绕过 HTTPS入口直接访问应用端口。

这个方案减少了单节点环境中的入口组件，并统一由 Nginx 处理公网流量。

### 追问

Ingress、NodePort 和 LoadBalancer 有什么区别？

### 追问的回答

NodePort 在节点地址上开放固定端口，把流量转到 Service；LoadBalancer 通常依赖云平台创建外部负载均衡器；Ingress 表示 HTTP/HTTPS 的域名和路径路由规则，但必须由 Ingress Controller 实际执行。

当前只有一个节点，也没有云负载均衡器，因此使用宿主机 Nginx 加回环 NodePort。若变成多节点，外部流量需要先到达一个能感知节点和 Pod 状态的统一入口，继续绑定单机 Nginx 就不合适了。

## 10. 双 VPS 与架构边界

### 问题

你刚才提到 MySQL、Redis 在 VPS1，而 K3s 在 VPS2，为什么这样拆分？

### 回答

拆分的主要目的是职责和资源隔离。VPS2 负责公网入口、应用计算、WebSocket 和 Kafka；VPS1 负责 MySQL 持久化和 Redis 缓存。应用滚动发布或 K3s 工作负载波动时，不会直接操作数据库容器和数据卷。

### 追问

两台 VPS 之间如何通信，怎样避免 MySQL 和 Redis 暴露公网？

### 追问的回答

应用通过云私网或 VPN 地址访问 VPS1。MySQL 和 Redis 只绑定私网地址，宿主机防火墙只允许 VPS2 的私网地址访问对应端口；公网侧不开放 3306 和 6379。

Kubernetes Secret 保存数据库地址和凭证，通过环境变量注入 Fable 容器。这样网络入口限制和应用认证分别承担一层保护，真实凭证也不会写进镜像或提交到仓库。

# Docker 与 Kubernetes 独立八股

## 11. 容器与虚拟机

### 问题

Docker 容器和虚拟机有什么区别？

### 回答

虚拟机通过 Hypervisor 虚拟硬件，每个虚拟机运行自己的操作系统内核；容器通常共享宿主机内核，通过 namespace 隔离进程、网络和文件系统视图，通过 cgroup 限制和统计 CPU、内存等资源。

因此容器启动更快、资源开销更低，但它不是一台完整的虚拟机，也受到宿主机内核兼容性和安全边界的约束。

### 追问

namespace 和 cgroup 分别解决什么问题？

### 追问的回答

namespace 解决进程“能够看见什么”，例如 PID namespace 隔离进程编号，network namespace 隔离网卡、路由和端口，mount namespace 隔离挂载视图。

cgroup 解决进程“能够使用多少资源”，可以统计和限制 CPU、内存、IO 等资源。容器隔离通常需要两者配合：只有 namespace 没有资源限制，一个容器仍可能耗尽宿主机资源；只有 cgroup 则不能隔离进程和网络视图。

## 12. 镜像与容器

### 问题

Docker 镜像和容器是什么关系？

### 回答

镜像是不可变的分层文件系统和运行配置，容器是镜像的一次运行实例。多个容器可以共享同一组只读镜像层，每个容器在运行时拥有自己的可写层。

容器删除后，可写层中的数据也会消失，所以数据库文件或其他持久化数据应存放在 volume 或外部存储中，而不是依赖容器可写层。

### 追问

Docker 镜像为什么要采用分层结构？

### 追问的回答

分层可以复用存储和构建缓存。多个镜像如果拥有相同基础层，宿主机只需要保存一份；构建时前面的指令及输入没有变化，也可以直接复用对应缓存层。

需要注意，删除后续层中的文件并不会自动从早期层移除文件内容。因此敏感文件不应该先复制进镜像再在下一层删除，而应通过构建上下文控制或多阶段构建避免进入最终镜像。

## 13. Kubernetes 核心组件

### 问题

Kubernetes 集群主要由哪些组件组成？

### 回答

控制面主要包括 API Server、etcd、Scheduler 和 Controller Manager。API Server 是统一入口；etcd 保存集群状态；Scheduler 为未调度 Pod 选择节点；Controller Manager 中的控制器持续协调实际状态与期望状态。

节点侧主要运行 kubelet、容器运行时和网络代理。kubelet 根据 PodSpec 驱动容器运行时创建和管理容器，kube-proxy 或其他网络实现负责 Service 相关转发。

### 追问

创建一个 Deployment 后，Pod 大致是怎么运行起来的？

### 追问的回答

客户端把 Deployment提交给 API Server并持久化到 etcd。Deployment Controller 创建 ReplicaSet，ReplicaSet Controller 再创建所需 Pod。Scheduler 为未绑定节点的 Pod 选择节点，目标节点上的 kubelet 获取 PodSpec，调用容器运行时拉取镜像并启动容器。

各控制器不是执行一次就结束，而是持续观察和协调。例如 Pod 数量少于 replicas 时，ReplicaSet Controller 会继续创建 Pod，使实际状态重新接近期望状态。

## 14. Pod

### 问题

为什么 Pod 是 Kubernetes 的最小调度单位，而不是容器？

### 回答

Pod 表示一组需要共同调度和共享生命周期的容器。同一 Pod 内的容器共享网络 namespace，可以通过 localhost通信，也可以挂载共同的 volume。Kubernetes 调度时会把整个 Pod 放到同一个节点，而不会把其中容器分别调度到不同节点。

通常一个 Pod 运行一个主要业务容器；只有日志代理、网络代理等与主容器生命周期紧密相关的辅助进程，才适合采用 sidecar 形式放在同一个 Pod 中。

### 追问

同一个 Pod 中的多个容器可以使用相同端口吗？

### 追问的回答

不可以同时监听相同 IP 和端口。因为同一 Pod 内的容器共享网络 namespace，它们看到的是同一组网络接口和端口空间。一个容器已经监听 8080 后，另一个容器再监听相同地址的 8080 会发生端口冲突。

不同 Pod 拥有各自的网络 namespace 和 Pod IP，因此多个 Pod 都可以在容器内部监听 8080，再由 Service 提供统一访问入口。

## 15. Deployment 与 StatefulSet

### 问题

Deployment 和 StatefulSet 有什么区别？

### 回答

Deployment 适合实例可以互换的无状态服务，Pod 名称和实例身份不稳定，默认支持并行滚动更新。StatefulSet 为每个副本提供稳定序号和网络身份，并按顺序创建、删除和更新 Pod，还可以通过 volumeClaimTemplates 为每个副本关联独立 PVC。

StatefulSet 只提供稳定身份和生命周期顺序，不会自动完成数据库主从复制或业务数据一致性，这些仍需要应用自身协议实现。

### 追问

什么场景应该优先使用 StatefulSet？

### 追问的回答

实例需要稳定网络标识、稳定存储归属或有序启停时适合 StatefulSet，例如 Kafka、ZooKeeper 和部分数据库集群。普通无状态 HTTP API通常优先使用 Deployment，因为副本更容易替换和扩缩容。

判断关键不是“服务有没有状态”这句话本身，而是实例身份是否可互换、存储是否需要跟随固定副本，以及应用协议是否依赖有序启动。

## 16. Service

### 问题

Kubernetes Service 有什么作用？

### 回答

Pod IP 会随重建发生变化，Service 通过 selector 选择一组 Pod，并为它们提供稳定的虚拟 IP 和服务发现名称。符合 selector 且处于 Ready 状态的 Pod 会进入对应 EndpointSlice，流量再被转发到这些后端地址。

ClusterIP 用于集群内部访问；NodePort 在节点地址上开放固定端口；LoadBalancer 通常请求云平台创建外部负载均衡器。Service 负责四层网络入口和转发，不理解登录用户或业务会话状态。

### 追问

Service 是怎样知道后端有哪些 Pod 的？

### 追问的回答

Service 的 selector 与 Pod label 进行匹配，EndpointSlice Controller 根据匹配结果和 Pod 就绪状态维护 EndpointSlice。代理或数据面读取这些端点信息，将访问 Service 的流量转发到具体 Pod IP。

如果 Service 的 selector 写错、Pod label 不匹配或 readiness 一直失败，Service 本身仍可能存在，但 EndpointSlice 中没有可用后端，请求就无法到达业务容器。

## 17. 资源 requests 与 limits

### 问题

Kubernetes 中 requests 和 limits 有什么区别？

### 回答

requests 表示容器期望获得的资源基线，Scheduler 根据所有容器的 requests 判断节点是否能够容纳 Pod；limits 表示容器可使用资源的上限。

CPU 超过 limit 通常会被节流，内存超过 limit 则可能触发 OOM Kill。requests 设置过低会让节点过度调度，设置过高则可能导致 Pod 长期 Pending，所以参数应根据实际资源观测确定。HPA 使用 CPU 利用率扩缩容时，也会把 CPU request 作为计算基准。

### 追问

为什么 HPA 使用 CPU 利用率时需要设置 CPU request？

### 追问的回答

CPU 利用率通常按“当前 CPU 使用量除以 CPU request”计算。如果没有设置 request，控制器缺少计算利用率的基准，基于 CPU 利用率的扩缩容就无法正常得出期望副本数。

例如容器 request 为 200m，当前使用 100m，对应利用率约为 50%。request 不是可随意填写的形式参数，它会同时影响调度、QoS 和 HPA 计算。

## 18. 常见故障排查

### 问题

Pod 一直处于 Pending、CrashLoopBackOff 或 Ready=false 时，你会怎么排查？

### 回答

我会先执行 `kubectl get pod -o wide` 查看节点和整体状态，再用 `kubectl describe pod` 查看 Conditions、容器状态和 Events，最后根据容器状态读取当前日志或 `kubectl logs --previous` 获取上一次崩溃日志。

Pending 重点检查节点资源、调度约束、PVC 和镜像拉取前置条件；CrashLoopBackOff 重点检查进程退出码、配置、依赖和 liveness；Ready=false 重点检查 readiness 的端口、路径、超时和外部依赖。

### 追问

Pod 显示 Running，为什么 Service 仍然可能访问不到它？

### 追问的回答

Running 只表示 Pod 已经被调度并且至少有容器正在运行，不代表 Pod 已经 Ready。readiness 失败时 Pod 不会作为常规可用端点接收 Service 流量。

还需要检查 Service selector 是否匹配 Pod label、Service 的 `targetPort` 是否与容器监听端口一致、EndpointSlice 是否包含该 Pod，以及 NetworkPolicy 或应用监听地址是否阻断访问。
