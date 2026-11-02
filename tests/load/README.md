# 公开黑盒与消息负载

驱动只访问当前三角色拓扑的公共 HTTP、WebSocket v2 与 MCP；账户、会话、群及消息通过公共接口创建，不直接读写 MySQL、Redis、Kafka 或内部 RPC。每次使用新的 UUID `run-id` 创建独立测试身份；仅在专用测试环境运行，测试数据会保留在所选环境中，环境所有者负责按本轮资源范围清理。

从仓库根目录启动[本地拓扑](../../deploy/local/README.md)，然后执行：

```sh
go run ./cmd/blackbox -base-url http://127.0.0.1:18080 \
  -run-id "$(python3 -c 'import uuid; print(uuid.uuid4())')" \
  -report /tmp/fable-blackbox-report.json

go run ./cmd/bench -base-url http://127.0.0.1:18080 \
  -run-id "$(python3 -c 'import uuid; print(uuid.uuid4())')" \
  -report /tmp/fable-bench-report.json \
  -pairs 2 -rate 2 -duration 10s -receive-timeout 5s -min-receive-ratio 1
```

`blackbox` 验证注册登录、单聊、群聊、稳定 ID 重试与冲突、同步分页、重连恢复、MCP 授权与撤权及无效输入。详细阶段及失败以实际 JSON 报告为准。它不负责停止服务或注入后端故障；受控 Job 重启、DLQ 等故障验收由[拓扑测试](../../docs/test-environment.md) 执行。

`bench` 使用不同用户对发送实际消息。`-pairs` 控制不同用户对的数量，`-rate` 是所有用户对合计的每秒尝试发送 tick 数，`-duration`、`-receive-timeout` 控制测量与等待边界。生成器忙时跳过 tick，报告 `generator_busy_ticks` 与实际 `achieved_attempt_rate`，不能把请求速率当成达成吞吐；执行 `go run ./cmd/bench -h` 查看参数语义。HTTPS 私有测试 CA 可用 `-ca-file`，不通过关闭证书验证处理。报告记录尝试、保存、不确定、拒绝、在线收到、同步找回、重复推送、修复后仍缺失及生成器错误；保存与在线接收延迟分别包含 samples、p50、p95、p99，保存成功与 Kafka 排队均不等于对方在线收到。同步找回的消息不能计作按时在线送达。

每轮保留提交 SHA、部署拓扑与依赖版本、账户数、参数、开始/结束时间及 JSON 原始报告；同时由环境所有者采集各角色 CPU/内存、连接数、Kafka lag 与重试/DLQ 指标，说明首个瓶颈。驱动报告不包含云资源指标，外部采集须与同一轮时间窗口对应。可用 `-max-save-p95`、`-max-receive-p95`、`-min-receive-ratio` 设置本轮验收阈值；阈值必须在实际测量前按场景明确，不从小规模 CI 样本推导生产 SLO。

[CI](../../.github/workflows/ci.yml) 在托管 2/2/2 拓扑启动后执行公共黑盒和两对用户、10 秒的有界负载，使用 `-min-receive-ratio 1` 要求全部消息在线收到，不能仅靠同步找回通过；上传报告及安全角色日志；这验证入口和测量流程，不是容量认证。当前尚无 Azure 订阅，AKS 资源、云端黑盒与压测尚未执行，不能将本地结果写成云端结果。旧固定目标压测脚本与远程发布工作流已移除；历史材料通过 Git 查阅。
