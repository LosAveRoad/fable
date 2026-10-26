# 压测脚本

本文由仓库维护者维护，说明脚本实际覆盖的场景。架构与验收边界见[总架构](../../docs/architecture.md)。从仓库根目录执行，在独立负载机上运行 k6，并明确设置自己的测试目标。

## 场景与参数

| 脚本 | 实际行为 | 主要参数 |
| --- | --- | --- |
| [healthz.js](healthz.js) | 固定到达率访问 `/healthz` | `BASE_URL`、`TARGET_RPS`、`DURATION` |
| [api.js](api.js) | 仍然只访问 `/healthz`；按 VU 阶梯升压，不是业务 API 压测 | `BASE_URL`；阶段固定在脚本中 |
| [business.js](business.js) | 查询已加入群列表及可选群历史，不发送消息 | `BASE_URL`、`TARGET_RPS`、`DURATION`、`AUTH_TOKEN`、可选 `GROUP_ID` |
| [websocket.js](websocket.js) | 使用 Token 建立连接，五秒后关闭；不验证收发消息 | `WS_BASE`、`VUS`、`DURATION`、`WS_TOKEN` |

例如，对本机后端运行基础 HTTP 场景：

```sh
k6 run -e BASE_URL=http://127.0.0.1:8080 -e TARGET_RPS=10 -e DURATION=30s --summary-export=summary.json tests/load/healthz.js
```

令牌使用专用测试用户。多个 VU 复用一个 WebSocket Token 时，同一实例会触发同用户连接替换，因此不能将该结果解释为独立用户容量。

## 手动工作流

[load-test.yml](../../.github/workflows/load-test.yml) 提供手动运行和结果 Artifact，环境配置见[密钥说明](../../deploy/secrets/README.md)。工作流传入 `WS_VUS`，但脚本实际读取 `VUS`，当前工作流的 WebSocket 并发输入不会按预期生效；本次未修改工作流或脚本。

[run-fixed-ip.ps1](run-fixed-ip.ps1) 另提供 PowerShell/Docker 入口。默认域名和 IP 是原部署环境配置，应显式指定目标。采集脚本为 [collect-vps1.sh](collect-vps1.sh) 和 [collect-vps2.sh](collect-vps2.sh)。

## 结果记录

仓库当前没有随文档提供已填写的容量报告。运行后应保存提交 SHA、依赖和部署拓扑、测试账户数、场景参数、原始输出及摘要，说明成功率、延迟和首个瓶颈证据。

以上脚本无法证明跨实例消息成功率、群聊扇出正确性、Kafka 重放幂等或滚动发布恢复。连接 Upgrade 成功与 `/healthz` 返回 200 都不能替代端到端收发验收。
