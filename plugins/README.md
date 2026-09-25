# Plugins

`server` 会从 `-plugins` 指定目录加载可执行文件插件。

调用方式：

- 启动参数默认是 `-plugins plugins`
- 每个可执行文件会在事件发生时被调用一次
- 第一个参数是 hook 名称
- 事件 JSON 会通过 stdin 传入

当前 hook：

- `agent_connected`
- `task_result`
- `transfer_done`
- `metrics_report`

可参考同目录的 `example-plugin.sh.sample`。

## 负载字段

插件是**本地可执行文件**、无沙箱隔离，因此传进来的负载只包含该 hook 本身需要的信息，
**不含任何凭据**：`agent_connected` 给的是 agent 身份（`agent_id` / `hostname` / `os` / `arch` /
`ip_addrs` / `tags` / `fingerprint` / `version` / `connected_at`），
**没有** agent 的共享 token。要加字段就在 `internal/server/plugins.go` 的事件结构体里**显式**加，
不要把线上的 wire 结构体直接透传——那种写法会把后来新增的敏感字段自动漏给插件。
