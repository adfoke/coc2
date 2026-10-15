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

## 运行预算与进程组

每个钩子**每次事件**有 **5 秒**预算（`coc2 schema` 的 `limits.plugin_hook_timeout_secs`
报的是同一个常量）。超时就杀掉它。

插件在**自己的进程组**里运行，所以：

- 超时杀掉的是**整棵进程树**，不只是 `argv[0]` 那个进程 —— 插件里 `cmd &` 拉起来的
  东西会跟着一起走。
- **钩子的进程树不会活过钩子**：插件结束后进程组里剩下的进程也会被回收。
  所以别把插件写成"转发给一个常驻后台进程"—— 那个后台进程会被杀掉。
  真需要常驻，就让它由**服务之外**的东西（systemd 之类）管，插件只负责通知它。
- 如果确实有东西逃出了进程组（`setsid`、双重 fork），它会在 2 秒宽限后被切断，
  并记一条 `plugin hook left processes behind`。

**钩子是"发了就不管"的**（`Trigger` 不阻塞调用方），所以每次事件都会给每个插件起一个进程。
别写跑很久的插件 —— 它在事件路径上，超时预算是硬的。

## 可观测性

| 日志 | 级别 | 含义 |
|---|---|---|
| `plugin hook ran` | **debug** | 钩子正常跑完了，带 `plugin` / `hook` / `duration` |
| `plugin hook failed` | warn | 插件非 0 退出、被杀，或启动不了；`output` 字段是它的 stdout+stderr |
| `plugin hook left processes behind` | warn | 插件自己结束了，但进程组外的进程还攥着它的输出管道 |

- **默认级别（info）下，成功的钩子不写任何日志。** 要排查「事件到插件了没有」就
  `log_level: debug` —— 这条是为此存在的：没有它，**「钩子从没被调用」和「调用了但没干成事」
  从服务端日志上长得一模一样**。
- 它刻意是 debug 而不是 info：**每次事件、每个插件一条**，info 级别下繁忙机群会按
  metrics_report 的频率刷屏。
- 两类日志都带 **`duration`** —— 拿它当"这个钩子正在逼近 5 秒预算"的早期信号
  （见上面的「运行预算与进程组」）。失败时它还能区分「耗满预算被杀」和「秒退」。

`coc2 plugins list` 报的是**启动时**加载的那批；往目录里加插件要重启 server。

## 负载字段

插件是**本地可执行文件**、无沙箱隔离，因此传进来的负载只包含该 hook 本身需要的信息，
**不含任何凭据**：`agent_connected` 给的是 agent 身份（`agent_id` / `hostname` / `os` / `arch` /
`ip_addrs` / `tags` / `fingerprint` / `version` / `connected_at`），
**没有** agent 的共享 token。要加字段就在 `internal/server/plugins.go` 的事件结构体里**显式**加，
不要把线上的 wire 结构体直接透传——那种写法会把后来新增的敏感字段自动漏给插件。
