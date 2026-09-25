# CoC2 真机跨洋端到端测试报告

**被测版本**：`1a21800` *Stagger batch dispatch across a random window*（工作区干净）
**测试者**：Alma（受 john 授权，使用两台 VPS + 本机）
**日期**：2026-09-25
**仓库自带测试**：`make check`（gofmt + go vet + go test ./...）**全绿**

**结论**：核心链路（命令执行 / 文件传输 / 批量错峰 / 审计 / 重连补发 / 鉴权 / TLS）在三真机跨洋拓扑上
**功能正确、性能可用**。发现 **1 个必修缺陷**（`SIGTERM` 停不掉健康的 agent，附带一个真实事故级副作用）
和 **1 个安全问题**（插件拿到共享 token 明文），另有 3 处文档/观测量缺口。

> **两个缺陷已于 2026-09-25 修复**，各自带回归测试（先确认测试在旧代码上会失败），
> 并在同一套真机上复验通过 —— 见 [§8 修复记录](#8-修复记录2026-09-25)。文档缺口尚未处理。

---

## 1. 拓扑

| 角色 | 主机 | 位置 | 说明 |
|---|---|---|---|
| Server | `216.234.143.109`（DMIT） | 美国 LA | Agent 面 `:8080`、Operator TCP `:8081`、TLS 实例 `:8443/:8444` |
| Agent `la-node` | 同上 | 美国 LA | 与 Server 同机回连 |
| Agent `jp-node` | `198.13.57.247`（Vultr） | 日本东京 | 跨洋回连 |
| Agent `mac-node` | 本机 Mac | 中国·家宽 NAT 后 | 跨洋回连 |
| 操控端 | 本机 `coc2` CLI | 中国 | 公网 `http://…:8081` + Bearer |

三台 Linux 均为 Ubuntu 26.04 / x86_64 / 1 vCPU / ~950MB。二进制本地交叉编译
（`GOOS=linux GOARCH=amd64` + 本机 `darwin/arm64`）；仓库自带的 `bin/` 是 9/15 的旧产物，测试全程用 HEAD 现编。
Server 用 `openssl rand -hex 32` 生成的真 token；配置 `0600`。

---

## 2. 缺陷一（必修）：`SIGTERM` 停不掉健康的 agent

### 复现（受控、可重复）

对一个**全新、连接正常**的 agent 发一次 `SIGTERM`：

| 时刻 | 观测 |
|---|---|
| T0 | `probe-sigterm` 上线，日志 `wire=protobuf` |
| T0+8s | `SIGTERM`（`kill` 返回 0） |
| T0+5…45s | 进程仍 `Ssl` 存活；server 侧 `online=true`；`last_seen_at` 仍在刷新；**agent 日志无任何关闭记录** |
| T0+50s | 再补 2 发 `SIGTERM` → 仍存活（累计 3 发无效） |
| 之后 | 给它派任务 → 立刻返回 `canceled`（`exit -1`、stdout/stderr 全空） |
| `SIGKILL` | 立即退出，server 3 秒内 `online=false` |

**即：`SIGTERM` 既不让它退出，也不让它停止接活。** 它留在舰队里、被当作在线，
但之后派给它的每个任务都瞬间以 `canceled` 结束。

### 真实副作用（不是理论推演）

测试中我用了 `timeout 8 ./bin/agent …` 想限制一次 TLS 试连，
结果**整条命令挂满 252 秒才被外部掐断**——因为 `timeout` 默认发 SIGTERM，而 agent 不理它，
管道永不关闭。这不是新问题，就是同一个 bug 在运维脚本里的表现：
任何用 SIGTERM 停 agent 的编排（systemd / docker stop / k8s）都会走到超时强杀。

### 根因

`internal/agent/client.go` 的 `runOnce()` 主循环：

```go
for {
    opcode, raw, err := conn.ReadMessage()   // 阻塞；无读超时；无 ctx 分支
    if err != nil { return err }
    ...
}
```

同文件约 195 行注释明写「No read deadline here」。`cmd/agent/main.go` 用
`signal.NotifyContext(…, SIGTERM)` 只做了一件事：取消根 context。而：

- `heartbeatLoop` 有 `case <-ctx.Done()`；`startTask` / 传输处理也从 ctx 派生 —— 它们**知道**要停；
- 主循环卡在 `ReadMessage()` 上，**永远看不到 ctx 被取消**；
- 于是进程只在 socket 出错时才退出。连接一直健康 ⇒ 永不退出。

旁证：`SIGSTOP` 冻住 agent → server 在 70s（`pong_wait`）后正确判离线；
`SIGCONT` 之后 agent 才因为连接已断而退出 —— 说明「健康连接下看不到 ctx」正是症结。

### 影响

1. **没有优雅停机路径**：滚动重启必然吃满超时后强杀。
2. **舰队视图说谎**：停不掉的 agent 仍显示 `online`，编排会把活儿继续派过去。
3. **任务静默变 `canceled`**：我一开始把「派发后 60ms 变 canceled」误判成别的，
   实际就是「agent 已被 SIGTERM 弄成僵尸」的产物。而 `canceled` 语义是「有人要求取消」——
   运维看到会误判成有人手工干预。

### 建议修法（最小改动）

让阻塞的读循环随 ctx 醒来，任选其一：

```go
go func() { <-ctx.Done(); conn.Close() }()        // ReadMessage 立刻返回错误
// 或：给 conn 设读 deadline 并随 ctx 刷新
```

`conn.Close()` 幂等，不影响 `defer conn.Close()`；`runOnce` 返回后
`Run` 看到 `ctx.Err() != nil` 即退出。

---

## 3. 缺陷二（安全）：插件拿到的是含共享 token 的 `hello`

`internal/server/client.go:99` 把**整个** `AgentHello` 交给插件：

```go
a.service.plugins.Trigger("agent_connected", hello)
```

而 `AgentHello.Token` 就是全舰队的共享 agent token。实测插件日志原文：

```
agent_connected {"agent_id":"la-node","token":"d6080cb6…（64 位十六进制，明文）","hostname":"DMIT-…
```

插件按 `plugins/README.md` 是**本地可执行文件**、无沙箱隔离（README 自己列在「已知限制」里）。
于是：任何插件（包括第三方样例、被替换的文件）都白拿一份能冒充任意 agent 的凭据。

**建议**：`Trigger` 前把 `Token` 置空（`hello.Token = ""`），或定义专门的
`agentConnectedEvent` 结构体只带 `agent_id/hostname/os/arch/tags/fingerprint`。
顺带核对 `task_result` / `transfer_done` / `metrics_report` 三个负载里是否也有类似敏感字段
（本次未发现）。

---

## 4. 文档 / 观测量缺口（**已全部处理**，见 §9）

1. **1 MiB 输出上限没进文档**。`internal/agent/client.go:32` `maxTaskOutputBytes = 1 << 20`，
   stdout / stderr 各一份，超了追加 `"\n[output truncated]"`。
   实测 2,000,000 字节 stdout → 收到 1,048,595 字节 = `1<<20` + 19 字节标记，长度精确对得上。
   对一个「为 AI 而生的 CLI」这是关键契约：只读文档的 agent 会把截断当完整输出。
   建议进 README「已知限制」+ `coc2 schema`。**行为本身正确。**

2. **启用 `-operator-listen` 后，UDS 面也强制 token**。README 表格写「socket 免 token / TCP 强制 token」，
   易被读成两面独立；实际 `hub.go:1016`：

   ```go
   authRequired := s.cfg.OperatorListen != "" || s.cfg.OperatorUDSPath == ""
   ```

   一旦开了 TCP 逃生门，本机 UDS 调用也必须带 token（代码注释如此，文档没跟上）。
   我第一次 `curl --unix-socket ./coc2.sock …/healthz` 拿到 `{"error":"unauthorized"}` 就是这个。

3. **非 UTF-8 的「不再被静默损坏」只在线上 + 存储成立**。原始字节确实完整入库：
   用 `SELECT CAST(stdout AS BLOB)` 从 SQLite 读回，`41 FF FE 42` **原封不动**；
   但 CLI 的 JSON 出参把它们变成 `41 EFBFBD EFBFBD 42`（两个 U+FFFD）。
   建议文档写清边界：保真到 server/DB，损失发生在 JSON 序列化那一步。

4. **成功续传没有日志**。agent 只在 `send transfer resume` **失败**时打日志；
   续传成功不留痕，我是靠对照实验才定量确认的（见下）。建议成功续传加一行 info。

5. **mTLS 与 operator TCP 面互斥**：开启 `client_ca` 后 operator TCP 面也要求客户端证书
   （`hub.go:229` 套的是同一个 `tlsCfg`），而 `coc2` CLI 全局 flag 里**没有客户端证书参数**
   （只有 `-server/-token/--pretty/-timeout/-insecure`）。实测 `-insecure health` 仍报
   `remote error: tls: certificate required`，exit 2。所以「mTLS + 远程 operator 面 + CLI」目前无法组合，
   需要配 `-operator-uds` 走本机，或给 CLI 加 `-client-cert/-client-key`。

---

## 5. 功能验证：通过

### 命令执行

| 用例 | 结果 |
|---|---|
| 单机 `uptime`（东京，跨洋） | `all_ok=true`，`exit 0`，`duration_ms=5` |
| 非零退出码 `exit 7` | `state=failed`、`exit_code=7`，CLI 退出码 1 |
| stdout / stderr 分流 | 各归各位，未混 |
| 100KB stdout | 端到端 sha256 与本地期望**逐字节一致** |
| 2MB stdout | 截断在 1 MiB + 标记（行为正确，文档缺失，见 §4.1） |
| 非 UTF-8 `41 FF FE 42` | 原始字节完整入库（BLOB 验证），CLI JSON 侧变 U+FFFD |
| 超时 `sleep 300` + `--exec-timeout 3` | `state=timeout`；东京机 `sleep 300` **0 残留** |
| 取消 `sleep 300` | `cancel_requested` → `canceled`；东京机 2 个进程 → **0 残留** |
| 并发上限 | 20 个 `sleep 45` 抢发 → **恰好 16 个 dispatched、4 个 `failed: agent is already running the maximum number of tasks`**，与文档一致 |
| Agent 离线期下发 + 重连自动补发 | 断线期下发 → 排队；agent 重连后自动补发并成功（`delivered-after-reconnect`） |
| 家宽 NAT 后的 agent（Mac） | 正常回连并执行，`hostname` 返回 `Johns-MacBook-Air.local` |

### 文件传输（跨洋，SHA256 双向核验）

| 用例 | 体积 | 结果 |
|---|---|---|
| push 服务端 → 东京 | 64 MiB | 5.18s（≈12.4 MiB/s），`checksum_verified=true`，东京端独立 sha256 一致 |
| pull 东京 → 服务端 | 32 MiB | 1.1s（≈30 MiB/s），两端 sha256 一致 |
| 传输中取消 | 512 MiB | 取消时 `.part` 已落 187,957,248 字节并**保留**；`status=canceled` |
| 断点续传 | 512 MiB | 从 179.2 MiB 接上，17.99s；**对照组全量 25.63s**（按剩余字节理论 16.7s）→ 确认是续传非重传；终态 sha256 与源一致，`.part` 已清理 |
| 取消已完成的传输 | — | 服务端正确拒绝：`transfer_not_canceled: transfer ended as success`（exit 1） |

### 批量错峰（HEAD commit 的核心声明）

`--tag` 扇出 2–3 目标 × 5 轮，从服务端 SQLite 直读 `release_at - created_at`：

```
0.213s  0.938s  3.276s  4.120s  4.294s  4.656s  6.979s  6.989s
```

8 个样本均匀铺在 `[0, 8s]` 窗口内；单目标 `release_at = NULL`（立即下发）—— 声明成立。

**服务端重启存活**（声明：延迟落库，重启不丢）：`--spread 45s` 下发 2 目标 → 3 秒后 `pkill` 重启 Server →
`la-node` 的 `release_at`（T+8.0s）已过，重启后 **1 毫秒内**被精确唤醒下发；
`mac-node` 的 `release_at`（T+28s）在重启 20 秒后仍按原定时刻触发。
两个任务的 `release_at` 与停机前**逐字节相同**。✔

### 鉴权与边界

| 用例 | 期望 | 实测 |
|---|---|---|
| 错误 token（operator TCP） | exit 3 | exit 3 |
| 端口黑洞 | exit 2 | exit 2 |
| 内置 dev token 启动 | 拒绝启动 | `panic: refusing to start with the built-in development token…` |
| fan-out 不带 `--yes` | exit 4 | exit 4，`code=needs_yes` |
| `--spread 5m` + 默认 `--wait-timeout` | exit 4 | exit 4，报错把两个数都算给你看 |
| 空选择器 | exit 4 | exit 4 |
| `--group` 传组名（非 id） | — | `400 no target agents`（schema 已写明要 `group ids`） |
| `require_tls` 且未配证书 | 拒绝启动 | exit 1 + `fatal: require_tls is set but no TLS certificate configured`（写进 `log_file`） |
| 带 `Origin` 的 WS 握手 | 403 | 403 |
| `Origin: null` | 403 | 403 |
| 无 `Origin` 的合法握手 | 101 | 101 |
| agent 面 `/api/v1/agents` | 404 | 404（管理 API 不在这个面） |
| agent 面 `/healthz` | 200 免鉴权 | `{"ok":true,"plane":"agent"}` |
| operator 面 `/healthz` 无 token | 401 | 401 |
| 错误 token 的 agent | 拒入 + 落审计 | `auth_failed: token mismatch`，审计 `actor=unauthorized, status=401`；**未登记进 agents 表** |
| 空列表 | `[]` 非 `null` | `[]` ✔ |
| `schema` 自描述 | 机器可读 | 6468 字节，含 exit_codes / 全局 flag / 每条命令的 flag 类型与默认值 |

### 对抗性鉴权测试（自写探针，复现 agent 面 3 条声明）

用仓库自身的 `internal/protocol` 编解码写了个 WS 探针，对一个**正在执行**的 jp-node 任务
（`sleep 90`，状态 `dispatched`）发起三种伪造：

| 攻击 | 服务端反应 | 任务是否被改写 |
|---|---|---|
| 1. 不 hello，直接发 `task_result` | `{"code":"not_registered","message":"hello is required first"}`，随后连接超时 | ✅ 仍 `dispatched` |
| 2. 错误 token hello 后发 `task_result` | `auth_failed: token mismatch` → 连接被关（1006） | ✅ 仍 `dispatched` |
| 3. **正确 token**，以 `spoof-agent` 身份伪造 jp-node 的结果 | hello 通过，但结果 `applied=false`；审计 `ok=false, status=400, actor=spoof-agent` | ✅ 仍 `dispatched`，stdout 未被 `FORGED-BY-SPOOFER` 污染 |

三条声明（hello 前置、token 恒定时间比较、结果只认归属）**均成立**。

### TLS 1.3 / mTLS

自签 CA + server 证书（SAN 含 IP）+ client 证书：

| 用例 | 结果 |
|---|---|
| `wss://` + `-ca-cert` + `-client-cert/key` | 连上，`wire=protobuf` ✔ |
| `wss://` + `-ca-cert`，无客户端证书 | `remote error: tls: certificate required`（mTLS 生效） |
| 明文 `ws://` 打 TLS 端口 | `websocket: bad handshake`（降级被拒） |
| 缺客户端证书时的 mTLS 提示 | 见 §4.5（CLI 无法提供客户端证书） |

### 插件（四种钩子全部触发）

```
metrics_report: 53    agent_connected: 10    task_result: 7    transfer_done: 2
```

`transfer_done` 用一次 push + 一次 pull 补测，负载含 `direction/status/size/bytes_transferred`。
`plugins list` / `metrics overview` 的 `plugins` 计数正确。

### 审计（oplog）

- operator 面：`plane=tcp, actor=token, source=223.88.222.37:5993`（本机公网出口）+ 路径 + 命令原文摘要 + `status`；
- agent 面：`plane=agent, actor=jp-node, source=198.13.57.247:41790` + `task_result status=… applied=true/false`；
- 失败鉴权：`actor=unauthorized, status=401`；
- **两种 `canceled` 的区分只存在于审计里**（见 §2 影响 3）：operator 取消留下
  `POST /api/v1/tasks/<id>/cancel`，而 agent 被 SIGTERM 后上报的是
  `task_result status=canceled applied=true`。任务记录本身**逐字节相同**（`exit -1`、空 stdout/stderr）。

### 指标

`agents metrics` 返回 `cpu_count / goroutines / process_memory_bytes / root_disk_free|total_bytes / uptime_secs`；
`agents history` 有历史（限 3 条返回 2 条，且 `uptime_secs` 随时间递增）；`metrics overview`
的 `online_agents` / `total_agents` / `pending_tasks` / `active_transfers` / `oplog_entries` / `plugins` 均正确。

---

## 6. 方法学备注（下次可复用）

- **测 Worker/进程自身开销看它自己报的数，别用客户端 RTT** —— 这次同理：
  `tasks` 记录里的 `duration_ms`（5–6ms）与客户端墙上时间（跨洋往返 1s+）差两个数量级。
- **要对 Agent 做危险操作时，别用 `pkill -f "<字符串>"`**：命令行里含同样字符串会**杀掉自己的 ssh shell**
  （我踩了两次，exit 255）。用 `pkill -x agent`，或 `pgrep`/`grep` 的 `[b]racket` 技巧。
- **`timeout <n> <cmd>` 对未修此 bug 的 agent 无效**（它忽略 SIGTERM）。测试里用 `timeout -s KILL`。
- **zsh 里 `GID` 是保留变量**（赋值报 `failed to change group ID`）；`$ids` 不做单词分割，循环要用 `for id in $=ids`。
- **`-config` 不传时会读默认 `config.yaml`**，其 `log_file` 会把启动错误从 stderr 挪进文件——
  排查「静默退出」前先确认日志去哪了（我差点误报一次）。
- 量级对照实验比单点观测可信：续传这件事，一次 17.99s 说明不了什么，
  加上「全量 25.63s」和「按剩余字节理论 16.7s」两条参照才闭环。

---

## 7. 测试痕迹与清理

- **测试机改动**：LA 机 `/root/coc2-test/`（三个二进制 + 配置 + SQLite + 证书 + 插件样例），
  ufw 放行 `8080/8081/8443/8444`；东京机 `/root/coc2-test/`（agent + 日志）。测试结束后已全部清理、ufw 规则移除。
- **两台机器上原有的服务（xray / nginx / 订阅）全程未改**，只做只读访问 + 新增独立目录。
- **本仓库**：测试阶段只读；临时交叉编译产物与探针源码在测试后清除，
  本报告 + 下述修复是唯一留存物。

---

## 8. 修复记录（2026-09-25）

两个缺陷都在被测版本上修掉，改动共 3 个文件 / 44 + 11 + 19 行。

### 8.1 `SIGTERM` 停不掉健康的 agent

**改动**：`internal/agent/client.go`

| 位置 | 内容 |
|---|---|
| `runOnce()` | 连接建立后起一个 watcher：`ctx.Done()` → `conn.Close()`，把阻塞的 `ReadMessage()` 叫醒；用 `stopWatch` channel 在正常收尾时释放它，不泄漏 goroutine |
| `Run()` | ctx 已取消时（即正在停机）先 `waitRunningTasks(shutdownGrace)` 再返回 |
| `startTask()` | `taskWG.Add(1)` / `defer c.taskWG.Done()`，让停机能等到任务 goroutine 清完进程组 |
| 新增 | `shutdownGrace = 5s` 常量 + `waitRunningTasks(timeout)` |
| `Client` | 新增 `taskWG sync.WaitGroup` |

`waitRunningTasks` 是**故意加的**，不是顺手：主循环一醒来进程就会退出，
如果不等任务 goroutine 把 `SIGKILL` 送到进程组，就会留下孤儿进程 ——
正好破坏 README 承诺的「取消/超时终止整棵进程树，不留孤儿进程」。

**回归测试**：`internal/agent/shutdown_test.go`

- `TestRunExitsOnContextCancellation` —— 两个子用例（空闲会话 / 执行中），
  fake server 建立会话后**保持连接健康且不发任何东西**，正是当初卡死的状态，然后取消 ctx 要求 `Run` 返回。
- `TestShutdownKillsRunningTaskTree` —— 任务命令是 `sleep 2; echo orphaned > <marker>`，
  停机后等 3 秒断言 marker **没有**出现（进程组真被杀了，而不是被丢弃）。

**测试有效性**（这是关键，不然测试等于装饰）：

```
撤掉修复 → FAIL  TestRunExitsOnContextCancellation (30.01s)
                idle_session: Run did not return after cancellation: a SIGTERM would leave the agent running
                mid-task:     同上
装回修复 → ok    coc2/internal/agent  0.303s
```

**真机复验**（同两台 VPS，同拓扑）：

| 场景 | 修复前 | 修复后 |
|---|---|---|
| 空闲健康 agent 收 SIGTERM | 3 发无效，4 分钟后仍在心跳 | **立即退出**，server 随即 `online=false` |
| 执行中收 SIGTERM | 同上 | agent 退出，东京机 `sleep 120` **残留 0** |
| 空闲收 SIGINT（Ctrl-C） | — | **生效** |
| 执行中收 SIGINT | — | agent 退出，`sleep 90` **残留 0** |
| `timeout 8 ./bin/agent …`（决定性的实践症状） | **挂满 252 秒** | **8.02 秒**返回 |
| 被遗弃任务的回收器兜底 | — | 两个任务均落 `timeout` /「no result received before timeout」 |

### 8.2 插件拿到共享 token 明文

**改动**：`internal/server/plugins.go` 新增 `agentConnectedEvent`（显式字段，无 `Token`）；
`internal/server/client.go` 的 `Trigger("agent_connected", …)` 改为传这个类型。

特意用**独立类型**而不是「把 `hello.Token` 置空后照传」：置空是隐式约定，将来给
`AgentHello` 加字段会自动泄漏到插件；独立类型逼着以后加字段必须显式写进来。
`plugins/README.md` 从未把 payload 字段写成契约，所以不构成破坏性变更。

**回归测试**：`internal/server/plugin_token_test.go`
走真实 hello 路径 + 真插件脚本捕获 stdin，断言 payload 里**没有** token，
同时断言 `agent_id` 和 `tags` 还在（防止「修」成传空对象）。

**测试有效性**：

```
撤掉修复 → FAIL  agent_connected payload leaked the shared token:
                 {"agent_id":"plugin-probe","token":"fleet-secret-token", … }
装回修复 → ok
```

**真机复验**：插件收到的 payload 是

```json
{"agent_id":"jp-node","hostname":"vultr","os":"linux","arch":"amd64",
 "ip_addrs":["198.13.57.247"],"fingerprint":"e0f2eb58…","version":"v0.4.0",
 "connected_at":"2026-09-25T07:41:48.844Z"}
```

`grep -c <token>` 命中 **0**。

### 8.3 修复后的整体验证

- `make check`（gofmt + vet + go test ./...）**全绿**
- `make test-race` **全绿**（新增了 goroutine + WaitGroup，race 必须过）
- 两个新测试都确认「旧代码上失败、新代码上通过」

---

## 9. 第二轮：文档缺口 / 观测量 / mTLS 死角（2026-09-25，john "一起处理下吧"）

§4 的 5 条 + §8.4 的 disconnect 全部处理完。改动 8 个文件（3 个新增）。

### 9.1 审计加 `disconnect` 行

`internal/server/hub.go` 的 `unregister()` 现在落一行 agent 面审计（`Kind: "disconnect"`），
带会话时长，并区分两种结局：

| 结局 | 摘要 |
|---|---|
| 会话真的结束了 | `session ended (session 7s)` |
| 被新连接顶替 | `session superseded by a newer connection (session 2s)` |

为了算时长给 `agentConn` 加了 `registeredAt`（在 `register()` 里赋值）。

**顺带修了一个真的行为 bug**：`unregister()` 原来无条件 `SetAgentOnline(id, false)`，
但 `register()` 会先把同一 id 的旧连接踢掉、再写新连接——旧连接的 teardown 随后跑，
于是**把还活着的新连接的在线标记擦掉**。加了 `superseded` 判定：顶替场景不动 online 标记。

**但要说清楚证据强度**：这条竞态我**没能构造出可观测的失败**。
我写了竞争式测试（两次 dial 后连续断言 online），结果**撤掉修复仍然通过**——
因为 `register()` 是先踢旧的再写新的，旧连接的 teardown 在实践中总是落在新连接 upsert 之前，
所以正常情况下根本不会翻成 offline；就算偶尔翻了，一个 ping 周期内 `touch()` 也会修回来。
也就是说：**这是一处靠 goroutine 顺序侥幸不发作的脆弱点，不是已复现的线上故障。**
修复本身无害且更明确，但别把它当成"修了一个线上 bug"来讲。

因此测试改成**确定性地驱动那个顺序**：先注册新连接，再手工构造一个"被顶替的" agentConn
（fake 的 `conn`/`send`/`done`，`drained` 已关闭）并调 `unregister`，断言 online 仍为 true。
把守卫改回无条件时该测试**确实 FAIL**（已验），所以它守的是这个不变量，不是那次侥幸的时序。

### 9.2 1 MiB 输出上限进文档 + 进 schema

上限原来只写在 `internal/agent/client.go` 的常量里，文档和 schema 都没有——
对"为 AI 而生的 CLI"这是个坏缺口：只读文档的 agent 会把截断结果当完整结果。

- 把 `MaxTaskOutputBytes` 和 `MaxConcurrentTasks` 挪到新的 `internal/common/limits.go`：
  **schema 报的数和 agent 实际执行的是同一个常量**，不会各自漂移。
- `coc2 schema` 新增 `limits` 段：

```json
"limits": {
  "task_output_bytes_per_stream": 1048576,
  "task_output_truncation_marker": "[output truncated]",
  "task_output_note": "stdout and stderr are each capped at ...",
  "concurrent_tasks_per_agent": 16,
  "concurrent_tasks_note": "dispatches beyond this are answered with a failed result ..."
}
```

- README「已知限制」补两条（输出上限 + 并发上限），写明"看到 `[output truncated]` 就是被截断，没看到就是完整的"。

### 9.3 `-operator-listen` 下 UDS 也校验 token —— README 改准

原来的表格容易被读成"两个面各自独立判断"。实际判定是
`authRequired := OperatorListen != "" || OperatorUDSPath == ""` ——
**一旦开了 TCP 逃生门，本地 socket 调用也要带 token**。
表格和 `-operator-listen` 参数行都改成了明确表述。

### 9.4 非 UTF-8 的保真边界写进"线协议"段

原来只说"不再被静默损坏"，没说边界在哪。现在写明：原始字节从 Agent → Server → SQLite
**全程不损坏**，损失**只发生在 CLI 序列化成 JSON 的那一刻**（JSON 字符串承载不了任意字节）；
看到替换符 ≠ 链路或存储坏过，要原始字节就从 API/DB 取。

### 9.5 续传成功加日志

`beginUpload()` 在 `offset > 0` 时打一行 info（之前只有**通知失败**才打日志，
成功续传不留痕）。真机实测：

```
{"level":"info","msg":"resuming partial upload","transfer_id":"…",
 "remote_path":"/root/coc2-v2/recv/r-256m.bin","offset":114819072,"size":268435456}
```

### 9.6 CLI 补客户端证书支持（原来的 mTLS 死角）

原来 `client_ca` 一开，operator TCP 面也要求客户端证书，而 CLI 全局 flag 里
**没有任何客户端证书参数**（只有 `-server/-token/--pretty/-timeout/-insecure`），
于是"mTLS + 远程 operator 面 + CLI"根本无法组合，只能退回本机 socket。

新增三个全局 flag：`-ca-cert` / `-client-cert` / `-client-key`，并进 `schema`。
`-insecure` 保持原义（跳过校验），且**不会因为给了客户端证书就自动跳过校验**。

真机验收（mTLS server，`:8444`）：

| 场景 | 期望 | 实测 |
|---|---|---|
| `-insecure`（修复前唯一可行方式） | — | exit 2，`tls: certificate required`（跳过校验也过不了 mTLS） |
| `-ca-cert` + `-client-cert` + `-client-key` | 通过 | **exit 0**，`{"ok":true,"plane":"operator"}` |
| 有 CA，无客户端证书 | exit 2 | exit 2 |
| 有客户端证书，无 CA（自签） | exit 2 | exit 2，`certificate is not trusted` |
| 只给 cert 不给 key | exit 4 | exit 4，`must be given together` |
| 错误 token | exit 3 | exit 3 |

第 2 行就是**修复前做不到的那一格**。agent 侧也顺带验了 `wss://` + mTLS 回连正常。

### 9.7 新增测试

| 文件 | 覆盖 |
|---|---|
| `internal/server/disconnect_test.go` | disconnect 行内容 / superseded 标记 / 顶替不擦 online（确定性驱动） |
| `internal/cli/tls_test.go` | 无材料不装覆盖 / mTLS 材料装载 / 五类错误输入 → exit 4 / 三个 flag 真能被解析 |

`TestTLSFlagsAreGlobal` 第一版**只查了 flag 名字表**，而这次正好踩到"表里有、FlagSet 上没注册"
的漏注册（`flag provided but not defined`）——**那个弱版本放过了真实缺陷**。
改成走真实 registry 解析后，撤掉注册它确实 FAIL（已验）。

### 9.8 第二轮的真机验收汇总

| 项 | 结果 |
|---|---|
| `disconnect session ended (session 7s)` | ✔ |
| `disconnect session superseded by a newer connection (session 2s)`，且 agent **仍在线** | ✔ |
| 插件 payload 无 token（`grep -c` 命中 0），身份/tags 仍在 | ✔ |
| `schema` 的 `limits` + 8 个全局 flag（走 mTLS 真机） | ✔ |
| CLI 走 mTLS TCP（6 个场景） | ✔ |
| 续传成功日志 + 终态 sha256 与源一致（256 MiB，从 114,819,072 字节接上） | ✔ |
| SIGTERM 仍能立即停 agent（回归） | ✔ |
| `make check` / `make test-race` | 全绿 |

### 9.9 仍未做

- 改动**未提交**（等 john 决定）。
- §6 方法学里那条"改完立刻 `gofmt -l` + `go build`"这次救了两次：`Edit` 工具先后把
  `internal/agent/client.go`、`internal/cli/registry.go` 的**文件尾部追加了重复片段**，
  还静默丢过 3 处改动（flag 注册没落盘，靠 `schema` 输出对不上才发现）。
  两处都是 `git checkout`/截断后重放修好的，全部改了尾部核对。
