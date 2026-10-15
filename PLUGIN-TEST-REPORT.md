# 插件模块测试报告

**被测版本**：`9125d42` *Add the WAN test report these fixes came from*（工作区干净）
**测试者**：Alma（受 john 授权，使用 LA 2 VPS `216.234.143.109` + 本机 Mac）
**日期**：2026-10-16
**范围**：`internal/server/plugins.go`（`PluginManager`）、四个钩子的调用点、钩子负载、
`plugins list` / `metrics overview` 里的插件观测量。

**结论**：四种钩子在真机跨洋拓扑上**全部按文档触发**，负载字段与 `plugins/README.md` 一致、
**无凭据泄漏**，失败插件不拖累兄弟插件、不影响服务可用性。
发现 **1 个必修缺陷**：**5 秒预算只杀插件进程本身，管不住它留下的后代进程** ——
后代持有 stdout/stderr 管道时 `CombinedOutput` 会一直等，钩子 goroutine 被挂住、
**失败一条日志都不打**（真机实测被拖到 ~40s，本机探针复现到 30s / 永久）。
另有 4 处语义/文档缺口（§4）。

> **该缺陷已于 2026-10-16 修复**（john 选定「按杀进程的方式处理」），改 4 个文件、
> 带 4 组回归测试、**在同一套真机上复验通过**（原缺陷形状从 ~40s 迟到变成 +5.02s 准时报出，
> 遗留进程存活数 0）。改动与证据见 **[§8 修复记录](#8-修复记录2026-10-16)**。
> 同一天 john 还让把 §4.2（成功的钩子不留痕）处理掉 → **§9**。

新增回归测试 `internal/server/plugins_contract_test.go`（8 个测试，含 3 处注入 bug 验证），
`make check` 与 `go test -race` 全绿。

---

## 1. 拓扑

| 角色 | 主机 | 位置 | 说明 |
|---|---|---|---|
| Server A（功能验收） | `216.234.143.109` | 美国 LA | agent 面 `:18080`、operator `:18081`；插件目录含 4 个条目 |
| Server B（缺陷量测） | 同上 | 美国 LA | `:18082/:18083`；插件是两个"形状不同"的挂死插件 |
| Agent `la2-node` | 同上 | 美国 LA | 与本机 server 回连（loopback） |
| Agent `mac-node` | 本机 Mac | 中国·家宽 NAT 后 | **跨洋**回连 `ws://…:18080` |
| 操控端 | 本机 `coc2` CLI | 中国 | 公网 `http://…:18081` + Bearer |

二进制由 HEAD 现编（`GOOS=linux GOARCH=amd64` + 本机 `darwin/arm64`）；
token 用 `openssl rand -hex 32` 生成、`0600`。测试全程走明文 `ws://`，
两端那两条"未加密 websocket"警告是**预期内**的，不是发现。

Server A 的插件目录刻意做成混合目录（这是发现语义的一部分）：

```
plugins/10-record.sh      可执行  记录每次调用（每进程一个文件）
plugins/20-fail.sh        可执行  往 stderr 写一行后 exit 9
plugins/README.txt        不可执行
plugins/subdir/30-nested.sh 可执行但在子目录里
```

---

## 2. 真机功能验收（通过）

| 项 | 结果 |
|---|---|
| `plugins list` | 恰好 2 个：`10-record.sh` / `20-fail.sh`；**README.txt 与子目录里的脚本都没被加载**（`grep` 载荷记录 0 命中） |
| `metrics overview` | `plugins: 2`、`online_agents: 2`、`total_agents: 2` |
| 四种钩子全部触发 | 一次运行内 `agent_connected` 2 · `task_result` 2 · `transfer_done` 2 · `metrics_report` 20 |
| **无凭据泄漏** | 26 份载荷里 `grep` fleet token → **0 命中** |
| 调用契约 | `argv[1]` = 钩子名、stdin = 事件 JSON（本机测试逐字段断言） |
| 失败插件被隔离 | `20-fail.sh` 每次都失败（26 次），兄弟插件 `10-record.sh` 26 次**全部照常执行**；服务全程可用 |
| 失败可见 | 服务端日志 `plugin hook failed` 带 `plugin` / `hook` / `output`，`output` 就是插件的 stderr 原文 |
| 任务执行 | `uname -sr; id -un; date -u` → `Linux 7.0.0-38-generic`；Mac 上 `sw_vers -productVersion` → `27.0.1` |
| 传输 | push 300,000 字节 → Mac，两端 sha256 **逐字节相同**（`a4df5553…25`）；pull `/etc/hostname` 回来 16 字节、`checksum_verified: true` |
| 服务可用性 | 全程 `healthz` 200、CLI 正常；缺陷量测结束后仍 200 |

四种钩子的真实载荷（截取）：

```jsonc
// agent_connected
{"agent_id":"mac-node","hostname":"Johns-MacBook-Air.local","os":"darwin","arch":"arm64",
 "ip_addrs":["192.168.31.212"],"tags":["env=mac","role=test"],
 "fingerprint":"4fcab5d2…4172","version":"v0.4.0","connected_at":"2026-10-15T21:33:32.295763Z"}
// task_result（stdout 已被 agent 侧截断到 1 MiB 上限）
{"task_id":"02cf2048c5efb186","agent_id":"mac-node","status":"success","exit_code":0,
 "stdout":"27.0.1\n","stderr":"","duration_ms":24,"completed_at":"2026-10-15T21:33:51.856878Z"}
// transfer_done
{"transfer_id":"b5bdfe21c15fe29f","agent_id":"la2-node","direction":"download",
 "local_path":"/root/coc2-plugin-test/pulled.txt","remote_path":"/etc/hostname",
 "status":"success","size":16,"bytes_transferred":16,"chunk_size":262144,
 "checksum_sha256":"c6a60a91…7daa","checksum_verified":true,
 "created_at":"…","completed_at":"…"}
// metrics_report
{"agent_id":"mac-node","timestamp":"2026-10-15T21:34:12.297581Z","uptime_secs":40,
 "cpu_count":10,"goroutines":5,"process_memory_bytes":12863752,
 "root_disk_total_bytes":494384795648,"root_disk_free_bytes":362022404096}
```

字段集合与 `plugins/README.md` 一一对应，没有任何多余键（本机测试把这条做成了白名单断言，
见 §5）。

---

## 3. ~~缺陷一（必修）~~：5 秒预算管不住插件的后代进程，而且**静默**
> **状态：已修（2026-10-16）**，见 [§8](#8-修复记录2026-10-16)。本节保留缺陷当时的形态与证据。

### 症状

一个插件如果在 5 秒预算内没退出，**并且它留下了仍在运行的子进程**，那么这个钩子的
goroutine 会被挂住直到那个子进程消失；在这期间：
- 「超时了」这件事**一条日志都不会打**（`Trigger` 只在 `cmd.CombinedOutput()` 返回错误时打 warn，
  它在挂住期间根本不返回）；
- 该 goroutine 连同它的**两个管道 fd** 和**一份完整载荷**（`task_result` 最大 ~1 MiB）一起被扣住；
- 每个事件都会再扣一份 —— 长时间运行的插件（`tail -f`、`nohup`、daemon）会让它**永久累积**。

### 本机最小复现（可直接跑）

```go
ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
cmd := exec.CommandContext(ctx, pluginPath, "hook")
cmd.Stdin = bytes.NewReader(payload)
_, err := cmd.CombinedOutput()   // 与 internal/server/plugins.go 同一条路径
```

| 插件脚本 | 实测耗时 | err | 说明 |
|---|---|---|---|
| `exec sleep 30` | **5.00s** | `signal: killed` | 单进程：预算**真的生效** |
| `sleep 30`（shell fork 出 sleep） | **30.09s** | `signal: killed` | 被 `sleep` 持有的管道拖住 |
| `sleep 8 &` + `exec sleep 300` | **8.30s** | `signal: killed` | 报告迟到 3.3s（迟到量 = 后代寿命） |
| `sleep 12 &` + `exit 0`（**成功**） | **12.29s** | `nil` | 成功路径不打日志 → **完全静默** |
| `sleep 8 &` + `exec sleep 300` + `WaitDelay=2s` | **7.00s** | `signal: killed` | 有界（5+2） |
| `sleep 12 &` + `exit 0` + `WaitDelay=2s` | **2.08s** | `exec: WaitDelay expired before I/O complete` | 有界，且**有错误可记** |

同一个 5s 超时，"`exec`" 与 "fork" 的差别就是 5.00s 与 30.09s 的差别 ——
所以这不是理论推演，而是脚本写法的一字之差。

### 真机量测（Server B，5s 预算，1 个 `agent_connected` 事件触发两个插件）

```
10-hang-exec.sh  : #!/bin/sh\nexec sleep 300
20-descendant.sh : #!/bin/sh\nsleep 40 &\nexec sleep 300
```

| 时刻 | `plugin hook failed` 行数 | server 进程 fd 数 |
|---|---|---|
| 事件前（基线） | 0 | 12 |
| t+8s | **1**（只有 `10-hang-exec.sh`） | **14** |
| t+20s | **1** | **14** |
| t+45s | **2** | 13 |

- 两个插件都在 ~5s 被杀（错误都是 `signal: killed`，归因正确），但
  `20-descendant.sh` 的失败**直到它那个 `sleep 40` 结束才被记录** —— 迟了约 40 秒，
  整整 35 秒里这条失败对运维**不存在**。
- **对照**（Server A，两个良性插件）：3 个事件（共 6 次钩子调用）前后
  fd **14 → 14**、线程 9 不变。也就是说泄漏是"留后代的插件"特有的，不是模块常态。

### 根因

1. `exec.CommandContext` 的超时只 `Kill()` 到**直接子进程**（这里是 `/bin/sh`）；它的后代
   被 reparent 到 init，继续活着。
2. 插件进程的 stdout/stderr 是 `CombinedOutput` 建的管道。管道**只有在所有写端关闭后才 EOF**，
   而那个还活着的后代**继承着这两个 fd** → `Wait()` 一直等 I/O 完成 → goroutine 不返回。
3. 代码里没有设 `cmd.WaitDelay`，所以这个等待**没有上界**。
   （Go 1.20+ 的 `Cmd.WaitDelay` 就是为这个场景加的：进程退出后 I/O 仍不收尾，等满该时长就
   关掉管道并返回 `exec: WaitDelay expired before I/O complete`。）

### 影响

- **静默**：运维看到的是"这事没发生过"，而不是"插件超时了"。同类形状在仓库里已有先例
  （见 `WAN-TEST-REPORT.md` §5 的"两种 `canceled` 只存在于审计里"）。
- **资源**：每个事件泄漏一个 goroutine + 2 个管道 fd + 一份载荷副本（实测最大 ~1.0 MiB）。
  插件起一个长效 daemon 就是永久泄漏；server 是长命进程，只增不减。
- **语义**：README 里任务侧承诺"取消/超时终止整棵进程树"，插件侧没有任何承诺 ——
  但读者会默认同样成立（实际不成立）。

### 修法与决定（两档，都已在探针里验证过）

- **最小档**：`cmd.WaitDelay = 2 * time.Second`。把无界等待变成有界等待，
  并把现在的"静默"变成一条可读的 warn（对良性插件零影响）。
- **完整档**：再补进程组，与任务执行侧对齐（`internal/agent/client.go:471` 已是这个写法）——
  超时杀整棵树。
  ⚠️ 它是产品决定：会连插件**故意**留在后台的进程一起杀掉。

**john 2026-10-16 选了完整档**，并且**两档都上了**（进程组 + `WaitDelay` 兜底）。
实现、实测前后对照、真机复验与注入验证见 [§8](#8-修复记录2026-10-16)。

---

## 4. 语义 / 文档缺口（都是"读代码 + 真机实测"确认的行为，不是猜）

1. **`agent_connected` 是"每个连接一次"，不是"每个 agent 上线一次"。**
   实测：把 `mac-node` 停掉重连 → 计数 2 → 3，`mac-node` 出现两次。
   插件作者按"上线"语义去发通知会重复；README 没写这条。
2. ~~**成功不落日志。** 失败有 `plugin hook failed`，成功一个字都没有 ——
   一次运行里"钩子到底跑没跑过"只能靠插件自己留痕。~~ **已处理（2026-10-16，见 §9）**：
   补了一行 **debug** 级 `plugin hook ran`（带 `plugin` / `hook` / `duration`），
   默认 info 级别下仍然零噪音。修 §3 时另**新增**了一类 warn
   （`plugin hook left processes behind`）—— 三类日志的分工见 `plugins/README.md`。
3. **`task_result` 给的是"给终端看的"视图，不是库里那份原始字节。**
   实测 3 MB 输出的任务 → 插件收到 `stdout` 1,048,595 字节 + 尾部 `[output truncated]`
   （agent 侧 1 MiB/流上限），载荷文件 1,048,770 字节。
   而且载荷是 **JSON 文本**：`encoding/json` 会把字符串里非 UTF-8 的字节替换成 U+FFFD
   （实测 `"a\xff\xfe b"` → `"a\ufffd\ufffdb"`）—— 与 README "线协议"节声明的那条损失边界
   **是同一条**，只是插件也踩在它上面。要在插件里拿到原始字节只能走 API/DB，钩子给不了。
4. **插件在启动时一次性加载。** 往目录里丢新插件、改可执行位都需要重启 server；
   `plugins list` 报的是加载那一刻的快照。

---

## 5. 新增回归测试

`internal/server/plugins_contract_test.go`（第一轮新增、第二轮按修复改写，未提交）：

| 测试 | 盯的是什么 |
|---|---|
| `TestPluginDiscoveryContract` | 目录不存在 / 空字符串 / 传进来是个文件都是 0 插件不报错；只加载**可执行的普通文件**、**不递归**；`List()` 返回副本 |
| `TestPluginInvocationContract` | `argv[1]` = 钩子名、stdin = 事件 JSON、每个插件每事件调用一次 |
| `TestPluginTriggerReturnsBeforePluginFinishes` | 钩子是 fire-and-forget，慢插件不阻塞调用方（调用方就是连接 goroutine） |
| `TestPluginTimeoutKillsHangingPlugin` | 单进程挂死：~5s 被杀、有 warn、归因正确、超时后那一步**永远不执行**、兄弟插件不受影响 |
| `TestPluginProcessTreeDoesNotOutliveTheHook` | **§3 缺陷的回归测试**（修完后替换掉了原来那条"断言缺陷行为"的测试）：两种形状（shell fork 出 sleeper / 后台 sleeper + 挂死的 shell）都必须在预算内报出，且**后代进程必须死** |
| `TestPluginThatBackgroundsAndExitsIsReported` | 插件**成功退出但留下进程**：不得静默（原来挂 12.29s、一个字不打），现在 ~2s 报 `plugin hook left processes behind`，遗留进程被杀 |
| `TestFailingPluginIsIsolatedAndLogged` | 失败插件被隔离且 stderr 进日志 |
| `TestPluginPayloadsAreWhitelisted` | 三个由 agent 面帧驱动的钩子：载荷键集合 = 文档白名单（多一个键就红）、身份取自连接而不是报文自称、载荷里不含 fleet token |
| `TestPluginManagerTrigger`（已有）/ `TestAgentConnectedPluginPayloadOmitsToken`（已有） | 原有回归测试未改动 |

**注入 bug 验证有牙**（各注入一次、跑一次、还原）：

| 注入 | 结果 |
|---|---|
| 给 `agentConnectedEvent` 加回 `Token` 字段并透传 `hello.Token` | `TestPluginPayloadsAreWhitelisted` **红**，直接打出泄漏的 token |
| 把预算从 `5s` 改成 `30s` | `TestPluginTimeoutKillsHangingPlugin` **红** |
| 把 `Trigger` 的 `go func()` 改成同步调用 | `TestPluginTriggerReturnsBeforePluginFinishes` **红**（阻塞 2.32s） |

`make check`（gofmt + vet + 全部测试）**全绿**；`go test -race ./internal/server/ -run TestPlugin` **绿**。

---

## 6. 方法学备注（下次可复用）

- **别把两个机制混在一次测量里。** 我第一版测试同时塞了"挂死"和"留后代"，量出来的是二者的叠加，
  于是第一版结论是错的（我以为是"孤儿子进程没被清理"，真正起作用的是**管道 EOF**）。
  拆成"单进程挂死 / 留后代挂死 / 成功但留后代"三个变量分别跑 Go 探针，才一眼看出开关是
  `exec` 与 fork 的区别。**探针（20 行 Go）比读代码快得多。**
- **管道不 EOF 这件事没有错误、没有日志、没有超时** —— 又是一个"指标全绿但没干活"的形状。
  观测面对：① 插件自己的留痕文件；② 服务端 `plugin hook failed` 行数；
  ③ `/proc/<pid>/fd` 计数。三个面互相印证比单看日志可靠（这次 fd 从 12 → 14 就是最硬的证据）。
- **对照实验要同机同时刻**：Server A（良性插件）与 Server B（留后代）在同一次运行里，
  同样的事件、同样的机器，差别只在插件形状。
- 我一开始想用"插件不读 stdin + 大载荷"造第二个缺陷（猜 `broken pipe` 假失败），
  **探针直接把这个猜测否掉了**（2 MiB 载荷下 `exit 0` 的插件 0.17s 正常返回）。
  先量再写结论，省了一条假发现。
- ★ **修完之后，"我的测试挂了"要先怀疑测试**：`TestPluginThatBackgroundsAndExitsIsReported`
  第一次跑是红的（15s 超时），根因是**我的等待器只匹配旧的那条日志文案**，
  而修复新增了一类消息 —— 产品代码没问题。等待器改成按消息前缀匹配后即绿。
- ★ **两个看起来独立的旋钮可能是耦合的**：`Setpgid` 与 `cmd.Cancel` 就是这样 ——
  少了 `Setpgid`，`kill(-pid)` 会 `ESRCH`，于是**连"超时杀子进程"这个原本能用的行为也一起没了**。
  注入验证正好把这个 coupling 暴露出来（去掉 `Setpgid` → "后代存活"，
  而不是"行为退化但不报错"）。修法：`Cancel` 做成"先杀组、再杀进程本身"，
  任何一半单独存在都不会静默失效。

---

## 7. 测试痕迹与清理

- **LA 2 (`216.234.143.109`)**：测试目录 `/root/coc2-plugin-test` 与上传包已删除；
  ufw 里为本次加的两条规则（18080/18081 仅本机出口 IP）已删除并核对回原始 6 条；
  18080–18083 **无监听**；`pgrep` 无 coc2 进程。
- **原有服务未受影响**：`nginx` / `agentshare` 全程 active，`agentshare /healthz` 200，
  443/80/22 从未改动；测试用的四个端口与它们不冲突（agentshare 在 `127.0.0.1:8080`，
  本次刻意避开 8080）。
- **本机**：跨洋 agent 已停；`/tmp/coc2-plugin-wan`、`/tmp/pluginprobe` 等临时件已删。
- **仓库（第一轮）**：新增 `internal/server/plugins_contract_test.go`（未提交）与本报告。
  当时生产代码一个字没改 —— 修法等 john 定，尤其"要不要杀插件进程组"。

### 7.1 修复后的第二次真机复验（2026-10-16 晚，同一台 LA 2）
- 用**修复后的代码**重编重传（`/root/coc2-fix-test`），两个 server 实例 + 两个 agent，
  三个缺陷形状的插件各一个。
- **没加 ufw 规则**（这轮 agent 全部走 loopback，全程没对外开端口）→ 复验后 ufw 仍是最初那 6 条。
- 测完：目录、二进制、`token`、脚本全删；18080–18083 无监听；无 `agent` / `sleep` 残留进程；
  `nginx` / `agentshare` 全程 active，`agentshare /healthz` 200。
- ⚠️ 本轮踩到的两个操作坑记在 §8.6：**`tar` 打包把 macOS 的 `._` 同伴文件也带上，
  而它们是 755 —— 会被当成插件加载**；**`pkill -f <目录名>` 把自己的 ssh 会话杀掉**（退出 255）。

**顺带一提（与本次无关，没动它）**：LA 2 的 ufw 里 `443/tcp` 现在是 `ALLOW Anywhere`
（注释 `https: dl direct + cf`），与记忆里"443 只放行 CF 的 22 个网段"不一致 ——
你如果是自己有意开的就当没说。

---

## 8. 修复记录（2026-10-16）

**决定**：john 选**「按杀进程的方式处理」**（我给的选项 ②）—— 插件在自己的进程组里跑，
超时杀整棵树，并且**钩子的进程树不活过钩子**。代价他已确认接受：
插件**故意**留在后台的进程也会被杀（`plugins/README.md` 里写明了替代做法）。

### 8.1 改了什么（4 个文件）

| 文件 | 改动 |
|---|---|
| `internal/server/plugins.go` | `SysProcAttr{Setpgid:true}` + 自定义 `cmd.Cancel`（杀 `-pgid`，再兜底杀进程本身）+ `cmd.WaitDelay = 2s`；钩子结束后统一回收进程组；新增一类 warn `plugin hook left processes behind` |
| `internal/common/limits.go` | 新增导出常量 `PluginHookTimeout = 5s`（原来这个 5 是**内联字面量**，改它得翻源码） |
| `internal/cli/registry.go` | `coc2 schema` 的 `limits` 报 `plugin_hook_timeout_secs` + 说明（这仓库的规矩：能被查到的限制才算契约） |
| `plugins/README.md` | 新增「运行预算与进程组」「可观测性」两节：预算、进程组语义、两类 warn、加插件要重启 |

三个细节值得说：

- **`Setpgid` 和 `Cancel` 是一对，不能只留一个。** 队列里 `pid` 只有当它是组长时才是组 id；
  少了 `Setpgid`，`kill(-pid)` 会 `ESRCH`、**连直接子进程都不杀**（超时彻底失效）。
  所以 `Cancel` 做成两步：先杀组、再杀进程本身 —— 拆掉任一半都不会静默变成"不杀"。
- **`WaitDelay` 不是可选项**：它兜住的是**逃出进程组**的东西（`setsid`、双重 fork）。
  没有它，那个等待仍然是无界的 —— 只是把"常见的泄漏"换成"罕见的泄漏"。
- **回收放在 `CombinedOutput` **返回之后**、`Zap` 的 warn **之前**：先杀干净再记日志，
  免得"记了日志但进程还在"。

### 8.2 新语义（一句话）

> 一个钩子的进程树，既不活过它的 5 秒预算，也不活过钩子本身。

### 8.3 本机实测：修复前 / 修复后（同一批脚本，5s 预算）

| 插件脚本 | 修复前 | 修复后 |
|---|---|---|
| `exec sleep 30`（单进程挂死） | 5.00s，`signal: killed` | **5.01s**，`signal: killed` |
| `sleep 30`（shell fork） | **30.09s**，`signal: killed` | **5.02s** ✅ |
| `sleep 8 &` + `exec sleep 300` | **8.30s**（迟到 3.3s） | **5.00s** ✅ |
| `sleep 12 &` + `exit 0`（成功） | **12.29s**，`err=nil` → **完全静默** | **2.31s**，`plugin hook left processes behind` ✅ |

### 8.4 真机复验（LA 2 `216.234.143.109`，修复后的二进制）

一个 `agent_connected` 事件同时触发三个缺陷形状的插件，从事件时刻起算：

```
30-backgrounder.sh  plugin hook left processes behind  +2.03s  exec: WaitDelay expired before I/O complete
10-hang-exec.sh     plugin hook failed                 +5.02s  signal: killed
20-descendant.sh    plugin hook failed                 +5.02s  signal: killed
```

| 观测 | 修复前（同机同形状） | 修复后 |
|---|---|---|
| `20-descendant.sh` 被报出的时刻 | **t+45s**（它那个 `sleep 40` 一结束才报） | **+5.02s** |
| `30-backgrounder.sh`（成功 + 留进程） | **静默，一条都不打** | **+2.03s**，有明确消息 |
| 事件后遗留进程（`sleep 40` / `sleep 300`） | 存活 | **0 个** |
| server 进程 fd | 12 → **14** → 13 | 12 → 13（= 12 + 一条在线的 agent 连接） |
| 连接建立→断开后的 fd | — | **13 → 12，回到基线**（单跑一次连接/断开：无 agent 12 / 有 agent 13 / 断开 12） |

后来那行 fd 检查是专门为了把"+1"解释清楚：**它是在线的 agent 连接，不是泄漏**
（这也是修复前那次量测收在 13 的原因 —— 那个 `+1` 同样是连接）。

### 8.5 回归测试与"有牙"验证

新增/改写的 4 条（`internal/server/plugins_contract_test.go`）：
`TestPluginProcessTreeDoesNotOutliveTheHook`（两种形状 + 断言后代进程必须死）、
`TestPluginThatBackgroundsAndExitsIsReported`、`TestPluginTimeoutKillsHangingPlugin`（沿用）、
`TestPluginTriggerReturnsBeforePluginFinishes`（沿用）。

**注入验证（每处注入一次、跑一次、还原）**：

| 注入 | 结果 |
|---|---|
| 去掉 `Setpgid` | `descendant pid … survived the hook` —— **红** |
| 去掉钩子结束时的进程组回收 | 同上 —— **红** |
| 去掉 `WaitDelay` | 15s 内一条报告都没有 —— **红** |
| （上一轮）给 payload 加回 token / 预算改 30s / `Trigger` 改同步 | 各自对应的测试 —— **红** |

`make check`（gofmt + vet + 全部测试）全绿；`go test -race ./internal/server/ -run TestPlugin` 绿。

### 8.6 这轮的两个操作坑（都是我自己踩的）

1. **`tar` 会把 macOS 的 `._` 同伴文件一起带上，而它们是 755** —— 落进插件目录后
   **会被当成插件加载**（`._10-record.sh` 之类），污染测试结果。这次是在跑之前
   `ls -la` 看出来的。→ 打包加 **`COPYFILE_DISABLE=1`**，或落地后
   `find . -name '._*' -delete`。
2. **`pkill -f <目录名>` 会把自己的 ssh 会话杀掉**（退出 255、零输出）——
   因为那条 ssh 的命令行本身就含这个字符串（我记忆里有这条，还是踩了两次：
   第一次用 `pkill -f coc2-fix-test`，第二次连 `pgrep -f '[c]oc2-fix-test'`
   也被同一条命令里的 `rm -rf /root/coc2-fix-test` 匹配到）。
   → **停止进程和"提到路径"分两条命令发**，或者只按端口/`-x` 去杀。

### 8.7 刻意没做的

- ~~**没动"成功的钩子不写日志"**~~ → **john 随后让加，已按 debug 级处理，见 §9。**
  （当时的顾虑 —— "info 级别下日志量会随 agent 数增长" —— 正是最终选择 debug 的理由：
  默认级别零噪音，排查时才开。）
- **没给插件加并发上限**：任务侧有 `MaxConcurrentTasks=16`，插件侧没有。
  现在每个事件每个插件起一个进程 —— 事件速率由 agent 心跳/任务量决定，暂时不是瓶颈。
  真要有几百个 agent + 多个重插件，这条会变成下一个 `MaxConcurrentTasks`。
- **没改 `Trigger` 的"发了就不管"**：它是刻意的（事件路径不能被插件阻塞），
  只把"不管"的边界从"无限"收到"5s + 2s"。
- ⚠️ **一个新的、很小的行为后果**（记录在案，不是遗漏）：插件现在不在 server 的进程组里，
  所以"给 server 的进程组发信号"（终端里 Ctrl-C、或 `KillMode=process` 的 systemd 单元）
  不会再连带给插件。systemd 默认的 `KillMode=control-group` 仍会连 cgroup 一起收，
  而且每个钩子本来就有 5s 上限 —— 实际影响可以忽略，但它是"独立进程组"的必然代价。

---

## 9. 可观测性补齐：成功的钩子留痕（2026-10-16，john：「把 2 加上吧」）

§4.2 那条缺口的关闭。**一行 debug + 一处文档 + 一个测试**，改动很小但补的是
一个"看起来没问题"的盲区。

### 9.1 改了什么

| 文件 | 改动 |
|---|---|
| `internal/server/plugins.go` | 钩子正常结束时 `Debug("plugin hook ran", plugin, hook, duration)`；**失败那条也补上 `duration`** |
| `plugins/README.md` | 「可观测性」一节改成三类日志的表格（含级别列）+ 为什么是 debug |
| `internal/server/plugins_contract_test.go` | 新增 `TestSuccessfulHookIsVisibleAtDebugLevel` |

**为什么是 debug 而不是 info**：这条日志**每次事件、每个插件一行** ——
info 级别下繁忙机群的插件日志量会按 `metrics_report`（每心跳一次）的频率增长。
而它要解决的问句（"事件到底到我插件了没有"）是**排查时**才问的，
所以默认级别零噪音、排查时 `log_level: debug` 打开 —— 两个目标同时满足。

### 9.2 为什么这条值得加（缺口的具体形状）

原来：**「钩子从没被调用」和「调用了但没干成事」在服务端日志上完全一样**（都是什么都没有）。
插件作者排查"我的插件没反应"时，唯一能区分两者的证据**在插件自己那边** ——
而插件恰好是那个"可能根本没跑起来"的东西。这是自证循环。

`duration` 是顺手但实用的第二重价值：**它是"这个钩子正在逼近 5 秒预算"的早期信号**，
也是失败时区分"耗满预算被杀"和"秒退"的唯一字段。

### 9.3 实测

**本机**（测试覆盖两个方向）：debug 级别下能看到 `plugin hook ran` + 三个字段、
duration 与脚本 sleep 的 200ms 相符；**info 级别下一条都没有**。
注入验证两处：删掉这行日志 → 红；把它降成 info → 红（"在默认级别刷日志"）。

**真机**（LA 2，`log_level: debug`，一个 `agent_connected` 触发三个插件：快 / ~3s / 失败）：

```
warn  plugin hook failed   30-fail.sh   agent_connected   0.015s   ← 秒退
debug plugin hook ran      10-ok.sh     agent_connected   0.019s
debug plugin hook ran      20-slow.sh   agent_connected   3.018s   ← 逼近 5s 预算的早期信号
```

**同一个事件、`log_level: info`：含 `plugin` 的日志行 = `0`。** 两个目标都实测到了。

### 9.4 没做的

- 没做**计数**（`metrics overview` 里的钩子调用/失败/超时数）：那是更大的一圈改动
  （要加 Store 表和 janitor），而"逐事件看"已经被 debug 日志覆盖了。
  真有人要按钩子做告警再上。
- 没在成功路径上加任何 info/warn。
