# 插件

`server` 从 `-plugins` 目录（默认 `./plugins`）加载两类插件：

| 文件 | 类型 | 怎么跑 |
|---|---|---|
| **`*.js` / `*.mjs`** | **JavaScript 插件（推荐）** | 进程内、由 [moejs](https://github.com/Calcium-Ion/moejs) 跑，一个插件一个 runtime 池 |
| 任何**带可执行位**的文件 | 可执行插件（已弃用） | 每个事件起一个进程，hook 名在 `argv[1]`，事件 JSON 走 stdin |

同一目录可以混着放。往目录里加插件要**重启 server** —— 加载只发生在启动时。

---

## JavaScript 插件

```js
// plugins/policy.js
export function task_dispatch(event) {
  // 想在任务落到 agent 之前拦下它，就用这个 hook
  if (event.hint.command.includes("rm -rf")) {
    return { decision: "deny", reason: "refusing a destructive command" };
  }
  return { decision: "allow" };
}

export function task_result(event) {
  if (event.hint.exit_code === 0) return;   // 常见判断全在 hint 里
  const full = JSON.parse(coc2.event(event.id));  // 真要正文才取
  console.log("failed:", full.stdout.slice(0, 200));
}
```

只导出你关心的 hook，其余的不会被调用。

### 你会收到什么：事件信封（schema 2）

```json
{
  "schema": 2,
  "event": "task_result",
  "id": "evt_9f1c…",
  "at": "2026-10-22T05:30:00Z",
  "ref": "/api/v1/events/evt_9f1c…",
  "hint": {
    "task_id": "t-0001", "agent_id": "ag-0001",
    "status": "failed", "exit_code": 1,
    "duration_ms": 812, "stdout_bytes": 1048576, "stderr_bytes": 0
  }
}
```

**信封里没有正文。** `task_result` 的 stdout/stderr 每个上限 1 MiB，如果内联给每个插件，
就是每个插件每次事件一份内存 —— 这是插件跑在 server 进程里最不能有的形状。
所以：

- **`hint`** 是"不取任何东西也能做完最常见的那个判断"所需的最小摘要。
- **`id` / `ref`** 是取完整负载的地址。取法有两个，取的是**同一份字节**：
  - 进程内：`coc2.event(event.id)` → JSON 文本（自己 `JSON.parse`）
  - 进程外：`GET /api/v1/events/<id>`（operator 面，带 token）

`id` 只在你被投递到这个事件时才认你 —— 别的插件的事件 id 拿不到内容。
它也不是路径、不是 URL：`coc2.event("/etc/passwd")` 一律报错。
5 分钟后过期（`coc2 schema` 的 `limits.plugin_event_ttl_secs`）。

### 各 hook 的 hint

| hook | 什么时候 | 能返回决定吗 | hint 里有什么 |
|---|---|---|---|
| `agent_connected` | agent 上线 | ✗ 通知 | 整个事件（本来就小） |
| `task_result` | 任务结束 | ✗ 通知 | task_id/agent_id/status/exit_code/duration_ms/stdout_bytes/stderr_bytes |
| `transfer_done` | 传输结束 | ✗ 通知 | transfer_id/agent_id/status/size/checksum_sha256（**不含路径**） |
| `metrics_report` | 每 agent 每 30s | ✗ 通知 | 整个事件（本来就小） |
| **`task_dispatch`** | **任务落库前** | **✓ 可以 deny** | task_id/agent_id/type/**command**/command_bytes/command_truncated/timeout_secs/priority |

`task_dispatch` 是唯一同步的 hook：它有 200 ms 预算（`plugin_decision_timeout_secs`），
在这段时间里 operator 的请求等着你。其余四个是"发了就不管"，**永远不等你的返回值** ——
它们所在的位置（任务已经跑完、心跳热路径）等不起。

### 返回值

```js
return { decision: "allow" };                        // 或者干脆不 return
return { decision: "deny", reason: "why not" };      // 只有 task_dispatch 有意义
```

- `deny` → 任务**不会落库**、不会发给 agent，`POST /api/v1/tasks` 回 **403**，
  响应里带 `denied` / `denied_by` / `deny_reason`。
- 未知的 `decision` 值 → 当 `allow`，但**记一条 warn**（typo 不该变成故障，也不该被藏起来）。
- **超时 / 抛异常 / 返回垃圾 → 一律放行（fail open），并且写一条审计**
  （`params_summary` 里有 `decision=timeout` 或 `decision=malformed`）。
  一个卡住的插件不能把整个机群的调度停掉；但"为什么黑名单没拦住"必须能查到。
- 多个插件都导出 `task_dispatch` 时，**任意一个 deny 就 deny**，共用同一个 200 ms 预算。

### 你能用的宿主能力：`console` 和 `coc2`，就这两个

| 全局 | 说明 |
|---|---|
| `console.log/warn/error(...)` | 进 server 日志（**debug 级**），单条上限 2 KiB |
| `coc2.envelope` | 信封版本号，现在是 `2` |
| `coc2.event(id)` | 取事件正文（JSON 字符串）；不认识的 id 抛错 |

除此之外没有文件系统、没有网络、没有 `require`、没有 `import`。
`eval` 和 `new Function` **关闭**（`EvalError`）—— 插件里不该有"读代码的人看不见的代码"。

### 边界（这些是结构性的，不是承诺）

| 性质 | 怎么保证 |
|---|---|
| **插件之间互不可见** | 每个插件自己的 runtime 池，global、原型改动、留的状态都不共享 |
| **内存有上限** | 每个 runtime 32 MiB（`plugin_memory_limit_bytes`）。引擎用**中断**执行，脚本 `try/catch` **拦不住** |
| **死循环能被停掉** | 5 秒预算到点由另一个 goroutine `Interrupt`，不依赖插件配合 |
| **不污染解析** | 进程内引擎没有 stdout 可污染：日志走 `console`，返回值是结构化对象 |
| **并发有上限** | 每插件最多 8 个 runtime；全局最多 64 个 hook 在飞；超出的异步事件**丢弃并记 warn** |

**换掉的是崩溃隔离。** 插件报错、超时、超内存都不影响 server（引擎把 panic 变 error），
但引擎自身的 bug 现在能打到 server —— 这是为了省掉"每事件一个进程"（约 3 ms 起）
和"每改一次插件要重新编译部署"而做的交换，见 `docs/plugin-refactor.md` §5。
反过来说：**别在插件里做需要在独立崩溃域里跑的事**（包一层外部二进制、跑别人的代码）。

---

## 可执行插件（已弃用，仍可用）

老契约没变：每个事件出现一次，`argv[1]` 是 hook 名，事件 JSON 在 stdin，
成功时的输出被丢弃。原有的进程组语义全部保留（见下节）。

**它们现在能收到新信封了**：`-plugins-envelope=2`（或 `plugins_envelope: 2`）会让可执行插件
也收到 schema-2 信封，并在环境里带 `COC2_PLUGIN_ENVELOPE=2`。
默认是 `1`（原样负载），这样升级不会打断已经在跑的老插件。

⚠️ **可执行插件不能做策略**：它的输出本来就被丢弃（没有返回通道），所以 `task_dispatch`
对它是**纯通知**，服务端也**不会同步调用它**（那等于在请求路径上白 fork 一个进程）。
要拦任务只能用 JS 插件。

## 运行预算与进程组（可执行插件）

- 每个钩子**每次事件** 5 秒预算（`coc2 schema` 的 `plugin_hook_timeout_secs`）。
- 插件在**自己的进程组**里跑：超时杀掉的是**整棵进程树**。
- **钩子的进程树不会活过钩子**：结束后进程组里剩下的进程也会被回收。
  别写"转发给一个常驻后台进程"—— 那个进程会被杀掉。要常驻就让 systemd 管，插件只负责通知。
- 真有东西逃出进程组（`setsid`、双重 fork），2 秒宽限后切断，并记
  `plugin hook left processes behind`。

## 可观测性

| 日志 | 级别 | 含义 |
|---|---|---|
| `plugin hook ran` | debug | 钩子跑完了，带 `plugin` / `hook` / `duration`（两种插件都有） |
| `plugin console` | debug | JS 插件的 `console.*`，带 `plugin` / `level` / `message` |
| `plugin hook failed` | warn | 跑挂了；`error` 是原因，JS 插件还带 `js_stack`（它的 JS 调用栈） |
| `plugin hook timed out` | warn | 到预算被中断；`duration` 会贴着上限（比如 `5.002s`） |
| `plugin hook exceeded its memory limit` | warn | 分配超上限被中断；`js_stack` 指出在哪一行分配 |
| `plugin hook left processes behind` | warn | 插件自己结束了，但进程组外的进程还攥着输出管道（可执行插件） |
| `plugin returned an unknown decision, allowing` | warn | 返回值里的 `decision` 不认识 |
| `plugin hook dropped: too many hooks in flight` | warn | 异步事件超过并发上限被丢 |
| `load javascript plugin` / `plugin disabled` | error | 编译/加载失败被跳过；之后这个插件不再跑 |

★ **失败一定有日志**：JS 插件没有退出码、没有 stderr，如果超时不写一行，
「我的钩子每次都被中断」和「我的钩子从没被调用」在日志上**完全一样**。
这是这条 warn 存在的唯一理由（真机上发现过一次它缺失，现在是 `TestTimedOutAsyncHookIsLogged`）。
`task_dispatch` 失败放行不再单独打一条 —— 钩子自己的失败已经打过了，
"放行"这件事记在**审计**里（`plane=plugin` 的行），不重复刷日志。

- **默认（info）下成功的钩子不写日志。** 要排查「事件到插件了没有」就 `log_level: debug`
  —— 没有它，**「钩子从没被调用」和「调用了但没干成事」在日志上一模一样**。
- 刻意是 debug 而不是 info：每次事件每个插件一条，info 下繁忙机群会按心跳频率刷屏。
- 都带 **`duration`**，拿它当"这个钩子正在逼近预算"的早期信号。
- 一个插件编译不过**不影响 server 启动**：它被跳过并记 error，其余照常。

## 负载里有什么（安全边界）

插件跑在 server 旁边，所以传进来的负载**只包含该 hook 需要的信息，不含任何凭据**：
`agent_connected` 给的是 agent 身份，**没有** agent 的共享 token
（那个 token 认证整个机群）。要加字段就在 `internal/server/plugin_event.go` 的 hint 里
**显式**加 —— 契约测试断言的是**字段集合**，多加一个字段会让测试失败，逼那次改动变成一次决定。

`task_dispatch` 例外地给了 `command`：策略插件看不见命令就只能瞎拒，而命令正是被决定的东西。
它按 8 KiB 截断，并带 `command_bytes` / `command_truncated` 说明截过。

## 示例

`plugins/example-plugin.js.sample`（JS，看形状用）和
`plugins/example-plugin.sh.sample`（可执行，看老形状用）。
去掉 `.sample` 后缀、放进 `-plugins` 目录、重启 server 即可。
