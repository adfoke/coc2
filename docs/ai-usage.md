# AI 操作手册 — coc2 CLI

本文写给 AI/自动化代理。人类当然也能用，但这里的契约是为程序化调用设计的。

## 学习成本 = 1 条命令

```bash
coc2 schema
```

输出机器可读 JSON：全部命令、flag（名称/类型/默认值/描述）、位置参数、全局 flag、退出码语义。先拉一次 schema，不要猜命令形状。

## 核心契约

1. **stdout 永远只有一个紧凑 JSON 文档**（成功时）。日志、提示、人类文案绝不进 stdout。加 `--pretty` 得缩进版。
2. **错误是 stderr 上的 JSON**：`{"error":{"code":"...","message":"..."}}`。用退出码分支，用 code 细分。
3. **退出码**（稳定承诺）：
   | code | 含义 | 建议动作 |
   |---|---|---|
   | 0 | 成功 | — |
   | 1 | 服务端错误 / 任务失败 / 传输失败 / 等待超时 | 读 stdout 里的 results[] 定位哪个 agent 失败 |
   | 2 | 连不上 operator 面 | 检查 server 进程与 socket 路径 |
   | 3 | 鉴权失败 | TCP 面需要 `-token` 或 `COC2_TOKEN` |
   | 4 | 用法错误 | 重读 schema，别硬试 |
4. **零交互**：没有任何提示、确认等待、spinner。破坏性批量操作靠 `--yes` flag 显式声明。

## 连接

- 同机默认：什么都不用配，CLI 默认连 `./coc2.sock`（server 启动目录下的 UDS，免 token）。
- 远程/TCP：`-server http://host:8081 -token $T`，或 env `COC2_SERVER` / `COC2_TOKEN`。
- 全局 flag 可出现在 argv 任意位置（`coc2 agents list --pretty -server /x.sock` 合法）。
- server 侧的 `token` 若仍是内置的 `coc2-dev-token`，server 会**拒绝启动**（它是公开值）。运维要用 `-token $(openssl rand -hex 32)`，或写进 `config.yaml` 的 `token`；本地一次性运行可加 `-allow-dev-token`。遇到 server 起不来看这条。

## 典型工作流

### 巡检

```bash
coc2 metrics overview          # {total_agents, online_agents, pending_tasks, ...}
coc2 agents list --online      # 在线清单
coc2 run --cmd "uptime" --agents id1 --wait   # 单机探活
```

### 批量执行（>1 目标必须 --yes）

```bash
coc2 run --cmd "df -h" --tag env=prod --yes --wait --exec-timeout 30
```

`--wait` 输出聚合：

```json
{"all_ok":false,"results":[{"task_id":"..","agent_id":"..","state":"success","exit_code":0,"stdout":"..","stderr":"","duration_ms":12}]}
```

任一 agent 非 `success` → 整体 exit 1，但 results 仍完整在 stdout——**先解析再放弃**。`state` 终态枚举：`success|failed|timeout|canceled`；非终态：`queued|dispatched|cancel_requested`。

不 `--wait` 时立即返回 `{"tasks":[{task_id,agent_id,dispatched,queued_only}]}`；`queued_only=true` 表示 agent 离线、任务待补发。之后用 `coc2 tasks get <id>` 轮询。

### 文件

```bash
coc2 push --agent a1 --local ./app.bin --remote /opt/app.bin --wait
coc2 pull --agent a1 --remote /var/log/x.log --local ./x.log --wait
```

路径都是**相对哪台机器**：`--local` 在 server 那台，`--remote` 在 agent 那台。`--wait` 结束标志 `status=success`（含 SHA256 校验；失败会保留 `.part`，重试自动续传）。

### 任务管控

```bash
coc2 tasks list --limit 50
coc2 tasks cancel <task_id>   # 会终止 agent 上整棵进程树
```

### 分组

```bash
coc2 groups create --name web --agents a1,a2
coc2 groups add <group_id> a3 a4
coc2 groups remove <group_id> a2
```

## 陷阱清单

- `--exec-timeout`（任务执行超时，秒，默认 60）≠ 全局 `-timeout`（HTTP 请求超时，duration，默认 30s）≠ `--wait-timeout`（CLI 侧轮询预算，默认 90s，固定值不随 exec 联动）。批量或长命令记得 `--wait-timeout` 给足；超了返回 exit 1 + `wait_timeout`，任务本身可能还在跑——用 `tasks get` 续查。
- `run` 不带 `--wait` 只代表「已送达 agent」，不代表已执行完（至多一次语义，dispatch 即算数）。
- 空列表统一返回 `[]`（S5 后 server 已修复 null→[]；若对端是旧版 server，解析仍建议兜 `null`）。
- `groups add/remove` 是多轮读-改-写，并发下可能互相覆盖成员。
