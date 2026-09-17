package cli

import (
	"fmt"
	"net/url"
	"time"
)

// Write-path commands: run (create tasks), tasks cancel, groups mutations,
// push/pull file transfers. All support --wait by polling to a terminal
// state with capped exponential backoff (500ms..2s).

const (
	taskSuccess  = "success"
	taskFailed   = "failed"
	taskTimeout  = "timeout"
	taskCanceled = "canceled"
)

func taskTerminal(state string) bool {
	switch state {
	case taskSuccess, taskFailed, taskTimeout, taskCanceled:
		return true
	}
	return false
}

func transferTerminal(status string) bool {
	// "cancel_requested" is NOT terminal: the transfer flips to "canceled"
	// once the agent confirms (or the reaper finalizes it), and --wait
	// should surface that outcome rather than stop early.
	switch status {
	case taskSuccess, taskFailed, taskCanceled:
		return true
	}
	return false
}

// poll fetches path every backoff tick until accept returns ok=true or the
// budget expires. Last payload is returned either way.
func poll[T any](g *Globals, path string, budget time.Duration, accept func(T) bool) (T, error) {
	var last T
	deadline := time.Now().Add(budget)
	delay := 500 * time.Millisecond
	for {
		err := g.Client.Get(path, &last)
		if err != nil {
			return last, err
		}
		if accept(last) {
			return last, nil
		}
		if time.Now().After(deadline) {
			return last, fail("wait_timeout", fmt.Sprintf("still not terminal after %s: %s", budget, path), ExitFailure)
		}
		time.Sleep(delay)
		if delay *= 2; delay > 2*time.Second {
			delay = 2 * time.Second
		}
	}
}

type taskEnvelope struct {
	Task   map[string]any  `json:"task"`
	Result *map[string]any `json:"result,omitempty"`
	State  string          `json:"state"`
}

type dispatchResponse struct {
	TaskID     string `json:"task_id"`
	AgentID    string `json:"agent_id"`
	Dispatched bool   `json:"dispatched"`
	QueuedOnly bool   `json:"queued_only"`
}

// defaultFanoutSpread is the stagger window `run` applies when it fans out and
// the operator expressed no preference. Batch dispatch to a fleet is not
// latency-sensitive — nobody is watching a hundred hosts answer in real time —
// whereas hitting them all in the same instant is exactly the herd the server's
// spread exists to break up. An explicit --spread 0s still means "send now".
const defaultFanoutSpread = 8 * time.Second

func addWriteCommands(r *Registry) {
	r.Add(&Command{
		Name:    "run",
		Summary: "Dispatch a shell command to agents (--wait blocks for results)",
		Flags: []FlagSpec{
			{Name: "cmd", Type: "string", Desc: "shell command to run (required)"},
			{Name: "agents", Type: "stringlist", Desc: "target agent ids (comma separated, flag repeatable)"},
			{Name: "group", Type: "stringlist", Desc: "target group ids"},
			{Name: "tag", Type: "stringlist", Desc: "target tags (k or k=v)"},
			{Name: "exec-timeout", Type: "int", Default: "60", Desc: "execution timeout in SECONDS per agent (distinct from global -timeout, which is the HTTP request duration)"},
			{Name: "priority", Type: "int", Default: "0", Desc: "dispatch priority; higher runs first when an agent has queued work"},
			{Name: "spread", Type: "duration", Default: "0s", Desc: "randomize dispatch times across targets within this window, so a batch does not hit every agent in the same second; a fan-out defaults to 8s, pass 0s to send immediately"},
			{Name: "wait", Type: "bool", Desc: "block until every task reaches a terminal state"},
			{Name: "wait-timeout", Type: "duration", Default: "90s", Desc: "CLI polling budget for --wait; must cover exec-timeout"},
			{Name: "yes", Type: "bool", Desc: "REQUIRED whenever the selector can fan out: any --group/--tag value, or more than one --agents token"},
		},
		Run: func(g *Globals, cf *CmdFlags) error {
			cmd := cf.String("cmd")
			if cmd == "" {
				return fail("usage", "run requires --cmd", ExitUsage)
			}
			targets := map[string]any{
				"agent_ids": cf.List("agents"),
				"group_ids": cf.List("group"),
				"tags":      cf.List("tag"),
			}
			n := len(cf.List("agents")) + len(cf.List("group")) + len(cf.List("tag"))
			if n == 0 {
				return fail("usage", "run requires at least one of --agents/--group/--tag", ExitUsage)
			}
			// Fan-out needs explicit confirmation: multiple ids, or any
			// group/tag selector whose server-side expansion is unknown here.
			fanout := n > 1 || len(cf.List("group")) > 0 || len(cf.List("tag")) > 0
			if fanout && !cf.Bool("yes") {
				return fail("needs_yes", "multi-agent dispatch requires --yes (explicit confirmation)", ExitUsage)
			}

			// A fan-out gets a window without being asked. cf.Set is what keeps
			// the two intents apart: an omitted --spread means "you pick", an
			// explicit 0s means "send now" and is honoured as such.
			spread := cf.Duration("spread")
			if fanout && !cf.Set("spread") {
				spread = defaultFanoutSpread
			}
			if spread < 0 || spread > time.Hour {
				return fail("usage", "--spread must be between 0s and 1h", ExitUsage)
			}
			// A spread batch is not all dispatched at once: the last target
			// only enters the queue up to --spread after the CLI starts its
			// clock. If the polling budget does not also cover the execution
			// time that follows, --wait gives up (exit 1) on tasks that are
			// merely still queued — so reject the combination up front instead.
			if spread > 0 && cf.Bool("wait") {
				budget := cf.Duration("wait-timeout")
				execTimeout := time.Duration(cf.Int("exec-timeout")) * time.Second
				if spread+execTimeout > budget {
					return fail("usage", fmt.Sprintf(
						"--wait-timeout %s is too small: --spread %s + --exec-timeout %s exceeds it; raise --wait-timeout",
						budget, spread, execTimeout), ExitUsage)
				}
			}
			targets["command"] = cmd
			targets["timeout_secs"] = cf.Int("exec-timeout")
			if p := cf.Int("priority"); p != 0 {
				targets["priority"] = p
			}
			if spread > 0 {
				// Omitted entirely at 0, so an operator who never sets
				// --spread sends the same body as before this flag existed.
				targets["spread_ms"] = int(spread.Milliseconds())
			}

			var resp struct {
				Count int                `json:"count"`
				Tasks []dispatchResponse `json:"tasks"`
			}
			if err := g.Client.Post("/api/v1/tasks/batch", targets, &resp); err != nil {
				return err
			}

			if !cf.Bool("wait") {
				return Emit(g.Stdout, map[string]any{"tasks": resp.Tasks}, g.Pretty)
			}

			results := make([]map[string]any, 0, len(resp.Tasks))
			allOK := true
			for _, t := range resp.Tasks {
				env, err := poll[taskEnvelope](g, "/api/v1/tasks/"+url.PathEscape(t.TaskID), cf.Duration("wait-timeout"),
					func(e taskEnvelope) bool { return taskTerminal(e.State) })
				if err != nil {
					return err
				}
				row := map[string]any{"task_id": t.TaskID, "agent_id": t.AgentID, "state": env.State}
				if env.Result != nil {
					for _, k := range []string{"exit_code", "stdout", "stderr", "duration_ms"} {
						if v, ok := (*env.Result)[k]; ok {
							row[k] = v
						}
					}
				}
				if env.State != taskSuccess {
					allOK = false
				}
				results = append(results, row)
			}
			if err := Emit(g.Stdout, map[string]any{"all_ok": allOK, "results": results}, g.Pretty); err != nil {
				return err
			}
			if !allOK {
				return fail("task_failed", "one or more tasks did not succeed", ExitFailure)
			}
			return nil
		},
	})

	r.Add(&Command{
		Name:    "tasks cancel",
		Summary: "Request cancellation of a task",
		Params:  []string{"task_id"},
		Run: func(g *Globals, cf *CmdFlags) error {
			if err := mustArgs(cf, 1, "tasks cancel <task_id>"); err != nil {
				return err
			}
			var out map[string]any
			if err := g.Client.Post("/api/v1/tasks/"+url.PathEscape(cf.Args()[0])+"/cancel", nil, &out); err != nil {
				return err
			}
			return Emit(g.Stdout, out, g.Pretty)
		},
	})

	r.Add(&Command{
		Name:    "groups create",
		Summary: "Create or update a group (full replacement of members)",
		Flags: []FlagSpec{
			{Name: "name", Type: "string", Desc: "group name (required, unique; collision -> exit 1)"},
			{Name: "id", Type: "string", Desc: "explicit id; with existing id this UPDATES the group (full member replace); empty = create new"},
			{Name: "desc", Type: "string", Desc: "description"},
			{Name: "agents", Type: "stringlist", Desc: "member agent ids"},
		},
		Run: func(g *Globals, cf *CmdFlags) error {
			if cf.String("name") == "" {
				return fail("usage", "groups create requires --name", ExitUsage)
			}
			body := map[string]any{"name": cf.String("name")}
			if id := cf.String("id"); id != "" {
				body["id"] = id
			}
			if d := cf.String("desc"); d != "" {
				body["description"] = d
			}
			if a := cf.List("agents"); a != nil {
				body["agent_ids"] = a
			}
			var out map[string]any
			if err := g.Client.Post("/api/v1/groups", body, &out); err != nil {
				return err
			}
			return Emit(g.Stdout, out, g.Pretty)
		},
	})

	type groupEnvelope struct {
		Name        string   `json:"name"`
		Description string   `json:"description"`
		AgentIDs    []string `json:"agent_ids"`
	}

	groupMemberCmd := func(name, summary string, mutate func(set, ops []string) []string) {
		r.Add(&Command{
			Name:    name,
			Summary: summary,
			Params:  []string{"group_id", "agent_ids..."},
			Flags: []FlagSpec{
				{Name: "agents", Type: "stringlist", Desc: "agent ids (alternative to positionals)"},
			},
			Run: func(g *Globals, cf *CmdFlags) error {
				if err := mustArgs(cf, 1, name+" <group_id> [agent_id...]"); err != nil {
					return err
				}
				groupID := cf.Args()[0]
				var grp groupEnvelope
				if err := g.Client.Get("/api/v1/groups/"+url.PathEscape(groupID), &grp); err != nil {
					return err
				}
				ids := append([]string{}, cf.Args()[1:]...)
				if len(ids) == 0 {
					ids = cf.List("agents")
				}
				if len(ids) == 0 {
					return fail("usage", name+" needs agent ids as positionals or --agents", ExitUsage)
				}
				var out map[string]any
				if err := g.Client.Post("/api/v1/groups", map[string]any{
					"id": groupID, "name": grp.Name, "description": grp.Description,
					"agent_ids": mutate(grp.AgentIDs, ids),
				}, &out); err != nil {
					return err
				}
				return Emit(g.Stdout, out, g.Pretty)
			},
		})
	}
	groupMemberCmd("groups add", "Add agents to a group", func(set, add []string) []string {
		have := map[string]bool{}
		for _, s := range set {
			have[s] = true
		}
		for _, a := range add {
			if !have[a] {
				set = append(set, a)
				have[a] = true
			}
		}
		return set
	})
	groupMemberCmd("groups remove", "Remove agents from a group", func(set, rm []string) []string {
		drop := map[string]bool{}
		for _, r := range rm {
			drop[r] = true
		}
		out := set[:0:0]
		for _, s := range set {
			if !drop[s] {
				out = append(out, s)
			}
		}
		return out
	})

	transferFlags := []FlagSpec{
		{Name: "agent", Type: "string", Desc: "target agent id (required)"},
		{Name: "local", Type: "string", Desc: "path on the server machine (required)"},
		{Name: "remote", Type: "string", Desc: "path on the agent machine (required)"},
		{Name: "chunk", Type: "int", Desc: "chunk size bytes (0 = server default)"},
		{Name: "wait", Type: "bool", Desc: "block until transfer completes"},
		{Name: "wait-timeout", Type: "duration", Default: "120s", Desc: "poll budget for --wait"},
	}
	addTransfer := func(name, summary, endpoint string) {
		r.Add(&Command{
			Name: name, Summary: summary, Flags: transferFlags,
			Run: func(g *Globals, cf *CmdFlags) error {
				if cf.String("agent") == "" || cf.String("local") == "" || cf.String("remote") == "" {
					return fail("usage", name+" requires --agent --local --remote", ExitUsage)
				}
				body := map[string]any{
					"agent_id":    cf.String("agent"),
					"local_path":  cf.String("local"),
					"remote_path": cf.String("remote"),
				}
				if c := cf.Int("chunk"); c > 0 {
					body["chunk_size"] = c
				}
				var tx map[string]any
				if err := g.Client.Post(endpoint, body, &tx); err != nil {
					return err
				}
				if !cf.Bool("wait") {
					return Emit(g.Stdout, tx, g.Pretty)
				}
				id, _ := tx["id"].(string)
				if id == "" {
					id, _ = tx["transfer_id"].(string)
				}
				if id == "" {
					return fail("bad_response", "transfer id missing from server response", ExitFailure)
				}
				final, err := poll[map[string]any](g, "/api/v1/transfers/"+url.PathEscape(id), cf.Duration("wait-timeout"),
					func(m map[string]any) bool { s, _ := m["status"].(string); return transferTerminal(s) })
				if err != nil {
					return err
				}
				if err := Emit(g.Stdout, final, g.Pretty); err != nil {
					return err
				}
				if s, _ := final["status"].(string); s != taskSuccess {
					return fail("transfer_failed", fmt.Sprintf("transfer ended as %s", s), ExitFailure)
				}
				return nil
			},
		})
	}
	addTransfer("push", "Send a file from the server host to an agent", "/api/v1/files/upload")
	addTransfer("pull", "Fetch a file from an agent to the server host", "/api/v1/files/download")

	r.Add(&Command{
		Name:    "transfers cancel",
		Summary: "Request cancellation of an in-flight transfer (partial files are kept for resume)",
		Params:  []string{"transfer_id"},
		Flags: []FlagSpec{
			{Name: "wait", Type: "bool", Desc: "block until the transfer reaches a terminal state (agent confirm or reaper grace)"},
			{Name: "wait-timeout", Type: "duration", Default: "90s", Desc: "poll budget for --wait; must cover the 30s confirm grace"},
		},
		Run: func(g *Globals, cf *CmdFlags) error {
			if err := mustArgs(cf, 1, "transfers cancel <transfer_id>"); err != nil {
				return err
			}
			id := url.PathEscape(cf.Args()[0])
			var out map[string]any
			if err := g.Client.Post("/api/v1/transfers/"+id+"/cancel", nil, &out); err != nil {
				return err
			}
			if !cf.Bool("wait") {
				return Emit(g.Stdout, out, g.Pretty)
			}
			// The POST answered cancel_requested (or a terminal status if it
			// already finished). Wait for the real terminal outcome.
			final, err := poll[map[string]any](g, "/api/v1/transfers/"+id, cf.Duration("wait-timeout"),
				func(m map[string]any) bool { s, _ := m["status"].(string); return transferTerminal(s) })
			if err != nil {
				return err
			}
			if err := Emit(g.Stdout, final, g.Pretty); err != nil {
				return err
			}
			if s, _ := final["status"].(string); s != taskCanceled {
				return fail("transfer_not_canceled", fmt.Sprintf("transfer ended as %s", s), ExitFailure)
			}
			return nil
		},
	})
}
