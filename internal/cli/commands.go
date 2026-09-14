package cli

import (
	"fmt"
	"net/url"
	"strings"
)

// RegisterAll wires every operator command into the registry.
func RegisterAll(r *Registry) {
	addHealth(r)
	addAgents(r)
	addGroups(r)
	addTasks(r)
	addMetrics(r)
	addTransfers(r)
	addPlugins(r)
	addAudit(r)
	addWriteCommands(r)
}

func mustArgs(cf *CmdFlags, n int, usage string) error {
	if len(cf.Args()) < n {
		return fail("usage", usage, ExitUsage)
	}
	return nil
}

func queryInt(name string, v int) string {
	if v <= 0 {
		return ""
	}
	return fmt.Sprintf("%s=%d", name, v)
}

func joinQuery(parts ...string) string {
	q := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			q = append(q, p)
		}
	}
	if len(q) == 0 {
		return ""
	}
	return "?" + strings.Join(q, "&")
}

func addHealth(r *Registry) {
	r.Add(&Command{
		Name:    "health",
		Summary: "Check operator plane reachability and auth",
		Run: func(g *Globals, cf *CmdFlags) error {
			var out map[string]any
			if err := g.Client.Get("/healthz", &out); err != nil {
				return err
			}
			return Emit(g.Stdout, out, g.Pretty)
		},
	})
}

func addAgents(r *Registry) {
	r.Add(&Command{
		Name:    "agents list",
		Summary: "List known agents with online state",
		Flags: []FlagSpec{
			{Name: "filter", Type: "string", Desc: "only agents matching this tag (k or k=v)"},
			{Name: "online", Type: "bool", Desc: "only online agents"},
		},
		Run: func(g *Globals, cf *CmdFlags) error {
			var agents []map[string]any
			if err := g.Client.Get("/api/v1/agents", &agents); err != nil {
				return err
			}
			filtered := make([]map[string]any, 0, len(agents))
			for _, a := range agents {
				if cf.Bool("online") && a["online"] != true {
					continue
				}
				if f := cf.String("filter"); f != "" && !agentMatchesTag(a, f) {
					continue
				}
				filtered = append(filtered, a)
			}
			return Emit(g.Stdout, filtered, g.Pretty)
		},
	})

	r.Add(&Command{
		Name:    "agents get",
		Summary: "Get one agent by id",
		Params:  []string{"agent_id"},
		Run: func(g *Globals, cf *CmdFlags) error {
			if err := mustArgs(cf, 1, "agents get <agent_id>"); err != nil {
				return err
			}
			var agents []map[string]any
			if err := g.Client.Get("/api/v1/agents", &agents); err != nil {
				return err
			}
			id := cf.Args()[0]
			for _, a := range agents {
				if a["agent_id"] == id {
					return Emit(g.Stdout, a, g.Pretty)
				}
			}
			return fail("not_found", "agent "+id+" not found", ExitFailure)
		},
	})

	r.Add(&Command{
		Name:    "agents metrics",
		Summary: "Latest metrics snapshot for one agent",
		Params:  []string{"agent_id"},
		Run: func(g *Globals, cf *CmdFlags) error {
			if err := mustArgs(cf, 1, "agents metrics <agent_id>"); err != nil {
				return err
			}
			var out map[string]any
			if err := g.Client.Get("/api/v1/agents/"+url.PathEscape(cf.Args()[0])+"/metrics", &out); err != nil {
				return err
			}
			return Emit(g.Stdout, out, g.Pretty)
		},
	})

	r.Add(&Command{
		Name:    "agents history",
		Summary: "Metric history for one agent (newest first)",
		Params:  []string{"agent_id"},
		Flags: []FlagSpec{
			{Name: "limit", Type: "int", Default: "50", Desc: "max samples (<=500)"},
		},
		Run: func(g *Globals, cf *CmdFlags) error {
			if err := mustArgs(cf, 1, "agents history <agent_id>"); err != nil {
				return err
			}
			var out []map[string]any
			path := "/api/v1/agents/" + url.PathEscape(cf.Args()[0]) + "/metrics/history" +
				joinQuery(queryInt("limit", cf.Int("limit")))
			if err := g.Client.Get(path, &out); err != nil {
				return err
			}
			return Emit(g.Stdout, out, g.Pretty)
		},
	})
}

func agentMatchesTag(agent map[string]any, filter string) bool {
	key, want, hasWant := strings.Cut(filter, "=")
	tags, _ := agent["tags"].([]any)
	for _, t := range tags {
		tag, _ := t.(string)
		if !hasWant {
			if tag == key || strings.HasPrefix(tag, key+"=") {
				return true
			}
			continue
		}
		if tag == key+"="+want {
			return true
		}
	}
	return false
}

func addGroups(r *Registry) {
	r.Add(&Command{
		Name:    "groups list",
		Summary: "List groups",
		Run: func(g *Globals, cf *CmdFlags) error {
			var out []map[string]any
			if err := g.Client.Get("/api/v1/groups", &out); err != nil {
				return err
			}
			return Emit(g.Stdout, out, g.Pretty)
		},
	})

	r.Add(&Command{
		Name:    "groups get",
		Summary: "Get one group by id",
		Params:  []string{"group_id"},
		Run: func(g *Globals, cf *CmdFlags) error {
			if err := mustArgs(cf, 1, "groups get <group_id>"); err != nil {
				return err
			}
			var out map[string]any
			if err := g.Client.Get("/api/v1/groups/"+url.PathEscape(cf.Args()[0]), &out); err != nil {
				return err
			}
			return Emit(g.Stdout, out, g.Pretty)
		},
	})
}

func addTasks(r *Registry) {
	r.Add(&Command{
		Name:    "tasks list",
		Summary: "List recent tasks (newest first)",
		Flags: []FlagSpec{
			{Name: "limit", Type: "int", Default: "20", Desc: "max tasks (<=100)"},
		},
		Run: func(g *Globals, cf *CmdFlags) error {
			var out []map[string]any
			path := "/api/v1/tasks" + joinQuery(queryInt("limit", cf.Int("limit")))
			if err := g.Client.Get(path, &out); err != nil {
				return err
			}
			return Emit(g.Stdout, out, g.Pretty)
		},
	})

	r.Add(&Command{
		Name:    "tasks get",
		Summary: "Get one task by id",
		Params:  []string{"task_id"},
		Run: func(g *Globals, cf *CmdFlags) error {
			if err := mustArgs(cf, 1, "tasks get <task_id>"); err != nil {
				return err
			}
			var out map[string]any
			if err := g.Client.Get("/api/v1/tasks/"+url.PathEscape(cf.Args()[0]), &out); err != nil {
				return err
			}
			return Emit(g.Stdout, out, g.Pretty)
		},
	})
}

func addMetrics(r *Registry) {
	r.Add(&Command{
		Name:    "metrics overview",
		Summary: "Fleet-wide counters: agents online, pending tasks, transfers",
		Run: func(g *Globals, cf *CmdFlags) error {
			var out map[string]any
			if err := g.Client.Get("/api/v1/metrics/overview", &out); err != nil {
				return err
			}
			return Emit(g.Stdout, out, g.Pretty)
		},
	})
}

func addTransfers(r *Registry) {
	r.Add(&Command{
		Name:    "transfers list",
		Summary: "List recent file transfers (audit log)",
		Flags: []FlagSpec{
			{Name: "limit", Type: "int", Default: "20", Desc: "max transfers (<=100)"},
		},
		Run: func(g *Globals, cf *CmdFlags) error {
			var out []map[string]any
			path := "/api/v1/transfers" + joinQuery(queryInt("limit", cf.Int("limit")))
			if err := g.Client.Get(path, &out); err != nil {
				return err
			}
			return Emit(g.Stdout, out, g.Pretty)
		},
	})

	r.Add(&Command{
		Name:    "transfers get",
		Summary: "Get one transfer (live state or audit record)",
		Params:  []string{"transfer_id"},
		Run: func(g *Globals, cf *CmdFlags) error {
			if err := mustArgs(cf, 1, "transfers get <transfer_id>"); err != nil {
				return err
			}
			var out map[string]any
			if err := g.Client.Get("/api/v1/transfers/"+url.PathEscape(cf.Args()[0]), &out); err != nil {
				return err
			}
			return Emit(g.Stdout, out, g.Pretty)
		},
	})
}

func addPlugins(r *Registry) {
	r.Add(&Command{
		Name:    "plugins list",
		Summary: "List loaded plugins",
		Run: func(g *Globals, cf *CmdFlags) error {
			var out []map[string]any
			if err := g.Client.Get("/api/v1/plugins", &out); err != nil {
				return err
			}
			return Emit(g.Stdout, out, g.Pretty)
		},
	})
}

func addAudit(r *Registry) {
	r.Add(&Command{
		Name:    "audit list",
		Summary: "Operator action log: who did what, from where, with what result",
		Flags: []FlagSpec{
			{Name: "limit", Type: "int", Default: "50", Desc: "max entries (<=500)"},
			{Name: "actor", Type: "string", Desc: "only this actor (unix username on UDS, token[:name] on TCP)"},
			{Name: "agent", Type: "string", Desc: "only ops targeting this agent id"},
			{Name: "path", Type: "string", Desc: "only this endpoint path (exact)"},
			{Name: "ref", Type: "string", Desc: "only ops referencing this id (task/transfer/group)"},
			{Name: "since", Type: "string", Desc: "RFC3339 or YYYY-MM-DD (UTC)"},
			{Name: "until", Type: "string", Desc: "RFC3339 or YYYY-MM-DD (UTC)"},
			{Name: "failed", Type: "bool", Desc: "only rejected/failed entries (incl. auth failures)"},
		},
		Run: func(g *Globals, cf *CmdFlags) error {
			var out []map[string]any
			path := "/api/v1/oplog" + joinQuery(
				queryInt("limit", cf.Int("limit")),
				queryStr("actor", cf.String("actor")),
				queryStr("agent", cf.String("agent")),
				queryStr("path", cf.String("path")),
				queryStr("ref", cf.String("ref")),
				queryStr("since", cf.String("since")),
				queryStr("until", cf.String("until")),
				queryFlag("failed", cf.Bool("failed")),
			)
			if err := g.Client.Get(path, &out); err != nil {
				return err
			}
			return Emit(g.Stdout, out, g.Pretty)
		},
	})
}

func queryStr(name, v string) string {
	if v == "" {
		return ""
	}
	return name + "=" + url.QueryEscape(v)
}

func queryFlag(name string, on bool) string {
	if !on {
		return ""
	}
	return name + "=1"
}
