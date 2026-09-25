package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"coc2/internal/common"
)

// CmdFlags gives commands typed access to their parsed flags plus the
// positional args left over.
type CmdFlags struct {
	values map[string]any
	args   []string
	set    map[string]bool
}

func (f *CmdFlags) String(name string) string {
	if v, ok := f.values[name]; ok {
		return *(v.(*string))
	}
	return ""
}

func (f *CmdFlags) Int(name string) int {
	if v, ok := f.values[name]; ok {
		return *(v.(*int))
	}
	return 0
}

func (f *CmdFlags) Bool(name string) bool {
	if v, ok := f.values[name]; ok {
		return *(v.(*bool))
	}
	return false
}

func (f *CmdFlags) Duration(name string) time.Duration {
	if v, ok := f.values[name]; ok {
		return *(v.(*time.Duration))
	}
	return 0
}

func (f *CmdFlags) List(name string) []string {
	if v, ok := f.values[name]; ok {
		return *v.(*stringList)
	}
	return nil
}

// Args returns positional arguments.
func (f *CmdFlags) Args() []string { return f.args }

// Set reports whether the operator passed this flag explicitly, as opposed to
// it merely holding its declared default. Callers need that distinction to read
// an omitted flag differently from one deliberately set to the zero value:
// `run` treats an absent --spread as "pick a sensible window for me" but an
// explicit 0s as "send right now".
func (f *CmdFlags) Set(name string) bool { return f.set[name] }

// Command is one leaf subcommand.
type Command struct {
	Name    string                               `json:"name"` // e.g. "agents list"
	Summary string                               `json:"summary"`
	Flags   []FlagSpec                           `json:"flags,omitempty"`
	Params  []string                             `json:"params,omitempty"` // positional args
	Run     func(g *Globals, cf *CmdFlags) error `json:"-"`
}

type FlagSpec struct {
	Name    string `json:"name"`
	Type    string `json:"type"` // string|int|bool|duration|stringlist
	Default string `json:"default,omitempty"`
	Desc    string `json:"desc"`
}

// Globals carry cross-command flags resolved before dispatch.
type Globals struct {
	Client   *Client
	Stdout   io.Writer
	Stderr   io.Writer
	Pretty   bool
	Timeout  time.Duration
	Server   string
	Token    string
	Insecure bool
	// Client-side TLS material for the operator plane, needed when the server
	// runs with client_ca (mTLS).
	CACert     string
	ClientCert string
	ClientKey  string
}

// Registry owns all commands.
type Registry struct {
	cmds []*Command
}

func (r *Registry) Add(c *Command) {
	for _, existing := range r.cmds {
		if existing.Name == c.Name {
			panic("duplicate command " + c.Name)
		}
	}
	r.cmds = append(r.cmds, c)
}

func (r *Registry) find(name string) *Command {
	for _, c := range r.cmds {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// DefaultTarget resolves the operator plane endpoint: env > ./coc2.sock.
func DefaultTarget() string {
	if v := os.Getenv("COC2_SERVER"); v != "" {
		return v
	}
	return "./coc2.sock"
}

// DefaultToken reads the bearer token from the environment.
func DefaultToken() string {
	return os.Getenv("COC2_TOKEN")
}

// Run executes the registry with CLI args and returns the process exit code.
func (r *Registry) Run(args []string) int {
	return r.RunWithIO(args, os.Stdout, os.Stderr)
}

// RunWithIO is Run with injectable stdout/stderr for tests.
func (r *Registry) RunWithIO(args []string, stdout, stderr io.Writer) int {
	g := &Globals{Stdout: stdout, Stderr: stderr}

	// Global flags are accepted at any position in argv.
	cmdArgs, globalArgs := hoistGlobals(args)
	if err := r.parseGlobals(g, globalArgs); err != nil {
		EmitError(g.Stderr, err)
		return ExitUsage
	}

	if len(cmdArgs) == 0 {
		r.usageErr(g, "missing command")
		return ExitUsage
	}
	switch cmdArgs[0] {
	case "help":
		return r.usage(g)
	case "schema":
		return r.emitSchema(g)
	}

	name, rest := r.matchCommand(cmdArgs)
	cmd := r.find(name)
	if cmd == nil {
		r.usageErr(g, fmt.Sprintf("unknown command %q", strings.Join(cmdArgs, " ")))
		return ExitUsage
	}

	cf, err := r.parseCommandFlags(cmd, rest)
	if err != nil {
		EmitError(g.Stderr, err)
		return ExitUsage
	}

	g.Client = NewClient(g.Server, g.Token, g.Timeout, g.Insecure, TLSFiles{
		CACert:     g.CACert,
		ClientCert: g.ClientCert,
		ClientKey:  g.ClientKey,
	})
	if err := cmd.Run(g, cf); err != nil {
		EmitError(g.Stderr, err)
		return ExitCode(err)
	}
	return ExitOK
}

// parseGlobals accepts global flags in one pass and reports leftovers.
func (r *Registry) parseGlobals(g *Globals, args []string) error {
	fs := flag.NewFlagSet("coc2", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&g.Server, "server", DefaultTarget(), "operator plane: unix socket path or http(s):// URL")
	fs.StringVar(&g.Token, "token", DefaultToken(), "operator token (needed with the TCP operator plane)")
	fs.BoolVar(&g.Pretty, "pretty", false, "indent JSON output")
	fs.DurationVar(&g.Timeout, "timeout", 30*time.Second, "HTTP request timeout")
	fs.BoolVar(&g.Insecure, "insecure", false, "skip TLS verification (https targets)")
	fs.StringVar(&g.CACert, "ca-cert", "", "CA bundle to verify the operator plane's TLS certificate")
	fs.StringVar(&g.ClientCert, "client-cert", "", "client certificate for a server running with client_ca (mTLS)")
	fs.StringVar(&g.ClientKey, "client-key", "", "private key for -client-cert")
	if err := fs.Parse(args); err != nil {
		return fail("usage", err.Error(), ExitUsage)
	}
	return nil
}

// matchCommand resolves the two-word command name; Go's stdlib flag package
// requires flags before positionals, so command flags must follow the name.
func (r *Registry) matchCommand(args []string) (string, []string) {
	if len(args) >= 2 && r.find(args[0]+" "+args[1]) != nil {
		return args[0] + " " + args[1], args[2:]
	}
	return args[0], args[1:]
}

// parseCommandFlags registers a command's flag specs, parses args, and
// returns typed accessors plus positional leftovers.
func (r *Registry) parseCommandFlags(cmd *Command, args []string) (*CmdFlags, error) {
	fs := flag.NewFlagSet(cmd.Name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	values := map[string]any{}
	for _, spec := range cmd.Flags {
		switch spec.Type {
		case "string":
			v := fs.String(spec.Name, spec.Default, spec.Desc)
			values[spec.Name] = v
		case "int":
			v := fs.Int(spec.Name, atoi(spec.Default), spec.Desc)
			values[spec.Name] = v
		case "bool":
			v := fs.Bool(spec.Name, spec.Default == "true", spec.Desc)
			values[spec.Name] = v
		case "duration":
			v := fs.Duration(spec.Name, parseDur(spec.Default), spec.Desc)
			values[spec.Name] = v
		case "stringlist":
			v := &stringList{}
			fs.Var(v, spec.Name, spec.Desc)
			values[spec.Name] = v
		default:
			return nil, fail("usage", fmt.Sprintf("command %q declares unknown flag type %q", cmd.Name, spec.Type), ExitUsage)
		}
	}
	// Go's stdlib flag package stops at the first positional, so run Parse
	// repeatedly to accept interspersed flags and positionals.
	var positional []string
	rest := args
	for {
		if err := fs.Parse(rest); err != nil {
			return nil, fail("usage", err.Error(), ExitUsage)
		}
		rest = fs.Args()
		if len(rest) == 0 {
			break
		}
		positional = append(positional, rest[0])
		rest = rest[1:]
	}
	// Visit reports only the flags actually present on the command line, which
	// is what lets a command tell "omitted" from "set to the default value".
	// The FlagSet accumulates that record across the Parse calls above rather
	// than rebuilding it, so one pass here already covers every iteration.
	set := map[string]bool{}
	fs.Visit(func(fl *flag.Flag) { set[fl.Name] = true })
	return &CmdFlags{values: values, args: positional, set: set}, nil
}

// usage prints the command list; returns exit code 0.
func (r *Registry) usage(g *Globals) int {
	out := map[string]any{
		"usage":    "coc2 [-server <uds|url>] [-token <t>] <command> [flags]",
		"commands": r.commandNames(),
		"note":     "run `coc2 schema` for the full machine-readable command spec",
	}
	_ = Emit(g.Stdout, out, g.Pretty)
	return ExitOK
}

func (r *Registry) usageErr(g *Globals, msg string) {
	_ = json.NewEncoder(g.Stderr).Encode(map[string]any{
		"error":    map[string]string{"code": "usage", "message": msg},
		"commands": r.commandNames(),
	})
}

func (r *Registry) emitSchema(g *Globals) int {
	type schemaDoc struct {
		Name        string         `json:"name"`
		Version     string         `json:"version"`
		ExitCodes   map[string]int `json:"exit_codes"`
		Limits      map[string]any `json:"limits"`
		GlobalFlags []FlagSpec     `json:"global_flags"`
		Commands    []*Command     `json:"commands"`
	}
	doc := schemaDoc{
		Name:    "coc2",
		Version: "1",
		ExitCodes: map[string]int{
			"ok": ExitOK, "failure": ExitFailure, "connect": ExitConnect,
			"auth": ExitAuth, "usage": ExitUsage,
		},
		// Limits a programmatic caller has to know to interpret results
		// correctly. A truncated result that looks complete is worse than an
		// error: an agent that reads only this schema must be able to tell
		// "the command printed one line" from "the command printed more than
		// we kept".
		Limits: map[string]any{
			"task_output_bytes_per_stream":  common.MaxTaskOutputBytes,
			"task_output_truncation_marker": "[output truncated]",
			"task_output_note": "stdout and stderr are each capped at task_output_bytes_per_stream; " +
				"when a stream exceeds it the tail is dropped and a line containing " +
				"task_output_truncation_marker is appended",
			"concurrent_tasks_per_agent": common.MaxConcurrentTasks,
			"concurrent_tasks_note": "dispatches beyond this are answered with a failed result " +
				"(\"agent is already running the maximum number of tasks\"), never silently dropped",
		},
		GlobalFlags: []FlagSpec{
			{Name: "server", Type: "string", Default: DefaultTarget(), Desc: "operator plane unix socket path or http(s) URL; env COC2_SERVER"},
			{Name: "token", Type: "string", Desc: "bearer token for the TCP operator plane; env COC2_TOKEN"},
			{Name: "pretty", Type: "bool", Desc: "indent JSON"},
			{Name: "timeout", Type: "duration", Default: "30s", Desc: "HTTP timeout"},
			{Name: "insecure", Type: "bool", Desc: "skip TLS verification"},
			{Name: "ca-cert", Type: "string", Desc: "CA bundle to verify the operator plane's TLS certificate"},
			{Name: "client-cert", Type: "string", Desc: "client certificate for a server running with client_ca (mTLS)"},
			{Name: "client-key", Type: "string", Desc: "private key for -client-cert"},
		},
		Commands: r.cmds,
	}
	if err := Emit(g.Stdout, doc, g.Pretty); err != nil {
		return ExitFailure
	}
	return ExitOK
}

func (r *Registry) commandNames() []string {
	names := make([]string, 0, len(r.cmds))
	for _, c := range r.cmds {
		names = append(names, c.Name)
	}
	return names
}

// hoistGlobals pulls the five global flags (with their values) out of any
// position in args, returning (remaining args, global flag args).
func hoistGlobals(args []string) (rest []string, globals []string) {
	i := 0
	for i < len(args) {
		a := args[i]
		if strings.HasPrefix(a, "-") && len(a) > 1 {
			key := strings.TrimLeft(a, "-")
			hasVal := strings.Contains(key, "=")
			if eq := strings.Index(key, "="); eq >= 0 {
				key = key[:eq]
			}
			if isGlobalFlag(key) {
				globals = append(globals, a)
				if !hasVal && needsValue(key) && i+1 < len(args) {
					i++
					globals = append(globals, args[i])
				}
				i++
				continue
			}
		}
		rest = append(rest, a)
		i++
	}
	return
}

var globalFlags = map[string]bool{
	"server": true, "token": true, "pretty": true, "timeout": true, "insecure": true,
	"ca-cert": true, "client-cert": true, "client-key": true,
}

func isGlobalFlag(key string) bool { return globalFlags[key] }

func needsValue(key string) bool {
	return key != "pretty" && key != "insecure"
}

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error {
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			*s = append(*s, part)
		}
	}
	return nil
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func parseDur(s string) time.Duration {
	if s == "" {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}
