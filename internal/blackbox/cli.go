package blackbox

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// CLI runs either driver and writes an owner-readable JSON report to the explicit path.
// It returns a process exit code; operational details and credentials are never printed.
func CLI(ctx context.Context, args []string, load bool, stderr io.Writer) int {
	flags := flag.NewFlagSet("blackbox", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var cfg LoadConfig
	var path string
	flags.StringVar(&cfg.BaseURL, "base-url", "", "required public test ingress URL")
	flags.StringVar(&cfg.CAFile, "ca-file", "", "optional PEM trust root (hostname validation remains enabled)")
	flags.StringVar(&cfg.RunID, "run-id", "", "optional alphanumeric fixture run label")
	flags.StringVar(&path, "report", "", "required output JSON path")
	flags.DurationVar(&cfg.Timeout, "timeout", 0, "overall timeout (default flow 2m, load duration + 3m; max 15m)")
	flags.DurationVar(&cfg.StepTimeout, "step-timeout", 10*time.Second, "per operation/case timeout (100ms to 30s)")
	if load {
		flags.IntVar(&cfg.Pairs, "pairs", 2, "distinct user pairs / maximum concurrent sends (1 to 32)")
		flags.Float64Var(&cfg.Rate, "rate", 2, "aggregate sends per second (0.1 to 200)")
		flags.DurationVar(&cfg.Duration, "duration", 10*time.Second, "measured send interval (1s to 5m)")
		flags.DurationVar(&cfg.ReceiveTimeout, "receive-timeout", 5*time.Second, "shared online push drain deadline (100ms to 30s)")
		flags.DurationVar(&cfg.MaxSaveP95, "max-save-p95", 0, "optional p95 receipt latency threshold")
		flags.DurationVar(&cfg.MaxReceiveP95, "max-receive-p95", 0, "optional p95 online receive latency threshold")
		flags.Float64Var(&cfg.MinReceiveRatio, "min-receive-ratio", 0, "optional minimum online / stored ratio (0 to 1)")
	}
	if e := flags.Parse(args); e != nil {
		if e == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if path == "" || cfg.BaseURL == "" || flags.NArg() != 0 {
		fmt.Fprintln(stderr, "-base-url and -report are required; positional arguments are unsupported")
		return 2
	}
	var report any
	var err error
	if load {
		report, err = RunLoad(ctx, cfg)
	} else {
		report, err = Run(ctx, cfg.Config)
	}
	if writeJSON(path, report) != nil {
		fmt.Fprintln(stderr, "cannot write report")
		return 2
	}
	if err != nil {
		fmt.Fprintln(stderr, "run failed; inspect the JSON report")
		return 1
	}
	return 0
}
func writeJSON(path string, v any) error {
	f, e := os.CreateTemp(filepath.Dir(path), ".fable-report-*.json")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if e = encoder.Encode(v); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	return os.Rename(name, path)
}
