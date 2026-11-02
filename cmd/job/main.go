// Command job consumes saved pushes and provides bounded DLQ replay previews.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"mychat/internal/job/app"
)

func main() {
	path := flag.String("config", "config/job.example.toml", "Job configuration file")
	flag.Parse()
	command := "serve"
	if flag.NArg() > 0 {
		command = flag.Arg(0)
	}
	if command != "serve" && command != "replay-dlq" || command == "serve" && flag.NArg() > 1 {
		fmt.Fprintln(os.Stderr, "usage: job -config FILE serve | replay-dlq -run-id ID -partition N -from-offset N -to-offset N [-execute]")
		os.Exit(2)
	}
	cfg, err := app.LoadConfig(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if command == "serve" {
		err = app.Run(ctx, cfg)
	} else {
		flags := flag.NewFlagSet("replay-dlq", flag.ExitOnError)
		o := app.ReplayOptions{}
		flags.StringVar(&o.RunID, "run-id", "", "explicit replay run identity")
		flags.IntVar(&o.Partition, "partition", -1, "DLQ partition")
		flags.Int64Var(&o.FromOffset, "from-offset", -1, "inclusive first DLQ offset")
		flags.Int64Var(&o.ToOffset, "to-offset", -1, "inclusive last DLQ offset")
		flags.BoolVar(&o.Execute, "execute", false, "publish validated pending tasks; default previews only")
		_ = flags.Parse(flag.Args()[1:])
		if flags.NArg() != 0 {
			fmt.Fprintln(os.Stderr, "unexpected replay arguments")
			os.Exit(2)
		}
		var result app.ReplaySummary
		result, err = app.Replay(ctx, cfg, o)
		_ = json.NewEncoder(os.Stdout).Encode(result)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
