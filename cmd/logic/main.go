// Command logic serves Fable business HTTP/MCP and authenticated internal RPC.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"mychat/internal/logic/app"
)

func main() {
	path := flag.String("config", "config/logic.example.toml", "Logic configuration file")
	flag.Parse()
	command := "serve"
	if flag.NArg() > 0 {
		command = flag.Arg(0)
	}
	if flag.NArg() > 1 || command != "serve" && command != "init-schema" {
		fmt.Fprintln(os.Stderr, "usage: logic -config FILE [serve|init-schema]")
		os.Exit(2)
	}
	cfg, err := app.LoadConfig(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if command == "init-schema" {
		err = app.InitSchema(ctx, cfg)
	} else {
		err = app.Run(ctx, cfg)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
