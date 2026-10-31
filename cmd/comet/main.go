// Command comet serves public websocket and private authenticated delivery RPC.
package main

import (
	"context"
	"flag"
	"fmt"
	"mychat/internal/comet/app"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	path := flag.String("config", "config/comet.example.toml", "Comet configuration file")
	flag.Parse()
	if flag.NArg() > 1 || flag.NArg() == 1 && flag.Arg(0) != "serve" {
		fmt.Fprintln(os.Stderr, "usage: comet -config FILE [serve]")
		os.Exit(2)
	}
	c, e := app.LoadConfig(*path)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if e = app.Run(ctx, c); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
