package main

import (
	"context"
	"mychat/internal/blackbox"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := blackbox.CLI(ctx, os.Args[1:], false, os.Stderr)
	stop()
	os.Exit(code)
}
