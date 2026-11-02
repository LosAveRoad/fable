// local-admin is a one-shot development setup command, not a fourth role.
package main

import (
	"context"
	"flag"
	"fmt"
	"mychat/internal/localdev"
	logicapp "mychat/internal/logic/app"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	file := flag.String("config", "", "Logic configuration")
	smoke := flag.Bool("smoke", false, "test selected running topology without schema changes")
	entry := flag.String("entry", "", "selected local HTTP ingress for smoke")
	dlq := flag.String("dlq", "fable.push.dlq.v1", "explicit local DLQ")
	flag.Parse()
	c, e := logicapp.LoadConfig(*file)
	if e == nil {
		signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		ctx, cancel := context.WithTimeout(signals, 45*time.Second)
		defer cancel()
		if *smoke {
			e = localdev.Smoke(ctx, c, *entry)
		} else {
			e = localdev.Prepare(ctx, c, *dlq)
		}
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	if *smoke {
		fmt.Println("selected topology bidirectional smoke passed")
	} else {
		fmt.Println("local database and explicit topics ready")
	}
}
