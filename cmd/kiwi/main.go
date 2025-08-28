// Spins up the kiwi server, compatible w/ the Redis protocol.

package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"

	"github.com/nobletooth/kiwi/pkg/port"
	"github.com/nobletooth/kiwi/pkg/storage"
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill)

	go func() { // Listen for OS interrupts in the background.
		select {
		case sig := <-signals:
			slog.Info("Received termination signal, cancelling server context.", "signal", sig)
			cancel()
		}
	}()

	store := storage.NewInMemoryKeyValueHolder()
	if err := port.RunRedisServer(ctx, store); err != nil {
		slog.Error("Kiwi server stopped.", "err", err)
		os.Exit(1)
	}
}
