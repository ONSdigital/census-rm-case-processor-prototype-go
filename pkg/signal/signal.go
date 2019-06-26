// Package signal provides convenience functions for dealing with system signals
package signal

import (
	"context"
	"os"
	"os/signal"
)

// HandleFunc registers a handler to be run in the event of a given set of
// signals being received by the process.
// It returns a cancel context function to allow termination of the go routine.
func HandleFunc(handler func(os.Signal), sigsToHandle ...os.Signal) func() {
	ctx, cancel := context.WithCancel(context.Background())

	// Create a buffered channel that will capture the signals received
	sigs := make(chan os.Signal, 1)

	// Register to watch SIGINT and SIGTERM
	signal.Notify(sigs, sigsToHandle...)

	go func(ctx context.Context) {
		// Wait to receive a signal (or context to kill the watcher)
		for {
			select {
			case <-ctx.Done():
				return
			case sig := <-sigs:
				handler(sig)
				return
			}
		}
	}(ctx)

	return cancel
}
