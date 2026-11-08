package main

// queue-lab worker (Go): the entry point. One image, two roles, chosen by the ROLE variable.
//

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	initSettings()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT) // docker stop sends SIGTERM
	go func() {
		<-signals
		stopping.Store(true)
	}()
	fmt.Printf("%s %s starting\n", role, workerID())
	if role == "consumer" {
		runConsumer()
	} else {
		runProducer()
	}
}
