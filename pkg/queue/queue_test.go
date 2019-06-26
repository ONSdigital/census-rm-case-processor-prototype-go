package queue_test

import (
	"log"

	"github.com/ONSdigital/census-rm-case-processor-prototype-go/pkg/queue"
)

func Example() {
	// Create a new client to interact with the queue.
	//
	// Here we define whether we want incoming, outgoing or both channels with the
	// `Mode` flag (`queue.InOnly`, `queue.OutOnly`, `queue.InOut`)
	client := &queue.Client{
		Mode: queue.InOut,
	}

	// Use the client to open a connection to the broker
	if err := client.Connect("amqp://user:pass@rabbit:5672"); err != nil {
		log.Fatal("Failed to connect to broker:", err)
	}
}
