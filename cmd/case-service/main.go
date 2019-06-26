package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"runtime"
	"syscall"
	"time"

	// "github.com/ONSdigital/rabbit-go"
	"github.com/ONSdigital/census-rm-case-processor-prototype-go/internal/receive"
	"github.com/ONSdigital/census-rm-case-processor-prototype-go/pkg/queue"
	"github.com/ONSdigital/census-rm-case-processor-prototype-go/pkg/signal"
	"github.com/julienschmidt/httprouter"
)

// TODO read from config
const (
	RabbitURI   = "amqp://guest:guest@localhost:5672"
	DatabaseURI = "postgresql://postgres:postgres@localhost:6432/postgres"

	PublishBufferSize = 10

	IncomingExchange = ""
)

// Queuer provides an interface to the queuing broker
type Queuer interface {
	Connect(string)
	Close()
	Subscribe(string, string, func())
}

func main() {

	// TODO config
	port := "5000"

	// Set up webserver to serve healthchecks
	server := setupWebServer(port)

	// TODO code in properly!
	// p := &rabbit.Publisher{}
	// p.Connect(RabbitURI)
	// TODO [end]

	// Create a buffered channel for incoming message workers to publish
	// back out to events.
	// pub := make(chan interface{}, PublishBufferSize)

	// Create a new queue client and subcribe to incoming messages
	client := &queue.Client{
		Mode: queue.InOut,
	}
	if err := client.Connect(RabbitURI); err != nil {
		log.Fatalf(`event="Failed to connect to queue broker" error="%v"`, err)
	}
	if err := client.SubscribeToTopic(
		"some.exchange",         // exchange name
		"incoming",              // queue name
		"example.inbound.queue", // topic binding
		receive.MessageHandler,  // handler
		nil,                     // results channel
		runtime.NumCPU(),        // Number of workers to spawn
	); err != nil {
		log.Fatalf(`event="Failed to subscribe" error="%v"`, err)
	}

	// Set up signal handler to watch for signals telling us to shut down. This
	// gives us chance to close our connections gracefully before exiting.
	cancelSigWatch := signal.HandleFunc(
		func(sig os.Signal) {
			log.Printf(`event="Shutting down after receiving signal" signal="%s"`, sig.String())

			if client != nil {
				client.Close()
			}

			if server != nil {
				log.Print(`event="Shutting down webserver"`)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := server.Shutdown(ctx); err != nil {
					log.Printf(`event="Failed to shutdown webserver" error="%v"`, err)
				}
			}

			log.Print(`event="Exiting"`)
			os.Exit(0)
		},
		syscall.SIGTERM,
		syscall.SIGINT,
	)
	defer cancelSigWatch()

	log.Printf(`event="Started" port="%s"`, port)
	wait := make(chan int)
	<-wait
}

func setupWebServer(port string) *http.Server {
	r := httprouter.New()

	// Very simple healthcheck handler to prove we're alive. In a more fleshed out
	// app this could actually healthcheck whether the connections are ok etc
	r.GET("/info", func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"healthy":true}`))
	})

	// Create the web server with a sensible set of defaults
	server := &http.Server{
		Addr:              ":" + port, // TODO config
		Handler:           r,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		ReadHeaderTimeout: 0,
		IdleTimeout:       0,
		MaxHeaderBytes:    0,
	}

	// Launch the web server in a go routine so we can return without it blocking
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf(`event="Failed to start webserver" error="%s"`, err)
		}
	}()

	return server
}
