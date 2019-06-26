package queue

import (
	"log"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

// Default values
const (
	TopicExchange = "amq.topic"
)

// Client contains the connection and state of the current queue client
type Client struct {
	Mode          uint8
	conn          *amqp.Connection
	subscriptions []uuid.UUID
	in            *amqp.Channel // Shared incoming channel
	out           *amqp.Channel // Shared publishing channl
}

// Subscription represents a single queue subscription
type Subscription struct {
	consumerTag uuid.UUID
	result      chan interface{}
}

// Define mode - incoming, outgoing or both
const (
	InOnly uint8 = iota + 1
	OutOnly
	InOut
)

// Connect will attempt to connect the client to given broker
// An error will be returned if the connection fails or if the connection seems
// to already be open.
func (c *Client) Connect(uri string) error {
	var err error
	if c.conn != nil && !c.conn.IsClosed() {
		return errors.New("client already connected")
	}

	c.conn, err = amqp.Dial(uri)
	if err != nil {
		return errors.Wrap(err, "failed to connect to broker")
	}

	// Open channels as appropriate for receiving and publishing
	if c.Mode == InOnly || c.Mode == InOut {
		if c.in, err = c.conn.Channel(); err != nil {
			c.conn.Close()
			return errors.Wrap(err, "failed to connect to broker: could not connect incoming channel")
		}

	}
	if c.Mode == OutOnly || c.Mode == InOut {
		if c.out, err = c.conn.Channel(); err != nil {
			c.conn.Close()
			return errors.Wrap(err, "failed to connect to broker: could not connect outgoing channel")
		}
	}
	return nil
}

// MessageHandlerFunc defines a function that will be executed by a subcribed
// comsumer for each message it receives.
type MessageHandlerFunc func(*Client, amqp.Delivery, chan<- interface{}, int) error

// SubscribeToTopic connects to a topic exchange and begins consuming. Received
// messages are passed to the supplied `handlerFunc`
//
// In the event of a shutdown, the Client will attempt to nack unprocessed messages
// back to the queue
//
// Optionally can supply a return channel that the handler can return results on
func (c *Client) SubscribeToTopic(exchange, queueName, bindingKey string, handlerFunc MessageHandlerFunc, result chan<- interface{}, workers int) error {
	if c.conn == nil {
		return errors.New("unable to subscribe: no open connection")
	}
	if c.in == nil {
		return errors.New("unable to subscribe: no open incoming channel")
	}

	if err := c.in.ExchangeDeclare(
		exchange, // name of the exchange
		"topic",  // type
		true,     // durable
		false,    // delete when complete
		false,    // internal
		false,    // noWait
		nil,      // arguments
	); err != nil {
		return errors.Wrap(err, "unable to declare exchange for subscribe")
	}

	// Declare the queue to connect to. If the queue doesn't already exist then
	// this will create it attached to the default exchange.
	queue, err := c.in.QueueDeclare(
		queueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no wait
		nil,   // args
	)
	if err != nil {
		return errors.Wrap(err, "unable to declare queue")
	}

	// Bind the queue to the default topic exchange
	if err := c.in.QueueBind(
		queue.Name,
		bindingKey, // binding key
		exchange,   // source exchange
		false,      // no wait
		nil,        // args
	); err != nil {
		return errors.Wrap(err, "unable to bind queue to exchange")
	}

	// Create a consumer tag for the subscription. This allows clean shutdown of
	// the consumer later when Close() is called.
	consumerTag, err := uuid.NewRandom()
	if err != nil {
		return errors.Wrap(err, "failed to generate consumer tag for subscription")
	}

	// Begin consuming from the queue. The consumed messages are put onto the msgs
	// channel that will be read by the handlers.
	msgs, err := c.in.Consume(
		queue.Name,
		consumerTag.String(), // consumer tag
		false,                // auto-ack
		false,                // exclusive
		false,                // no local
		false,                // no wait
		nil,                  // args
	)
	if err != nil {
		return errors.Wrap(err, "unable to start consuming on queue")
	}

	// Want to process in parallel as far as possible, so spin up a work
	// pool of our handlers
	for i := 0; i < workers; i++ {
		i := i // Local the var to avoid the closure trap (or it would _always_ be the max number)
		go func(work <-chan amqp.Delivery) {
			for m := range msgs {
				if err := handlerFunc(c, m, result, i); err != nil {
					m.Reject(true) // TODO determine whether to requeue or discard
					continue
				}
				m.Ack(false)
			}
		}(msgs)
	}

	c.subscriptions = append(c.subscriptions, consumerTag)

	// TODO
	// - Proper cancellation of workers when shutdown
	// ...
	// ...

	// go func() {

	// 	for d := range msgs {

	// 		// Create a context to enable canceling of the consumer
	// 		ctx, cancel := context.WithCancel(context.Background())

	// 		// Store the context to enable cancelling the worker
	// 		c.Subscriptions = append(c.Subscriptions, cancel)

	// 		o := make(chan error, 1)

	// 		log.Println(d)

	// 		go func() { o <- handlerFunc(d) }()
	// 		select {
	// 		case <-ctx.Done():
	// 			log.Println("Cancelling consumer")
	// 			<-o // Wait for handler func to return
	// 			d.Reject(false)
	// 			log.Println("Cancelled")
	// 		case err := <-o:
	// 			if err != nil {
	// 				// TODO handle error from handler correctly
	// 				log.Println("Error from handler", err)
	// 			}
	// 		}
	// 	}
	// }()

	return nil
}

// PublishToTopic will publish a message to the given topic exchange
func (c *Client) PublishToTopic(exchange, routingKey string, message []byte) error {
	if c.conn == nil {
		return errors.New("unable to publish: no open connection")
	}
	if c.in == nil {
		return errors.New("unable to publish: no open outgoing channel")
	}

	if err := c.out.ExchangeDeclare(
		exchange, // name of the exchange
		"topic",  // type
		true,     // durable
		false,    // delete when complete
		false,    // internal
		false,    // noWait
		nil,      // arguments
	); err != nil {
		return errors.Wrapf(err, "unable to declare '%s' exchange for publish", exchange)
	}

	if err := c.out.Publish(
		exchange,   // exchange
		routingKey, // routing key
		true,       // mandatory TODO
		false,      // immediate TODO
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		},
	); err != nil {
		return errors.Wrap(err, "failed to publish message")
	}

	return nil
}

// Close will close the connection to the broker
func (c *Client) Close() {
	if c.conn != nil {

		// Close existing consumer subscriptions (incoming consumers)
		if c.in != nil {
			for _, s := range c.subscriptions {
				log.Print("Closing subscription for consumer:", s.String()) // TODO remove
				c.in.Cancel(s.String(), false)
				// TODO finishing closing stuff
				//			need to wait until processing workers have finished
				// ...

			}
		}

		// TODO close publisher channel
		// ...

		c.conn.Close()
	}
}
