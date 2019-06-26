package receive

import (
	"encoding/json"
	"log"

	"github.com/ONSdigital/census-rm-case-processor-prototype-go/internal/event"
	"github.com/ONSdigital/census-rm-case-processor-prototype-go/pkg/queue"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

// Message represents the structure of an incoming message
type Message struct {
	Foo  string
	Test string
}

// MessageHandler processes incoming messages
func MessageHandler(q *queue.Client, delivery amqp.Delivery, result chan<- interface{}, workerID int) error {
	log.Printf(`event="Received message" worker_id="%d"`, workerID)

	log.Println(string(delivery.Body)) // TODO remove

	incoming := &Message{}
	if err := json.Unmarshal(delivery.Body, incoming); err != nil {
		log.Printf(`event="Failed to unmarshal message" worker_id="%d" error="%v"`, workerID, err)
		return err
	}

	// There should actually be some funky business logic going on here but we're
	// not actually implementing anything here yet ...
	// ...
	// ... SAVE THE CASE STUFF ...
	// ...
	// All we do is create an event payload we want to fire and pass it back to the
	// results channel so that the publishers can pick it up and send it onwards.
	collectionCaseID, err := uuid.NewRandom()
	if err != nil {
		return errors.Wrap(err, "failed to process message: unable to generate uuid")
	}

	exerciseID, err := uuid.NewRandom()
	if err != nil {
		return errors.Wrap(err, "failed to process message: unable to generate uuid")
	}

	e := event.CaseCreated{
		Event: event.Event{
			Type:    "CaseCreated",
			Channel: "rm",
		},
		Payload: event.Payload{
			CollectionCase: event.CollectionCase{
				ID:                   collectionCaseID.String(),
				CaseRef:              "10000000010",
				Survey:               "census",
				CollectionExerciseID: exerciseID.String(),
				SampleUnitRef:        "",
				State:                "actionable",
				ActionableFrom:       "2019-04-01T12:00Z",
				Address: event.Address{
					AddressLine1: "1 Main Street",
					AddressLine2: "Upper Upperingham",
					AddressLine3: "Lower Lowerington",
					AddressType:  "CE",
					ARID:         "XXXXX",
					Country:      "E",
					Longitude:    "-1.229710",
					Latitude:     "50.863849",
					Postcode:     "UP103UP",
					TownName:     "Royal Midtowncastlecesteringshirroutiehampton-upon-Twiddlebottom",
				},
			},
		},
	}

	message, err := json.Marshal(e)
	if err != nil {
		return errors.Wrap(err, "failed to marshal event for publishing")
	}

	// TODO config
	exchange := "myfanout.exchange"
	routingKey := "myfanout.rhqueue"

	// Publish the event
	if err := q.PublishToTopic(exchange, routingKey, message); err != nil {
		return errors.Wrap(err, "failed to publish event")
	}

	log.Println(`event="Published event"`)

	return nil
}
