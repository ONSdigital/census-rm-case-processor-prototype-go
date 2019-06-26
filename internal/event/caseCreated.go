package event

type (
	// CaseCreated is an event for a case creation
	CaseCreated struct {
		Event   Event
		Payload Payload
	}

	// Event defines an event
	Event struct {
		Type          string
		Channel       string
		DateTime      string
		TransactionID string
	}

	// Payload defines a message payload
	Payload struct {
		CollectionCase CollectionCase
	}

	// CollectionCase is a collection case
	CollectionCase struct {
		ID                   string
		CaseRef              string
		Survey               string
		CollectionExerciseID string
		SampleUnitRef        string
		Address              Address
		State                string
		ActionableFrom       string
	}

	// Address is a street address
	Address struct {
		AddressLine1 string
		AddressLine2 string
		AddressLine3 string
		TownName     string
		Postcode     string
		Country      string
		Latitude     string
		Longitude    string
		UPRN         string
		ARID         string
		AddressType  string
		EstabType    string
	}
)

// PublishCaseCreated publishes a case created event to the event fanout queue
func PublishCaseCreated() {
	// TODO
}
