package eventsourcing

import (
	"github.com/cannahum/eventsourcing-lite/eventstore"
)

// Serializer converts between Events and Records
type Serializer interface {
	// MarshalEvent converts an Event to a Record
	MarshalEvent(event Event) (eventstore.Record, error)

	// UnmarshalEvent converts an Event backed into a Record
	UnmarshalEvent(record eventstore.Record) (Event, error)
}
