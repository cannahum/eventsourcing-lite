package eventsourcing

import (
	"reflect"
	"time"
)

type Event interface {
	// AggregateID returns the id of the aggregate referenced by the event
	AggregateID() string

	// EventVersion contains the version number of this event
	EventVersion() int

	// EventAt indicates when the event occurred
	EventAt() time.Time

	// EventType returns the type and unique name of event
	EventType() (reflect.Type, string)
}
