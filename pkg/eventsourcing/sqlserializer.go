package eventsourcing

import (
	"reflect"

	"github.com/cannahum/eventsourcing-lite/pkg/eventstore"
)

type SQLSerializer struct {
	eventTypes map[string]reflect.Type
}

func (s *SQLSerializer) Bind(eventPrototypes ...Event) {
	for _, event := range eventPrototypes {
		eventType, t := event.EventType()
		s.eventTypes[t] = eventType
	}
}

func (s *SQLSerializer) MarshalEvent(event Event) (eventstore.Record, error) {

}

func (s *SQLSerializer) UnmarshalEvent(record eventstore.Record) (Event, error) {

}
