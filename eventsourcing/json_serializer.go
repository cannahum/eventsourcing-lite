package eventsourcing

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/cannahum/eventsourcing-lite/eventstore"
)

type jsonEvent struct {
	Type string          `json:"t"`
	Data json.RawMessage `json:"d"`
}

// JSONSerializer provides a simple serializer implementation
type JSONSerializer struct {
	eventTypes map[string]reflect.Type
}

// Bind registers the specified events with the serializer; may be called more than once
func (j *JSONSerializer) Bind(events ...Event) {
	for _, event := range events {
		eventType, t := event.EventType()
		j.eventTypes[t] = eventType
	}
}

// MarshalEvent converts an event into its persistent type, Record
func (j *JSONSerializer) MarshalEvent(ev Event) (eventstore.Record, error) {
	_, eventType := ev.EventType()

	data, err := json.Marshal(ev)
	if err != nil {
		return eventstore.Record{}, err
	}

	recordData, err2 := json.Marshal(jsonEvent{
		Type: eventType,
		Data: json.RawMessage(data),
	})
	if err2 != nil {
		return eventstore.Record{}, fmt.Errorf("unable to encode event")
	}

	return eventstore.Record{
		Version: ev.EventVersion(),
		Data:    recordData,
	}, nil
}

// UnmarshalEvent converts the persistent type, Record, into an Event instance
func (j *JSONSerializer) UnmarshalEvent(record eventstore.Record) (Event, error) {
	wrapper := jsonEvent{}
	err := json.Unmarshal(record.Data, &wrapper)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal event")
	}

	t, ok := j.eventTypes[wrapper.Type]
	if !ok {
		return nil, fmt.Errorf("unbound event type, %v", wrapper.Type)
	}

	v := reflect.New(t).Interface()
	err = json.Unmarshal(wrapper.Data, v)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal event data into %#v", v)
	}

	return v.(Event), nil
}

// NewJSONSerializer constructs a new JSONSerializer and populates it with the specified events.
// Bind may be subsequently called to add more events.
func NewJSONSerializer(events ...Event) *JSONSerializer {
	serializer := &JSONSerializer{
		eventTypes: map[string]reflect.Type{},
	}
	serializer.Bind(events...)

	return serializer
}
