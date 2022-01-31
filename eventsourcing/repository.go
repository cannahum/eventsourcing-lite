package eventsourcing

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/cannahum/eventsourcing-lite/eventstore"
)

// Repository is an object that knows how to serialize a specific type of entity.
// It also keeps a reference to the store associated with this entity.
type Repository struct {
	prototype  reflect.Type
	store      eventstore.EventStore
	serializer Serializer
	observers  []Observer
}

// Load retrieves the specified aggregate from the underlying store
func (r *Repository) Load(ctx context.Context, aggregateID string) (Aggregate, error) {
	history, err := r.store.Load(ctx, aggregateID, 0, 0)
	if err != nil {
		return nil, err
	}

	entryCount := len(history)
	if entryCount == 0 {
		return nil, fmt.Errorf("unable to find aggregate for id %s", aggregateID)
	}

	aggregate := r.newPrototype()

	for _, record := range history {
		event, serializerErr := r.serializer.UnmarshalEvent(record)
		if serializerErr != nil {
			return nil, serializerErr
		}

		aggregationErr := aggregate.On(event)
		if aggregationErr != nil {
			eventType, _ := event.EventType()
			return nil, fmt.Errorf("aggregate was unable to handle event, %v: %s", eventType, aggregationErr.Error())
		}

	}
	return aggregate, nil
}

// Apply creates new event(s) as a result of a command.
func (r *Repository) Apply(ctx context.Context, command Command) (Aggregate, error) {
	if command == nil {
		return nil, errors.New("command provided to Repository.Apply may not be nil")
	}
	aggregateID := command.AggregateID()
	if aggregateID == "" {
		return nil, errors.New("command provided to Repository.Apply may not contain a blank AggregateID")
	}

	aggregate, err := r.Load(ctx, aggregateID)
	if err != nil {
		aggregate = r.newPrototype()
	}

	h, ok := aggregate.(CommandHandler)
	if !ok {
		return nil, fmt.Errorf("aggregate, %v, does not implement CommandHandler", aggregate)
	}

	events, err := h.Apply(ctx, command)
	if err != nil {
		return nil, err
	}

	err = r.Save(ctx, events...)
	if err != nil {
		return nil, err
	}

	var reloaded Aggregate
	reloaded, err = r.Load(ctx, aggregateID)
	if err != nil {
		return nil, err
	}

	lastEvent := events[len(events)-1]
	for _, observer := range r.observers {
		if observer.WillObserve(reloaded, lastEvent) {
			err = observer.Observe(reloaded, lastEvent)
			if err != nil {
				observer.OnObserveFailed(err)
			}
		}
	}

	return reloaded, nil
}

// Save persists the events into the underlying Store
func (r *Repository) Save(ctx context.Context, events ...Event) error {
	if len(events) == 0 {
		return nil
	}
	aggregateID := events[0].AggregateID()

	history := make(eventstore.History, 0, len(events))
	for _, event := range events {
		record, err := r.serializer.MarshalEvent(event)
		if err != nil {
			return fmt.Errorf("could not marshal json from event %v", event)
		}
		history = append(history, record)
	}
	return r.store.Save(ctx, aggregateID, history...)
}

func (r *Repository) newPrototype() Aggregate {
	rNew := reflect.New(r.prototype)
	rIf := rNew.Interface()
	return rIf.(Aggregate)
}

// NewRepository is a factory function that creates a new Repository object
func NewRepository(
	t reflect.Type,
	store eventstore.EventStore,
	serializer Serializer,
	observers []Observer,
) *Repository {
	return &Repository{t, store, serializer, observers}
}
