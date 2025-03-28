package eventstore

import "context"

// EventStore provides an abstraction for the Repository to save data
type EventStore interface {
	// Save the provided serialized records to the store
	Save(ctx context.Context, aggregateID string, records ...Record) error

	// Load the history of events up to the version specified.
	// When toVersion is 0, all events will be loaded.
	// To start at the beginning, fromVersion should be set to 0
	Load(ctx context.Context, aggregateID string, fromVersion, toVersion int) (History, error)
}
