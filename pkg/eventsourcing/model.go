package eventsourcing

import "time"

// Model provides a default implementation of an Event
type Model struct {
	// ID contains the AggregateID
	ID string

	// Version contains the EventVersion
	Version int

	// At contains the EventAt
	At time.Time
}

// AggregateID implements the Event interface
func (m Model) AggregateID() string {
	return m.ID
}

// EventVersion implements the Event interface
func (m Model) EventVersion() int {
	return m.Version
}

// EventAt implements the Event interface
func (m Model) EventAt() time.Time {
	return m.At
}
