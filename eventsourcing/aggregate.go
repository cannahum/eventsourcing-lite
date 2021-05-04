package eventsourcing

// Aggregate stands for event-sourced model.
type Aggregate interface {
	On(event Event) error
}
