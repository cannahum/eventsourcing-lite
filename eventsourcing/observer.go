package eventsourcing

type Observer interface {
	WillObserve(aggregate Aggregate, event Event) bool
	Observe(Aggregate, Event) error
	OnObserveFailed(error)
}
