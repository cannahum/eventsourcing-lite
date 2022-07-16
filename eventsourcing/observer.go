package eventsourcing

type Observer interface {
	WillObserve(Aggregate, Event) bool
	Observe(Aggregate, Event) error
	OnObserveFailed(error)
}
