package eventsourcing

import "context"

type Observer interface {
	WillObserve(context.Context, Aggregate, Event) bool
	Observe(context.Context, Aggregate, Event) error
	OnObserveFailed(context.Context, error)
}
