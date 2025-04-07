package eventstore

import "time"

// Record represents the event in serialized form
type Record struct {
	ID        string
	Version   int
	EventType string
	Data      []byte `dynamodbav:"event_data"`
	Metadata  []byte
	CreatedAt *time.Time
}

// History represents
type History []Record

// Len implements sort.Interface
func (h History) Len() int {
	return len(h)
}

// Swap implements sort.Interface
func (h History) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Less implements sort.Interface
func (h History) Less(i, j int) bool {
	return h[i].Version < h[j].Version
}
