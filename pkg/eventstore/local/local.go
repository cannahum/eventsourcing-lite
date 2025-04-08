package localstore

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/cannahum/eventsourcing-lite/pkg/eventstore"
)

type memoryEventStore struct {
	mux        *sync.Mutex
	eventsByID map[string]eventstore.History
}

func (m *memoryEventStore) Save(_ context.Context, aggregateID string, records ...eventstore.Record) error {
	if _, ok := m.eventsByID[aggregateID]; !ok {
		m.eventsByID[aggregateID] = eventstore.History{}
	}

	m.eventsByID[aggregateID] = append(m.eventsByID[aggregateID], records...)
	sort.Sort(m.eventsByID[aggregateID])

	return nil
}

func (m *memoryEventStore) Load(_ context.Context, aggregateID string, fromVersion, toVersion int) (eventstore.History, error) {
	all, ok := m.eventsByID[aggregateID]
	if !ok {
		return nil, fmt.Errorf("no aggregate found with id, %v", aggregateID)
	}

	history := make(eventstore.History, 0, len(all))
	if len(all) > 0 {
		for _, record := range all {
			if v := record.Version; v >= fromVersion && (toVersion == 0 || v <= toVersion) {
				history = append(history, record)
			}
		}
	}

	return history, nil
}

// GetLocalStore returns an EventStore in memory - good for tests!
func GetLocalStore() eventstore.EventStore {
	return &memoryEventStore{
		mux:        &sync.Mutex{},
		eventsByID: map[string]eventstore.History{},
	}
}
