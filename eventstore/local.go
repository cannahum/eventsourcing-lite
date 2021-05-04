package eventstore

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

type memoryEventStore struct {
	mux        *sync.Mutex
	eventsByID map[string]History
}

func (m *memoryEventStore) Save(ctx context.Context, aggregateID string, records ...Record) error {
	if _, ok := m.eventsByID[aggregateID]; !ok {
		m.eventsByID[aggregateID] = History{}
	}

	history := append(m.eventsByID[aggregateID], records...)
	sort.Sort(history)
	m.eventsByID[aggregateID] = history

	return nil
}

func (m *memoryEventStore) Load(ctx context.Context, aggregateID string, fromVersion, toVersion int) (History, error) {
	all, ok := m.eventsByID[aggregateID]
	if !ok {
		return nil, fmt.Errorf("no aggregate found with id, %v", aggregateID)
	}

	history := make(History, 0, len(all))
	if len(all) > 0 {
		for _, record := range all {
			if v := record.Version; v >= fromVersion && (toVersion == 0 || v <= toVersion) {
				history = append(history, record)
			}
		}
	}

	return history, nil
}

func GetLocalStore() EventStore {
	return &memoryEventStore{
		mux:        &sync.Mutex{},
		eventsByID: map[string]History{},
	}
}
