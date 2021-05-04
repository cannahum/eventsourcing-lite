package eventsourcing

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cannahum/eventsourcing-lite/eventstore"
	"github.com/cannahum/eventsourcing-lite/utils/testutils"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

const tableNamePrfx = "todo_es_table_test_"
const hashKey = "todo_id"
const rangeKey = "version"

// Todo is a test object - implements Aggregate and CommandHandler interfaces
type Todo struct {
	ID        string
	Desc      string
	Done      bool
	Version   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Todo Commands
// CreateTodo
type CreateTodo struct {
	CommandModel
	Desc string
}

// MarkDone
type MarkDone struct {
	CommandModel
}

// MarkUndone - consider this an invalid command for these tests
type MarkUndone struct {
	CommandModel
}

// Todo events

// TodoCreated
type TodoCreated struct {
	Model
	Desc string
}

func (t TodoCreated) EventType() (reflect.Type, string) {
	return reflect.TypeOf(t), "TodoCreated"
}

type TodoDone struct {
	Model
}

func (t TodoDone) EventType() (reflect.Type, string) {
	return reflect.TypeOf(t), "TodoDone"
}

// TodoUndone - consider this an invalid event for these tests
type TodoUndone struct {
	Model
	InvalidField string
}

func (t TodoUndone) EventType() (reflect.Type, string) {
	return reflect.TypeOf(t), "TodoUndone"
}

// On implements the Aggregate interface
func (t *Todo) On(e Event) error {
	switch et := e.(type) {
	case *TodoCreated:
		t.Desc = et.Desc
		t.Done = false
		t.CreatedAt = et.EventAt()
	case *TodoDone:
		t.Done = true
	default:
		return fmt.Errorf("unable to handle event %v", et)
	}
	t.ID = e.AggregateID()
	t.Version = e.EventVersion()
	t.UpdatedAt = e.EventAt()
	return nil
}

// Apply implements the CommandHandler interface
func (t *Todo) Apply(ctx context.Context, command Command) ([]Event, error) {
	switch v := command.(type) {
	case *CreateTodo:
		todoCreated := &TodoCreated{
			Model: Model{ID: command.AggregateID(), Version: t.Version + 1, At: time.Now()},
			Desc:  v.Desc,
		}
		return []Event{todoCreated}, nil
	case *MarkDone:
		todoDone := &TodoDone{
			Model: Model{ID: command.AggregateID(), Version: t.Version + 1, At: time.Now()},
		}
		return []Event{todoDone}, nil
	default:
		return nil, fmt.Errorf("unhandled command, %v", v)
	}
}

func TestNew(t *testing.T) {
	tableName := tableNamePrfx + uuid.NewV4().String()
	testutils.CreateTestTable(tableName, hashKey)
	defer testutils.DestroyTestTable(tableName)

	store := eventstore.GetLocalStore()
	r := NewRepository(
		reflect.TypeOf(Todo{}),
		store,
		NewJSONSerializer(TodoCreated{}, TodoDone{}),
	)
	assert.NotNil(t, r)

	dStore := eventstore.GetDynamoDBStore(tableName, hashKey, rangeKey, testutils.GetAWSSessionInstance())
	r = NewRepository(
		reflect.TypeOf(Todo{}),
		dStore,
		NewJSONSerializer(TodoCreated{}, TodoDone{}),
	)
	assert.NotNil(t, r)
}

func TestSave(t *testing.T) {
	tableName := tableNamePrfx + uuid.NewV4().String()
	testutils.CreateTestTable(tableName, hashKey)
	defer testutils.DestroyTestTable(tableName)

	localStore := eventstore.GetLocalStore()
	dynamoStore := eventstore.GetDynamoDBStore(tableName, hashKey, rangeKey,testutils.GetAWSSessionInstance())

	serializer := NewJSONSerializer(TodoCreated{}, TodoDone{})

	localRepo := NewRepository(reflect.TypeOf(Todo{}), localStore, serializer)
	dynamoRepo := NewRepository(reflect.TypeOf(Todo{}), dynamoStore, serializer)
	for _, repo := range []*Repository{localRepo, dynamoRepo} {
		ctx := context.Background()

		t.Run("saving 0 events", func(ct *testing.T) {
			err := repo.Save(ctx)
			assert.Nil(ct, err)
		})

		t.Run("saving a single event", func(ct *testing.T) {
			var id = uuid.NewV4().String()

			todoCreatedEvent := TodoCreated{
				Model: Model{
					ID: id,
				},
				Desc: "Do this",
			}

			err := repo.Save(ctx, todoCreatedEvent)
			assert.Nil(ct, err)

			history, _ := repo.store.Load(ctx, id, 0, 0)
			for _, record := range history {
				e, _ := serializer.UnmarshalEvent(record)
				actual := e.(*TodoCreated)
				assert.Equal(ct, todoCreatedEvent, *actual)
			}
		})

		t.Run("saving many events", func(ct *testing.T) {
			var id = uuid.NewV4().String()

			todoCreatedEvent := TodoCreated{
				Model: Model{
					ID:      id,
					Version: 1,
				},
				Desc: "Do that",
			}

			todoDoneEvent := TodoDone{
				Model: Model{
					ID:      id,
					Version: 2,
				},
			}
			err := repo.Save(ctx, todoCreatedEvent)
			assert.Nil(ct, err)
			err = repo.Save(ctx, todoDoneEvent)
			assert.Nil(ct, err)

			expectedEvents := []Event{
				todoCreatedEvent,
				todoDoneEvent,
			}

			history, _ := repo.store.Load(ctx, id, 0, 0)
			createdEvent, _ := serializer.UnmarshalEvent(history[0])
			actualCreated := createdEvent.(*TodoCreated)
			doneEvent, _ := serializer.UnmarshalEvent(history[1])
			actualDone := doneEvent.(*TodoDone)
			actualEvents := []Event{
				*actualCreated,
				*actualDone,
			}

			assert.Equal(ct, expectedEvents, actualEvents)
		})
	}
}

func TestLoad(t *testing.T) {
	tableName := tableNamePrfx + uuid.NewV4().String()
	testutils.CreateTestTable(tableName, hashKey)
	defer testutils.DestroyTestTable(tableName)

	localStore := eventstore.GetLocalStore()
	dynamoStore := eventstore.GetDynamoDBStore(tableName, hashKey, rangeKey,testutils.GetAWSSessionInstance())
	serializer := NewJSONSerializer(TodoCreated{}, TodoDone{})
	localRepo := NewRepository(reflect.TypeOf(Todo{}), localStore, serializer)
	dynamoRepo := NewRepository(reflect.TypeOf(Todo{}), dynamoStore, serializer)

	ctx := context.Background()
	for _, repo := range []*Repository{localRepo, dynamoRepo} {
		t.Run("non-existent aggregate", func(ct *testing.T) {
			r, err := repo.Load(ctx, "some-id")
			assert.Nil(ct, r)
			assert.NotNil(ct, err)
		})

		t.Run("existent aggregate with multiple events", func(ct *testing.T) {
			var id = uuid.NewV4().String()

			todoCreatedEvent := TodoCreated{
				Model: Model{
					ID:      id,
					Version: 1,
				},
				Desc: "Do that",
			}

			todoDoneEvent := TodoDone{
				Model: Model{
					ID:      id,
					Version: 2,
				},
			}
			_ = repo.Save(ctx, todoCreatedEvent)
			_ = repo.Save(ctx, todoDoneEvent)

			agg, err := repo.Load(ctx, id)
			assert.Nil(ct, err)
			todo := agg.(*Todo)
			expected := Todo{
				ID:      id,
				Desc:    "Do that",
				Done:    true,
				Version: 2,
			}

			// Timestamps can't be controlled. Steal from store:
			history, _ := repo.store.Load(ctx, id, 0, 0)
			createdEvent, _ := serializer.UnmarshalEvent(history[0])
			expected.CreatedAt = createdEvent.EventAt()
			doneEvent, _ := serializer.UnmarshalEvent(history[1])
			expected.UpdatedAt = doneEvent.EventAt()
			assert.Equal(ct, expected, *todo)
		})

		t.Run("serialization of unknown event (error)", func(ct *testing.T) {
			var id = uuid.NewV4().String()

			todoCreatedEvent := TodoCreated{
				Model: Model{
					ID:      id,
					Version: 1,
				},
				Desc: "Do that",
			}

			todoDoneEvent := TodoDone{
				Model: Model{
					ID:      id,
					Version: 2,
				},
			}
			_ = repo.Save(ctx, todoCreatedEvent)
			_ = repo.Save(ctx, todoDoneEvent)

			_, err := repo.Load(ctx, id)
			assert.Nil(ct, err)

			todoUndoneEvent := TodoUndone{
				Model: Model{
					ID:      id,
					Version: 3,
				},
			}
			_ = repo.Save(ctx, todoUndoneEvent)
			_, err = repo.Load(ctx, id)
			assert.NotNil(ct, err)
		})

		t.Run("invalid aggregation (error)", func(ct *testing.T) {
			serializer2 := NewJSONSerializer(TodoCreated{}, TodoDone{}, TodoUndone{})
			r2 := NewRepository(
				reflect.TypeOf(Todo{}),
				repo.store,
				serializer2,
			)

			var id = uuid.NewV4().String()

			todoCreatedEvent := TodoCreated{
				Model: Model{
					ID:      id,
					Version: 1,
				},
				Desc: "Do that",
			}

			todoDoneEvent := TodoDone{
				Model: Model{
					ID:      id,
					Version: 2,
				},
			}
			_ = r2.Save(ctx, todoCreatedEvent)
			_ = r2.Save(ctx, todoDoneEvent)

			_, err := r2.Load(ctx, id)
			assert.Nil(ct, err)

			todoUndoneEvent := TodoUndone{
				Model: Model{
					ID:      id,
					Version: 3,
				},
			}
			_ = r2.Save(ctx, todoUndoneEvent)
			_, err = r2.Load(ctx, id)
			assert.NotNil(ct, err)
		})
	}
}

func TestApply(t *testing.T) {
	tableName := tableNamePrfx + uuid.NewV4().String()
	testutils.CreateTestTable(tableName, hashKey)
	defer testutils.DestroyTestTable(tableName)

	localStore := eventstore.GetLocalStore()
	dynamoStore := eventstore.GetDynamoDBStore(tableName, hashKey, rangeKey, testutils.GetAWSSessionInstance())

	serializer := NewJSONSerializer(TodoCreated{}, TodoDone{})
	localRepo := NewRepository(reflect.TypeOf(Todo{}), localStore, serializer)
	dynamoRepo := NewRepository(reflect.TypeOf(Todo{}), dynamoStore, serializer)

	ctx := context.Background()
	for _, repo := range []*Repository{localRepo, dynamoRepo} {
		t.Run("non-existent aggregate - create new", func(ct *testing.T) {
			var id = uuid.NewV4().String()

			createCommand := &CreateTodo{
				CommandModel: CommandModel{
					ID: id,
				},
				Desc: "Do this",
			}

			err := repo.Apply(ctx, createCommand)
			assert.Nil(ct, err)

			doneCommand := &MarkDone{
				CommandModel: CommandModel{
					ID: id,
				},
			}

			err = repo.Apply(ctx, doneCommand)
			assert.Nil(ct, err)

			agg, _ := repo.Load(ctx, id)
			assert.Nil(ct, err)
			todo := agg.(*Todo)
			expected := Todo{
				ID:      id,
				Desc:    "Do this",
				Done:    true,
				Version: 2,
			}
			// Timestamps can't be controlled. Steal from store:
			history, _ := repo.store.Load(ctx, id, 0, 0)
			createdEvent, _ := serializer.UnmarshalEvent(history[0])
			expected.CreatedAt = createdEvent.EventAt()
			doneEvent, _ := serializer.UnmarshalEvent(history[1])
			expected.UpdatedAt = doneEvent.EventAt()

			assert.Equal(ct, expected, *todo)
		})

		t.Run("nil command (error)", func(ct *testing.T) {
			err := repo.Apply(ctx, nil)
			assert.NotNil(ct, err)
		})

		t.Run("blank aggregateID (error)", func(ct *testing.T) {
			createCommand := &CreateTodo{
				CommandModel: CommandModel{
					ID: "",
				},
				Desc: "Do this",
			}

			err := repo.Apply(ctx, createCommand)
			assert.NotNil(ct, err)
		})

		t.Run("invalid command (error)", func(ct *testing.T) {
			var id = uuid.NewV4().String()

			createCommand := &CreateTodo{
				CommandModel: CommandModel{
					ID: id,
				},
				Desc: "Do this",
			}

			markUndoneCommand := &MarkUndone{
				CommandModel: CommandModel{
					ID: id,
				},
			}

			err := repo.Apply(ctx, createCommand)
			assert.Nil(ct, err)

			err = repo.Apply(ctx, markUndoneCommand)
			assert.NotNil(ct, err)
		})

		t.Run("command does not implement CommandHandler interface (error)", func(ct *testing.T) {
			var id = uuid.NewV4().String()

			createCommand := &CreateTodo{
				CommandModel: CommandModel{
					ID: id,
				},
				Desc: "Do this",
			}

			markUndoneCommand := &MarkUndone{
				CommandModel: CommandModel{
					ID: id,
				},
			}

			err := repo.Apply(ctx, createCommand)
			assert.Nil(ct, err)

			err = repo.Apply(ctx, markUndoneCommand)
			assert.NotNil(ct, err)
		})
	}
}
