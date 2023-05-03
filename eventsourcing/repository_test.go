package eventsourcing

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/cannahum/eventsourcing-lite/eventstore"
	"github.com/cannahum/eventsourcing-lite/utils/testutils"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

const tableNamePrfx = "todo_es_table_test_"
const hashKey = "todo_id"
const rangeKey = "version"

// Create an Amazon DynamoDB client.
var db = dynamodb.NewFromConfig(testutils.GetAWSCfg())

// MyTodo is a test object - implements Aggregate and CommandHandler interfaces
type MyTodo struct {
	ID        string
	Desc      string
	Done      bool
	Version   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

// MyTodo Commands
// CreateTodo
type CreateTodo struct {
	CommandModel
	Desc string
}

// MarkDone
type MarkDone struct {
	CommandModel
}

// MarkUndone
type MarkUndone struct {
	CommandModel
}

// DoUnknown - consider this an invalid command for these tests
type DoUnknown struct {
	CommandModel
}

// MyTodo events

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

type TodoUndone struct {
	Model
}

func (t TodoUndone) EventType() (reflect.Type, string) {
	return reflect.TypeOf(t), "TodoUndone"
}

// TodoUnknown - consider this an invalid event for these tests
type TodoUnknown struct {
	Model
	InvalidField string
}

func (t TodoUnknown) EventType() (reflect.Type, string) {
	return reflect.TypeOf(t), "TodoUnknown"
}

// On implements the Aggregate interface
func (t *MyTodo) On(e Event) error {
	switch et := e.(type) {
	case *TodoCreated:
		t.Desc = et.Desc
		t.Done = false
		t.CreatedAt = et.EventAt()
	case *TodoDone:
		t.Done = true
	case *TodoUndone:
		t.Done = false
	default:
		return fmt.Errorf("unable to handle event %v", et)
	}
	t.ID = e.AggregateID()
	t.Version = e.EventVersion()
	t.UpdatedAt = e.EventAt()
	return nil
}

// Apply implements the CommandHandler interface
func (t *MyTodo) Apply(_ context.Context, command Command) ([]Event, error) {
	switch v := command.(type) {
	case *CreateTodo:
		todoCreated := &TodoCreated{
			Model: Model{ID: command.AggregateID(), Version: t.Version + 1, At: time.Now()},
			Desc:  v.Desc,
		}
		return []Event{todoCreated}, nil
	case *MarkDone:
		if t.Done {
			return nil, fmt.Errorf("MyTodo %s is already done", t.ID)
		}
		todoDone := &TodoDone{
			Model: Model{ID: command.AggregateID(), Version: t.Version + 1, At: time.Now()},
		}
		return []Event{todoDone}, nil
	case *MarkUndone:
		if !t.Done {
			return nil, fmt.Errorf("MyTodo %s is already undone", t.ID)
		}
		todoUndone := &TodoUndone{
			Model: Model{ID: command.AggregateID(), Version: t.Version + 1, At: time.Now()},
		}
		return []Event{todoUndone}, nil
	default:
		return nil, fmt.Errorf("unhandled command, %v", v)
	}
}

func TestNew(t *testing.T) {
	tableName := tableNamePrfx + uuid.NewV4().String()
	testutils.CreateTestTable(tableName, hashKey, db)
	defer testutils.DestroyTestTable(tableName, db)

	store := eventstore.GetLocalStore()
	r := NewRepository(
		reflect.TypeOf(MyTodo{}),
		store,
		NewJSONSerializer(TodoCreated{}, TodoDone{}, TodoUndone{}),
		nil,
	)
	assert.NotNil(t, r)

	dStore := eventstore.GetDynamoDBStore(tableName, hashKey, rangeKey, dynamodb.NewFromConfig(testutils.GetAWSCfg()))
	r = NewRepository(
		reflect.TypeOf(MyTodo{}),
		dStore,
		NewJSONSerializer(TodoCreated{}, TodoDone{}, TodoUndone{}),
		nil,
	)
	assert.NotNil(t, r)
}

func TestSave(t *testing.T) {
	tableName := tableNamePrfx + uuid.NewV4().String()
	testutils.CreateTestTable(tableName, hashKey, db)
	defer testutils.DestroyTestTable(tableName, db)

	localStore := eventstore.GetLocalStore()
	dynamoStore := eventstore.GetDynamoDBStore(tableName, hashKey, rangeKey, dynamodb.NewFromConfig(testutils.GetAWSCfg()))

	serializer := NewJSONSerializer(TodoCreated{}, TodoDone{}, TodoUndone{})

	localRepo := NewRepository(reflect.TypeOf(MyTodo{}), localStore, serializer, nil)
	dynamoRepo := NewRepository(reflect.TypeOf(MyTodo{}), dynamoStore, serializer, nil)
	for _, repo := range []*Repository{localRepo, dynamoRepo} {
		ctx := context.Background()

		t.Run("saving 0 events", func(ct *testing.T) {
			err := repo.Save(ctx)
			assert.NoError(ct, err)
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
			assert.NoError(ct, err)

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
			assert.NoError(ct, err)
			err = repo.Save(ctx, todoDoneEvent)
			assert.NoError(ct, err)

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
	testutils.CreateTestTable(tableName, hashKey, db)
	defer testutils.DestroyTestTable(tableName, db)

	localStore := eventstore.GetLocalStore()
	dynamoStore := eventstore.GetDynamoDBStore(tableName, hashKey, rangeKey, dynamodb.NewFromConfig(testutils.GetAWSCfg()))
	serializer := NewJSONSerializer(TodoCreated{}, TodoDone{}, TodoUndone{})
	localRepo := NewRepository(reflect.TypeOf(MyTodo{}), localStore, serializer, nil)
	dynamoRepo := NewRepository(reflect.TypeOf(MyTodo{}), dynamoStore, serializer, nil)

	ctx := context.Background()
	for _, repo := range []*Repository{localRepo, dynamoRepo} {
		t.Run("non-existent aggregate", func(ct *testing.T) {
			r, err := repo.Load(ctx, "some-id")
			assert.Nil(ct, r)
			assert.Error(ct, err)
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
			assert.NoError(ct, err)
			todo := agg.(*MyTodo)
			expected := MyTodo{
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
			assert.NoError(ct, err)

			todoUndoneEvent := TodoUnknown{
				Model: Model{
					ID:      id,
					Version: 3,
				},
			}
			_ = repo.Save(ctx, todoUndoneEvent)
			_, err = repo.Load(ctx, id)
			assert.Error(ct, err)
		})

		t.Run("invalid aggregation (error)", func(ct *testing.T) {
			serializer2 := NewJSONSerializer(TodoCreated{}, TodoDone{}, TodoUnknown{})
			r2 := NewRepository(
				reflect.TypeOf(MyTodo{}),
				repo.store,
				serializer2,
				nil,
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
			assert.NoError(ct, err)

			todoUndoneEvent := TodoUnknown{
				Model: Model{
					ID:      id,
					Version: 3,
				},
			}
			_ = r2.Save(ctx, todoUndoneEvent)
			_, err = r2.Load(ctx, id)
			assert.Error(ct, err)
		})
	}
}

func TestApply(t *testing.T) {
	tableName := tableNamePrfx + uuid.NewV4().String()
	testutils.CreateTestTable(tableName, hashKey, db)
	defer testutils.DestroyTestTable(tableName, db)

	localStore := eventstore.GetLocalStore()
	dynamoStore := eventstore.GetDynamoDBStore(tableName, hashKey, rangeKey, dynamodb.NewFromConfig(testutils.GetAWSCfg()))

	serializer := NewJSONSerializer(TodoCreated{}, TodoDone{}, TodoUndone{})
	localRepo := NewRepository(reflect.TypeOf(MyTodo{}), localStore, serializer, nil)
	dynamoRepo := NewRepository(reflect.TypeOf(MyTodo{}), dynamoStore, serializer, nil)

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

			r, err := repo.Apply(ctx, createCommand)
			returned := r.(*MyTodo)
			assert.NoError(ct, err)
			assert.NotNil(ct, returned)
			assert.Equal(ct, MyTodo{
				ID:        id,
				Desc:      "Do this",
				Done:      false,
				Version:   1,
				CreatedAt: returned.CreatedAt,
				UpdatedAt: returned.UpdatedAt,
			}, *returned)

			doneCommand := &MarkDone{
				CommandModel: CommandModel{
					ID: id,
				},
			}

			_, err = repo.Apply(ctx, doneCommand)
			assert.NoError(ct, err)

			agg, _ := repo.Load(ctx, id)
			assert.NoError(ct, err)
			todo := agg.(*MyTodo)
			expected := MyTodo{
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
			returned, err := repo.Apply(ctx, nil)
			assert.Error(ct, err)
			assert.Nil(ct, returned)
		})

		t.Run("blank aggregateID (error)", func(ct *testing.T) {
			createCommand := &CreateTodo{
				CommandModel: CommandModel{
					ID: "",
				},
				Desc: "Do this",
			}

			returned, err := repo.Apply(ctx, createCommand)
			assert.Error(ct, err)
			assert.Nil(ct, returned)
		})

		t.Run("invalid command (error)", func(ct *testing.T) {
			var id = uuid.NewV4().String()

			createCommand := &CreateTodo{
				CommandModel: CommandModel{
					ID: id,
				},
				Desc: "Do this",
			}

			markUndoneCommand := &DoUnknown{
				CommandModel: CommandModel{
					ID: id,
				},
			}

			returned, err := repo.Apply(ctx, createCommand)
			assert.NoError(ct, err)
			assert.NotNil(ct, returned)

			returned, err = repo.Apply(ctx, markUndoneCommand)
			assert.Error(ct, err)
			assert.Nil(ct, returned)
		})

		t.Run("command does not implement CommandHandler interface (error)", func(ct *testing.T) {
			var id = uuid.NewV4().String()

			createCommand := &CreateTodo{
				CommandModel: CommandModel{
					ID: id,
				},
				Desc: "Do this",
			}

			markUndoneCommand := &DoUnknown{
				CommandModel: CommandModel{
					ID: id,
				},
			}

			returned, err := repo.Apply(ctx, createCommand)
			assert.NoError(ct, err)
			assert.NotNil(ct, returned)

			returned, err = repo.Apply(ctx, markUndoneCommand)
			assert.Error(ct, err)
			assert.Nil(ct, returned)
		})
	}
}
