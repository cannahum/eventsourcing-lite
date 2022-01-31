package eventsourcing

import (
	"context"
	"errors"
	"fmt"
	"github.com/cannahum/eventsourcing-lite/eventstore"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

type MyTodoDefaultObserver struct {
	hasBeenObserved bool
	howManyTimes    int
}

func (o *MyTodoDefaultObserver) WillObserve(_ Aggregate, _ Event) bool {
	return true
}

func (o *MyTodoDefaultObserver) Observe(_ Aggregate, _ Event) error {
	o.hasBeenObserved = true
	o.howManyTimes++
	return nil
}

func (o *MyTodoDefaultObserver) OnObserveFailed(e error) {
	fmt.Println(e)
}

type MyTodoCreatedObserver struct {
	hasBeenObserved bool
	howManyTimes    int
}

func (o *MyTodoCreatedObserver) WillObserve(_ Aggregate, e Event) bool {
	switch e.(type) {
	case *TodoCreated:
		return true
	default:
		return false
	}
}

func (o *MyTodoCreatedObserver) Observe(a Aggregate, _ Event) error {
	_ = a.(*MyTodo)
	o.hasBeenObserved = true
	o.howManyTimes++
	return nil
}

func (o *MyTodoCreatedObserver) OnObserveFailed(e error) {
	fmt.Println(e)
}

type MyTodoDoneObserver struct {
	isDone       bool
	howManyTimes int
}

func (o *MyTodoDoneObserver) WillObserve(_ Aggregate, e Event) bool {
	switch e.(type) {
	case *TodoDone, *TodoUndone:
		return true
	default:
		return false
	}
}

func (o *MyTodoDoneObserver) Observe(a Aggregate, e Event) error {
	td := a.(*MyTodo)
	switch e.(type) {
	case *TodoDone:
		if o.isDone {
			return errors.New("observer is already DONE")
		}
		if !td.Done {
			return errors.New("aggregate is UNDONE")
		}
		o.isDone = true
	case *TodoUndone:
		if !o.isDone {
			return errors.New("observer is already UNDONE")
		}
		if td.Done {
			return errors.New("aggregate is DONE")
		}
		o.isDone = false
	}
	o.howManyTimes++
	return nil
}

func (o *MyTodoDoneObserver) OnObserveFailed(e error) {
	fmt.Println(e)
}

type MyTodoFailingObserver struct {
	howManyTimes int
	hasFailed    bool
}

func (o *MyTodoFailingObserver) WillObserve(_ Aggregate, _ Event) bool {
	return true
}

func (o *MyTodoFailingObserver) Observe(_ Aggregate, _ Event) error {
	if o.howManyTimes == 3 {
		return errors.New("this is now failing")
	}
	o.howManyTimes++
	return nil
}

func (o *MyTodoFailingObserver) OnObserveFailed(e error) {
	fmt.Println(e)
	o.hasFailed = true
}

func TestApplyObservers(t *testing.T) {
	localStore := eventstore.GetLocalStore()
	serializer := NewJSONSerializer(TodoCreated{}, TodoDone{}, TodoUndone{})

	defaultObserver := MyTodoDefaultObserver{}
	createdObserver := MyTodoCreatedObserver{}
	doneObserver := MyTodoDoneObserver{}
	failingObserver := MyTodoFailingObserver{}

	repo := NewRepository(reflect.TypeOf(MyTodo{}), localStore, serializer, []Observer{
		&defaultObserver,
		&createdObserver,
		&doneObserver,
		&failingObserver,
	})

	ctx := context.Background()
	_, err := repo.Apply(ctx, nil)
	assert.Error(t, err)

	// If Apply results in error, observers shouldn't run
	assert.False(t, defaultObserver.hasBeenObserved)
	assert.False(t, createdObserver.hasBeenObserved)
	assert.Equal(t, 0, doneObserver.howManyTimes)

	// Create Room
	var id = uuid.NewV4().String()

	createCommand := &CreateTodo{
		CommandModel: CommandModel{
			ID: id,
		},
		Desc: "Do this and observe",
	}

	_, err = repo.Apply(ctx, createCommand)
	assert.NoError(t, err)
	assert.Equal(t, 1, defaultObserver.howManyTimes)
	assert.True(t, createdObserver.hasBeenObserved)
	assert.Equal(t, 1, createdObserver.howManyTimes)
	assert.Equal(t, 0, doneObserver.howManyTimes)
	assert.False(t, failingObserver.hasFailed)

	doneCommand := &MarkDone{CommandModel{id}}
	undoneCommand := &MarkUndone{CommandModel{id}}

	_, err = repo.Apply(ctx, doneCommand)
	assert.NoError(t, err)
	assert.Equal(t, 2, defaultObserver.howManyTimes)
	assert.Equal(t, 1, createdObserver.howManyTimes)
	assert.True(t, doneObserver.isDone)
	assert.Equal(t, 1, doneObserver.howManyTimes)
	assert.False(t, failingObserver.hasFailed)

	// Done Command one more time (should result error)
	_, err = repo.Apply(ctx, doneCommand)
	assert.Error(t, err)
	assert.Equal(t, 2, defaultObserver.howManyTimes)
	assert.Equal(t, 1, createdObserver.howManyTimes)
	assert.True(t, doneObserver.isDone)
	assert.Equal(t, 1, doneObserver.howManyTimes)
	assert.False(t, failingObserver.hasFailed)

	_, err = repo.Apply(ctx, undoneCommand)
	assert.NoError(t, err)
	assert.Equal(t, 3, defaultObserver.howManyTimes)
	assert.Equal(t, 1, createdObserver.howManyTimes)
	assert.False(t, doneObserver.isDone)
	assert.Equal(t, 2, doneObserver.howManyTimes)
	assert.False(t, failingObserver.hasFailed)

	_, err = repo.Apply(ctx, doneCommand)
	assert.NoError(t, err)

	assert.Equal(t, 4, defaultObserver.howManyTimes)
	assert.Equal(t, 1, createdObserver.howManyTimes)
	assert.True(t, doneObserver.isDone)
	assert.Equal(t, 3, doneObserver.howManyTimes)
	assert.True(t, failingObserver.hasFailed)
}
