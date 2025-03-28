package dynamodbstore

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/cannahum/eventsourcing-lite/pkg/eventstore"
	"github.com/cannahum/eventsourcing-lite/utils/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

const hashKey = "todo_id"
const rangeKey = "version"

var conf = testutils.NewConfig()

func TestGetDynamoDBStore(t *testing.T) {
	db := dynamodb.NewFromConfig(conf.GetAWSCfg())
	tableName := "todo_es_table_test_" + uuid.NewString()

	// Create a fake table
	testutils.CreateTestTable(tableName, hashKey, db)
	defer testutils.DestroyTestTable(tableName, db)

	s := GetDynamoDBStore(tableName, hashKey, rangeKey, db)
	ctx := context.Background()

	t.Run("test Load function", func(ct *testing.T) {
		result, err := s.Load(ctx, "agg-id", 0, 0)
		assert.Nil(t, err)
		assert.Equal(t, eventstore.History{}, result)
	})

	t.Run("test Save function", func(ct *testing.T) {
		aggID := uuid.NewString()
		// Create a few records
		records := []eventstore.Record{
			{
				Version: 1,
				Data:    []byte("first data 1"),
			},
		}

		err := s.Save(ctx, aggID, records...)
		assert.Nil(ct, err)
	})

	t.Run("test Save existing event (error)", func(ct *testing.T) {
		aggID := uuid.NewString()
		// Create a few records
		records := []eventstore.Record{
			{
				Version: 1,
				Data:    []byte("first data 1"),
			},
		}

		competingRecords := []eventstore.Record{
			{
				Version: 1,
				Data:    []byte("some other data"),
			},
		}

		err := s.Save(ctx, aggID, records...)
		assert.Nil(ct, err)

		err2 := s.Save(ctx, aggID, records...)
		assert.NotNil(ct, err2)

		err3 := s.Save(ctx, aggID, competingRecords...)
		assert.NotNil(ct, err3)

		competingRecords = append(competingRecords, eventstore.Record{
			Version: 2,
			Data:    []byte("new data"),
		})
		err4 := s.Save(ctx, aggID, competingRecords...)
		assert.NotNil(ct, err4)
	})

	t.Run("test Save -> Load", func(ct *testing.T) {
		aggID := uuid.NewString()
		// Create a few records
		records := []eventstore.Record{
			{
				Version: 1,
				Data:    []byte("first data 1"),
			},
		}

		_ = s.Save(ctx, aggID, records...)

		readVersion, err := s.Load(ctx, aggID, 0, 0)
		assert.Nil(ct, err)
		assert.Equal(ct, eventstore.History(records), readVersion)
	})

	t.Run("test Save nothing", func(ct *testing.T) {
		aggID := uuid.NewString()
		// Create a few records
		var records []eventstore.Record

		err := s.Save(ctx, aggID, records...)
		assert.Nil(ct, err)

		result, _ := s.Load(ctx, aggID, 0, 0)
		assert.Equal(t, eventstore.History{}, result)
	})

	t.Run("test Save - over 25 items not permitted (error)", func(ct *testing.T) {
		aggID := uuid.NewString()
		// Create a few records
		var records []eventstore.Record

		for i := 0; i < 30; i++ {
			records = append(records, eventstore.Record{
				Version: i + 1,
				Data:    []byte("some data"),
			})
		}

		err := s.Save(ctx, aggID, records...)
		assert.NotNil(ct, err)
	})

	t.Run("test Save - items with duplicate version (error)", func(ct *testing.T) {
		aggID := uuid.NewString()
		// Create a few records
		records := []eventstore.Record{
			{
				Version: 1,
				Data:    []byte("first data"),
			},
			{
				Version: 1, // Same version as above
				Data:    []byte("second data"),
			},
		}

		err := s.Save(ctx, aggID, records...)
		assert.NotNil(ct, err)
	})

	t.Run("test Save -> Load multiple items", func(ct *testing.T) {
		aggID := uuid.NewString()
		// Create a few records
		records := []eventstore.Record{
			{
				Version: 1,
				Data:    []byte("first data"),
			},
			{
				Version: 2,
				Data:    []byte("second data"),
			},
			{
				Version: 3,
				Data:    []byte("third data"),
			},
		}

		_ = s.Save(ctx, aggID, records...)

		readVersion, err := s.Load(ctx, aggID, 0, 0)
		assert.Nil(ct, err)
		assert.Equal(ct, eventstore.History(records), readVersion)
	})

	t.Run("test Save -> Load partial items", func(ct *testing.T) {
		aggID := uuid.NewString()
		// Create a few records
		records := []eventstore.Record{
			{
				Version: 1,
				Data:    []byte("first data"),
			},
			{
				Version: 2,
				Data:    []byte("second data"),
			},
			{
				Version: 3,
				Data:    []byte("third data"),
			},
			{
				Version: 4,
				Data:    []byte("fourth data"),
			},
			{
				Version: 5,
				Data:    []byte("fifth data"),
			},
		}

		_ = s.Save(ctx, aggID, records...)

		readVersion, err := s.Load(ctx, aggID, 0, 0)
		assert.Nil(ct, err)
		assert.Equal(ct, eventstore.History(records), readVersion)

		secondToFourth, err := s.Load(ctx, aggID, 2, 4)
		assert.Nil(ct, err)
		assert.Equal(ct, eventstore.History(records[1:4]), secondToFourth)

		thirdOnwards, err := s.Load(ctx, aggID, 3, 0)
		assert.Nil(ct, err)
		assert.Equal(ct, eventstore.History(records[2:]), thirdOnwards)
	})
}
