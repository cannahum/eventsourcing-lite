package eventstore

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"testing"

	"github.com/cannahum/eventsourcing-lite/utils/testutils"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

const hashKey = "todo_id"
const rangeKey = "version"

func TestGetDynamoDBStore(t *testing.T) {
	db := dynamodb.New(testutils.GetAWSSessionInstance())
	tableName := "todo_es_table_test_" + uuid.NewV4().String()

	// Create a fake table
	testutils.CreateTestTable(tableName, hashKey, db)
	defer testutils.DestroyTestTable(tableName, db)

	s := GetDynamoDBStore(tableName, hashKey, rangeKey, testutils.GetAWSSessionInstance())
	ctx := context.Background()

	t.Run("test Load function", func(ct *testing.T) {
		result, err := s.Load(ctx, "agg-id", 0, 0)
		assert.Nil(t, err)
		assert.Equal(t, History{}, result)
	})

	t.Run("test Save function", func(ct *testing.T) {
		aggID := uuid.NewV4().String()
		// Create a few records
		records := []Record{
			{
				Version: 1,
				Data:    []byte("first data 1"),
			},
		}

		err := s.Save(ctx, aggID, records...)
		assert.Nil(ct, err)
	})

	t.Run("test Save existing event (error)", func(ct *testing.T) {
		aggID := uuid.NewV4().String()
		// Create a few records
		records := []Record{
			{
				Version: 1,
				Data:    []byte("first data 1"),
			},
		}

		competingRecords := []Record{
			{
				Version: 1,
				Data:    []byte("some other data"),
			},
		}

		err := s.Save(ctx, aggID, records...)
		assert.Nil(ct, err)

		// Saving the same thing should not return an error
		// It's as if this succeeded.
		err2 := s.Save(ctx, aggID, records...)
		assert.Nil(ct, err2)

		err3 := s.Save(ctx, aggID, competingRecords...)
		assert.NotNil(ct, err3)

		competingRecords = append(competingRecords, Record{
			Version: 2,
			Data:    []byte("new data"),
		})
		err4 := s.Save(ctx, aggID, competingRecords...)
		assert.Equal(ct, err4.Error(), ConditionalCheckFailed)
	})

	t.Run("test Save -> Load", func(ct *testing.T) {
		aggID := uuid.NewV4().String()
		// Create a few records
		records := []Record{
			{
				Version: 1,
				Data:    []byte("first data 1"),
			},
		}

		_ = s.Save(ctx, aggID, records...)

		readVersion, err := s.Load(ctx, aggID, 0, 0)
		assert.Nil(ct, err)
		assert.Equal(ct, History(records), readVersion)
	})

	t.Run("test Save nothing", func(ct *testing.T) {
		aggID := uuid.NewV4().String()
		// Create a few records
		var records []Record

		err := s.Save(ctx, aggID, records...)
		assert.Nil(ct, err)

		result, _ := s.Load(ctx, aggID, 0, 0)
		assert.Equal(t, History{}, result)
	})

	t.Run("test Save - over 25 items not permitted (error)", func(ct *testing.T) {
		aggID := uuid.NewV4().String()
		// Create a few records
		var records []Record

		for i := 0; i < 30; i++ {
			records = append(records, Record{
				Version: i + 1,
				Data:    []byte("some data"),
			})
		}

		err := s.Save(ctx, aggID, records...)
		assert.NotNil(ct, err)
	})

	t.Run("test Save - items with duplicate version (error)", func(ct *testing.T) {
		aggID := uuid.NewV4().String()
		// Create a few records
		records := []Record{
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
		aggID := uuid.NewV4().String()
		// Create a few records
		records := []Record{
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
		assert.Equal(ct, History(records), readVersion)
	})

	t.Run("test Save -> Load partial items", func(ct *testing.T) {
		aggID := uuid.NewV4().String()
		// Create a few records
		records := []Record{
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
		assert.Equal(ct, History(records), readVersion)

		secondToFourth, err := s.Load(ctx, aggID, 2, 4)
		assert.Nil(ct, err)
		assert.Equal(ct, History(records[1:4]), secondToFourth)

		thirdOnwards, err := s.Load(ctx, aggID, 3, 0)
		assert.Nil(ct, err)
		assert.Equal(ct, History(records[2:]), thirdOnwards)
	})
}
