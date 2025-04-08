package postgresstore

import (
	"context"
	"testing"

	"github.com/cannahum/eventsourcing-lite/pkg/eventstore"
	"github.com/cannahum/eventsourcing-lite/utils/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestDBCreation(t *testing.T) {
	conf := testutils.NewConfig()
	dbConf, err := testutils.NewPostgres(conf.Postgres)
	assert.NoError(t, err)
	dbName := "test_dbcreation_" + uuid.NewString()[0:6]
	dbConf.DBName = dbName
	// Create and destroy (deferred) test database
	err = dbConf.CreateTestDB()
	assert.NoError(t, err)
	err = RunMigrations(dbConf.DBConn, dbName)
	assert.NoError(t, err)
	defer func(dt *testing.T) {
		err = dbConf.DropTestDB()
		assert.NoError(t, err)
	}(t)

	s := NewPostgresEventStore(dbConf.DBConn)
	ctx := context.Background()

	t.Run("test Load function", func(tt *testing.T) {
		result, err := s.Load(ctx, "agg-id", 0, 0)
		assert.NoError(tt, err)
		assert.Equal(t, eventstore.History{}, result)
	})

	t.Run("test Save function", func(tt *testing.T) {
		aggID := uuid.NewString()
		// Create a few records
		records := []eventstore.Record{
			{
				Version: 1,
				Data:    []byte(`{ "data": "data 1" }`),
			},
		}
		competingRecords := []eventstore.Record{
			{
				Version: 1,
				Data:    []byte(`{ "data": "data 2" }`),
			},
		}
		// ACT
		err0 := s.Save(ctx, aggID, records...)
		err1 := s.Save(ctx, aggID, records...)
		err2 := s.Save(ctx, aggID, competingRecords...)
		competingRecords = append(competingRecords, eventstore.Record{
			Version: 2,
			Data:    []byte(`{ "data": "data 2" }`),
		})
		err3 := s.Save(ctx, aggID, competingRecords...)
		// ASSERT
		assert.NoError(tt, err0)
		assert.Error(tt, err0)
		assert.Error(tt, err1)
		assert.Error(tt, err2)
		assert.Error(tt, err3)
		reloaded, err := s.Load(ctx, aggID, 0, 0)
		assert.NoError(tt, err)
		assert.Equal(tt, eventstore.History(records), reloaded)
	})
}
