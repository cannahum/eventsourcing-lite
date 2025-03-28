package postgresstore

import (
	"testing"
	"time"

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
	// Create test database
	err = dbConf.CreateTestDB()
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)
	err = dbConf.DropTestDB()
	assert.NoError(t, err)
}
