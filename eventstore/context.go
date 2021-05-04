package eventstore

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// DBCtxKeyType is the type of the key that holds the EventStoreDB in ctx objects
type DBCtxKeyType string

// EventStoreDBKey is the unique key that we use to pass around the DynamoDB in ctx
const EventStoreDBKey DBCtxKeyType = "event_store_db_key"

// GetEventStoreFromCtx retrieves the DB object from ctx
func GetEventStoreFromCtx(ctx context.Context) *dynamodb.DynamoDB {
	return ctx.Value(EventStoreDBKey).(*dynamodb.DynamoDB)
}
