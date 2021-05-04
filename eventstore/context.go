package eventstore

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DBCtxKeyType string

const EventStoreDBKey DBCtxKeyType = "event_store_db_key"

func GetEventStoreFromCtx(ctx context.Context) *dynamodb.DynamoDB {
	return ctx.Value(EventStoreDBKey).(*dynamodb.DynamoDB)
}
