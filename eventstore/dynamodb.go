package eventstore

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"reflect"
	"sort"
	"strconv"
)

// ConditionalCheckFailed is const for DB error
const ConditionalCheckFailed = "ConditionalCheckFailed"

// DynamoDBStore is an event store implementation using DynamoDB
// This is an object that represents metadata on this table
type DynamoDBStore struct {
	tableName string
	hashKey   string
	rangeKey  string
	api       *dynamodb.Client
}

// GetDynamoDBStore returns a new DB store instance
func GetDynamoDBStore(tableName, partitionKey, rangeKey string, db *dynamodb.Client) *DynamoDBStore {
	store := DynamoDBStore{
		tableName: tableName,
		hashKey:   partitionKey,
		rangeKey:  rangeKey,
	}
	store.api = db
	return &store
}

// Load implements the EventStore interface and reads all events for a specific aggregateID
func (s *DynamoDBStore) Load(ctx context.Context, aggregateID string, fromVersion, toVersion int) (History, error) {
	input := &dynamodb.QueryInput{
		TableName:      aws.String(s.tableName),
		Select:         types.SelectAllAttributes,
		ConsistentRead: aws.Bool(true),
		ExpressionAttributeNames: map[string]string{
			"#key": s.hashKey,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":key": &types.AttributeValueMemberS{Value: aggregateID},
		},
	}

	if toVersion > 0 {
		input.KeyConditionExpression = aws.String("#key = :key AND #partition BETWEEN :from AND :to")
		input.ExpressionAttributeNames["#partition"] = s.rangeKey
		input.ExpressionAttributeValues[":from"] = &types.AttributeValueMemberN{Value: strconv.Itoa(fromVersion)}
		input.ExpressionAttributeValues[":to"] = &types.AttributeValueMemberN{Value: strconv.Itoa(toVersion)}
	} else if fromVersion > 0 {
		input.KeyConditionExpression = aws.String("#key = :key AND #partition >= :from")
		input.ExpressionAttributeNames["#partition"] = s.rangeKey
		input.ExpressionAttributeValues[":from"] = &types.AttributeValueMemberN{Value: strconv.Itoa(fromVersion)}
	} else {
		input.KeyConditionExpression = aws.String("#key = :key")
	}

	history := make(History, 0, toVersion)
	out, err := s.api.Query(ctx, input)
	if err != nil {
		return nil, err
	}
	var records []Record
	if err = attributevalue.UnmarshalListOfMaps(out.Items, &records); err != nil {
		return nil, err
	}
	return append(history, records...), nil
}

// Save implements the EventStore interface and stores an event in DynamoDB
func (s *DynamoDBStore) Save(ctx context.Context, aggregateID string, records ...Record) error {
	if len(records) == 0 {
		return nil
	}

	if len(records) > 25 {
		return errors.New("not implemented: can't save 25 events at a time")
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].Version < records[j].Version
	})

	for i := len(records) - 2; i >= 0; i-- {
		if records[i].Version == records[i+1].Version {
			return errors.New("duplicate version detected")
		}
	}

	input := &dynamodb.TransactWriteItemsInput{}

	for _, e := range records {
		keyClause := map[string]types.AttributeValue{}
		keyClause[s.hashKey] = &types.AttributeValueMemberS{Value: aggregateID}
		keyClause[s.rangeKey] = &types.AttributeValueMemberN{Value: strconv.Itoa(e.Version)}

		twi := types.TransactWriteItem{
			Update: &types.Update{
				TableName: aws.String(s.tableName),
				Key:       keyClause,
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":r": &types.AttributeValueMemberB{Value: e.Data},
				},
				ConditionExpression: aws.String(
					fmt.Sprintf("attribute_not_exists(%s)", s.rangeKey),
				),
				UpdateExpression: aws.String("set event_data = :r"),
			},
		}
		input.TransactItems = append(input.TransactItems, twi)
	}

	_, err := s.api.TransactWriteItems(ctx, input)
	if err != nil {
		if txnCanceled, ok := err.(*types.TransactionCanceledException); ok {
			// awsError.Code().
			// if len() {}
			// if v.Code() == ConditionalCheckFailed {
			// 	return s.ensureIdempotent(ctx, aggregateID, records...)
			// }
			for _, reason := range txnCanceled.CancellationReasons {
				if *reason.Code == ConditionalCheckFailed {
					return s.ensureIdempotent(ctx, aggregateID, records...)
				}
			}
		}
		return err
	}
	return nil
}

func (s *DynamoDBStore) ensureIdempotent(ctx context.Context, aggregateID string, records ...Record) error {
	if len(records) == 0 {
		return nil
	}

	version := records[len(records)-1].Version
	history, err := s.Load(ctx, aggregateID, 0, version)
	if err != nil {
		return err
	}
	if len(history) < len(records) {
		return errors.New(ConditionalCheckFailed)
	}

	recent := history[len(history)-len(records):]
	fmt.Printf("equality: %t\n", reflect.DeepEqual(recent[0].Version, records[0].Version))
	fmt.Printf("equality: %t\n", reflect.DeepEqual(recent[0].Data, records[0].Data))
	if !reflect.DeepEqual(recent, History(records)) {
		return errors.New(ConditionalCheckFailed)
	}
	return nil
}
