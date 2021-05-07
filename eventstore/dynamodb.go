package eventstore

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// ConditionalCheckFailed is const for DB error
const ConditionalCheckFailed = "ConditionalCheckFailed"

// DynamoDBStore is an event store implementation using DynamoDB
// This is an object that represents metadata on this table
type DynamoDBStore struct {
	tableName string
	hashKey   string
	rangeKey  string
	api       *dynamodb.DynamoDB
}

// GetDynamoDBStore returns a new DB store instance
func GetDynamoDBStore(tableName, partitionKey, rangeKey string, awsSession *session.Session) *DynamoDBStore {
	store := DynamoDBStore{
		tableName: tableName,
		hashKey:   partitionKey,
		rangeKey:  rangeKey,
	}

	api := dynamodb.New(awsSession)
	store.api = api

	// Set up streams
	if awsSession.Config.Endpoint != nil {
		if !strings.Contains(*awsSession.Config.Endpoint, "localhost") {
			store.setUpStreams()
		}
	}

	return &store
}

// Load implements the EventStore interface and reads all events for a specific aggregateID
func (s *DynamoDBStore) Load(ctx context.Context, aggregateID string, fromVersion, toVersion int) (History, error) {
	input := &dynamodb.QueryInput{
		TableName:      aws.String(s.tableName),
		Select:         aws.String("ALL_ATTRIBUTES"),
		ConsistentRead: aws.Bool(true),
		ExpressionAttributeNames: map[string]*string{
			"#key": aws.String(s.hashKey),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":key": {S: aws.String(aggregateID)},
		},
	}

	if toVersion > 0 {
		input.KeyConditionExpression = aws.String("#key = :key AND #partition BETWEEN :from AND :to")
		input.ExpressionAttributeNames["#partition"] = aws.String(s.rangeKey)
		input.ExpressionAttributeValues[":from"] = &dynamodb.AttributeValue{N: aws.String(strconv.Itoa(fromVersion))}
		input.ExpressionAttributeValues[":to"] = &dynamodb.AttributeValue{N: aws.String(strconv.Itoa(toVersion))}
	} else if fromVersion > 0 {
		input.KeyConditionExpression = aws.String("#key = :key AND #partition >= :from")
		input.ExpressionAttributeNames["#partition"] = aws.String(s.rangeKey)
		input.ExpressionAttributeValues[":from"] = &dynamodb.AttributeValue{N: aws.String(strconv.Itoa(fromVersion))}
	} else {
		input.KeyConditionExpression = aws.String("#key = :key")
	}

	history := make(History, 0, toVersion)
	out, err := s.api.Query(input)
	if err != nil {
		return nil, err
	}

	for _, item := range out.Items {
		var version int
		var data []byte
		if versionAttr, ok := item[s.rangeKey]; !ok {
			continue
		} else {
			versionStr := versionAttr.N
			version, err = strconv.Atoi(*versionStr)
			if err != nil {
				return nil, err
			}
		}

		data = item["event_data"].B
		history = append(history, Record{
			Version: version,
			Data:    data,
		})
	}

	return history, nil
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
		keyClause := map[string]*dynamodb.AttributeValue{}
		keyClause[s.hashKey] = &dynamodb.AttributeValue{S: aws.String(aggregateID)}
		keyClause[s.rangeKey] = &dynamodb.AttributeValue{N: aws.String(strconv.Itoa(e.Version))}

		twi := dynamodb.TransactWriteItem{
			Update: &dynamodb.Update{
				TableName: aws.String(s.tableName),
				Key:       keyClause,
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":r": {B: e.Data},
				},
				ConditionExpression: aws.String(
					fmt.Sprintf("attribute_not_exists(%s)", s.rangeKey),
				),
				UpdateExpression: aws.String("set event_data = :r"),
			},
		}
		input.TransactItems = append(input.TransactItems, &twi)
	}

	_, err := s.api.TransactWriteItems(input)
	if err != nil {
		if txnCanceled, ok := err.(*dynamodb.TransactionCanceledException); ok {
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

func (s *DynamoDBStore) setUpStreams() {
	_, descErr := s.api.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(s.tableName),
	})
	if descErr != nil {
		panic(descErr)
	}
	// fmt.Println(tableDesc.Table)
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
