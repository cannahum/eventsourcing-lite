package testutils

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func CreateTestTable(tableName, hashKey string, db *dynamodb.Client) {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(hashKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("version"),
				AttributeType: types.ScalarAttributeTypeN,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(hashKey),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("version"),
				KeyType:       types.KeyTypeRange,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		TableName: aws.String(tableName),
	}

	_, err := db.CreateTable(context.TODO(), input)
	if err != nil {
		if _, alreadyExists := err.(*types.ResourceInUseException); !alreadyExists {
			panic(err)
		}
		fmt.Println("Table already exists")
		return
	}

	maxWaitTime := time.Minute
	waiter := dynamodb.NewTableExistsWaiter(db)
	err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, maxWaitTime)
	if err != nil {
		panic(err)
	}
	fmt.Printf("table %s is ready for use\n", tableName)
}

// DestroyTestTable - Destroy the local DynamoDB table created for your test
// If you're using a table in AWS (remote), then don't destroy, reuse instead.
func DestroyTestTable(tableName string, db *dynamodb.Client) {
	_, err := db.DeleteTable(context.TODO(), &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		panic(fmt.Sprintf("Could not delete table: %v", err))
	}
	fmt.Println("Deleted test table")
}
