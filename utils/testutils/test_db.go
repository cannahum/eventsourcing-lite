package testutils

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	Creating string = "CREATING"
	Active   string = "ACTIVE"
)

var db = dynamodb.New(GetAWSSessionInstance())

func CreateTestTable(tableName, hashKey string) {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(hashKey),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("version"),
				AttributeType: aws.String("N"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(hashKey),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("version"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		TableName: aws.String(tableName),
		StreamSpecification: &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String("NEW_AND_OLD_IMAGES"),
		},
	}

	table, err := db.CreateTable(input)
	if err != nil {
		if _, alreadyExists := err.(*dynamodb.ResourceInUseException); !alreadyExists {
			panic(err)
		}
		fmt.Println("Table already exists")
		return
	}

	pollCounter := 0
	for pollCounter < 60 {
		status := *table.TableDescription.TableStatus
		switch status {
		case Active:
			fmt.Println("Table has been created")
			return
		case Creating:
			// Wait a second
			time.Sleep(1 * time.Second)
			pollCounter++
			continue
		default:
			panic("Unknown TableStatus " + status)
		}
	}

	panic("Table has not been created")
}

// DestroyTestTable - Destroy the local DynamoDB table created for your test
// If you're using a table in AWS (remote), then don't destroy, reuse instead.
func DestroyTestTable(tableName string) {
	_, err := db.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		panic(fmt.Sprintf("Could not delete table: %v", err))
	}
	fmt.Println("Deleted test table")
}
