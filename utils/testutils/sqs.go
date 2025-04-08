package testutils

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func CreateTestQueue(name string, isFifo bool, q *sqs.Client) string {
	var queueName *string
	if isFifo {
		queueName = aws.String(fmt.Sprintf("%s.fifo", name))
	} else {
		queueName = aws.String(name)
	}

	var attrs map[string]string
	if isFifo {
		attrs = map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		}
	}

	qq, err := q.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
		QueueName:  queueName,
		Attributes: attrs,
	})
	if err != nil {
		panic(err)
	}
	return *qq.QueueUrl
}

func DestroyQueue(q *sqs.Client, url string) {
	_, err := q.DeleteQueue(context.TODO(), &sqs.DeleteQueueInput{QueueUrl: aws.String(url)})
	if err != nil {
		fmt.Printf("failed to delete test queue %s\n", url)
	}
}
