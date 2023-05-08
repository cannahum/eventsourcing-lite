package testutils

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/kelseyhightower/envconfig"
)

type AwsConfig struct {
	Region          string `default:"us-east-1"`
	AccessKeyID     string `split_words:"true"`
	SecretAccessKey string `split_words:"true"`
	DynamoDB        *DynamoDB
	Sqs             *SQS
}

type DynamoDB struct {
	Endpoint string
}

type SQS struct {
	QueueName string
	Endpoint  string
}

func NewConfig() *AwsConfig {
	var conf AwsConfig
	envconfig.MustProcess("AWS_CONFIG", &conf)
	return &conf
}

func (c *AwsConfig) GetAWSCfg() aws.Config {
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if region == c.Region {
			if service == dynamodb.ServiceID {
				return aws.Endpoint{
					PartitionID:       "aws",
					URL:               c.DynamoDB.Endpoint,
					SigningRegion:     c.Region,
					HostnameImmutable: true,
				}, nil
			}
			if service == sqs.ServiceID {
				return aws.Endpoint{
					PartitionID:       "aws",
					URL:               c.Sqs.Endpoint,
					SigningRegion:     c.Region,
					HostnameImmutable: true,
				}, nil
			}
		}
		// returning EndpointNotFoundError will allow the service to fall back to its default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(c.Region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, ""),
		),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		panic(err)
	}
	return cfg
}
