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
	Region    string `default:"us-east-1"`
	AccessID  string `envconfig:"ACCESS_KEY_ID"`
	SecretKey string `envconfig:"SECRET_ACCESS_KEY"`
	DynamoDb  *DynamoDb
	Sqs       *Sqs
}

type DynamoDb struct {
	Endpoint string `envconfig:"ENDPOINT"`
}

type Sqs struct {
	QueueName string `envconfig:"QUEUE_NAME"`
	Endpoint  string `envconfig:"ENDPOINT"`
}

var awsConf AwsConfig

func init() {
	envconfig.MustProcess("AWS_CONFIG", &awsConf)
}

func GetAWSCfg() aws.Config {
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if region == awsConf.Region {
			if service == dynamodb.ServiceID {
				return aws.Endpoint{
					PartitionID:       "aws",
					URL:               awsConf.DynamoDb.Endpoint,
					SigningRegion:     awsConf.Region,
					HostnameImmutable: true,
				}, nil
			}
			if service == sqs.ServiceID {
				return aws.Endpoint{
					PartitionID:       "aws",
					URL:               awsConf.Sqs.Endpoint,
					SigningRegion:     awsConf.Region,
					HostnameImmutable: true,
				}, nil
			}
		}
		// returning EndpointNotFoundError will allow the service to fallback to it's default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(awsConf.Region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(awsConf.AccessID, awsConf.SecretKey, ""),
		),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		panic(err)
	}
	return cfg
}
