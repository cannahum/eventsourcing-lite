package testutils

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/kelseyhightower/envconfig"
)

var sess *session.Session

// GetAWSSessionInstance is a quick way to retrieve AWS session. Uses environment variables.
func GetAWSSessionInstance() *session.Session {
	if sess == nil {
		var conf DynamoDBConfig
		envconfig.MustProcess("AWSCONFIG", &conf)
		sess = GetAWSSession(conf)
	}
	return sess
}

// DynamoDBConfig is an object that we fill from .env.
type DynamoDBConfig struct {
	Region    string
	Endpoint  string `envconfig:"DYNAMODB_ENDPOINT"`
	AccessID  string `envconfig:"ACCESS_KEY_ID"`
	SecretKey string `envconfig:"SECRET_ACCESS_KEY"`
}

// GetAWSSession returns a singleton AWS connection object
func GetAWSSession(conf DynamoDBConfig) *session.Session {
	var err error
	if sess == nil {
		awsConf := aws.Config{
			Region: aws.String(conf.Region),
			Credentials: credentials.NewStaticCredentials(
				conf.AccessID,
				conf.SecretKey,
				"",
			),
		}
		if conf.Endpoint != "" {
			awsConf.Endpoint = aws.String(conf.Endpoint)
		}
		sess, err = session.NewSession(&awsConf)
		if err != nil {
			panic(err)
		}
	}
	return sess
}
