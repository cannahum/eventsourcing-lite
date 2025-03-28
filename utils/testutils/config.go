package testutils

import "github.com/kelseyhightower/envconfig"

type Config struct {
	IsTest    bool `split_words:"true"`
	AwsConfig AwsConfig
	Postgres  PostgresConf
}

func NewConfig() Config {
	var awsConf AwsConfig
	envconfig.MustProcess("AWS_CONFIG", &awsConf)
	var postgresConf PostgresConf
	envconfig.MustProcess("POSTGRES", &postgresConf)
	var conf Config
	envconfig.MustProcess("", &conf)
	conf.AwsConfig = awsConf
	conf.Postgres = postgresConf
	return conf
}
